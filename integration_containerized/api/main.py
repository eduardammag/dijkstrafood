import os
from datetime import datetime
from typing import List, Optional
from contextlib import asynccontextmanager

import boto3
import psycopg2
from boto3.dynamodb.conditions import Key
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from decimal import Decimal

from broker import (
    publish_new_order,
    publish_ready_for_delivery,
    publish_delivery_assignment,
)

load_dotenv()

USE_DYNAMO = os.getenv("USE_DYNAMO", "true").lower() == "true"

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "dijkstrafood")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

DYNAMO_TABLE = os.getenv("DYNAMO_TABLE", "CourierLocation")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
os.makedirs(STATIC_DIR, exist_ok=True)

dynamodb = None
realtime_table = None

if USE_DYNAMO:
    try:
        aws_session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN,
            region_name=AWS_REGION
        )

        sts = aws_session.client("sts")
        identity = sts.get_caller_identity()
        print("AWS caller identity:", identity)

        dynamodb = aws_session.resource("dynamodb")
        realtime_table = dynamodb.Table(DYNAMO_TABLE)
        realtime_table.load()

        print(f"DynamoDB conectado com sucesso na tabela {DYNAMO_TABLE}")

    except Exception as e:
        print("DynamoDB não disponível:", repr(e))
        USE_DYNAMO = False


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        sslmode=os.getenv("DB_SSLMODE", "prefer"),
    )


def init_db():
    conn = None
    try:
        conn = get_connection()
        conn.autocommit = True

        squema_path = os.path.join(BASE_DIR, "database", "squema.sql")
        seed_path = os.path.join(BASE_DIR, "database", "seed.sql")

        with conn.cursor() as cur:
            if os.path.exists(squema_path):
                with open(squema_path, "r", encoding="utf-8") as f:
                    cur.execute(f.read())
                print("squema carregado")
            else:
                print("squema.sql não encontrado")

            if os.path.exists(seed_path):
                with open(seed_path, "r", encoding="utf-8") as f:
                    sql = f.read().strip()
                    if sql:
                        cur.execute(sql)
                print("Seed carregado")
            else:
                print("seed.sql não encontrado")
    except Exception as e:
        print("Erro ao inicializar banco:", e)
    finally:
        if conn:
            conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class Item(BaseModel):
    name: str
    quantity: int


class OrderRequest(BaseModel):
    client_id: int
    restaurant_id: int
    items: List[Item]


class StatusUpdate(BaseModel):
    status: str


class CourierLocationUpdate(BaseModel):
    latitude: float
    longitude: float
    order_id: Optional[int] = None


class AssignCourierRequest(BaseModel):
    courier_id: int
    route_to_pickup: List[List[float]]
    route_to_delivery: List[List[float]]


def serialize_order_row(row):
    return {
        "order_id": row[0],
        "client_id": row[1],
        "restaurant_id": row[2],
        "order_status": row[3],
        "created_at": row[4].isoformat() if row[4] else None,
    }


def serialize_items(rows):
    return [{"item_name": r[0], "quantity": r[1]} for r in rows]


def serialize_events(rows):
    return [
        {"event_status": r[0], "created_at": r[1].isoformat() if r[1] else None}
        for r in rows
    ]


@app.get("/")
def health():
    return {"status": "API running"}


@app.post("/orders")
def create_order(order: OrderRequest):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO orders (client_id, restaurant_id, order_status)
                    VALUES (%s, %s, %s)
                    RETURNING order_id
                    """,
                    (order.client_id, order.restaurant_id, "pending"),
                )
                order_id = cur.fetchone()[0]

                for item in order.items:
                    cur.execute(
                        """
                        INSERT INTO order_items (order_id, item_name, quantity)
                        VALUES (%s, %s, %s)
                        """,
                        (order_id, item.name, item.quantity),
                    )

                cur.execute(
                    """
                    INSERT INTO order_events (order_id, event_status)
                    VALUES (%s, %s)
                    """,
                    (order_id, "pending"),
                )

        publish_new_order(
            order_id=order_id,
            client_id=order.client_id,
            restaurant_id=order.restaurant_id,
            items=[item.model_dump() for item in order.items],
        )

        return {"message": "Order created successfully", "order_id": order_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.put("/orders/{order_id}/status")
def update_status(order_id: int, body: StatusUpdate):
    conn = get_connection()
    try:
        restaurant_row = None
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE orders
                    SET order_status = %s
                    WHERE order_id = %s
                    """,
                    (body.status, order_id),
                )

                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="Order not found")

                cur.execute(
                    """
                    INSERT INTO order_events (order_id, event_status)
                    VALUES (%s, %s)
                    """,
                    (order_id, body.status),
                )

                cur.execute(
                    """
                    SELECT restaurant_id
                    FROM orders
                    WHERE order_id = %s
                    """,
                    (order_id,),
                )
                restaurant_row = cur.fetchone()

        if body.status == "ready_for_delivery" and restaurant_row:
            publish_ready_for_delivery(order_id, restaurant_row[0])

        return {"message": "Status updated"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/orders/{order_id}")
def get_order(order_id: int):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT order_id, client_id, restaurant_id, order_status, created_at
                FROM orders
                WHERE order_id = %s
                """,
                (order_id,),
            )
            order = cur.fetchone()
            if not order:
                raise HTTPException(status_code=404, detail="Order not found")

            cur.execute(
                """
                SELECT item_name, quantity
                FROM order_items
                WHERE order_id = %s
                """,
                (order_id,),
            )
            items = cur.fetchall()

            cur.execute(
                """
                SELECT event_status, created_at
                FROM order_events
                WHERE order_id = %s
                ORDER BY created_at
                """,
                (order_id,),
            )
            events = cur.fetchall()

        return {
            "order": serialize_order_row(order),
            "items": serialize_items(items),
            "events": serialize_events(events),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/orders/{order_id}/dispatch-data")
def get_order_dispatch_data(order_id: int):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT order_id, client_id, restaurant_id, order_status
                FROM orders
                WHERE order_id = %s
                """,
                (order_id,),
            )
            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Order not found")

            return {
                "order_id": row[0],
                "client_id": row[1],
                "restaurant_id": row[2],
                "order_status": row[3],
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/couriers/available")
def get_available_couriers():
    return {
        "couriers": [
            {"id": 1, "lat": -22.1210, "lon": -51.3880},
            {"id": 2, "lat": -22.1250, "lon": -51.3920},
            {"id": 3, "lat": -22.1290, "lon": -51.3950},
        ]
    }


@app.post("/orders/{order_id}/assign-courier")
def assign_courier(order_id: int, body: AssignCourierRequest):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT client_id, restaurant_id
                    FROM orders
                    WHERE order_id = %s
                    """,
                    (order_id,),
                )
                order_row = cur.fetchone()

                if not order_row:
                    raise HTTPException(status_code=404, detail="Order not found")

                client_id, restaurant_id = order_row

                cur.execute(
                    """
                    UPDATE orders
                    SET order_status = %s
                    WHERE order_id = %s
                    """,
                    ("courier_assigned", order_id),
                )

                cur.execute(
                    """
                    INSERT INTO order_events (order_id, event_status)
                    VALUES (%s, %s)
                    """,
                    (order_id, f"courier_assigned:{body.courier_id}"),
                )

        publish_delivery_assignment(
            order_id=order_id,
            courier_id=body.courier_id,
            restaurant_id=restaurant_id,
            client_id=client_id,
            route_to_pickup=body.route_to_pickup,
            route_to_delivery=body.route_to_delivery,
        )

        return {
            "message": "Courier assigned",
            "order_id": order_id,
            "courier_id": body.courier_id,
            "route_to_pickup": body.route_to_pickup,
            "route_to_delivery": body.route_to_delivery,
        }
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


from decimal import Decimal

@app.post("/couriers/{courier_id}/location")
def update_courier_location(courier_id: int, body: CourierLocationUpdate):
    if not USE_DYNAMO:
        raise HTTPException(status_code=503, detail="DynamoDB disabled")

    try:
        sts = aws_session.client("sts")
        identity = sts.get_caller_identity()
        print("AWS caller identity no POST location:", identity)

        timestamp = datetime.utcnow().isoformat()

        realtime_table.put_item(
            Item={
                "courier_id": str(courier_id),
                "timestamp": timestamp,
                "latitude": Decimal(str(body.latitude)),
                "longitude": Decimal(str(body.longitude)),
                "order_id": str(body.order_id) if body.order_id is not None else "none"
            }
        )

        return {
            "message": "Location updated",
            "timestamp": timestamp
        }

    except Exception as e:
        print("Erro ao salvar localização no DynamoDB:", repr(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/couriers/{courier_id}/location")
def get_latest_courier_location(courier_id: int):
    if not USE_DYNAMO:
        raise HTTPException(status_code=503, detail="DynamoDB disabled")

    try:
        response = realtime_table.query(
            KeyConditionExpression=Key("courier_id").eq(str(courier_id)),
            ScanIndexForward=False,
            Limit=1,
        )

        items = response.get("Items", [])
        if not items:
            raise HTTPException(status_code=404, detail="Location not found")

        return items[0]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))