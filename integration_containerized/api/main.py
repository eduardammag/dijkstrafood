import os
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional

import boto3
import psycopg2
from boto3.dynamodb.conditions import Key
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from broker import (
    publish_new_order,
    publish_ready_for_delivery,
)
from order_status import (
    normalize_status,
    is_valid_status,
    validate_transition,
)

# -------------------------
# Paths + .env
# -------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

STATIC_DIR = os.path.join(BASE_DIR, "static")
os.makedirs(STATIC_DIR, exist_ok=True)

# -------------------------
# Config
# -------------------------
USE_DYNAMO = os.getenv("USE_DYNAMO", "true").lower() == "true"
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "dijkstrafood")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_SSLMODE = os.getenv("DB_SSLMODE", "prefer")

DYNAMO_TABLE = os.getenv("DYNAMO_TABLE", "CourierLocation")

print("USE_DYNAMO:", USE_DYNAMO)
print("AWS_REGION:", AWS_REGION)
print("DYNAMO_TABLE:", DYNAMO_TABLE)
print("AWS_ACCESS_KEY_ID exists:", bool(os.getenv("AWS_ACCESS_KEY_ID")))
print("AWS_SECRET_ACCESS_KEY exists:", bool(os.getenv("AWS_SECRET_ACCESS_KEY")))
print("AWS_SESSION_TOKEN exists:", bool(os.getenv("AWS_SESSION_TOKEN")))
print(
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI:",
    os.getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"),
)

# -------------------------
# DynamoDB
# -------------------------
aws_session = None
dynamodb = None
realtime_table = None

if USE_DYNAMO:
    try:
        aws_session = boto3.Session(region_name=AWS_REGION)

        sts = aws_session.client("sts")
        identity = sts.get_caller_identity()
        print("AWS caller identity:", identity)

        dynamodb = aws_session.resource("dynamodb")
        realtime_table = dynamodb.Table(DYNAMO_TABLE)
        realtime_table.load()

        print(f"DynamoDB conectado com sucesso na tabela {DYNAMO_TABLE}")

    except Exception as e:
        print("DynamoDB não disponível:", repr(e))
        traceback.print_exc()
        USE_DYNAMO = False

print("USE_DYNAMO final:", USE_DYNAMO)

# -------------------------
# DB
# -------------------------
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        sslmode=DB_SSLMODE,
    )


def init_db():
    conn = None
    try:
        conn = get_connection()
        conn.autocommit = True

        schema_path = os.path.join(BASE_DIR, "database", "schema.sql")
        seed_path = os.path.join(BASE_DIR, "database", "seed.sql")

        print("BASE_DIR:", BASE_DIR)
        print("Schema path:", schema_path)
        print("Seed path:", seed_path)
        print("Schema exists:", os.path.exists(schema_path))
        print("Seed exists:", os.path.exists(seed_path))

        with conn.cursor() as cur:
            if os.path.exists(schema_path):
                try:
                    with open(schema_path, "r", encoding="utf-8") as f:
                        cur.execute(f.read())
                    print("Schema carregado")
                except Exception as e:
                    print("Schema já existe (ok):", e)
            else:
                print("schema.sql não encontrado")

            if os.path.exists(seed_path):
                try:
                    with open(seed_path, "r", encoding="utf-8") as f:
                        sql = f.read().strip()
                        if sql:
                            cur.execute(sql)
                    print("Seed inserido")
                except Exception as e:
                    print("Seed já inserido (ok):", e)
            else:
                print("seed.sql não encontrado")

    except Exception as e:
        print("Erro ao inicializar banco:", e)
    finally:
        if conn:
            conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Iniciando aplicação...")
    yield
    print("Encerrando aplicação...")


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# -------------------------
# Models
# -------------------------
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


class UserCreate(BaseModel):
    user_name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    user_type: str


class RestaurantCreate(BaseModel):
    restaurant_name: str
    cuisine_type: Optional[str] = None
    restaurant_latitude: Optional[float] = None
    restaurant_longitude: Optional[float] = None
    creator_user_id: int


class CourierCreate(BaseModel):
    user_id: int
    vehicle_type: Optional[str] = None
    is_available: bool = True


# -------------------------
# Serializers
# -------------------------
def serialize_order_row(row):
    return {
        "order_id": row[0],
        "client_id": row[1],
        "restaurant_id": row[2],
        "courier_id": row[3],
        "order_status": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
    }


def serialize_order_history(rows):
    return [
        {
            "order_id": r[0],
            "client_id": r[1],
            "restaurant_id": r[2],
            "courier_id": r[3],
            "order_status": r[4],
            "created_at": r[5].isoformat() if r[5] else None,
        }
        for r in rows
    ]


def serialize_items(rows):
    return [{"item_name": r[0], "quantity": r[1]} for r in rows]


def serialize_events(rows):
    return [
        {
            "event_id": r[0],
            "event_type": r[1],
            "from_status": r[2],
            "to_status": r[3],
            "event_message": r[4],
            "latitude": r[5],
            "longitude": r[6],
            "created_at": r[7].isoformat() if r[7] else None,
        }
        for r in rows
    ]


# -------------------------
# Helpers
# -------------------------
def find_active_order_for_courier(cur, courier_id: int) -> Optional[int]:
    cur.execute(
        """
        SELECT order_id
        FROM orders
        WHERE courier_id = %s
          AND order_status IN ('PICKED_UP', 'IN_TRANSIT')
        ORDER BY created_at DESC, order_id DESC
        LIMIT 1
        """,
        (courier_id,),
    )
    row = cur.fetchone()
    return row[0] if row else None


# -------------------------
# Routes
# -------------------------
@app.get("/")
def health():
    return {"status": "API running"}


@app.post("/users")
def create_user(user: UserCreate):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (
                        user_name,
                        email,
                        phone,
                        latitude,
                        longitude,
                        user_type
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING user_id
                    """,
                    (
                        user.user_name,
                        user.email,
                        user.phone,
                        user.latitude,
                        user.longitude,
                        user.user_type,
                    ),
                )
                user_id = cur.fetchone()[0]

        return {
            "message": "User created successfully",
            "user_id": user_id,
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/restaurants")
def create_restaurant(restaurant: RestaurantCreate):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO restaurants (
                        restaurant_name,
                        cuisine_type,
                        restaurant_latitude,
                        restaurant_longitude,
                        creator_user_id
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING restaurant_id
                    """,
                    (
                        restaurant.restaurant_name,
                        restaurant.cuisine_type,
                        restaurant.restaurant_latitude,
                        restaurant.restaurant_longitude,
                        restaurant.creator_user_id,
                    ),
                )
                restaurant_id = cur.fetchone()[0]

        return {
            "message": "Restaurant created successfully",
            "restaurant_id": restaurant_id,
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/restaurants")
def list_restaurants():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    restaurant_id,
                    restaurant_name,
                    cuisine_type,
                    restaurant_latitude,
                    restaurant_longitude,
                    creator_user_id
                FROM restaurants
                ORDER BY restaurant_id ASC
                """
            )
            rows = cur.fetchall()

        restaurants = [
            {
                "restaurant_id": r[0],
                "restaurant_name": r[1],
                "cuisine_type": r[2],
                "restaurant_latitude": r[3],
                "restaurant_longitude": r[4],
                "creator_user_id": r[5],
            }
            for r in rows
        ]

        return {"restaurants": restaurants}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/couriers")
def create_courier(courier: CourierCreate):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO couriers (
                        user_id,
                        vehicle_type,
                        is_available
                    )
                    VALUES (%s, %s, %s)
                    RETURNING user_id
                    """,
                    (
                        courier.user_id,
                        courier.vehicle_type,
                        courier.is_available,
                    ),
                )
                courier_id = cur.fetchone()[0]

        return {
            "message": "Courier created successfully",
            "courier_id": courier_id,
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/couriers")
def list_couriers():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    c.user_id,
                    c.vehicle_type,
                    c.is_available,
                    u.user_name,
                    u.latitude,
                    u.longitude
                FROM couriers c
                JOIN users u
                    ON u.user_id = c.user_id
                ORDER BY c.user_id ASC
                """
            )
            rows = cur.fetchall()

        couriers = [
            {
                "courier_id": row[0],
                "vehicle_type": row[1],
                "is_available": row[2],
                "user_name": row[3],
                "latitude": row[4],
                "longitude": row[5],
            }
            for row in rows
        ]

        return {"couriers": couriers}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/couriers/available")
def get_available_couriers():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    c.user_id,
                    u.latitude,
                    u.longitude
                FROM couriers c
                JOIN users u
                    ON u.user_id = c.user_id
                WHERE c.is_available = TRUE
                ORDER BY c.user_id ASC
                """
            )
            rows = cur.fetchall()

        return {
            "couriers": [
                {"id": row[0], "lat": row[1], "lon": row[2]}
                for row in rows
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/orders")
def create_order(order: OrderRequest):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT user_id
                    FROM users
                    WHERE user_id = %s AND user_type = 'client'
                    """,
                    (order.client_id,),
                )
                client_row = cur.fetchone()
                if not client_row:
                    raise HTTPException(status_code=404, detail="Client not found")

                cur.execute(
                    """
                    SELECT restaurant_id
                    FROM restaurants
                    WHERE restaurant_id = %s
                    """,
                    (order.restaurant_id,),
                )
                restaurant_row = cur.fetchone()
                if not restaurant_row:
                    raise HTTPException(status_code=404, detail="Restaurant not found")

                initial_status = "CONFIRMED"

                cur.execute(
                    """
                    INSERT INTO orders (client_id, restaurant_id, order_status)
                    VALUES (%s, %s, %s)
                    RETURNING order_id
                    """,
                    (order.client_id, order.restaurant_id, initial_status),
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
                    INSERT INTO order_events (
                        order_id,
                        event_type,
                        from_status,
                        to_status,
                        event_message
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        order_id,
                        "ORDER_CREATED",
                        None,
                        initial_status,
                        "Order created and confirmed",
                    ),
                )

        publish_new_order(
            order_id=order_id,
            client_id=order.client_id,
            restaurant_id=order.restaurant_id,
            items=[item.model_dump() for item in order.items],
        )

        return {
            "message": "Order created successfully",
            "order_id": order_id,
            "order_status": initial_status,
        }
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/clients/{client_id}/orders")
def get_client_order_history(client_id: int):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id
                FROM users
                WHERE user_id = %s AND user_type = 'client'
                """,
                (client_id,),
            )
            client_row = cur.fetchone()

            if not client_row:
                raise HTTPException(status_code=404, detail="Client not found")

            cur.execute(
                """
                SELECT
                    order_id,
                    client_id,
                    restaurant_id,
                    courier_id,
                    order_status,
                    created_at
                FROM orders
                WHERE client_id = %s
                ORDER BY created_at ASC, order_id ASC
                """,
                (client_id,),
            )
            rows = cur.fetchall()

        return {
            "client_id": client_id,
            "orders": serialize_order_history(rows),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.put("/orders/{order_id}/status")
def update_status(order_id: int, body: StatusUpdate):
    conn = get_connection()
    try:
        requested_status = normalize_status(body.status)

        if not is_valid_status(requested_status):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid status: {requested_status}",
            )

        courier_id_to_release = None
        restaurant_id = None
        current_status = None

        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT order_status, restaurant_id, courier_id
                    FROM orders
                    WHERE order_id = %s
                    """,
                    (order_id,),
                )
                order_row = cur.fetchone()

                if not order_row:
                    raise HTTPException(status_code=404, detail="Order not found")

                current_status = normalize_status(order_row[0])
                restaurant_id = order_row[1]
                courier_id_to_release = order_row[2]

                if not validate_transition(current_status, requested_status):
                    raise HTTPException(
                        status_code=409,
                        detail=f"Invalid transition: {current_status} -> {requested_status}",
                    )

                cur.execute(
                    """
                    UPDATE orders
                    SET order_status = %s
                    WHERE order_id = %s
                    """,
                    (requested_status, order_id),
                )

                cur.execute(
                    """
                    INSERT INTO order_events (
                        order_id,
                        event_type,
                        from_status,
                        to_status,
                        event_message
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        order_id,
                        "STATUS_CHANGED",
                        current_status,
                        requested_status,
                        f"Status changed from {current_status} to {requested_status}",
                    ),
                )

                if requested_status == "DELIVERED" and courier_id_to_release is not None:
                    cur.execute(
                        """
                        UPDATE couriers
                        SET is_available = TRUE
                        WHERE user_id = %s
                        """,
                        (courier_id_to_release,),
                    )

        if requested_status == "READY_FOR_PICKUP" and restaurant_id is not None:
            publish_ready_for_delivery(order_id, restaurant_id)

        return {
            "message": "Status updated",
            "order_id": order_id,
            "previous_status": current_status,
            "new_status": requested_status,
        }

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
                SELECT order_id, client_id, restaurant_id, courier_id, order_status, created_at
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
                SELECT
                    event_id,
                    event_type,
                    from_status,
                    to_status,
                    event_message,
                    latitude,
                    longitude,
                    created_at
                FROM order_events
                WHERE order_id = %s
                ORDER BY created_at ASC, event_id ASC
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


@app.get("/orders/{order_id}/events")
def get_order_events(order_id: int):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT order_id
                FROM orders
                WHERE order_id = %s
                """,
                (order_id,),
            )
            order_exists = cur.fetchone()
            if not order_exists:
                raise HTTPException(status_code=404, detail="Order not found")

            cur.execute(
                """
                SELECT
                    event_id,
                    event_type,
                    from_status,
                    to_status,
                    event_message,
                    latitude,
                    longitude,
                    created_at
                FROM order_events
                WHERE order_id = %s
                ORDER BY created_at ASC, event_id ASC
                """,
                (order_id,),
            )
            events = cur.fetchall()

        return {
            "order_id": order_id,
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
                SELECT
                    o.order_id,
                    o.client_id,
                    o.restaurant_id,
                    o.courier_id,
                    o.order_status,
                    u.latitude AS client_latitude,
                    u.longitude AS client_longitude,
                    r.restaurant_latitude,
                    r.restaurant_longitude
                FROM orders o
                JOIN users u
                    ON u.user_id = o.client_id
                JOIN restaurants r
                    ON r.restaurant_id = o.restaurant_id
                WHERE o.order_id = %s
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
                "courier_id": row[3],
                "order_status": row[4],
                "client_latitude": row[5],
                "client_longitude": row[6],
                "restaurant_latitude": row[7],
                "restaurant_longitude": row[8],
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/orders/{order_id}/assign-courier")
def assign_courier(order_id: int, body: AssignCourierRequest):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT client_id, restaurant_id, order_status, courier_id
                    FROM orders
                    WHERE order_id = %s
                    """,
                    (order_id,),
                )
                order_row = cur.fetchone()

                if not order_row:
                    raise HTTPException(status_code=404, detail="Order not found")

                client_id, restaurant_id, current_status, current_courier_id = order_row

                normalized_status = normalize_status(current_status)

                if normalized_status != "READY_FOR_PICKUP":
                    raise HTTPException(
                        status_code=409,
                        detail=f"Order must be READY_FOR_PICKUP to assign courier. Current status: {normalized_status}",
                    )

                if current_courier_id is not None:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Order already has courier {current_courier_id}",
                    )

                cur.execute(
                    """
                    SELECT user_id, is_available
                    FROM couriers
                    WHERE user_id = %s
                    """,
                    (body.courier_id,),
                )
                courier_row = cur.fetchone()

                if not courier_row:
                    raise HTTPException(status_code=404, detail="Courier not found")

                if not courier_row[1]:
                    raise HTTPException(status_code=409, detail="Courier is not available")

                cur.execute(
                    """
                    UPDATE orders
                    SET courier_id = %s
                    WHERE order_id = %s
                    """,
                    (body.courier_id, order_id),
                )

                cur.execute(
                    """
                    UPDATE couriers
                    SET is_available = FALSE
                    WHERE user_id = %s
                    """,
                    (body.courier_id,),
                )

                cur.execute(
                    """
                    INSERT INTO order_events (
                        order_id,
                        event_type,
                        from_status,
                        to_status,
                        event_message
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        order_id,
                        "COURIER_ASSIGNED",
                        normalized_status,
                        normalized_status,
                        f"Courier {body.courier_id} assigned to order",
                    ),
                )

        return {
            "message": "Courier assigned",
            "order_id": order_id,
            "courier_id": body.courier_id,
            "client_id": client_id,
            "restaurant_id": restaurant_id,
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


@app.post("/couriers/{courier_id}/location")
def update_courier_location(courier_id: int, body: CourierLocationUpdate):
    conn = get_connection()
    try:
        timestamp = datetime.now(timezone.utc).isoformat()
        resolved_order_id = body.order_id

        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT user_id
                    FROM couriers
                    WHERE user_id = %s
                    """,
                    (courier_id,),
                )
                courier_row = cur.fetchone()

                if not courier_row:
                    raise HTTPException(status_code=404, detail="Courier not found")

                cur.execute(
                    """
                    UPDATE users
                    SET latitude = %s, longitude = %s
                    WHERE user_id = %s
                    """,
                    (body.latitude, body.longitude, courier_id),
                )

                if resolved_order_id is None:
                    resolved_order_id = find_active_order_for_courier(cur, courier_id)

                if resolved_order_id is not None:
                    cur.execute(
                        """
                        SELECT order_id
                        FROM orders
                        WHERE order_id = %s
                        """,
                        (resolved_order_id,),
                    )
                    order_row = cur.fetchone()

                    if order_row:
                        cur.execute(
                            """
                            INSERT INTO order_events (
                                order_id,
                                event_type,
                                event_message,
                                latitude,
                                longitude
                            )
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (
                                resolved_order_id,
                                "COURIER_LOCATION_UPDATED",
                                f"Courier {courier_id} location updated",
                                body.latitude,
                                body.longitude,
                            ),
                        )

        if USE_DYNAMO:
            realtime_table.put_item(
                Item={
                    "courier_id": str(courier_id),
                    "timestamp": timestamp,
                    "latitude": Decimal(str(body.latitude)),
                    "longitude": Decimal(str(body.longitude)),
                    "order_id": str(resolved_order_id) if resolved_order_id is not None else "none",
                }
            )

        return {
            "message": "Location updated",
            "courier_id": courier_id,
            "order_id": resolved_order_id,
            "timestamp": timestamp,
        }
    except HTTPException:
        raise
    except Exception as e:
        print("Erro ao salvar localização:", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


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