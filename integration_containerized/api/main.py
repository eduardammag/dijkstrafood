import os
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

# rodar API:
# uvicorn main:app --reload --host 0.0.0.0 --port 8000

# -------------------------
# Carregar variáveis
# -------------------------
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

DB_HOST = os.getenv("DB_HOST", "dijkstrafood-db.cz4w0o0yshed.us-east-1.rds.amazonaws.com")
DB_NAME = os.getenv("DB_NAME", "dijkstrafood")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres12345")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

DYNAMO_TABLE = os.getenv("DYNAMO_TABLE", "CourierLocation")
USE_DYNAMO = os.getenv("USE_DYNAMO", "false").lower() == "true"

# -------------------------
# Caminhos
# -------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
os.makedirs(STATIC_DIR, exist_ok=True)

# -------------------------
# DynamoDB
# -------------------------
realtime_table = None

def init_dynamo():
    global realtime_table, USE_DYNAMO

    if not USE_DYNAMO:
        print("DynamoDB desabilitado por configuração")
        return

    try:
        # Em ECS, o ideal é usar a task role automaticamente.
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        realtime_table = dynamodb.Table(DYNAMO_TABLE)

        # valida acesso e existência da tabela
        realtime_table.load()
        print(f"DynamoDB conectado: {DYNAMO_TABLE}")

    except Exception as e:
        print(f"DynamoDB indisponível, desativando: {e}")
        realtime_table = None
        USE_DYNAMO = False

# -------------------------
# Conexão RDS
# -------------------------
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        sslmode="require",
        connect_timeout=5,
    )

def check_db_connection() -> tuple[bool, str]:
    conn = None
    try:
        conn = get_connection()
        return True, "ok"
    except Exception as e:
        return False, str(e)
    finally:
        if conn is not None:
            conn.close()

# -------------------------
# INIT DB (schema + seed)
# -------------------------
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

        if not os.path.exists(schema_path):
            raise RuntimeError(f"Schema file não encontrado: {schema_path}")

        with conn.cursor() as cur:
            with open(schema_path, "r", encoding="utf-8") as f:
                cur.execute(f.read())
            print("Schema carregado")

            if os.path.exists(seed_path):
                with open(seed_path, "r", encoding="utf-8") as f:
                    cur.execute(f.read())
                print("Seed inserido")

            cur.execute("""
                SELECT setval(
                    pg_get_serial_sequence('users', 'user_id'),
                    COALESCE((SELECT MAX(user_id) FROM users), 1),
                    true
                );
            """)

            cur.execute("""
                SELECT setval(
                    pg_get_serial_sequence('restaurants', 'restaurant_id'),
                    COALESCE((SELECT MAX(restaurant_id) FROM restaurants), 1),
                    true
                );
            """)

            cur.execute("""
                SELECT setval(
                    pg_get_serial_sequence('orders', 'order_id'),
                    COALESCE((SELECT MAX(order_id) FROM orders), 1),
                    true
                );
            """)

            cur.execute("""
                SELECT setval(
                    pg_get_serial_sequence('order_events', 'event_id'),
                    COALESCE((SELECT MAX(event_id) FROM order_events), 1),
                    true
                );
            """)

            cur.execute("""
                SELECT setval(
                    pg_get_serial_sequence('order_items', 'item_id'),
                    COALESCE((SELECT MAX(item_id) FROM order_items), 1),
                    true
                );
            """)

            print("Sequências ajustadas")

    except Exception as e:
        print("Erro ao inicializar banco:", e)
        raise

    finally:
        if conn is not None:
            conn.close()

# -------------------------
# LIFESPAN
# -------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Iniciando aplicação...")

    init_dynamo()

    # Não impedir a API de subir só porque o banco falhou no startup.
    try:
        init_db()
    except Exception as e:
        print("Startup prosseguiu sem init_db:", e)

    yield

    print("Encerrando aplicação...")

# -------------------------
# FastAPI
# -------------------------
app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# -------------------------
# MODELOS
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
# Helpers
# -------------------------
def serialize_order_row(row):
    if row is None:
        return None

    return {
        "order_id": row[0],
        "client_id": row[1],
        "restaurant_id": row[2],
        "courier_id": row[3],
        "order_status": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
    }

def serialize_event_rows(rows):
    return [
        {
            "event_status": row[0],
            "created_at": row[1].isoformat() if row[1] else None,
        }
        for row in rows
    ]

def serialize_item_rows(rows):
    return [{"item_name": row[0], "quantity": row[1]} for row in rows]

# -------------------------
# Health
# -------------------------
@app.get("/")
def health():
    return {"status": "API running"}

@app.get("/health/db")
def health_db():
    ok, detail = check_db_connection()
    if not ok:
        raise HTTPException(status_code=500, detail=f"database error: {detail}")
    return {"database": "ok"}

@app.get("/health/full")
def health_full():
    db_ok, db_detail = check_db_connection()

    return {
        "api": "ok",
        "database": "ok" if db_ok else "error",
        "database_detail": db_detail if not db_ok else None,
        "dynamo_enabled": USE_DYNAMO,
        "dynamo_table": DYNAMO_TABLE if USE_DYNAMO else None,
    }

# -------------------------
# Usuários
# -------------------------
@app.post("/users")
def create_user(user: UserCreate):
    conn = None
    try:
        conn = get_connection()
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

        return {"message": "User created successfully", "user_id": user_id}

    except Exception as e:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn is not None:
            conn.close()

# -------------------------
# Restaurantes
# -------------------------
@app.post("/restaurants")
def create_restaurant(restaurant: RestaurantCreate):
    conn = None
    try:
        conn = get_connection()
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
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn is not None:
            conn.close()

# -------------------------
# Entregadores
# -------------------------
@app.post("/couriers")
def create_courier(courier: CourierCreate):
    conn = None
    try:
        conn = get_connection()
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
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn is not None:
            conn.close()

# -------------------------
# Pedidos
# -------------------------
@app.post("/orders")
def create_order(order: OrderRequest):
    conn = None
    try:
        conn = get_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO orders (client_id, restaurant_id, order_status)
                    VALUES (%s, %s, %s)
                    RETURNING order_id
                    """,
                    (order.client_id, order.restaurant_id, "confirmed"),
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
                        order_id, event_type, from_status, to_status, event_message
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        order_id,
                        "status_change",
                        None,
                        "confirmed",
                        "Order confirmed",
                    ),
                )

        return {
            "message": "Order created successfully",
            "order_id": order_id,
            "order_status": "confirmed",
        }

    except Exception as e:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn is not None:
            conn.close()

@app.put("/orders/{order_id}/status")
def update_status(order_id: int, body: StatusUpdate):
    conn = None
    try:
        conn = get_connection()
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

        return {"message": "Status updated"}

    except HTTPException:
        raise

    except Exception as e:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn is not None:
            conn.close()

@app.get("/orders/{order_id}")
def get_order(order_id: int):
    conn = None
    try:
        conn = get_connection()
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
                ORDER BY item_name
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
            "items": serialize_item_rows(items),
            "events": serialize_event_rows(events),
        }

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn is not None:
            conn.close()

@app.get("/orders/{order_id}/events")
def get_order_events(order_id: int):
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
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
            "order_id": order_id,
            "events": serialize_event_rows(events),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn is not None:
            conn.close()

# -------------------------
# Localização do entregador
# -------------------------
@app.post("/couriers/{courier_id}/location")
def update_courier_location(courier_id: int, body: CourierLocationUpdate):
    if not USE_DYNAMO or realtime_table is None:
        raise HTTPException(status_code=503, detail="DynamoDB disabled")

    try:
        timestamp = datetime.now(timezone.utc).isoformat()

        realtime_table.put_item(
            Item={
                "courier_id": str(courier_id),
                "timestamp": timestamp,
                "latitude": Decimal(str(body.latitude)),
                "longitude": Decimal(str(body.longitude)),
                "order_id": str(body.order_id) if body.order_id is not None else "none",
            }
        )

        return {"message": "Location updated", "timestamp": timestamp}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/couriers/{courier_id}/location")
def get_latest_courier_location(courier_id: int):
    if not USE_DYNAMO or realtime_table is None:
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
    
@app.get("/orders/{order_id}/status")
def get_order_status(order_id: int):
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT order_id, client_id, restaurant_id, order_status, created_at
                FROM orders
                WHERE order_id = %s
                """,
                (order_id,),
            )
            row = cur.fetchone()

            if row is None:
                raise HTTPException(status_code=404, detail="Order not found")

            return {
                "order_id": row[0],
                "client_id": row[1],
                "restaurant_id": row[2],
                "status": row[3],
                "created_at": row[4].isoformat() if row[4] else None,
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn is not None:
            conn.close()