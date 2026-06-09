import os
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional

import boto3
import psycopg2
import requests
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "dijkstrafood")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_SSLMODE = os.getenv("DB_SSLMODE", "disable")

DYNAMO_TABLE = os.getenv("DYNAMO_TABLE", "CourierLocation")
USE_DYNAMO = os.getenv("USE_DYNAMO", "false").lower() == "true"

ANALYTICS_ENABLED = os.getenv("ANALYTICS_ENABLED", "false").lower() == "true"
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "").strip()

RESTAURANT_SIMULATOR_URL = os.getenv("RESTAURANT_SIMULATOR_URL", "http://restaurant-simulator:8004").rstrip("/")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
NOTIFY_TIMEOUT_SECONDS = float(os.getenv("NOTIFY_TIMEOUT_SECONDS", "2"))
NOTIFY_MAX_ATTEMPTS = int(os.getenv("NOTIFY_MAX_ATTEMPTS", "3"))
NOTIFY_RETRY_BACKOFF_SECONDS = float(os.getenv("NOTIFY_RETRY_BACKOFF_SECONDS", "0.2"))
BACKGROUND_NOTIFICATION_WORKERS = int(os.getenv("BACKGROUND_NOTIFICATION_WORKERS", "16"))
BACKGROUND_KINESIS_WORKERS = int(os.getenv("BACKGROUND_KINESIS_WORKERS", "8"))

KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "").strip()
KINESIS_ENDPOINT_URL = os.getenv("KINESIS_ENDPOINT_URL", "").strip() or None
KINESIS_ENABLED = os.getenv("KINESIS_ENABLED", "true").lower() == "true"
KINESIS_CONNECT_TIMEOUT_SECONDS = float(os.getenv("KINESIS_CONNECT_TIMEOUT_SECONDS", "1"))
KINESIS_READ_TIMEOUT_SECONDS = float(os.getenv("KINESIS_READ_TIMEOUT_SECONDS", "1"))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
os.makedirs(STATIC_DIR, exist_ok=True)

realtime_table = None
kinesis_client = None
analytics_stream = None
notification_executor = ThreadPoolExecutor(max_workers=BACKGROUND_NOTIFICATION_WORKERS)
kinesis_executor = ThreadPoolExecutor(max_workers=BACKGROUND_KINESIS_WORKERS)


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


class AssignCourierRequest(BaseModel):
    courier_id: int
    route_to_pickup: Optional[list] = None
    route_to_delivery: Optional[list] = None


def init_dynamo():
    global realtime_table, USE_DYNAMO

    if not USE_DYNAMO:
        print("DynamoDB disabled by configuration")
        return

    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        realtime_table = dynamodb.Table(DYNAMO_TABLE)
        realtime_table.load()
        print(f"DynamoDB connected: {DYNAMO_TABLE}")
    except Exception as exc:
        print(f"DynamoDB unavailable, disabling: {exc}")
        realtime_table = None
        USE_DYNAMO = False


def init_kinesis():
    global kinesis_client, KINESIS_ENABLED

    if not KINESIS_ENABLED:
        print("Kinesis publishing disabled by configuration")
        return

    if not KINESIS_STREAM_NAME:
        print("Kinesis stream name not set; disabling Kinesis publishing")
        KINESIS_ENABLED = False
        return

    try:
        client_kwargs = {
            "service_name": "kinesis",
            "region_name": AWS_REGION,
            "config": Config(
                connect_timeout=KINESIS_CONNECT_TIMEOUT_SECONDS,
                read_timeout=KINESIS_READ_TIMEOUT_SECONDS,
                retries={"max_attempts": 1},
            ),
        }
        if KINESIS_ENDPOINT_URL:
            client_kwargs["endpoint_url"] = KINESIS_ENDPOINT_URL

        kinesis_client = boto3.client(**client_kwargs)
        print(f"Kinesis publisher enabled: stream={KINESIS_STREAM_NAME}")
    except Exception as exc:
        print(f"Kinesis unavailable, disabling publisher: {exc}")
        kinesis_client = None
        KINESIS_ENABLED = False


def publish_kinesis_event(payload: dict):
    if not KINESIS_ENABLED or kinesis_client is None:
        return

    order_id = payload.get("order_id", "unknown")
    partition_key = str(order_id)

    try:
        kinesis_client.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=json.dumps(payload).encode("utf-8"),
            PartitionKey=partition_key,
        )
    except Exception as exc:
        print(f"Kinesis publish failed: {exc}")


def dispatch_kinesis_event_async(payload: dict):
    kinesis_executor.submit(publish_kinesis_event, payload)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def publish_order_created_event(order_id: int, client_id: int, restaurant_id: int, status: str):
    dispatch_kinesis_event_async(
        {
            "event_type": "ORDER_CREATED",
            "order_id": str(order_id),
            "client_id": client_id,
            "restaurant_id": restaurant_id,
            "status": status,
            "timestamp": utc_now_iso(),
        }
    )


def publish_order_status_changed_event(order_id: int, old_status: str, new_status: str):
    dispatch_kinesis_event_async(
        {
            "event_type": "ORDER_STATUS_CHANGED",
            "order_id": str(order_id),
            "old_status": old_status,
            "new_status": new_status,
            "timestamp": utc_now_iso(),
        }
    )


def publish_courier_assigned_event(order_id: int, courier_id: int):
    dispatch_kinesis_event_async(
        {
            "event_type": "ORDER_COURIER_ASSIGNED",
            "order_id": str(order_id),
            "courier_id": courier_id,
            "timestamp": utc_now_iso(),
        }
    )
def init_analytics_stream():
    global analytics_stream

    if not ANALYTICS_ENABLED:
        print("Analytics stream disabled by configuration")
        return
    if not KINESIS_STREAM_NAME:
        print("Analytics enabled but KINESIS_STREAM_NAME is empty; disabling event publishing")
        return

    analytics_stream = boto3.client("kinesis", region_name=AWS_REGION)
    print(f"Analytics stream configured: {KINESIS_STREAM_NAME}")


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        sslmode=DB_SSLMODE,
        connect_timeout=5,
    )


def check_db_connection() -> tuple[bool, str]:
    conn = None
    try:
        conn = get_connection()
        return True, "ok"
    except Exception as exc:
        return False, str(exc)
    finally:
        if conn is not None:
            conn.close()


def init_db():
    conn = None
    try:
        conn = get_connection()
        conn.autocommit = True

        schema_path = os.path.join(BASE_DIR, "database", "schema.sql")
        seed_path = os.path.join(BASE_DIR, "database", "seed.sql")

        if not os.path.exists(schema_path):
            raise RuntimeError(f"Schema file not found: {schema_path}")

        with conn.cursor() as cur:
            with open(schema_path, "r", encoding="utf-8") as file:
                cur.execute(file.read())

            if os.path.exists(seed_path):
                with open(seed_path, "r", encoding="utf-8") as file:
                    cur.execute(file.read())

            cur.execute(
                """
                SELECT setval(
                    pg_get_serial_sequence('users', 'user_id'),
                    COALESCE((SELECT MAX(user_id) FROM users), 1),
                    true
                );
                """
            )
            cur.execute(
                """
                SELECT setval(
                    pg_get_serial_sequence('restaurants', 'restaurant_id'),
                    COALESCE((SELECT MAX(restaurant_id) FROM restaurants), 1),
                    true
                );
                """
            )
            cur.execute(
                """
                SELECT setval(
                    pg_get_serial_sequence('orders', 'order_id'),
                    COALESCE((SELECT MAX(order_id) FROM orders), 1),
                    true
                );
                """
            )
            cur.execute(
                """
                SELECT setval(
                    pg_get_serial_sequence('order_events', 'event_id'),
                    COALESCE((SELECT MAX(event_id) FROM order_events), 1),
                    true
                );
                """
            )
            cur.execute(
                """
                SELECT setval(
                    pg_get_serial_sequence('order_items', 'item_id'),
                    COALESCE((SELECT MAX(item_id) FROM order_items), 1),
                    true
                );
                """
            )

    except Exception as exc:
        print(f"Error while initializing database: {exc}")
        raise
    finally:
        if conn is not None:
            conn.close()


def normalize_status(raw_status: str) -> str:
    return raw_status.strip().upper()


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
    serialized = []
    for row in rows:
        serialized.append(
            {
                "event_type": row[0],
                "from_status": row[1],
                "to_status": row[2],
                "event_status": row[2] or row[1],
                "event_message": row[3],
                "latitude": row[4],
                "longitude": row[5],
                "created_at": row[6].isoformat() if row[6] else None,
            }
        )
    return serialized


def serialize_item_rows(rows):
    return [{"item_name": row[0], "quantity": row[1]} for row in rows]


def insert_order_event(
    cur,
    order_id: int,
    event_type: str,
    from_status: Optional[str],
    to_status: Optional[str],
    event_message: str,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
):
    cur.execute(
        """
        INSERT INTO order_events (
            order_id,
            event_type,
            from_status,
            to_status,
            event_message,
            latitude,
            longitude
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING event_id, created_at
        """,
        (
            order_id,
            event_type,
            from_status,
            to_status,
            event_message,
            latitude,
            longitude,
        ),
    )
    event_id, created_at = cur.fetchone()

    publish_order_event(
        {
            "event_id": event_id,
            "order_id": order_id,
            "event_type": event_type,
            "from_status": from_status,
            "to_status": to_status,
            "event_status": to_status or from_status,
            "event_message": event_message,
            "latitude": latitude,
            "longitude": longitude,
            "created_at": created_at.replace(tzinfo=timezone.utc).isoformat() if created_at else None,
        }
    )


def publish_order_event(event: dict):
    if not ANALYTICS_ENABLED or analytics_stream is None:
        return

    try:
        analytics_stream.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=(json.dumps(event, default=str) + "\n").encode("utf-8"),
            PartitionKey=str(event["order_id"]),
        )
    except Exception as exc:
        print(f"Analytics event publish failed: {exc}")


def notify_restaurant_simulator(order_id: int, restaurant_id: int, client_id: int):
    payload = {
        "order_id": order_id,
        "restaurant_id": restaurant_id,
        "client_id": client_id,
    }
    response = requests.post(
        f"{RESTAURANT_SIMULATOR_URL}/orders",
        json=payload,
        timeout=NOTIFY_TIMEOUT_SECONDS,
    )
    response.raise_for_status()


def notify_restaurant_simulator_with_retry(order_id: int, restaurant_id: int, client_id: int):
    last_error = None
    for attempt in range(1, NOTIFY_MAX_ATTEMPTS + 1):
        try:
            notify_restaurant_simulator(order_id, restaurant_id, client_id)
            return
        except Exception as exc:
            last_error = exc
            if attempt < NOTIFY_MAX_ATTEMPTS:
                time.sleep(NOTIFY_RETRY_BACKOFF_SECONDS * attempt)

    print(
        f"Restaurant simulator notification failed for order {order_id} "
        f"after {NOTIFY_MAX_ATTEMPTS} attempts: {last_error}"
    )


def dispatch_restaurant_notification_async(order_id: int, restaurant_id: int, client_id: int):
    notification_executor.submit(
        notify_restaurant_simulator_with_retry,
        order_id,
        restaurant_id,
        client_id,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting API application")

    init_dynamo()
    init_kinesis()
    init_analytics_stream()

    try:
        init_db()
    except Exception as exc:
        print(f"Startup continued without init_db: {exc}")

    yield

    notification_executor.shutdown(wait=False, cancel_futures=True)
    kinesis_executor.shutdown(wait=False, cancel_futures=True)
    print("Shutting down API application")


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


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

    except Exception as exc:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if conn is not None:
            conn.close()


@app.get("/users")
def list_users(user_type: Optional[str] = None):
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            if user_type:
                cur.execute(
                    """
                    SELECT user_id, user_name, email, phone, latitude, longitude, user_type
                    FROM users
                    WHERE user_type = %s
                    ORDER BY user_id
                    """,
                    (user_type,),
                )
            else:
                cur.execute(
                    """
                    SELECT user_id, user_name, email, phone, latitude, longitude, user_type
                    FROM users
                    ORDER BY user_id
                    """
                )
            rows = cur.fetchall()

        users = [
            {
                "user_id": row[0],
                "user_name": row[1],
                "email": row[2],
                "phone": row[3],
                "latitude": row[4],
                "longitude": row[5],
                "user_type": row[6],
            }
            for row in rows
        ]
        return {"users": users}

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if conn is not None:
            conn.close()


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

    except Exception as exc:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if conn is not None:
            conn.close()


@app.get("/restaurants")
def list_restaurants():
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT restaurant_id, restaurant_name, cuisine_type, restaurant_latitude, restaurant_longitude
                FROM restaurants
                ORDER BY restaurant_id
                """
            )
            rows = cur.fetchall()

        restaurants = [
            {
                "restaurant_id": row[0],
                "restaurant_name": row[1],
                "cuisine_type": row[2],
                "restaurant_latitude": row[3],
                "restaurant_longitude": row[4],
            }
            for row in rows
        ]
        return {"restaurants": restaurants}

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if conn is not None:
            conn.close()


@app.post("/couriers")
def create_courier(courier: CourierCreate):
    conn = None
    try:
        conn = get_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO couriers (user_id, vehicle_type, is_available)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id)
                    DO UPDATE SET
                        vehicle_type = EXCLUDED.vehicle_type,
                        is_available = EXCLUDED.is_available
                    RETURNING user_id
                    """,
                    (
                        courier.user_id,
                        courier.vehicle_type,
                        courier.is_available,
                    ),
                )
                courier_id = cur.fetchone()[0]

        return {"message": "Courier created successfully", "courier_id": courier_id}

    except Exception as exc:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if conn is not None:
            conn.close()


@app.get("/couriers")
def list_couriers():
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.user_id, u.user_name, c.vehicle_type, c.is_available, u.latitude, u.longitude
                FROM couriers c
                JOIN users u ON u.user_id = c.user_id
                ORDER BY c.user_id
                """
            )
            rows = cur.fetchall()

        couriers = [
            {
                "courier_id": row[0],
                "name": row[1],
                "vehicle_type": row[2],
                "is_available": row[3],
                "lat": row[4],
                "lon": row[5],
            }
            for row in rows
        ]
        return {"couriers": couriers}

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if conn is not None:
            conn.close()


@app.get("/couriers/available")
def list_available_couriers():
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.user_id, u.user_name, c.vehicle_type, u.latitude, u.longitude
                FROM couriers c
                JOIN users u ON u.user_id = c.user_id
                WHERE c.is_available = TRUE
                ORDER BY c.user_id
                """
            )
            rows = cur.fetchall()

        couriers = [
            {
                "id": row[0],
                "name": row[1],
                "vehicle_type": row[2],
                "lat": row[3],
                "lon": row[4],
            }
            for row in rows
        ]
        return {"couriers": couriers}

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if conn is not None:
            conn.close()


@app.post("/orders")
def create_order(order: OrderRequest):
    conn = None
    order_id = None
    try:
        conn = get_connection()
        with conn:
            with conn.cursor() as cur:
                initial_status = "PENDING"
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

                insert_order_event(
                    cur=cur,
                    order_id=order_id,
                    event_type="STATUS_CHANGE",
                    from_status=None,
                    to_status=initial_status,
                    event_message="Order created and waiting for restaurant simulation",
                )

        dispatch_restaurant_notification_async(
            order_id=order_id,
            restaurant_id=order.restaurant_id,
            client_id=order.client_id,
        )

        publish_order_created_event(
            order_id=order_id,
            client_id=order.client_id,
            restaurant_id=order.restaurant_id,
            status=initial_status,
        )

        return {
            "message": "Order created successfully",
            "order_id": order_id,
            "order_status": initial_status,
        }

    except Exception as exc:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if conn is not None:
            conn.close()


@app.put("/orders/{order_id}/status")
def update_status(order_id: int, body: StatusUpdate):
    conn = None
    try:
        new_status = normalize_status(body.status)
        conn = get_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT order_status, courier_id FROM orders WHERE order_id = %s",
                    (order_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise HTTPException(status_code=404, detail="Order not found")

                current_status = row[0]
                courier_id = row[1]

                if current_status == new_status:
                    return {"message": "Status unchanged", "status": current_status}

                cur.execute(
                    """
                    UPDATE orders
                    SET order_status = %s
                    WHERE order_id = %s
                    """,
                    (new_status, order_id),
                )

                insert_order_event(
                    cur=cur,
                    order_id=order_id,
                    event_type="STATUS_CHANGE",
                    from_status=current_status,
                    to_status=new_status,
                    event_message=f"Order status changed from {current_status} to {new_status}",
                )

                if new_status == "DELIVERED" and courier_id is not None:
                    cur.execute(
                        "UPDATE couriers SET is_available = TRUE WHERE user_id = %s",
                        (courier_id,),
                    )

        publish_order_status_changed_event(
            order_id=order_id,
            old_status=current_status,
            new_status=new_status,
        )

        return {"message": "Status updated", "status": new_status}

    except HTTPException:
        raise
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
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
            order_row = cur.fetchone()

            if order_row is None:
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
            item_rows = cur.fetchall()

            cur.execute(
                """
                SELECT event_type, from_status, to_status, event_message, latitude, longitude, created_at
                FROM order_events
                WHERE order_id = %s
                ORDER BY created_at, event_id
                """,
                (order_id,),
            )
            event_rows = cur.fetchall()

        return {
            "order": serialize_order_row(order_row),
            "items": serialize_item_rows(item_rows),
            "events": serialize_event_rows(event_rows),
        }

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
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
                SELECT event_type, from_status, to_status, event_message, latitude, longitude, created_at
                FROM order_events
                WHERE order_id = %s
                ORDER BY created_at, event_id
                """,
                (order_id,),
            )
            event_rows = cur.fetchall()

        return {
            "order_id": order_id,
            "events": serialize_event_rows(event_rows),
        }

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if conn is not None:
            conn.close()


@app.get("/orders/{order_id}/status")
def get_order_status(order_id: int):
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
            row = cur.fetchone()

            if row is None:
                raise HTTPException(status_code=404, detail="Order not found")

            return {"order": serialize_order_row(row)}

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if conn is not None:
            conn.close()


@app.get("/orders/{order_id}/dispatch-data")
def get_dispatch_data(order_id: int):
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    o.order_id,
                    o.client_id,
                    o.restaurant_id,
                    u.latitude AS client_latitude,
                    u.longitude AS client_longitude,
                    r.restaurant_latitude,
                    r.restaurant_longitude,
                    o.order_status,
                    o.courier_id
                FROM orders o
                JOIN users u ON u.user_id = o.client_id
                JOIN restaurants r ON r.restaurant_id = o.restaurant_id
                WHERE o.order_id = %s
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
                "client_latitude": row[3],
                "client_longitude": row[4],
                "restaurant_latitude": row[5],
                "restaurant_longitude": row[6],
                "order_status": row[7],
                "courier_id": row[8],
            }

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if conn is not None:
            conn.close()


@app.post("/orders/{order_id}/assign-courier")
def assign_courier(order_id: int, body: AssignCourierRequest):
    conn = None
    try:
        conn = get_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT courier_id FROM orders WHERE order_id = %s",
                    (order_id,),
                )
                existing = cur.fetchone()
                if existing is None:
                    raise HTTPException(status_code=404, detail="Order not found")

                if existing[0] is not None and int(existing[0]) != int(body.courier_id):
                    raise HTTPException(status_code=409, detail="Order already assigned to another courier")

                cur.execute(
                    """
                    UPDATE couriers
                    SET is_available = FALSE
                    WHERE user_id = %s AND is_available = TRUE
                    """,
                    (body.courier_id,),
                )
                if cur.rowcount == 0 and existing[0] is None:
                    raise HTTPException(status_code=409, detail="Courier is not available")

                cur.execute(
                    "UPDATE orders SET courier_id = %s WHERE order_id = %s",
                    (body.courier_id, order_id),
                )

                insert_order_event(
                    cur=cur,
                    order_id=order_id,
                    event_type="COURIER_ASSIGNED",
                    from_status=None,
                    to_status=None,
                    event_message=f"Courier {body.courier_id} assigned",
                )

        publish_courier_assigned_event(order_id=order_id, courier_id=body.courier_id)

        return {
            "message": "Courier assigned",
            "order_id": order_id,
            "courier_id": body.courier_id,
        }

    except HTTPException:
        raise
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if conn is not None:
            conn.close()


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

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


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
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
