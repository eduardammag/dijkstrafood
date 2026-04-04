import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List
from datetime import datetime
import psycopg2
import boto3
from contextlib import asynccontextmanager

# rodar API:
# uvicorn main:app --reload --host 0.0.0.0 --port 8000

# -------------------------
# Carregar variáveis do .env
# -------------------------
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

DB_HOST = os.getenv("DB_HOST", "SEU_RDS_ENDPOINT")
DB_NAME = os.getenv("DB_NAME", "dijkstrafood")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "SUA_SENHA")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

DYNAMO_TABLE = os.getenv("DYNAMO_TABLE", "OrdersRealtime")

# -------------------------
# Conexão DynamoDB
# -------------------------
dynamodb = boto3.resource(
    'dynamodb',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

realtime_table = dynamodb.Table(DYNAMO_TABLE)

# -------------------------
# Caminhos
# -------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

if not os.path.exists(STATIC_DIR):
    os.makedirs(STATIC_DIR)

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
        sslmode="require"
    )

# -------------------------
# 🔥 EXECUTAR SCHEMA.SQL

def init_db():
    try:
        conn = get_connection()
        conn.autocommit = True

        schema_path = "database\squema.sql"
        seed_path = "database\seed.sql"

        with conn.cursor() as cur:

            # -------------------------
            # Rodar schema.sql
            # -------------------------
            if os.path.exists(schema_path):
                with open(schema_path, "r", encoding="utf-8") as f:
                    cur.execute(f.read())
                print("✅ Schema criado")
            else:
                print("⚠️ schema.sql não encontrado")

            # -------------------------
            # Rodar seed.sql
            # -------------------------
            if os.path.exists(seed_path):
                with open(seed_path, "r", encoding="utf-8") as f:
                    cur.execute(f.read())
                print("✅ Dados inseridos (seed)")
            else:
                print("⚠️ seed.sql não encontrado")

        conn.close()

    except Exception as e:
        print("❌ Erro ao inicializar banco:", e)






# -------------------------
# ✅ LIFESPAN (NOVO PADRÃO)
# -------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Iniciando aplicação...")
    init_db()
    yield
    print("🛑 Encerrando aplicação...")

# -------------------------
# FastAPI
# -------------------------
app = FastAPI(lifespan=lifespan)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# -------------------------
# Modelos
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

# -------------------------
# Health Check
# -------------------------
@app.get("/")
def health():
    return {"status": "API running"}

# -------------------------
# Criar Pedido
# -------------------------
@app.post("/orders")
def create_order(order: OrderRequest):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO orders (client_id, restaurant_id, order_status)
                    VALUES (%s, %s, %s)
                    RETURNING order_id
                """, (order.client_id, order.restaurant_id, "pending"))

                order_id = cur.fetchone()[0]

                for item in order.items:
                    cur.execute("""
                        INSERT INTO order_items (order_id, item_name, quantity)
                        VALUES (%s, %s, %s)
                    """, (order_id, item.name, item.quantity))

                cur.execute("""
                    INSERT INTO order_events (order_id, event_status)
                    VALUES (%s, %s)
                """, (order_id, "pending"))

        realtime_table.put_item(
            Item={
                'order_id': str(order_id),
                'client_id': str(order.client_id),
                'restaurant_id': str(order.restaurant_id),
                'status': 'pending',
                'updated_at': datetime.utcnow().isoformat(),
                'items': [{'name': i.name, 'quantity': i.quantity} for i in order.items]
            }
        )

        return {"message": "Order created successfully", "order_id": order_id}

    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()

# -------------------------
# Atualizar Status
# -------------------------
@app.put("/orders/{order_id}/status")
def update_status(order_id: int, body: StatusUpdate):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE orders
                    SET order_status = %s
                    WHERE order_id = %s
                """, (body.status, order_id))

                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="Order not found")

                cur.execute("""
                    INSERT INTO order_events (order_id, event_status)
                    VALUES (%s, %s)
                """, (order_id, body.status))

        realtime_table.update_item(
            Key={'order_id': str(order_id)},
            UpdateExpression="SET #s = :status, updated_at = :now",
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={
                ':status': body.status,
                ':now': datetime.utcnow().isoformat()
            }
        )

        return {"message": "Status updated"}

    except HTTPException:
        raise

    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()

# -------------------------
# Buscar Pedido
# -------------------------
@app.get("/orders/{order_id}")
def get_order(order_id: int):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
            order = cur.fetchone()

            if not order:
                raise HTTPException(status_code=404, detail="Order not found")

            cur.execute("SELECT item_name, quantity FROM order_items WHERE order_id = %s", (order_id,))
            items = cur.fetchall()

            cur.execute("""
                SELECT event_status, created_at
                FROM order_events
                WHERE order_id = %s
                ORDER BY created_at
            """, (order_id,))
            events = cur.fetchall()

        try:
            dynamo_item = realtime_table.get_item(Key={'order_id': str(order_id)})
            realtime_status = dynamo_item.get('Item', {}).get('status', 'unknown')
        except Exception:
            realtime_status = 'unknown'

        return {
            "order": order,
            "items": items,
            "events": events,
            "realtime_status": realtime_status
        }

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()