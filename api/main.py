from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import psycopg2
import os
from dotenv import load_dotenv
import boto3
from datetime import datetime
from fastapi.staticfiles import StaticFiles

app.mount("/static", StaticFiles(directory="\dijkstrafood\static"), name="static")
# -------------------------
# CARREGAR VARIÁVEIS DE AMBIENTE
# -------------------------
load_dotenv()

# -------------------------
# CONFIGURAÇÃO AWS DYNAMODB
# -------------------------
dynamodb = boto3.resource(
    'dynamodb',
    region_name=os.getenv("AWS_REGION", "us-east-1"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
realtime_table = dynamodb.Table(os.getenv("DYNAMO_TABLE", "OrdersRealtime"))

# -------------------------
# FASTAPI APP
# -------------------------
app = FastAPI()

# -------------------------
# FUNÇÃO DE CONEXÃO COM RDS (PostgreSQL)
# -------------------------
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "SEU_RDS_ENDPOINT"),
        database=os.getenv("DB_NAME", "dijkstrafood"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "SUA_SENHA"),
        port=os.getenv("DB_PORT", "5432")
    )

# -------------------------
# MODELOS DE REQUISIÇÃO
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
# HEALTH CHECK
# -------------------------
@app.get("/")
def health():
    return {"status": "API running"}

# -------------------------
# CRIAR PEDIDO (RDS + DynamoDB)
# -------------------------
@app.post("/orders")
def create_order(order: OrderRequest):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # 1. Criar pedido RDS
                cur.execute("""
                    INSERT INTO orders (client_id, restaurant_id, order_status)
                    VALUES (%s, %s, %s)
                    RETURNING order_id
                """, (order.client_id, order.restaurant_id, "pending"))
                order_id = cur.fetchone()[0]

                # 2. Inserir itens
                for item in order.items:
                    cur.execute("""
                        INSERT INTO order_items (order_id, item_name, quantity)
                        VALUES (%s, %s, %s)
                    """, (order_id, item.name, item.quantity))

                # 3. Criar evento inicial
                cur.execute("""
                    INSERT INTO order_events (order_id, event_status)
                    VALUES (%s, %s)
                """, (order_id, "pending"))

        # 4. Salvar em DynamoDB (tempo real)
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
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()

# -------------------------
# ATUALIZAR STATUS DO PEDIDO (RDS + DynamoDB)
# -------------------------
@app.put("/orders/{order_id}/status")
def update_status(order_id: int, body: StatusUpdate):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # Atualizar RDS
                cur.execute("""
                    UPDATE orders
                    SET order_status = %s
                    WHERE order_id = %s
                """, (body.status, order_id))

                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="Order not found")

                # Registrar evento RDS
                cur.execute("""
                    INSERT INTO order_events (order_id, event_status)
                    VALUES (%s, %s)
                """, (order_id, body.status))

        # Atualizar DynamoDB
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
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()

# -------------------------
# BUSCAR DETALHES DO PEDIDO
# -------------------------
@app.get("/orders/{order_id}")
def get_order(order_id: int):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Pedido
            cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
            order = cur.fetchone()
            if not order:
                raise HTTPException(status_code=404, detail="Order not found")

            # Itens
            cur.execute("SELECT item_name, quantity FROM order_items WHERE order_id = %s", (order_id,))
            items = cur.fetchall()

            # Eventos
            cur.execute("""
                SELECT event_status, created_at
                FROM order_events
                WHERE order_id = %s
                ORDER BY created_at
            """, (order_id,))
            events = cur.fetchall()

        # Também traz o status em tempo real do DynamoDB
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