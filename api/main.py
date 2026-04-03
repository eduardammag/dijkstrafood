# Importa o FastAPI (framework web) e HTTPException (para retornar erros HTTP)
from fastapi import FastAPI, HTTPException

# BaseModel serve para validar os dados recebidos no corpo das requisições
from pydantic import BaseModel

# List é usado para tipagem de listas (ex: lista de itens)
from typing import List

# Biblioteca para conectar com PostgreSQL
import psycopg2

# Para acessar variáveis de ambiente (ex: senha do banco)
import os

# Permite carregar variáveis do arquivo .env
from dotenv import load_dotenv


# Carrega as variáveis de ambiente do arquivo .env (se existir)
load_dotenv()

# Cria a aplicação FastAPI
app = FastAPI()


# =============================
# FUNÇÃO DE CONEXÃO COM O BANCO
# =============================
def get_connection():
    """
    Cria e retorna uma conexão com o banco PostgreSQL.
    Usa variáveis de ambiente (ou valores padrão caso não existam).
    """
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "SEU_RDS_ENDPOINT"),
        database=os.getenv("DB_NAME", "dijkstrafood"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "SUA_SENHA"),
        port=os.getenv("DB_PORT", "5432")
    )


# =============================
# MODELOS (SCHEMA DAS REQUISIÇÕES)
# =============================

class Item(BaseModel):
    """
    Representa um item dentro do pedido.
    Exemplo:
    {
        "name": "Pizza",
        "quantity": 2
    }
    """
    name: str
    quantity: int


class OrderRequest(BaseModel):
    """
    Representa o corpo da requisição para criar um pedido.
    """
    client_id: int
    restaurant_id: int
    items: List[Item]  # lista de itens


class StatusUpdate(BaseModel):
    """
    Representa o corpo da requisição para atualizar status do pedido.
    """
    status: str


# =============================
# ENDPOINT DE TESTE (HEALTH CHECK)
# =============================

@app.get("/")
def health():
    """
    Endpoint simples para verificar se a API está funcionando.
    Acessar no navegador: http://localhost:8000/
    """
    return {"status": "API running"}


# =============================
# CRIAR PEDIDO
# =============================

@app.post("/orders")
def create_order(order: OrderRequest):
    """
    Cria um novo pedido no banco de dados.

    Fluxo:
    1. Cria o pedido na tabela 'orders'
    2. Insere os itens na tabela 'order_items'
    3. Registra evento inicial na tabela 'order_events'
    """

    # Abre conexão com o banco
    conn = get_connection()

    try:
        # 'with conn' inicia uma transação automaticamente
        with conn:
            # Cria um cursor (objeto que executa SQL)
            with conn.cursor() as cur:

                # 1. INSERIR PEDIDO
                cur.execute(
                    """
                    INSERT INTO orders (client_id, restaurant_id, order_status)
                    VALUES (%s, %s, %s)
                    RETURNING order_id
                    """,
                    # Valores que substituem os %s
                    (order.client_id, order.restaurant_id, "pending")
                )

                # Pega o ID do pedido recém criado
                order_id = cur.fetchone()[0]

                # 2. INSERIR ITENS DO PEDIDO
                for item in order.items:
                    cur.execute(
                        """
                        INSERT INTO order_items (order_id, item_name, quantity)
                        VALUES (%s, %s, %s)
                        """,
                        (order_id, item.name, item.quantity)
                    )

                # 3. CRIAR EVENTO INICIAL
                cur.execute(
                    """
                    INSERT INTO order_events (order_id, event_status)
                    VALUES (%s, %s)
                    """,
                    (order_id, "pending")
                )

        # Se tudo deu certo, retorna sucesso
        return {
            "message": "Order created successfully",
            "order_id": order_id
        }

    except Exception as e:
        # Se der erro, desfaz a transação
        conn.rollback()

        # Retorna erro HTTP 500 com mensagem
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        # Fecha a conexão com o banco (sempre executa)
        conn.close()


# =============================
# ATUALIZAR STATUS DO PEDIDO
# =============================

@app.put("/orders/{order_id}/status")
def update_status(order_id: int, body: StatusUpdate):
    """
    Atualiza o status de um pedido existente.

    Também registra esse evento na tabela 'order_events'.
    """

    conn = get_connection()

    try:
        with conn:
            with conn.cursor() as cur:

                # 1. ATUALIZAR STATUS NA TABELA
                cur.execute(
                    """
                    UPDATE orders
                    SET order_status = %s
                    WHERE order_id = %s
                    """,
                    (body.status, order_id)
                )

                # Se nenhuma linha foi afetada, pedido não existe
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="Order not found")

                # 2. REGISTRAR EVENTO
                cur.execute(
                    """
                    INSERT INTO order_events (order_id, event_status)
                    VALUES (%s, %s)
                    """,
                    (order_id, body.status)
                )

        return {"message": "Status updated"}

    except HTTPException:
        # Repassa erros já tratados
        raise

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()


# =============================
# BUSCAR DETALHES DO PEDIDO
# =============================

@app.get("/orders/{order_id}")
def get_order(order_id: int):
    """
    Retorna:
    - Dados do pedido
    - Itens do pedido
    - Histórico de eventos (status)
    """

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            # =============================
            # 1. BUSCAR PEDIDO
            # =============================
            cur.execute(
                "SELECT * FROM orders WHERE order_id = %s",
                (order_id,)
            )
            order = cur.fetchone()

            # Se não existir
            if not order:
                raise HTTPException(status_code=404, detail="Order not found")

            # =============================
            # 2. BUSCAR ITENS
            # =============================
            cur.execute(
                "SELECT item_name, quantity FROM order_items WHERE order_id = %s",
                (order_id,)
            )
            items = cur.fetchall()

            # =============================
            # 3. BUSCAR EVENTOS (HISTÓRICO)
            # =============================
            cur.execute(
                """
                SELECT event_status, created_at
                FROM order_events
                WHERE order_id = %s
                ORDER BY created_at
                """,
                (order_id,)
            )
            events = cur.fetchall()

        # Retorna tudo junto
        return {
            "order": order,
            "items": items,
            "events": events
        }

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()