import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", 5432)


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )


def marcar_ocupado(entregador_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE couriers
        SET is_available = FALSE
        WHERE user_id = %s
        AND is_available = TRUE
        """,
        (entregador_id,)
    )

    sucesso = cursor.rowcount > 0

    conn.commit()
    cursor.close()
    conn.close()

    return sucesso


def liberar_entregador(entregador_id, lat, lon):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE couriers
        SET is_available = TRUE
        WHERE user_id = %s
        """,
        (entregador_id,)
    )

    cursor.execute(
        """
        UPDATE users
        SET latitude = %s,
            longitude = %s
        WHERE user_id = %s
        """,
        (lat, lon, entregador_id)
    )

    conn.commit()
    cursor.close()
    conn.close()