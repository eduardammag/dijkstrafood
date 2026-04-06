from fastapi import FastAPI
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )


@app.post("/couriers/ocupar")
def ocupar(data: dict):
    courier_id = data["courier_id"]

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE couriers
        SET is_available = FALSE
        WHERE user_id = %s
        AND is_available = TRUE
        """,
        (courier_id,)
    )

    sucesso = cursor.rowcount > 0

    conn.commit()
    cursor.close()
    conn.close()

    if not sucesso:
        return {"erro": "indisponível"}

    return {"ok": True}


@app.post("/couriers/liberar")
def liberar(data: dict):
    courier_id = data["courier_id"]
    lat = data["lat"]
    lon = data["lon"]

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE couriers SET is_available = TRUE WHERE user_id = %s",
        (courier_id,)
    )

    cursor.execute(
        """
        UPDATE users
        SET latitude = %s,
            longitude = %s
        WHERE user_id = %s
        """,
        (lat, lon, courier_id)
    )

    conn.commit()
    cursor.close()
    conn.close()

    return {"ok": True}

@app.get("/couriers/disponiveis")
def listar_disponiveis():
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT u.user_id, u.latitude, u.longitude
        FROM users u
        JOIN couriers c ON u.user_id = c.user_id
        WHERE c.is_available = TRUE
        """
    )

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return [
        {"id": r[0], "lat": r[1], "lon": r[2]}
        for r in rows
    ]