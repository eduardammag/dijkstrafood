import time
from datetime import datetime
import boto3
import requests
import os

USER_SERVICE_URL = os.getenv("API_URL", "http://localhost:8000")
DYNAMO_TABLE = os.getenv("DYNAMO_TABLE", "CourierLocation")

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table(DYNAMO_TABLE)


def salvar_lote(entregador_id, posicoes):
    with table.batch_writer() as batch:
        for lat, lon in posicoes:
            batch.put_item(
                Item={
                    "entregador_id": str(entregador_id),
                    "timestamp": datetime.utcnow().isoformat(),
                    "lat": lat,
                    "lon": lon
                }
            )


def interpolar(p1, p2, passos=5):
    lat1, lon1 = p1
    lat2, lon2 = p2

    return [
        (
            lat1 + (lat2 - lat1) * t,
            lon1 + (lon2 - lon1) * t
        )
        for t in [i / passos for i in range(passos)]
    ]


def simular_movimento(entregador_id, rota_coords):
    buffer = []
    BATCH_SIZE = 20
    ultima_pos = None

    for i in range(len(rota_coords) - 1):
        p1 = rota_coords[i]
        p2 = rota_coords[i + 1]

        pontos = interpolar(p1, p2)

        for lat, lon in pontos:
            buffer.append((lat, lon))
            ultima_pos = (lat, lon)

            if len(buffer) >= BATCH_SIZE:
                salvar_lote(entregador_id, buffer)
                buffer = []

            time.sleep(0.1)

    if buffer:
        salvar_lote(entregador_id, buffer)

    if ultima_pos:
        try:
            requests.post(
                f"{USER_SERVICE_URL}/couriers/liberar",
                json={"courier_id": entregador_id},
                timeout=5
            )
        except:
            pass