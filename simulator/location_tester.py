import time
import requests
import math

#######################################################
# Rodar isso enquanto a simulação estiver acontecendo #
#######################################################

API_URL = "http://localhost:8000"


def distancia(p1, p2):
    return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)


def get_posicao(courier_id):
    r = requests.get(f"{API_URL}/couriers/{courier_id}/location")
    if r.status_code != 200:
        print("Erro ao buscar localização:", r.text)
        return None

    data = r.json()
    return float(data["latitude"]), float(data["longitude"])


def testar_movimento(courier_id, rota_esperada):
    print(f"\n🚴 Testando movimento do courier {courier_id}\n")

    for esperado in rota_esperada:
        real = get_posicao(courier_id)

        if not real:
            continue

        erro = distancia(real, esperado)

        print(f"Esperado: {esperado}")
        print(f"Real:     {real}")
        print(f"Erro:     {erro:.6f}")
        print("-" * 30)

        time.sleep(0.5)


# 👇 EXECUÇÃO DIRETA
if __name__ == "__main__":
    rota = [
        (-23.55, -46.63),
        (-23.551, -46.631),
        (-23.552, -46.632),
    ]

    testar_movimento(courier_id=1, rota_esperada=rota)