def gerar_rota_simples(p1, p2, passos=20):
    lat1, lon1 = p1
    lat2, lon2 = p2

    return [
        (
            lat1 + (lat2 - lat1) * t,
            lon1 + (lon2 - lon1) * t
        )
        for t in [i/passos for i in range(passos)]
    ]


def filtrar_entregadores(restaurante, entregadores, limite=0.05):
    return [
        e for e in entregadores
        if (e["lat"] - restaurante["lat"])**2 +
           (e["lon"] - restaurante["lon"])**2 < limite
    ]