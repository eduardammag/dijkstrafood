import random
import time
from typing import List

from models import User, UserType, Courier, Restaurant, Location


FIRST_NAMES = [
    "João", "Maria", "Pedro", "Ana", "Lucas", "Julia", "Marcos", "Carla",
    "Rafael", "Beatriz", "Gustavo", "Fernanda", "Thiago", "Larissa",
]

LAST_NAMES = [
    "Silva", "Souza", "Oliveira", "Santos", "Lima", "Costa", "Almeida",
    "Pereira", "Rodrigues", "Gomes",
]

RESTAURANT_CATALOG = {
    "pizza": [
        "Pizza Calabresa",
        "Pizza Margherita",
        "Pizza Portuguesa",
        "Pizza Quatro Queijos",
        "Refrigerante Lata",
        "Suco Natural",
    ],
    "hamburguer": [
        "X-Burger",
        "X-Salada",
        "X-Bacon",
        "Batata Frita",
        "Refrigerante Lata",
        "Milkshake",
    ],
    "japonesa": [
        "Temaki Salmão",
        "Combo Sushi 20 peças",
        "Hot Roll",
        "Yakisoba",
        "Guioza",
        "Refrigerante Lata",
    ],
    "brasileira": [
        "Feijoada",
        "Prato Feito",
        "Frango Grelhado",
        "Bife Acebolado",
        "Suco Natural",
        "Refrigerante Lata",
    ],
    "italiana": [
        "Lasanha",
        "Spaghetti à Bolonhesa",
        "Nhoque ao Sugo",
        "Ravioli",
        "Refrigerante Lata",
        "Suco Natural",
    ],
    "mexicana": [
        "Taco",
        "Burrito",
        "Quesadilla",
        "Nachos",
        "Refrigerante Lata",
        "Suco Natural",
    ],
    "arabe": [
        "Esfiha de Carne",
        "Kibe",
        "Beirute",
        "Kafta com Arroz",
        "Refrigerante Lata",
        "Suco Natural",
    ],
    "vegetariana": [
        "Hambúrguer Vegetariano",
        "Salada Completa",
        "Wrap Vegetariano",
        "Quiche de Legumes",
        "Suco Natural",
        "Água",
    ],
}

CUISINE_TYPES = list(RESTAURANT_CATALOG.keys())

VEHICLE_TYPES = [
    "bike",
    "motorcycle",
    "car",
]

SAO_PAULO_LAT_MIN = -23.70
SAO_PAULO_LAT_MAX = -23.45
SAO_PAULO_LON_MIN = -46.80
SAO_PAULO_LON_MAX = -46.40


def random_name() -> str:
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    return f"{first} {last}"


def random_email(name: str) -> str:
    slug = name.lower().replace(" ", ".")
    suffix = random.randint(1000, 9999)
    return f"{slug}{suffix}@example.com"


def random_phone() -> str:
    ddd = "11"
    part1 = random.randint(90000, 99999)
    part2 = random.randint(1000, 9999)
    return f"+55{ddd}{part1}{part2}"


def random_coordinate() -> tuple[float, float]:
    latitude = random.uniform(SAO_PAULO_LAT_MIN, SAO_PAULO_LAT_MAX)
    longitude = random.uniform(SAO_PAULO_LON_MIN, SAO_PAULO_LON_MAX)
    return latitude, longitude


def build_user(user_type: UserType) -> User:
    name = random_name()
    latitude, longitude = random_coordinate()
    return User(
        user_id=None,
        user_name=name,
        email=random_email(name),
        phone=random_phone(),
        latitude=latitude,
        longitude=longitude,
        user_type=user_type,
    )


def build_admin() -> User:
    return build_user(UserType.ADMIN)


def build_client() -> User:
    return build_user(UserType.CLIENT)


def build_courier_user() -> User:
    return build_user(UserType.COURIER)


def build_courier(user_id: int) -> Courier:
    return Courier(
        user_id=user_id,
        vehicle_type=random.choice(VEHICLE_TYPES),
        is_available=True,
    )


def build_restaurant(creator_user_id: int) -> Restaurant:
    cuisine_type = random.choice(CUISINE_TYPES)
    latitude, longitude = random_coordinate()

    return Restaurant(
        restaurant_id=None,
        restaurant_name=f"Restaurante {cuisine_type.capitalize()} {random.randint(1000, 9999)}",
        cuisine_type=cuisine_type,
        restaurant_latitude=latitude,
        restaurant_longitude=longitude,
        creator_user_id=creator_user_id,
    )


def build_order_items_for_restaurant(cuisine_type: str) -> List[dict]:
    catalog = RESTAURANT_CATALOG[cuisine_type]
    item_count = random.randint(1, min(4, len(catalog)))
    chosen_items = random.sample(catalog, k=item_count)

    return [
        {
            "item_name": item,
            "quantity": random.randint(1, 3),
        }
        for item in chosen_items
    ]


def interpolate_route(
    courier_id: int,
    start_lat: float,
    start_lon: float,
    end_lat: float,
    end_lon: float,
    points: int,
) -> List[Location]:
    if points < 2:
        points = 2

    route: List[Location] = []
    timestamp = time.time()

    for i in range(points):
        fraction = i / (points - 1)
        lat = start_lat + (end_lat - start_lat) * fraction
        lon = start_lon + (end_lon - start_lon) * fraction

        route.append(
            Location(
                courier_id=courier_id,
                latitude=lat,
                longitude=lon,
                timestamp=timestamp + i * 0.1,
            )
        )

    return route