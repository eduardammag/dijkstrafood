import argparse
import asyncio
from typing import List, Literal, Tuple, TypeVar

from client import ApiClient
from config import build_config
from data_generator import (
    build_admin,
    build_client,
    build_courier,
    build_courier_user,
    build_restaurant,
)
from load_runner import LoadRunner
from metrics import MetricsCollector
from models import Restaurant, User, UserType

SimulatorMode = Literal["auto", "populate", "load"]
T = TypeVar("T")


async def require_response_json_async(result, operation_name: str) -> dict:
    if not result.success:
        raise RuntimeError(
            f"{operation_name} failed: status={result.status_code}, error={result.error}"
        )

    if result.response_json is None:
        raise RuntimeError(f"{operation_name} returned no JSON response")

    return result.response_json


async def populate_admins(
    api_client: ApiClient,
    admin_count: int,
    metrics: MetricsCollector,
) -> List[User]:
    admins: List[User] = []

    for _ in range(admin_count):
        admin = build_admin()
        result = await api_client.create_user(admin)
        metrics.record("POST /users", result)

        payload = await require_response_json_async(result, "create_admin")
        admin.user_id = payload["user_id"]
        admins.append(admin)

    return admins


async def populate_clients(
    api_client: ApiClient,
    client_count: int,
    metrics: MetricsCollector,
) -> List[User]:
    clients: List[User] = []

    for _ in range(client_count):
        client = build_client()
        result = await api_client.create_user(client)
        metrics.record("POST /users", result)

        payload = await require_response_json_async(result, "create_client")
        client.user_id = payload["user_id"]
        clients.append(client)

    return clients


async def populate_couriers(
    api_client: ApiClient,
    courier_count: int,
    metrics: MetricsCollector,
) -> Tuple[List[User], List[int]]:
    courier_users: List[User] = []
    courier_user_ids: List[int] = []

    for _ in range(courier_count):
        courier_user = build_courier_user()

        create_user_result = await api_client.create_user(courier_user)
        metrics.record("POST /users", create_user_result)
        payload = await require_response_json_async(
            create_user_result,
            "create_courier_user",
        )

        courier_user.user_id = payload["user_id"]
        courier_users.append(courier_user)

        courier = build_courier(courier_user.user_id)
        create_courier_result = await api_client.create_courier(courier)
        metrics.record("POST /couriers", create_courier_result)

        await require_response_json_async(create_courier_result, "create_courier")
        courier_user_ids.append(courier_user.user_id)

    return courier_users, courier_user_ids


async def populate_restaurants(
    api_client: ApiClient,
    restaurant_count: int,
    creator_user_id: int,
    metrics: MetricsCollector,
) -> List[Restaurant]:
    restaurants: List[Restaurant] = []

    for _ in range(restaurant_count):
        restaurant = build_restaurant(creator_user_id=creator_user_id)

        result = await api_client.create_restaurant(restaurant)
        metrics.record("POST /restaurants", result)

        payload = await require_response_json_async(result, "create_restaurant")
        restaurant.restaurant_id = payload["restaurant_id"]
        restaurants.append(restaurant)

    return restaurants


async def list_users(api_client: ApiClient, user_type: UserType) -> List[User]:
    result = await api_client.list_users(user_type.value)
    payload = await require_response_json_async(result, f"list_users_{user_type.value}")
    users = payload.get("users", [])

    return [
        User(
            user_id=user["user_id"],
            user_name=user["user_name"],
            email=user.get("email") or f"user{user['user_id']}@example.com",
            phone=user.get("phone") or "",
            latitude=float(user.get("latitude") or 0.0),
            longitude=float(user.get("longitude") or 0.0),
            user_type=UserType(user["user_type"]),
        )
        for user in users
    ]


async def list_restaurants(api_client: ApiClient) -> List[Restaurant]:
    result = await api_client.list_restaurants()
    payload = await require_response_json_async(result, "list_restaurants")
    restaurants = payload.get("restaurants", [])

    return [
        Restaurant(
            restaurant_id=restaurant["restaurant_id"],
            restaurant_name=restaurant["restaurant_name"],
            cuisine_type=restaurant.get("cuisine_type") or "brasileira",
            restaurant_latitude=float(restaurant.get("restaurant_latitude") or 0.0),
            restaurant_longitude=float(restaurant.get("restaurant_longitude") or 0.0),
            creator_user_id=0,
        )
        for restaurant in restaurants
    ]


async def list_courier_users(api_client: ApiClient) -> List[User]:
    result = await api_client.list_couriers()
    payload = await require_response_json_async(result, "list_couriers")
    couriers = payload.get("couriers", [])

    return [
        User(
            user_id=courier["courier_id"],
            user_name=courier.get("name") or f"Courier {courier['courier_id']}",
            email=f"courier{courier['courier_id']}@example.com",
            phone="",
            latitude=float(courier.get("lat") or 0.0),
            longitude=float(courier.get("lon") or 0.0),
            user_type=UserType.COURIER,
        )
        for courier in couriers
    ]


def take_up_to(items: List[T], target_count: int) -> List[T]:
    if target_count <= 0:
        return items
    return items[:target_count]


async def ensure_population(
    api_client: ApiClient,
    config,
    metrics: MetricsCollector,
    mode: SimulatorMode,
) -> Tuple[List[User], List[User], List[User], List[Restaurant]]:
    admins = await list_users(api_client, UserType.ADMIN)
    clients = await list_users(api_client, UserType.CLIENT)
    courier_users = await list_courier_users(api_client)
    restaurants = await list_restaurants(api_client)

    print("\nExisting population found:")
    print(f"Admins: {len(admins)}")
    print(f"Clients: {len(clients)}")
    print(f"Couriers: {len(courier_users)}")
    print(f"Restaurants: {len(restaurants)}")

    if mode == "load":
        if not clients or not restaurants or not courier_users:
            raise RuntimeError(
                "Load mode requires existing clients, restaurants, and couriers."
            )
        return (
            take_up_to(admins, config.population.admins),
            take_up_to(clients, config.population.clients),
            take_up_to(courier_users, config.population.couriers),
            take_up_to(restaurants, config.population.restaurants),
        )

    if len(admins) < config.population.admins:
        print("\nCreating missing admins...")
        admins.extend(
            await populate_admins(
                api_client,
                config.population.admins - len(admins),
                metrics,
            )
        )

    if len(clients) < config.population.clients:
        print("Creating missing clients...")
        clients.extend(
            await populate_clients(
                api_client,
                config.population.clients - len(clients),
                metrics,
            )
        )

    if len(courier_users) < config.population.couriers:
        print("Creating missing couriers...")
        new_courier_users, _ = await populate_couriers(
            api_client,
            config.population.couriers - len(courier_users),
            metrics,
        )
        courier_users.extend(new_courier_users)

    if len(restaurants) < config.population.restaurants:
        if not admins:
            raise RuntimeError("At least one admin is required to create restaurants.")
        print("Creating missing restaurants...")
        restaurants.extend(
            await populate_restaurants(
                api_client,
                config.population.restaurants - len(restaurants),
                creator_user_id=admins[0].user_id,
                metrics=metrics,
            )
        )

    return (
        take_up_to(admins, config.population.admins),
        take_up_to(clients, config.population.clients),
        take_up_to(courier_users, config.population.couriers),
        take_up_to(restaurants, config.population.restaurants),
    )


def print_population_summary(
    admins: List[User],
    clients: List[User],
    courier_users: List[User],
    restaurants: List[Restaurant],
) -> None:
    print("\n===== POPULATION SUMMARY =====")
    print(f"Admins: {len(admins)}")
    print(f"Clients: {len(clients)}")
    print(f"Couriers: {len(courier_users)}")
    print(f"Restaurants: {len(restaurants)}")
    print("=" * 30)


async def async_main(
    scenario_name: str,
    duration_seconds_override: int | None = None,
    mode: SimulatorMode = "auto",
) -> None:
    config = build_config(
        scenario_name,
        duration_seconds_override=duration_seconds_override,
    )
    metrics = MetricsCollector()

    async with ApiClient(config) as api_client:
        print(f"\nStarting simulator with scenario: {scenario_name}")
        print(f"API Base URL: {config.api.base_url}")
        print(f"Mode: {mode}")

        admins, clients, courier_users, restaurants = await ensure_population(
            api_client=api_client,
            config=config,
            metrics=metrics,
            mode=mode,
        )

        print_population_summary(admins, clients, courier_users, restaurants)

        if mode == "populate":
            print("\nPopulation mode completed. Skipping load test.")
            return

        if not clients or not restaurants:
            raise RuntimeError(
                "Load test requires at least one client and one restaurant."
            )

        print("\nStarting load test...")

        runner = LoadRunner(
            config=config,
            api_client=api_client,
            metrics=metrics,
            clients=clients,
            restaurants=restaurants,
        )

        load_test_result = await runner.run()

        from report import (
            export_json_report,
            print_capacity_recommendation,
            print_load_test_summary,
            print_metrics,
            print_sla_evaluation,
        )

        print_load_test_summary(load_test_result)
        print_metrics(metrics)
        print_sla_evaluation(metrics)
        print_capacity_recommendation(load_test_result, metrics, config)

        if config.metrics.export_json:
            export_json_report(
                load_test_result,
                metrics,
                config.metrics.output_dir,
                config,
            )


def parse_args():
    parser = argparse.ArgumentParser(description="DijkFood load simulator")
    parser.add_argument(
        "--mode",
        choices=["auto", "populate", "load"],
        default="auto",
        help="auto: reuse/create missing population and run load; populate: only ensure population; load: reuse existing data only",
    )
    parser.add_argument(
        "--scenario",
        choices=["teste", "normal", "peak", "special"],
        default="normal",
        help="Load scenario to execute",
    )
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=None,
        help="Override scenario duration while keeping the same orders/s",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    asyncio.run(async_main(args.scenario, args.duration_seconds, args.mode))


if __name__ == "__main__":
    main()
