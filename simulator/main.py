import argparse
import asyncio
import json
from pathlib import Path
from typing import List, Tuple

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
from models import Restaurant, User
from workflow import WorkflowResult

POPULATION_STATE_FILE = Path(__file__).with_name("population_state.json")


async def require_response_json_async(result, operation_name: str) -> dict:
    if not result.success:
        raise RuntimeError(
            f"{operation_name} failed: status={result.status_code}, error={result.error}"
        )

    if result.response_json is None:
        raise RuntimeError(f"{operation_name} returned no JSON response")

    return result.response_json


async def populate_admins(api_client: ApiClient, admin_count: int, metrics: MetricsCollector) -> List[User]:
    admins: List[User] = []

    for _ in range(admin_count):
        admin = build_admin()
        result = await api_client.create_user(admin)
        metrics.record("POST /users", result)

        payload = await require_response_json_async(result, "create_admin")
        admin.user_id = payload["user_id"]
        admins.append(admin)

    return admins


async def populate_clients(api_client: ApiClient, client_count: int, metrics: MetricsCollector) -> List[User]:
    clients: List[User] = []

    for _ in range(client_count):
        client = build_client()
        result = await api_client.create_user(client)
        metrics.record("POST /users", result)

        payload = await require_response_json_async(result, "create_client")
        client.user_id = payload["user_id"]
        clients.append(client)

    return clients


async def populate_couriers(api_client: ApiClient, courier_count: int, metrics: MetricsCollector) -> Tuple[List[User], List[int]]:
    courier_users: List[User] = []
    courier_user_ids: List[int] = []

    for _ in range(courier_count):
        courier_user = build_courier_user()

        create_user_result = await api_client.create_user(courier_user)
        metrics.record("POST /users", create_user_result)
        payload = await require_response_json_async(create_user_result, "create_courier_user")

        courier_user.user_id = payload["user_id"]
        courier_users.append(courier_user)

        courier = build_courier(courier_user.user_id)
        create_courier_result = await api_client.create_courier(courier)
        metrics.record("POST /couriers", create_courier_result)

        await require_response_json_async(create_courier_result, "create_courier")
        courier_user_ids.append(courier_user.user_id)

    return courier_users, courier_user_ids


async def populate_restaurants(api_client: ApiClient, restaurant_count: int, creator_user_id: int, metrics: MetricsCollector) -> List[Restaurant]:
    restaurants: List[Restaurant] = []

    for _ in range(restaurant_count):
        restaurant = build_restaurant(creator_user_id=creator_user_id)

        result = await api_client.create_restaurant(restaurant)
        metrics.record("POST /restaurants", result)

        payload = await require_response_json_async(result, "create_restaurant")
        restaurant.restaurant_id = payload["restaurant_id"]
        restaurants.append(restaurant)

    return restaurants


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


def print_load_test_result(result) -> None:
    print("\n===== LOAD TEST SUMMARY =====")
    print(f"Scenario: {result.scenario_name}")
    print(f"Configured Orders/s: {result.configured_orders_per_second}")
    print(f"Duration (s): {result.duration_seconds}")
    print(f"Expected Orders: {result.expected_orders}")
    print(f"Attempted Orders: {result.attempted_orders}")
    print(f"Accepted Orders: {result.accepted_orders}")
    print(f"Delivered Orders: {result.delivered_orders}")
    print(f"Failed Orders: {result.failed_orders}")
    print(f"Accepted Throughput: {result.accepted_throughput:.2f} orders/s")
    print(f"Delivered Throughput: {result.delivered_throughput:.2f} orders/s")
    print("========================================")


def _serialize_user(user: User) -> dict:
    return {
        "user_id": user.user_id,
        "user_name": user.user_name,
        "email": user.email,
        "phone": user.phone,
        "latitude": user.latitude,
        "longitude": user.longitude,
        "user_type": user.user_type.value,
    }


def _deserialize_user(payload: dict) -> User:
    from models import UserType

    return User(
        user_id=payload["user_id"],
        user_name=payload["user_name"],
        email=payload["email"],
        phone=payload["phone"],
        latitude=payload["latitude"],
        longitude=payload["longitude"],
        user_type=UserType(payload["user_type"]),
    )


def _serialize_restaurant(restaurant: Restaurant) -> dict:
    return {
        "restaurant_id": restaurant.restaurant_id,
        "restaurant_name": restaurant.restaurant_name,
        "cuisine_type": restaurant.cuisine_type,
        "restaurant_latitude": restaurant.restaurant_latitude,
        "restaurant_longitude": restaurant.restaurant_longitude,
        "creator_user_id": restaurant.creator_user_id,
    }


def _deserialize_restaurant(payload: dict) -> Restaurant:
    return Restaurant(
        restaurant_id=payload["restaurant_id"],
        restaurant_name=payload["restaurant_name"],
        cuisine_type=payload["cuisine_type"],
        restaurant_latitude=payload["restaurant_latitude"],
        restaurant_longitude=payload["restaurant_longitude"],
        creator_user_id=payload["creator_user_id"],
    )


def load_population_state():
    if not POPULATION_STATE_FILE.exists():
        return None

    with POPULATION_STATE_FILE.open("r", encoding="utf-8") as state_file:
        payload = json.load(state_file)

    return {
        "admins": [_deserialize_user(item) for item in payload.get("admins", [])],
        "clients": [_deserialize_user(item) for item in payload.get("clients", [])],
        "courier_users": [_deserialize_user(item) for item in payload.get("courier_users", [])],
        "restaurants": [_deserialize_restaurant(item) for item in payload.get("restaurants", [])],
    }


def save_population_state(
    admins: List[User],
    clients: List[User],
    courier_users: List[User],
    restaurants: List[Restaurant],
) -> None:
    payload = {
        "admins": [_serialize_user(user) for user in admins],
        "clients": [_serialize_user(user) for user in clients],
        "courier_users": [_serialize_user(user) for user in courier_users],
        "restaurants": [_serialize_restaurant(restaurant) for restaurant in restaurants],
    }

    with POPULATION_STATE_FILE.open("w", encoding="utf-8") as state_file:
        json.dump(payload, state_file, indent=2)


def has_required_population(population_state: dict, config) -> bool:
    return (
        len(population_state["admins"]) >= config.population.admins
        and len(population_state["clients"]) >= config.population.clients
        and len(population_state["courier_users"]) >= config.population.couriers
        and len(population_state["restaurants"]) >= config.population.restaurants
    )


async def cached_population_matches_api(api_client: ApiClient, population_state: dict, config) -> bool:
    restaurants_result = await api_client.list_restaurants()
    remote_payload = await require_response_json_async(restaurants_result, "list_restaurants")
    remote_restaurants = remote_payload.get("restaurants", [])

    remote_restaurant_ids = {
        restaurant.get("restaurant_id")
        for restaurant in remote_restaurants
        if isinstance(restaurant, dict)
    }

    cached_restaurant_ids = {
        restaurant.restaurant_id
        for restaurant in population_state["restaurants"][: config.population.restaurants]
    }

    if not cached_restaurant_ids:
        return False

    return cached_restaurant_ids.issubset(remote_restaurant_ids)


async def async_main(scenario_name: str) -> None:
    config = build_config(scenario_name)
    metrics = MetricsCollector()

    async with ApiClient(config) as api_client:
        print(f"\nStarting simulator with scenario: {scenario_name}")
        print(f"API Base URL: {config.api.base_url}")

        cached_population = load_population_state()
        if cached_population and has_required_population(cached_population, config):
            try:
                cache_is_valid = await cached_population_matches_api(
                    api_client,
                    cached_population,
                    config,
                )
            except Exception as exc:
                print(f"\nCould not validate cached population against API: {exc}")
                cache_is_valid = False

            if cache_is_valid:
                print("\nExisting population found. Skipping population step...")
                admins = cached_population["admins"][: config.population.admins]
                clients = cached_population["clients"][: config.population.clients]
                courier_users = cached_population["courier_users"][: config.population.couriers]
                restaurants = cached_population["restaurants"][: config.population.restaurants]
            else:
                print("\nCached population is stale for the current API database. Recreating population...")
                admins = await populate_admins(api_client, config.population.admins, metrics)

                if not admins:
                    raise RuntimeError("No admins were created; cannot continue")

                creator_admin = admins[0]

                print("Creating clients...")
                clients = await populate_clients(api_client, config.population.clients, metrics)

                print("Creating couriers...")
                courier_users, _ = await populate_couriers(
                    api_client,
                    config.population.couriers,
                    metrics,
                )

                print("Creating restaurants...")
                restaurants = await populate_restaurants(
                    api_client,
                    config.population.restaurants,
                    creator_user_id=creator_admin.user_id,
                    metrics=metrics,
                )
                save_population_state(admins, clients, courier_users, restaurants)
                print(f"Population state saved to: {POPULATION_STATE_FILE}")
        else:
            print("\nCreating admins...")
            admins = await populate_admins(api_client, config.population.admins, metrics)

            if not admins:
                raise RuntimeError("No admins were created; cannot continue")

            creator_admin = admins[0]

            print("Creating clients...")
            clients = await populate_clients(api_client, config.population.clients, metrics)

            print("Creating couriers...")
            courier_users, _ = await populate_couriers(
                api_client,
                config.population.couriers,
                metrics,
            )

            print("Creating restaurants...")
            restaurants = await populate_restaurants(
                api_client,
                config.population.restaurants,
                creator_user_id=creator_admin.user_id,
                metrics=metrics,
            )
            save_population_state(admins, clients, courier_users, restaurants)
            print(f"Population state saved to: {POPULATION_STATE_FILE}")

        print_population_summary(admins, clients, courier_users, restaurants)

        print("\nStarting load test...")

        runner = LoadRunner(
            config=config,
            api_client=api_client,
            metrics=metrics,
            clients=clients,
            restaurants=restaurants,
        )

        load_test_result = await runner.run()

        from report import print_load_test_summary, print_metrics, export_json_report

        print_load_test_summary(load_test_result)
        print_metrics(metrics)

        if config.metrics.export_json:
            export_json_report(load_test_result, metrics, config.metrics.output_dir)


def parse_args():
    parser = argparse.ArgumentParser(description="DijkFood load simulator")
    parser.add_argument(
        "--scenario",
        choices=["teste", "normal", "peak", "special"],
        default="normal",
        help="Load scenario to execute",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    asyncio.run(async_main(args.scenario))


if __name__ == "__main__":
    main()
