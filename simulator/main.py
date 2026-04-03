import argparse
import asyncio
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


def require_response_json(result, operation_name: str) -> dict:
    if not result.success:
        raise RuntimeError(
            f"{operation_name} failed: status={result.status_code}, error={result.error}"
        )

    if result.response_json is None:
        raise RuntimeError(f"{operation_name} returned no JSON response")

    return result.response_json


def populate_admins(api_client: ApiClient, admin_count: int, metrics: MetricsCollector) -> List[User]:
    admins: List[User] = []

    for _ in range(admin_count):
        admin = build_admin()
        result = api_client.create_user(admin)
        metrics.record("POST /users", result)

        payload = require_response_json(result, "create_admin")

        admin.user_id = payload["user_id"]
        admins.append(admin)

    return admins


def populate_clients(api_client: ApiClient, client_count: int, metrics: MetricsCollector) -> List[User]:
    clients: List[User] = []

    for _ in range(client_count):
        client = build_client()
        result = api_client.create_user(client)
        metrics.record("POST /users", result)

        payload = require_response_json(result, "create_client")

        client.user_id = payload["user_id"]
        clients.append(client)

    return clients


def populate_couriers(
    api_client: ApiClient,
    courier_count: int,
    metrics: MetricsCollector,
) -> Tuple[List[User], List[int]]:
    courier_users: List[User] = []
    courier_user_ids: List[int] = []

    for _ in range(courier_count):
        courier_user = build_courier_user()

        create_user_result = api_client.create_user(courier_user)
        metrics.record("POST /users", create_user_result)

        payload = require_response_json(create_user_result, "create_courier_user")

        courier_user.user_id = payload["user_id"]
        courier_users.append(courier_user)

        courier = build_courier(courier_user.user_id)
        create_courier_result = api_client.create_courier(courier)
        metrics.record("POST /couriers", create_courier_result)

        require_response_json(create_courier_result, "create_courier")

        courier_user_ids.append(courier_user.user_id)

    return courier_users, courier_user_ids


def populate_restaurants(
    api_client: ApiClient,
    restaurant_count: int,
    creator_user_id: int,
    metrics: MetricsCollector,
) -> List[Restaurant]:
    restaurants: List[Restaurant] = []

    for _ in range(restaurant_count):
        restaurant = build_restaurant(creator_user_id=creator_user_id)

        result = api_client.create_restaurant(restaurant)
        metrics.record("POST /restaurants", result)

        payload = require_response_json(result, "create_restaurant")

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
    print("\n===== LOAD TEST RESULT =====")
    print(f"Scenario: {result.scenario_name}")
    print(f"Configured Orders/s: {result.configured_orders_per_second}")
    print(f"Duration (s): {result.duration_seconds}")
    print(f"Expected Orders: {result.expected_orders}")
    print(f"Attempted Orders: {result.attempted_orders}")
    print(f"Completed Orders: {result.completed_orders}")
    print(f"Successful Orders: {result.successful_orders}")
    print(f"Failed Orders: {result.failed_orders}")
    print(f"Elapsed Time (s): {result.elapsed_seconds:.2f}")
    print(f"Effective Throughput (orders/s): {result.effective_throughput:.2f}")
    print("=" * 30)


async def async_main(scenario_name: str) -> None:
    config = build_config(scenario_name)
    api_client = ApiClient(config)
    metrics = MetricsCollector()

    print(f"\nStarting simulator with scenario: {scenario_name}")
    print(f"API Base URL: {config.api.base_url}")

    # POPULATE SYSTEM
    print("\nCreating admins...")
    admins = populate_admins(api_client, config.population.admins, metrics)

    if not admins:
        raise RuntimeError("No admins were created; cannot continue")

    creator_admin = admins[0]

    print("Creating clients...")
    clients = populate_clients(api_client, config.population.clients, metrics)

    print("Creating couriers...")
    courier_users, courier_user_ids = populate_couriers(
        api_client,
        config.population.couriers,
        metrics,
    )

    print("Creating restaurants...")
    restaurants = populate_restaurants(
        api_client,
        config.population.restaurants,
        creator_user_id=creator_admin.user_id,
        metrics=metrics,
    )

    print_population_summary(admins, clients, courier_users, restaurants)

    # RUN LOAD TEST
    print("\nStarting load test...")

    runner = LoadRunner(
        config=config,
        api_client=api_client,
        metrics=metrics,
        clients=clients,
        restaurants=restaurants,
        courier_user_ids=courier_user_ids,
    )

    load_test_result = await runner.run()

    # REPORT
    from report import print_load_test_summary, print_metrics, export_json_report

    print_load_test_summary(load_test_result)
    print_metrics(metrics)

    if config.metrics.export_json:
        export_json_report(load_test_result, metrics, config.metrics.output_dir)


def parse_args():
    parser = argparse.ArgumentParser(description="DijkFood load simulator")
    parser.add_argument(
        "--scenario",
        choices=["normal", "peak", "special"],
        default="normal",
        help="Load scenario to execute",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    asyncio.run(async_main(args.scenario))


if __name__ == "__main__":
    main()