import asyncio
import time
from dataclasses import dataclass
from typing import List

from client import ApiClient
from config import SimulatorConfig
from metrics import MetricsCollector
from models import Restaurant, User
from workflow import OrderWorkflow, WorkflowContext, WorkflowResult


@dataclass
class LoadTestResult:
    scenario_name: str
    configured_orders_per_second: int
    duration_seconds: int
    expected_orders: int
    attempted_orders: int
    completed_orders: int
    successful_orders: int
    failed_orders: int
    elapsed_seconds: float

    @property
    def effective_throughput(self) -> float:
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.completed_orders / self.elapsed_seconds


class LoadRunner:
    def __init__(
        self,
        config: SimulatorConfig,
        api_client: ApiClient,
        metrics: MetricsCollector,
        clients: List[User],
        restaurants: List[Restaurant],
    ):
        self.config = config
        self.api_client = api_client
        self.metrics = metrics
        self.clients = clients
        self.restaurants = restaurants

        self.workflow = OrderWorkflow(
            api_client=self.api_client,
            config=self.config,
            metrics_callback=self.metrics.record,
        )

        self._client_index = 0
        self._restaurant_index = 0

    def _next_client(self) -> User:
        client = self.clients[self._client_index % len(self.clients)]
        self._client_index += 1
        return client

    def _next_restaurant(self) -> Restaurant:
        restaurant = self.restaurants[self._restaurant_index % len(self.restaurants)]
        self._restaurant_index += 1
        return restaurant

    async def _run_single_order(self) -> WorkflowResult:
        client = self._next_client()
        restaurant = self._next_restaurant()

        context = WorkflowContext(
            customer=client,
            restaurant=restaurant,
        )

        return await self.workflow.run(context)

    async def run(self) -> LoadTestResult:
        scenario = self.config.scenario
        orders_per_second = scenario.orders_per_second
        duration_seconds = scenario.duration_seconds
        expected_orders = orders_per_second * duration_seconds

        tasks: List[asyncio.Task] = []

        start = time.perf_counter()

        for _second in range(duration_seconds):
            second_start = time.perf_counter()

            for _ in range(orders_per_second):
                task = asyncio.create_task(self._run_single_order())
                tasks.append(task)

            elapsed_in_second = time.perf_counter() - second_start
            sleep_time = max(0.0, 1.0 - elapsed_in_second)
            await asyncio.sleep(sleep_time)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        elapsed_seconds = time.perf_counter() - start

        completed_orders = 0
        successful_orders = 0
        failed_orders = 0

        for result in results:
            completed_orders += 1

            if isinstance(result, Exception):
                failed_orders += 1
                continue

            if result.success:
                successful_orders += 1
            else:
                failed_orders += 1

        return LoadTestResult(
            scenario_name=scenario.name,
            configured_orders_per_second=orders_per_second,
            duration_seconds=duration_seconds,
            expected_orders=expected_orders,
            attempted_orders=len(tasks),
            completed_orders=completed_orders,
            successful_orders=successful_orders,
            failed_orders=failed_orders,
            elapsed_seconds=elapsed_seconds,
        )