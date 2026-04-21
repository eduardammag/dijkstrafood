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
    accepted_orders: int
    delivered_orders: int
    failed_orders: int
    emission_elapsed_seconds: float
    end_to_end_elapsed_seconds: float

    @property
    def configured_throughput(self) -> float:
        return float(self.configured_orders_per_second)

    @property
    def accepted_throughput(self) -> float:
        if self.emission_elapsed_seconds <= 0:
            return 0.0
        return self.accepted_orders / self.emission_elapsed_seconds

    @property
    def delivered_throughput(self) -> float:
        if self.end_to_end_elapsed_seconds <= 0:
            return 0.0
        return self.delivered_orders / self.end_to_end_elapsed_seconds


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
        self._selection_lock = asyncio.Lock()

    async def _next_client_and_restaurant(self):
        async with self._selection_lock:
            client = self.clients[self._client_index % len(self.clients)]
            restaurant = self.restaurants[self._restaurant_index % len(self.restaurants)]
            self._client_index += 1
            self._restaurant_index += 1
            return client, restaurant

    async def _run_single_order(self) -> WorkflowResult:
        client, restaurant = await self._next_client_and_restaurant()
        context = WorkflowContext(customer=client, restaurant=restaurant)
        return await self.workflow.run(context)

    async def run(self) -> LoadTestResult:
        scenario = self.config.scenario
        orders_per_second = scenario.orders_per_second
        duration_seconds = scenario.duration_seconds
        expected_orders = orders_per_second * duration_seconds

        tasks: List[asyncio.Task] = []
        start = time.perf_counter()

        for _ in range(duration_seconds):
            second_start = time.perf_counter()

            for _ in range(orders_per_second):
                tasks.append(asyncio.create_task(self._run_single_order()))

            elapsed_in_second = time.perf_counter() - second_start
            await asyncio.sleep(max(0.0, 1.0 - elapsed_in_second))

        emission_elapsed_seconds = time.perf_counter() - start
        results = await asyncio.gather(*tasks, return_exceptions=True)
        end_to_end_elapsed_seconds = time.perf_counter() - start

        accepted_orders = 0
        delivered_orders = 0
        failed_orders = 0

        for result in results:
            if isinstance(result, Exception):
                failed_orders += 1
                continue

            if result.created_order.success:
                accepted_orders += 1

            if result.success:
                delivered_orders += 1
            else:
                failed_orders += 1

        return LoadTestResult(
            scenario_name=scenario.name,
            configured_orders_per_second=orders_per_second,
            duration_seconds=duration_seconds,
            expected_orders=expected_orders,
            attempted_orders=len(tasks),
            accepted_orders=accepted_orders,
            delivered_orders=delivered_orders,
            failed_orders=failed_orders,
            emission_elapsed_seconds=emission_elapsed_seconds,
            end_to_end_elapsed_seconds=end_to_end_elapsed_seconds,
        )