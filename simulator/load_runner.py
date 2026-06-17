import asyncio
import time
from dataclasses import dataclass
from typing import Any, List, Optional

from client import ApiClient
from config import SimulatorConfig
from metrics import MetricsCollector
from models import Restaurant, User
from workflow import OrderWorkflow, WorkflowContext, WorkflowResult

REPORT_SETTLE_SECONDS = 15.0


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
    failure_examples: List[str]
    stopped_early: bool
    stop_reason: Optional[str]
    planned_duration_seconds: int
    planned_expected_orders: int
    report_settle_seconds: float

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

    @staticmethod
    def _collect_finished_results(done_tasks: set[asyncio.Task], results: List[Any]) -> None:
        for task in done_tasks:
            try:
                results.append(task.result())
            except Exception as exc:
                results.append(exc)

    async def run(self) -> LoadTestResult:
        scenario = self.config.scenario
        orders_per_second = scenario.orders_per_second
        duration_seconds = scenario.duration_seconds
        planned_expected_orders = orders_per_second * duration_seconds

        pending_tasks: set[asyncio.Task] = set()
        results: List[Any] = []
        start = time.perf_counter()
        stop_reason: Optional[str] = None

        for _ in range(duration_seconds):
            second_start = time.perf_counter()

            for _ in range(orders_per_second):
                pending_tasks.add(asyncio.create_task(self._run_single_order()))

            remaining_in_second = max(0.0, 1.0 - (time.perf_counter() - second_start))
            if pending_tasks:
                done_tasks, pending_tasks = await asyncio.wait(
                    pending_tasks,
                    timeout=remaining_in_second,
                )
                self._collect_finished_results(done_tasks, results)
            elif remaining_in_second > 0:
                await asyncio.sleep(remaining_in_second)

            if not pending_tasks:
                stop_reason = "all_emitted_orders_reached_terminal_state"
                break

        emission_elapsed_seconds = time.perf_counter() - start

        if pending_tasks:
            done_tasks, _ = await asyncio.wait(pending_tasks)
            self._collect_finished_results(done_tasks, results)

        if results:
            await asyncio.sleep(REPORT_SETTLE_SECONDS)

        end_to_end_elapsed_seconds = time.perf_counter() - start

        accepted_orders = 0
        delivered_orders = 0
        failed_orders = 0
        failure_examples: List[str] = []

        for result in results:
            if isinstance(result, Exception):
                failed_orders += 1
                if len(failure_examples) < 5:
                    failure_examples.append(f"exception: {result}")
                continue

            if result.created_order.success:
                accepted_orders += 1

            if result.success:
                delivered_orders += 1
            else:
                failed_orders += 1
                if len(failure_examples) < 5:
                    failure_examples.append(self._describe_failure(result))

        return LoadTestResult(
            scenario_name=scenario.name,
            configured_orders_per_second=orders_per_second,
            duration_seconds=duration_seconds,
            expected_orders=len(results),
            attempted_orders=len(results),
            accepted_orders=accepted_orders,
            delivered_orders=delivered_orders,
            failed_orders=failed_orders,
            emission_elapsed_seconds=emission_elapsed_seconds,
            end_to_end_elapsed_seconds=end_to_end_elapsed_seconds,
            failure_examples=failure_examples,
            stopped_early=stop_reason is not None,
            stop_reason=stop_reason,
            planned_duration_seconds=duration_seconds,
            planned_expected_orders=planned_expected_orders,
            report_settle_seconds=REPORT_SETTLE_SECONDS,
        )

    def _describe_failure(self, result: WorkflowResult) -> str:
        if not result.created_order.success:
            return (
                "create_order_failed "
                f"status={result.created_order.status_code} "
                f"error={result.created_order.error}"
            )

        if result.error is not None:
            return result.error

        final_status: Optional[str] = result.final_status
        return f"workflow_failed order_id={result.order_id} final_status={final_status}"
