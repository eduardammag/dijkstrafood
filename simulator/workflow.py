import asyncio
from dataclasses import dataclass
from typing import Callable, Optional

from client import ApiClient
from config import SimulatorConfig
from data_generator import build_order_items_for_restaurant
from models import RequestResult, Restaurant, User

INITIAL_STATUS_POLL_DELAY_SECONDS = 2.0
BACKGROUND_STATUS_POLL_SECONDS = 8.0
ACTIVE_STATUS_POLL_SECONDS = 3.0
STATUS_POLL_CONCURRENCY = 120


@dataclass
class WorkflowContext:
    customer: User
    restaurant: Restaurant


@dataclass
class WorkflowResult:
    success: bool
    order_id: Optional[int]
    created_order: RequestResult
    order_queries: list[RequestResult]
    final_status: Optional[str] = None
    observed_events: Optional[list] = None
    error: Optional[str] = None


class OrderWorkflow:
    def __init__(
        self,
        api_client: ApiClient,
        config: SimulatorConfig,
        metrics_callback: Optional[Callable[[str, RequestResult], None]] = None,
    ):
        self.api_client = api_client
        self.config = config
        self.metrics_callback = metrics_callback
        self._status_poll_semaphore = asyncio.Semaphore(STATUS_POLL_CONCURRENCY)

    def _record(self, endpoint_name: str, result: RequestResult) -> None:
        if self.metrics_callback is not None:
            self.metrics_callback(endpoint_name, result)

    @staticmethod
    def _next_poll_delay(final_status: Optional[str], status_code: int) -> float:
        if status_code == 404:
            return ACTIVE_STATUS_POLL_SECONDS
        if final_status in {"READY_FOR_PICKUP", "PICKED_UP", "IN_TRANSIT"}:
            return ACTIVE_STATUS_POLL_SECONDS
        return BACKGROUND_STATUS_POLL_SECONDS

    async def _observe_order(
        self,
        order_id: int,
    ) -> tuple[list[RequestResult], Optional[str], Optional[list], Optional[str]]:
        order_results: list[RequestResult] = []
        final_status: Optional[str] = None
        observed_events: Optional[list] = None

        await asyncio.sleep(INITIAL_STATUS_POLL_DELAY_SECONDS)

        while True:
            async with self._status_poll_semaphore:
                order_result = await self.api_client.get_order_status(order_id)
            self._record("GET /orders/{id}", order_result)
            order_results.append(order_result)

            if order_result.status_code == 404:
                await asyncio.sleep(self._next_poll_delay(final_status, order_result.status_code))
                continue

            if not order_result.success:
                return (
                    order_results,
                    final_status,
                    observed_events,
                    f"order_query_failed_status={order_result.status_code}",
                )

            if order_result.success and order_result.response_json:
                response_data = order_result.response_json
                order_data = response_data.get("order")

                if isinstance(order_data, dict):
                    final_status = order_data.get("order_status")

                if final_status == "DELIVERED":
                    break

            await asyncio.sleep(self._next_poll_delay(final_status, order_result.status_code))

        return order_results, final_status, observed_events, None

    async def run(self, context: WorkflowContext) -> WorkflowResult:
        items = build_order_items_for_restaurant(context.restaurant.cuisine_type)

        create_order_result = await self.api_client.create_order(
            client_id=context.customer.user_id,
            restaurant_id=context.restaurant.restaurant_id,
            items=items,
        )
        self._record("POST /orders", create_order_result)

        if not create_order_result.success:
            return WorkflowResult(
                success=False,
                order_id=None,
                created_order=create_order_result,
                order_queries=[],
                error="failed_to_create_order",
            )

        if not create_order_result.response_json:
            return WorkflowResult(
                success=False,
                order_id=None,
                created_order=create_order_result,
                order_queries=[],
                error="missing_order_response_json",
            )

        order_id = create_order_result.response_json.get("order_id")
        if order_id is None:
            return WorkflowResult(
                success=False,
                order_id=None,
                created_order=create_order_result,
                order_queries=[],
                error="missing_order_id",
            )

        order_queries, final_status, observed_events, observe_error = await self._observe_order(order_id)
        delivered = final_status == "DELIVERED"

        return WorkflowResult(
            success=delivered,
            order_id=order_id,
            created_order=create_order_result,
            order_queries=order_queries,
            final_status=final_status,
            observed_events=observed_events,
            error=observe_error,
        )
