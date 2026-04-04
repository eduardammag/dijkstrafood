import asyncio
from dataclasses import dataclass
from typing import Callable, Optional

from client import ApiClient
from config import SimulatorConfig
from data_generator import build_order_items_for_restaurant
from models import RequestResult, Restaurant, User


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

    def _record(self, endpoint_name: str, result: RequestResult) -> None:
        if self.metrics_callback is not None:
            self.metrics_callback(endpoint_name, result)

    async def _observe_order(
        self,
        order_id: int,
    ) -> tuple[list[RequestResult], Optional[str], Optional[list]]:
        order_results: list[RequestResult] = []
        final_status: Optional[str] = None
        observed_events: Optional[list] = None

        max_attempts = 10
        interval_seconds = 1.0

        for _ in range(max_attempts):
            order_result = self.api_client.get_order(order_id)
            self._record("GET /orders/{id}", order_result)
            order_results.append(order_result)

            if order_result.success and order_result.response_json:
                response_data = order_result.response_json

                # status em tempo real vem como realtime_status nessa API
                final_status = response_data.get("realtime_status")

                # eventos vêm embutidos dentro do GET /orders/{id}
                observed_events = response_data.get("events")

                if final_status == "DELIVERED":
                    break

            await asyncio.sleep(interval_seconds)

        return order_results, final_status, observed_events

    async def run(self, context: WorkflowContext) -> WorkflowResult:
        items = build_order_items_for_restaurant(context.restaurant.cuisine_type)

        create_order_result = self.api_client.create_order(
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
                final_status=None,
                observed_events=None,
                error="failed_to_create_order",
            )

        if not create_order_result.response_json:
            return WorkflowResult(
                success=False,
                order_id=None,
                created_order=create_order_result,
                order_queries=[],
                final_status=None,
                observed_events=None,
                error="missing_order_response_json",
            )

        order_id = create_order_result.response_json.get("order_id")
        if order_id is None:
            return WorkflowResult(
                success=False,
                order_id=None,
                created_order=create_order_result,
                order_queries=[],
                final_status=None,
                observed_events=None,
                error="missing_order_id",
            )

        order_queries, final_status, observed_events = await self._observe_order(order_id)

        return WorkflowResult(
            success=True,
            order_id=order_id,
            created_order=create_order_result,
            order_queries=order_queries,
            final_status=final_status,
            observed_events=observed_events,
            error=None,
        )