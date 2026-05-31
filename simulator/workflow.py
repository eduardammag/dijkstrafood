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

    async def _observe_order(self, order_id: int) -> tuple[list[RequestResult], Optional[str], Optional[list]]:
        order_results: list[RequestResult] = []
        final_status: Optional[str] = None
        observed_events: Optional[list] = None

        max_attempts = 60
        interval_seconds = 3.0
        consecutive_404 = 0

        for _ in range(max_attempts):
            order_result = await self.api_client.get_order_status(order_id)
            self._record("GET /orders/{id}", order_result)
            order_results.append(order_result)

            if order_result.status_code == 404:
                consecutive_404 += 1
                await asyncio.sleep(interval_seconds)
                continue

            consecutive_404 = 0

            if order_result.success and order_result.response_json:
                response_data = order_result.response_json
                order_data = response_data.get("order")

                if isinstance(order_data, dict):
                    final_status = order_data.get("order_status")

                if final_status == "DELIVERED":
                    break

            await asyncio.sleep(interval_seconds)

        return order_results, final_status, observed_events

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

        order_queries, final_status, observed_events = await self._observe_order(order_id)

        query_statuses = [q.status_code for q in order_queries]
        has_404 = any(code == 404 for code in query_statuses)
        has_other_errors = any((not q.success) and q.status_code != 404 for q in order_queries)
        delivered = final_status == "DELIVERED"

        success = delivered and not has_other_errors and not has_404

        error = None
        if has_404:
            error = "order_not_visible_after_creation"
        elif has_other_errors:
            error = "order_query_errors"
        elif not delivered:
            error = f"order_not_delivered_last_status={final_status}"

        return WorkflowResult(
            success=success,
            order_id=order_id,
            created_order=create_order_result,
            order_queries=order_queries,
            final_status=final_status,
            observed_events=observed_events,
            error=error,
        )