import asyncio
from dataclasses import dataclass
from typing import Callable, Optional

from client import ApiClient
from config import SimulatorConfig
from data_generator import build_order_items_for_restaurant, interpolate_route
from models import OrderStatus, RequestResult, Restaurant, User


@dataclass
class WorkflowContext:
    client: User
    restaurant: Restaurant
    courier_user_id: int


@dataclass
class WorkflowResult:
    success: bool
    order_id: Optional[int]
    created_order: Optional[RequestResult]
    confirmed_event: Optional[RequestResult]
    preparing_event: Optional[RequestResult]
    ready_for_pickup_event: Optional[RequestResult]
    picked_up_event: Optional[RequestResult]
    in_transit_event: Optional[RequestResult]
    delivered_event: Optional[RequestResult]
    location_results: list[RequestResult]
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

    async def _create_event(self, order_id: int, status: OrderStatus) -> RequestResult:
        result = self.api_client.create_order_event(order_id, status)
        self._record(f"POST /order-events [{status.value}]", result)
        return result

    async def _send_locations(
        self,
        courier_id: int,
        start_lat: float,
        start_lon: float,
        end_lat: float,
        end_lon: float,
    ) -> list[RequestResult]:
        if not self.config.location.enabled:
            return []

        route = interpolate_route(
            courier_id=courier_id,
            start_lat=start_lat,
            start_lon=start_lon,
            end_lat=end_lat,
            end_lon=end_lon,
            points=self.config.location.points_per_delivery,
        )

        results: list[RequestResult] = []

        for point in route:
            result = self.api_client.send_location(
                courier_id=point.courier_id,
                latitude=point.latitude,
                longitude=point.longitude,
            )
            self._record("POST /couriers/location", result)
            results.append(result)

            await asyncio.sleep(self.config.location.update_interval_seconds)

        return results

    async def run(self, context: WorkflowContext) -> WorkflowResult:
        items = build_order_items_for_restaurant(context.restaurant.cuisine_type)

        create_order_result = self.api_client.create_order(
            client_id=context.client.user_id,
            restaurant_id=context.restaurant.restaurant_id,
            courier_id=context.courier_user_id,
            items=items,
        )
        self._record("POST /orders", create_order_result)

        if not create_order_result.success:
            return WorkflowResult(
                success=False,
                order_id=None,
                created_order=create_order_result,
                confirmed_event=None,
                preparing_event=None,
                ready_for_pickup_event=None,
                picked_up_event=None,
                in_transit_event=None,
                delivered_event=None,
                location_results=[],
                error="failed_to_create_order",
            )

        if not create_order_result.response_json:
            return WorkflowResult(
                success=False,
                order_id=None,
                created_order=create_order_result,
                confirmed_event=None,
                preparing_event=None,
                ready_for_pickup_event=None,
                picked_up_event=None,
                in_transit_event=None,
                delivered_event=None,
                location_results=[],
                error="missing_order_response_json",
            )

        order_id = create_order_result.response_json.get("order_id")
        if order_id is None:
            return WorkflowResult(
                success=False,
                order_id=None,
                created_order=create_order_result,
                confirmed_event=None,
                preparing_event=None,
                ready_for_pickup_event=None,
                picked_up_event=None,
                in_transit_event=None,
                delivered_event=None,
                location_results=[],
                error="missing_order_id",
            )

        confirmed_result = await self._create_event(order_id, OrderStatus.CONFIRMED)
        if not confirmed_result.success:
            return WorkflowResult(
                success=False,
                order_id=order_id,
                created_order=create_order_result,
                confirmed_event=confirmed_result,
                preparing_event=None,
                ready_for_pickup_event=None,
                picked_up_event=None,
                in_transit_event=None,
                delivered_event=None,
                location_results=[],
                error="failed_confirmed_event",
            )

        await asyncio.sleep(self.config.delivery_flow.preparing_delay_seconds)

        preparing_result = await self._create_event(order_id, OrderStatus.PREPARING)
        if not preparing_result.success:
            return WorkflowResult(
                success=False,
                order_id=order_id,
                created_order=create_order_result,
                confirmed_event=confirmed_result,
                preparing_event=preparing_result,
                ready_for_pickup_event=None,
                picked_up_event=None,
                in_transit_event=None,
                delivered_event=None,
                location_results=[],
                error="failed_preparing_event",
            )

        await asyncio.sleep(self.config.delivery_flow.ready_for_pickup_delay_seconds)

        ready_result = await self._create_event(order_id, OrderStatus.READY_FOR_PICKUP)
        if not ready_result.success:
            return WorkflowResult(
                success=False,
                order_id=order_id,
                created_order=create_order_result,
                confirmed_event=confirmed_result,
                preparing_event=preparing_result,
                ready_for_pickup_event=ready_result,
                picked_up_event=None,
                in_transit_event=None,
                delivered_event=None,
                location_results=[],
                error="failed_ready_for_pickup_event",
            )

        await asyncio.sleep(self.config.delivery_flow.picked_up_delay_seconds)

        pickup_result = await self._create_event(order_id, OrderStatus.PICKED_UP)
        if not pickup_result.success:
            return WorkflowResult(
                success=False,
                order_id=order_id,
                created_order=create_order_result,
                confirmed_event=confirmed_result,
                preparing_event=preparing_result,
                ready_for_pickup_event=ready_result,
                picked_up_event=pickup_result,
                in_transit_event=None,
                delivered_event=None,
                location_results=[],
                error="failed_picked_up_event",
            )

        await asyncio.sleep(self.config.delivery_flow.in_transit_delay_seconds)

        in_transit_result = await self._create_event(order_id, OrderStatus.IN_TRANSIT)
        if not in_transit_result.success:
            return WorkflowResult(
                success=False,
                order_id=order_id,
                created_order=create_order_result,
                confirmed_event=confirmed_result,
                preparing_event=preparing_result,
                ready_for_pickup_event=ready_result,
                picked_up_event=pickup_result,
                in_transit_event=in_transit_result,
                delivered_event=None,
                location_results=[],
                error="failed_in_transit_event",
            )

        location_results = await self._send_locations(
            courier_id=context.courier_user_id,
            start_lat=context.restaurant.restaurant_latitude,
            start_lon=context.restaurant.restaurant_longitude,
            end_lat=context.client.latitude,
            end_lon=context.client.longitude,
        )

        await asyncio.sleep(self.config.delivery_flow.delivered_delay_seconds)

        delivered_result = await self._create_event(order_id, OrderStatus.DELIVERED)
        if not delivered_result.success:
            return WorkflowResult(
                success=False,
                order_id=order_id,
                created_order=create_order_result,
                confirmed_event=confirmed_result,
                preparing_event=preparing_result,
                ready_for_pickup_event=ready_result,
                picked_up_event=pickup_result,
                in_transit_event=in_transit_result,
                delivered_event=delivered_result,
                location_results=location_results,
                error="failed_delivered_event",
            )

        return WorkflowResult(
            success=True,
            order_id=order_id,
            created_order=create_order_result,
            confirmed_event=confirmed_result,
            preparing_event=preparing_result,
            ready_for_pickup_event=ready_result,
            picked_up_event=pickup_result,
            in_transit_event=in_transit_result,
            delivered_event=delivered_result,
            location_results=location_results,
            error=None,
        )