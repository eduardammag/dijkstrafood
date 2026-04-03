import time
import requests

from models import (
    User,
    Courier,
    Restaurant,
    OrderStatus,
    RequestResult,
)
from config import SimulatorConfig


class ApiClient:
    def __init__(self, config: SimulatorConfig):
        self.base_url = config.api.base_url
        self.timeout = config.api.timeout_seconds

    # INTERNAL REQUEST METHOD
    def _request(self, method: str, path: str, json=None) -> RequestResult:
        url = f"{self.base_url}{path}"

        start = time.perf_counter()

        try:
            response = requests.request(
                method,
                url,
                json=json,
                timeout=self.timeout,
            )

            latency = (time.perf_counter() - start) * 1000

            parsed_json = None
            try:
                parsed_json = response.json()
            except Exception:
                parsed_json = None

            return RequestResult(
                success=200 <= response.status_code < 300,
                latency_ms=latency,
                status_code=response.status_code,
                response_json=parsed_json,
                error=None if response.ok else response.text,
            )

        except Exception as e:
            latency = (time.perf_counter() - start) * 1000

            return RequestResult(
                success=False,
                latency_ms=latency,
                status_code=0,
                response_json=None,
                error=str(e),
            )
        
    # COURIERS

    def create_courier(self, courier: Courier) -> RequestResult:
        payload = {
            "user_id": courier.user_id,
            "vehicle_type": courier.vehicle_type,
            "is_available": courier.is_available,
        }

        return self._request("POST", "/couriers", json=payload)

    # RESTAURANTS

    def create_restaurant(self, restaurant: Restaurant) -> RequestResult:
        payload = {
            "restaurant_name": restaurant.restaurant_name,
            "cuisine_type": restaurant.cuisine_type,
            "restaurant_latitude": restaurant.restaurant_latitude,
            "restaurant_longitude": restaurant.restaurant_longitude,
            "creator_user_id": restaurant.creator_user_id,
        }

        return self._request("POST", "/restaurants", json=payload)

    # ORDERS

    def create_order(
        self,
        client_id: int,
        restaurant_id: int,
        courier_id: int,
        items: list[dict],
    ) -> RequestResult:
        payload = {
            "client_id": client_id,
            "restaurant_id": restaurant_id,
            "courier_id": courier_id,
            "items": items,
        }

        return self._request("POST", "/orders", json=payload)

    # ORDER EVENTS

    def create_order_event(
        self,
        order_id: int,
        status: OrderStatus,
    ) -> RequestResult:
        payload = {
            "order_id": order_id,
            "event_status": status.value,
        }

        return self._request("POST", "/order-events", json=payload)

    # LOCATION (FUTURO)

    def send_location(
        self,
        courier_id: int,
        latitude: float,
        longitude: float,
    ) -> RequestResult:
        payload = {
            "courier_id": courier_id,
            "latitude": latitude,
            "longitude": longitude,
        }

        return self._request("POST", "/couriers/location", json=payload)