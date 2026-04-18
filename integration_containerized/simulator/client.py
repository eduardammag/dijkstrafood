import time
import requests

from models import (
    User,
    Courier,
    Restaurant,
    RequestResult,
)
from config import SimulatorConfig


class ApiClient:
    def __init__(self, config: SimulatorConfig):
        self.base_url = config.api.base_url
        self.timeout = config.api.timeout_seconds

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

    # USERS
    def create_user(self, user: User) -> RequestResult:
        payload = {
            "user_name": user.user_name,
            "email": user.email,
            "phone": user.phone,
            "latitude": user.latitude,
            "longitude": user.longitude,
            "user_type": user.user_type.value,
        }
        return self._request("POST", "/users", json=payload)

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
        items: list[dict],
    ) -> RequestResult:
        payload = {
            "client_id": client_id,
            "restaurant_id": restaurant_id,
            "items": items,  # cada item precisa ser {"name": ..., "quantity": ...}
        }
        return self._request("POST", "/orders", json=payload)

    def get_order(self, order_id: int) -> RequestResult:
        return self._request("GET", f"/orders/{order_id}")