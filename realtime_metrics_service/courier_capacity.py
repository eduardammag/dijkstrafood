import json
import os
from urllib.error import URLError
from urllib.request import urlopen


API_URL = os.getenv("API_URL", "http://api:8000").rstrip("/")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))


def fetch_courier_capacity() -> tuple[dict[str, int] | None, str | None]:
    try:
        with urlopen(
            f"{API_URL}/couriers",
            timeout=REQUEST_TIMEOUT_SECONDS,
        ) as response:
            payload = json.load(response)
        couriers = payload.get("couriers", [])
        if not isinstance(couriers, list):
            return None, "invalid_payload"

        available = 0
        active = 0

        for courier in couriers:
            if not isinstance(courier, dict):
                continue
            is_available = courier.get("is_available")
            if is_available is True:
                available += 1
            elif is_available is False:
                active += 1

        total = len(couriers)
        return {
            "couriers_available": available,
            "active_couriers": active,
            "tracked_couriers": total,
            "total_couriers": total,
        }, None
    except (URLError, TimeoutError, ValueError, OSError) as exc:
        return None, f"{type(exc).__name__}: {exc}"
