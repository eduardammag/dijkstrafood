import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

COURIER_ID_IN_MESSAGE = re.compile(r"courier\s+(\d+)", re.IGNORECASE)


@dataclass
class ParsedEvent:
    event_type: str
    order_id: int | None = None
    status: str | None = None
    courier_id: int | None = None
    courier_available: bool | None = None
    format_name: str = "unknown"
    timestamp: float | None = None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return None


def _to_timestamp(value: Any) -> float | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None

        if candidate.endswith("Z"):
            candidate = candidate.replace("Z", "+00:00")

        try:
            return datetime.fromisoformat(candidate).timestamp()
        except ValueError:
            return None

    return None


def _extract_payload(raw_data: bytes) -> Any:
    text = raw_data.decode("utf-8", errors="replace").strip()
    if not text:
        return None

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw_text": text}


def _parse_flat_order_event(payload: dict[str, Any]) -> ParsedEvent | None:
    if "order_id" not in payload:
        return None

    explicit_event_type = str(payload.get("event_type", "")).upper().strip()
    if explicit_event_type in {"ORDER_CREATED", "ORDER_STATUS_CHANGED", "ORDER_COURIER_ASSIGNED"}:
        status = payload.get("new_status") or payload.get("status") or payload.get("order_status")
        normalized_type = {
            "ORDER_CREATED": "ORDER_CREATED",
            "ORDER_STATUS_CHANGED": "STATUS_CHANGE",
            "ORDER_COURIER_ASSIGNED": "COURIER_ASSIGNED",
        }[explicit_event_type]

        return ParsedEvent(
            event_type=normalized_type,
            order_id=_to_int(payload.get("order_id")),
            status=str(status).upper() if status else None,
            courier_id=_to_int(payload.get("courier_id")),
            format_name="flat_order_event_v2",
            timestamp=_to_timestamp(payload.get("timestamp") or payload.get("created_at")),
        )

    if {"event_type", "from_status", "to_status"}.intersection(payload.keys()):
        event_type = str(payload.get("event_type", "ORDER_EVENT")).upper()
        status = payload.get("to_status") or payload.get("status") or payload.get("order_status")
        courier_id = _to_int(payload.get("courier_id"))
        if courier_id is None:
            message = str(payload.get("event_message", ""))
            match = COURIER_ID_IN_MESSAGE.search(message)
            if match:
                courier_id = _to_int(match.group(1))

        return ParsedEvent(
            event_type=event_type,
            order_id=_to_int(payload.get("order_id")),
            status=str(status).upper() if status else None,
            courier_id=courier_id,
            format_name="flat_order_event",
            timestamp=_to_timestamp(payload.get("created_at") or payload.get("timestamp")),
        )

    return None


def _parse_status_update(payload: dict[str, Any]) -> ParsedEvent | None:
    if "order_id" in payload and {"status", "order_status", "to_status"}.intersection(payload.keys()):
        status = payload.get("status") or payload.get("order_status") or payload.get("to_status")
        return ParsedEvent(
            event_type="STATUS_CHANGE",
            order_id=_to_int(payload.get("order_id")),
            status=str(status).upper() if status else None,
            courier_id=_to_int(payload.get("courier_id")),
            format_name="status_update",
            timestamp=_to_timestamp(payload.get("created_at") or payload.get("timestamp")),
        )
    return None


def _parse_nested_order(payload: dict[str, Any]) -> ParsedEvent | None:
    order = payload.get("order")
    if isinstance(order, dict) and "order_id" in order:
        status = order.get("order_status") or order.get("status")
        return ParsedEvent(
            event_type="STATUS_CHANGE",
            order_id=_to_int(order.get("order_id")),
            status=str(status).upper() if status else None,
            courier_id=_to_int(order.get("courier_id")),
            format_name="nested_order",
            timestamp=_to_timestamp(order.get("created_at") or payload.get("timestamp")),
        )
    return None


def _parse_courier_availability(payload: dict[str, Any]) -> ParsedEvent | None:
    if "courier_id" in payload and {"is_available", "available"}.intersection(payload.keys()):
        availability = _to_bool(payload.get("is_available"))
        if availability is None:
            availability = _to_bool(payload.get("available"))

        return ParsedEvent(
            event_type="COURIER_AVAILABILITY",
            courier_id=_to_int(payload.get("courier_id")),
            courier_available=availability,
            format_name="courier_availability",
            timestamp=_to_timestamp(payload.get("created_at") or payload.get("timestamp")),
        )
    return None


def _parse_courier_location(payload: dict[str, Any]) -> ParsedEvent | None:
    has_coords = {"latitude", "lat"}.intersection(payload.keys()) and {"longitude", "lon"}.intersection(payload.keys())
    if "courier_id" in payload and has_coords:
        order_id = _to_int(payload.get("order_id"))
        return ParsedEvent(
            event_type="COURIER_LOCATION",
            courier_id=_to_int(payload.get("courier_id")),
            order_id=order_id,
            courier_available=False if order_id is not None else None,
            format_name="courier_location",
            timestamp=_to_timestamp(payload.get("created_at") or payload.get("timestamp")),
        )
    return None


def parse_event_bytes(raw_data: bytes) -> list[ParsedEvent]:
    payload = _extract_payload(raw_data)
    if payload is None:
        return []

    if isinstance(payload, list):
        events: list[ParsedEvent] = []
        for item in payload:
            if isinstance(item, dict):
                events.extend(parse_event_dict(item))
        return events

    if isinstance(payload, dict):
        if isinstance(payload.get("records"), list):
            events: list[ParsedEvent] = []
            for item in payload["records"]:
                if isinstance(item, dict):
                    events.extend(parse_event_dict(item))
            return events
        return parse_event_dict(payload)

    return []


def parse_event_dict(payload: dict[str, Any]) -> list[ParsedEvent]:
    for parser in (
        _parse_flat_order_event,
        _parse_status_update,
        _parse_nested_order,
        _parse_courier_availability,
        _parse_courier_location,
    ):
        parsed = parser(payload)
        if parsed is not None:
            return [parsed]

    return [
        ParsedEvent(
            event_type="UNKNOWN",
            format_name=f"unknown:{','.join(sorted(payload.keys()))}",
            timestamp=_to_timestamp(payload.get("timestamp") or payload.get("created_at")),
        )
    ]
