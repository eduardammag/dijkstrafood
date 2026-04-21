from typing import Optional

ORDER_STATUS_FLOW = [
    "CONFIRMED",
    "PREPARING",
    "READY_FOR_PICKUP",
    "PICKED_UP",
    "IN_TRANSIT",
    "DELIVERED",
]

VALID_TRANSITIONS = {
    "CONFIRMED": {"PREPARING"},
    "PREPARING": {"READY_FOR_PICKUP"},
    "READY_FOR_PICKUP": {"PICKED_UP"},
    "PICKED_UP": {"IN_TRANSIT"},
    "IN_TRANSIT": {"DELIVERED"},
    "DELIVERED": set(),
}


def normalize_status(status: str) -> str:
    if status is None:
        raise ValueError("status cannot be None")
    return status.strip().upper()


def is_valid_status(status: str) -> bool:
    return normalize_status(status) in VALID_TRANSITIONS


def validate_transition(current_status: str, new_status: str) -> bool:
    current = normalize_status(current_status)
    new = normalize_status(new_status)
    return new in VALID_TRANSITIONS.get(current, set())


def next_status(current_status: str) -> Optional[str]:
    current = normalize_status(current_status)
    allowed = list(VALID_TRANSITIONS.get(current, set()))
    if not allowed:
        return None
    return allowed[0]