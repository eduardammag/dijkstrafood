from dataclasses import dataclass
from typing import Optional
from enum import Enum


# ENUMS
class UserType(str, Enum):
    CLIENT = "client"
    COURIER = "courier"
    ADMIN = "admin"


class OrderStatus(str, Enum):
    CONFIRMED = "CONFIRMED"
    PREPARING = "PREPARING"
    READY_FOR_PICKUP = "READY_FOR_PICKUP"
    PICKED_UP = "PICKED_UP"
    IN_TRANSIT = "IN_TRANSIT"
    DELIVERED = "DELIVERED"


# CORE ENTITIES
@dataclass
class User:
    user_id: Optional[int]
    user_name: str
    email: str
    phone: str
    latitude: float
    longitude: float
    user_type: UserType


@dataclass
class Courier:
    user_id: int
    vehicle_type: str
    is_available: bool = True


@dataclass
class Restaurant:
    restaurant_id: Optional[int]
    restaurant_name: str
    cuisine_type: str
    restaurant_latitude: float
    restaurant_longitude: float
    creator_user_id: int


@dataclass
class Order:
    order_id: Optional[int]
    client_id: int
    restaurant_id: int
    courier_id: Optional[int]
    order_status: OrderStatus


@dataclass
class OrderEvent:
    event_id: Optional[int]
    order_id: int
    event_status: OrderStatus


# LOCATION (FUTURO - DynamoDB)
@dataclass
class Location:
    courier_id: int
    latitude: float
    longitude: float
    timestamp: float


# METRICS / RESULTADOS
@dataclass
class RequestResult:
    success: bool
    latency_ms: float
    status_code: int
    response_json: Optional[dict] = None
    error: Optional[str] = None