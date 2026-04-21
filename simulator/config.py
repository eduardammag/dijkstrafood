from dataclasses import dataclass
from typing import Literal


ScenarioName = Literal["normal", "peak", "special"]


@dataclass(frozen=True)
class ScenarioConfig:
    name: ScenarioName
    orders_per_second: int
    duration_seconds: int


@dataclass(frozen=True)
class ApiConfig:
    base_url: str
    timeout_seconds: float


@dataclass(frozen=True)
class PopulationConfig:
    admins: int
    clients: int
    restaurants: int
    couriers: int


@dataclass(frozen=True)
class DeliveryFlowConfig:
    preparing_delay_seconds: float
    ready_for_pickup_delay_seconds: float
    picked_up_delay_seconds: float
    in_transit_delay_seconds: float
    delivered_delay_seconds: float


@dataclass(frozen=True)
class LocationSimulationConfig:
    enabled: bool
    update_interval_seconds: float
    points_per_delivery: int


@dataclass(frozen=True)
class MetricsConfig:
    export_json: bool
    output_dir: str


@dataclass(frozen=True)
class SimulatorConfig:
    api: ApiConfig
    population: PopulationConfig
    delivery_flow: DeliveryFlowConfig
    location: LocationSimulationConfig
    metrics: MetricsConfig
    scenario: ScenarioConfig


NORMAL_SCENARIO = ScenarioConfig(
    name="normal",
    orders_per_second=10,
    duration_seconds=60,
)

PEAK_SCENARIO = ScenarioConfig(
    name="peak",
    orders_per_second=50,
    duration_seconds=60,
)

SPECIAL_SCENARIO = ScenarioConfig(
    name="special",
    orders_per_second=200,
    duration_seconds=60,
)


SCENARIOS: dict[ScenarioName, ScenarioConfig] = {
    "normal": NORMAL_SCENARIO,
    "peak": PEAK_SCENARIO,
    "special": SPECIAL_SCENARIO,
}


DEFAULT_CONFIG = SimulatorConfig(
    api=ApiConfig(
        base_url="http://dijkfood-demo-alb-1052284763.us-east-1.elb.amazonaws.com",
        timeout_seconds=10.0,
    ),
    population=PopulationConfig(
        admins=1,
        clients=300,
        restaurants=50,
        couriers=900,  # 3x mais entregadores que clientes
    ),
    delivery_flow=DeliveryFlowConfig(
        preparing_delay_seconds=0.5,
        ready_for_pickup_delay_seconds=0.5,
        picked_up_delay_seconds=0.5,
        in_transit_delay_seconds=1.0,
        delivered_delay_seconds=0.5,
    ),
    location=LocationSimulationConfig(
        enabled=True,
        update_interval_seconds=0.1,  # 100ms
        points_per_delivery=20,
    ),
    metrics=MetricsConfig(
        export_json=True,
        output_dir="simulator_output",
    ),
    scenario=NORMAL_SCENARIO,
)


def build_config(scenario_name: ScenarioName) -> SimulatorConfig:
    scenario = SCENARIOS[scenario_name]
    return SimulatorConfig(
        api=DEFAULT_CONFIG.api,
        population=DEFAULT_CONFIG.population,
        delivery_flow=DEFAULT_CONFIG.delivery_flow,
        location=DEFAULT_CONFIG.location,
        metrics=DEFAULT_CONFIG.metrics,
        scenario=scenario,
    )