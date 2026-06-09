import os
import json
import math
from datetime import datetime
from load_runner import LoadTestResult
from metrics import MetricsCollector
from config import SimulatorConfig

SLA_THRESHOLD_MS = 500.0
SLA_ENDPOINTS = (
    "POST /users",
    "POST /couriers",
    "POST /restaurants",
    "POST /orders",
    "GET /orders/{id}",
)
CAPACITY_HEADROOM_FACTOR = 1.30
DEFAULT_ROUTING_SECONDS = 0.2


def _success_rate(success: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return success / total


def _build_capacity_recommendation(
    result: LoadTestResult,
    metrics: MetricsCollector,
    config: SimulatorConfig,
) -> dict:
    target_orders_per_second = float(result.configured_orders_per_second)
    start_orders_per_second = max(
        1,
        math.floor(max(0.0, result.accepted_throughput) * 0.85),
    )

    order_metrics = metrics.metrics.get("POST /orders")
    status_metrics = metrics.metrics.get("GET /orders/{id}")

    order_total = order_metrics.total_requests() if order_metrics is not None else 0
    order_success_rate = _success_rate(
        order_metrics.success_count if order_metrics is not None else 0,
        order_total,
    )
    status_total = status_metrics.total_requests() if status_metrics is not None else 0
    status_success_rate = _success_rate(
        status_metrics.success_count if status_metrics is not None else 0,
        status_total,
    )
    delivery_success_rate = _success_rate(result.delivered_orders, result.accepted_orders)

    # The simulator already defines a nominal order lifecycle. We reuse it as the
    # planning baseline and add a fixed headroom factor for the initial capacity.
    restaurant_stage_seconds = (
        config.delivery_flow.preparing_delay_seconds
        + config.delivery_flow.ready_for_pickup_delay_seconds
    )
    courier_stage_seconds = (
        config.delivery_flow.picked_up_delay_seconds
        + config.delivery_flow.in_transit_delay_seconds
        + config.delivery_flow.delivered_delay_seconds
    )
    baseline_routing_seconds = DEFAULT_ROUTING_SECONDS
    if order_metrics is not None and order_metrics.total_requests() > 0:
        baseline_routing_seconds = max(
            DEFAULT_ROUTING_SECONDS,
            order_metrics.min_latency() / 1000.0,
        )

    routing_parallelism = math.ceil(
        target_orders_per_second * baseline_routing_seconds * CAPACITY_HEADROOM_FACTOR
    )
    restaurant_parallelism = math.ceil(
        target_orders_per_second * restaurant_stage_seconds * CAPACITY_HEADROOM_FACTOR
    )
    courier_capacity = math.ceil(
        target_orders_per_second * courier_stage_seconds * CAPACITY_HEADROOM_FACTOR
    )

    bottlenecks: list[str] = []
    if order_success_rate < 0.95:
        bottlenecks.append(
            "ingress de pedidos saturado: POST /orders com alta taxa de erro"
        )
    if order_metrics is not None and order_metrics.p95_latency() >= SLA_THRESHOLD_MS:
        bottlenecks.append(
            "fila de criacao de pedidos alta: POST /orders acima do SLA de latencia"
        )
    if status_success_rate < 0.95:
        bottlenecks.append(
            "consulta de status saturada: GET /orders/{id} com muitos erros"
        )
    if delivery_success_rate < 0.95:
        bottlenecks.append(
            "pipeline de entrega travado: pedidos aceitos nao estao chegando a DELIVERED"
        )

    return {
        "target_orders_per_second": result.configured_orders_per_second,
        "recommended_start_orders_per_second": start_orders_per_second,
        "headroom_factor": CAPACITY_HEADROOM_FACTOR,
        "observed": {
            "accepted_success_rate": _success_rate(result.accepted_orders, result.attempted_orders),
            "delivery_success_rate": delivery_success_rate,
            "post_orders_success_rate": order_success_rate,
            "get_order_status_success_rate": status_success_rate,
        },
        "assumptions": {
            "routing_seconds_per_order": baseline_routing_seconds,
            "restaurant_seconds_per_order": restaurant_stage_seconds,
            "courier_seconds_per_order": courier_stage_seconds,
        },
        "required_capacity_for_target_load": {
            "routing_parallel_requests": routing_parallelism,
            "restaurant_parallel_orders": restaurant_parallelism,
            "couriers": courier_capacity,
        },
        "current_population": {
            "restaurants": config.population.restaurants,
            "couriers": config.population.couriers,
        },
        "gaps_for_target_load": {
            "restaurants": max(0, restaurant_parallelism - config.population.restaurants),
            "couriers": max(0, courier_capacity - config.population.couriers),
        },
        "observed_bottlenecks": bottlenecks,
    }


def export_json_report(
    result: LoadTestResult,
    metrics: MetricsCollector,
    output_dir: str,
    config: SimulatorConfig,
):
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{output_dir}/report_{timestamp}.json"

    report_data = {
        "summary": {
            "scenario": result.scenario_name,
            "orders_per_second": result.configured_orders_per_second,
            "duration_seconds": result.duration_seconds,
            "expected_orders": result.expected_orders,
            "attempted_orders": result.attempted_orders,
            "accepted_orders": result.accepted_orders,
            "delivered_orders": result.delivered_orders,
            "failed_orders": result.failed_orders,
            "accepted_throughput": result.accepted_throughput,
            "delivered_throughput": result.delivered_throughput,
            "emission_elapsed_seconds": result.emission_elapsed_seconds,
            "end_to_end_elapsed_seconds": result.end_to_end_elapsed_seconds,
        },
        "endpoints": {},
        "capacity_recommendation": _build_capacity_recommendation(
            result=result,
            metrics=metrics,
            config=config,
        ),
    }

    for endpoint, data in metrics.metrics.items():
        report_data["endpoints"][endpoint] = {
            "total_requests": data.total_requests(),
            "success": data.success_count,
            "errors": data.error_count,
            "avg_latency": data.average_latency(),
            "p95_latency": data.p95_latency(),
            "min_latency": data.min_latency(),
            "max_latency": data.max_latency(),
        }

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=2)

    print(f"\nReport saved to: {filename}")


def print_capacity_recommendation(result: LoadTestResult, metrics: MetricsCollector, config: SimulatorConfig):
    recommendation = _build_capacity_recommendation(result, metrics, config)
    required = recommendation["required_capacity_for_target_load"]

    print("\n===== CAPACITY RECOMMENDATION =====")
    print(f"Target Orders/s: {recommendation['target_orders_per_second']}")
    print(
        "Recommended start Orders/s: "
        f"{recommendation['recommended_start_orders_per_second']}"
    )
    print(
        "Required routing parallelism: "
        f"{required['routing_parallel_requests']}"
    )
    print(
        "Required restaurant parallel orders: "
        f"{required['restaurant_parallel_orders']}"
    )
    print(f"Required couriers: {required['couriers']}")

    bottlenecks = recommendation["observed_bottlenecks"]
    if bottlenecks:
        print("Observed bottlenecks:")
        for bottleneck in bottlenecks:
            print(f"  - {bottleneck}")
    print("========================================")


def print_load_test_summary(result: LoadTestResult):
    print("\n===== LOAD TEST SUMMARY =====")
    print(f"Scenario: {result.scenario_name}")
    print(f"Configured Orders/s: {result.configured_orders_per_second}")
    print(f"Duration (s): {result.duration_seconds}")
    print(f"Expected Orders: {result.expected_orders}")
    print(f"Attempted Orders: {result.attempted_orders}")
    print(f"Accepted Orders: {result.accepted_orders}")
    print(f"Delivered Orders: {result.delivered_orders}")
    print(f"Failed Orders: {result.failed_orders}")
    print(f"Accepted Throughput: {result.accepted_throughput:.2f} orders/s")
    print(f"Delivered Throughput: {result.delivered_throughput:.2f} orders/s")
    print("========================================")


def print_metrics(metrics: MetricsCollector):
    print("\n===== ENDPOINT METRICS =====\n")

    for endpoint, data in metrics.metrics.items():
        print(f"Endpoint: {endpoint}")
        print(f"  Total Requests: {data.total_requests()}")
        print(f"  Success: {data.success_count}")
        print(f"  Errors: {data.error_count}")
        print(f"  Avg Latency: {data.average_latency():.2f} ms")
        print(f"  P95 Latency: {data.p95_latency():.2f} ms")
        print(f"  Min Latency: {data.min_latency():.2f} ms")
        print(f"  Max Latency: {data.max_latency():.2f} ms")
        print("----------------------------------------")


def print_sla_evaluation(metrics: MetricsCollector):
    print("\n===== SLA EVALUATION =====\n")

    overall_pass = True

    for endpoint in SLA_ENDPOINTS:
        data = metrics.metrics.get(endpoint)
        if data is None or data.total_requests() == 0:
            overall_pass = False
            print(f"{endpoint}: NO DATA")
            continue

        p95 = data.p95_latency()
        passed = p95 < SLA_THRESHOLD_MS
        overall_pass = overall_pass and passed
        status = "PASS" if passed else "FAIL"
        print(
            f"{endpoint}: {status} | "
            f"P95={p95:.2f} ms | "
            f"Success={data.success_count} | "
            f"Errors={data.error_count}"
        )

    print("----------------------------------------")
    print(
        "Overall SLA: "
        f"{'PASS' if overall_pass else 'FAIL'} "
        f"(target P95 < {SLA_THRESHOLD_MS:.0f} ms)"
    )
