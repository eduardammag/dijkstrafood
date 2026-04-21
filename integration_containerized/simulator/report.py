import os
import json
from datetime import datetime
from load_runner import LoadTestResult
from metrics import MetricsCollector


def export_json_report(result: LoadTestResult, metrics: MetricsCollector, output_dir: str):
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