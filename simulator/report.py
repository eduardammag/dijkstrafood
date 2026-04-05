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
            "completed_orders": result.completed_orders,
            "successful_orders": result.successful_orders,
            "failed_orders": result.failed_orders,
            "throughput": result.effective_throughput,
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

    with open(filename, "w") as f:
        json.dump(report_data, f, indent=2)

    print(f"\n📁 Report saved to: {filename}")