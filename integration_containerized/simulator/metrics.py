from dataclasses import dataclass, field
from typing import Dict, List
import statistics


@dataclass
class EndpointMetrics:
    latencies: List[float] = field(default_factory=list)
    success_count: int = 0
    error_count: int = 0

    def add(self, latency_ms: float, success: bool):
        self.latencies.append(latency_ms)
        if success:
            self.success_count += 1
        else:
            self.error_count += 1

    def total_requests(self) -> int:
        return self.success_count + self.error_count

    def average_latency(self) -> float:
        if not self.latencies:
            return 0.0
        return sum(self.latencies) / len(self.latencies)

    def p95_latency(self) -> float:
        if not self.latencies:
            return 0.0
        return statistics.quantiles(self.latencies, n=100)[94]

    def max_latency(self) -> float:
        return max(self.latencies) if self.latencies else 0.0

    def min_latency(self) -> float:
        return min(self.latencies) if self.latencies else 0.0


class MetricsCollector:
    def __init__(self):
        self.metrics: Dict[str, EndpointMetrics] = {}

    def record(self, endpoint: str, result):
        if endpoint not in self.metrics:
            self.metrics[endpoint] = EndpointMetrics()

        self.metrics[endpoint].add(
            latency_ms=result.latency_ms,
            success=result.success,
        )

    def report(self):
        print("\n===== METRICS REPORT =====\n")

        for endpoint, data in self.metrics.items():
            print(f"Endpoint: {endpoint}")
            print(f"  Total Requests: {data.total_requests()}")
            print(f"  Success: {data.success_count}")
            print(f"  Errors: {data.error_count}")
            print(f"  Avg Latency: {data.average_latency():.2f} ms")
            print(f"  P95 Latency: {data.p95_latency():.2f} ms")
            print(f"  Min Latency: {data.min_latency():.2f} ms")
            print(f"  Max Latency: {data.max_latency():.2f} ms")
            print("-" * 40)