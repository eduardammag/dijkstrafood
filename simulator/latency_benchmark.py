import argparse
import json
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

from config import build_config


def fetch_json(url: str, timeout: float = 5.0) -> dict[str, Any] | None:
    try:
        with urlopen(url, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return json.loads(body)
    except (URLError, json.JSONDecodeError, TimeoutError, OSError):
        return None


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])

    ordered = sorted(values)
    rank = (len(ordered) - 1) * p
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    weight = rank - low
    return ordered[low] * (1 - weight) + ordered[high] * weight


def sample_latencies(metrics_url: str) -> tuple[float | None, float | None]:
    payload = fetch_json(metrics_url)
    if payload is None:
        return None, None

    consumer_latency = payload.get("event_to_consumer_latency_ms_last")
    meta = payload.get("meta", {}) if isinstance(payload.get("meta"), dict) else {}
    produced_at_ms = meta.get("last_event_produced_at_ms")

    dashboard_latency = None
    if isinstance(produced_at_ms, (int, float)) and produced_at_ms > 0:
        dashboard_latency = max(0.0, (time.time() * 1000.0) - float(produced_at_ms))

    return (
        float(consumer_latency) if isinstance(consumer_latency, (int, float)) else None,
        dashboard_latency,
    )


def run_benchmark(
    scenario: str,
    sample_interval_seconds: float,
    post_capture_seconds: float,
) -> dict[str, Any]:
    config = build_config(scenario)
    metrics_url = config.api.base_url.rstrip("/") + ":8010/metrics"

    print(f"[latency] API base: {config.api.base_url}")
    print(f"[latency] Metrics URL: {metrics_url}")
    print(f"[latency] Running simulator scenario: {scenario}")

    simulator_cmd = [sys.executable, "main.py", "--scenario", scenario]
    process = subprocess.Popen(simulator_cmd, cwd=Path(__file__).resolve().parent)

    consumer_samples: list[float] = []
    dashboard_samples: list[float] = []
    started_at = time.time()

    try:
        while process.poll() is None:
            consumer_latency, dashboard_latency = sample_latencies(metrics_url)
            if consumer_latency is not None:
                consumer_samples.append(consumer_latency)
            if dashboard_latency is not None:
                dashboard_samples.append(dashboard_latency)
            time.sleep(sample_interval_seconds)

        capture_deadline = time.time() + post_capture_seconds
        while time.time() < capture_deadline:
            consumer_latency, dashboard_latency = sample_latencies(metrics_url)
            if consumer_latency is not None:
                consumer_samples.append(consumer_latency)
            if dashboard_latency is not None:
                dashboard_samples.append(dashboard_latency)
            time.sleep(sample_interval_seconds)

    finally:
        if process.poll() is None:
            process.terminate()

    finished_at = time.time()

    result = {
        "scenario": scenario,
        "started_at": datetime.fromtimestamp(started_at).isoformat(),
        "finished_at": datetime.fromtimestamp(finished_at).isoformat(),
        "sample_interval_seconds": sample_interval_seconds,
        "post_capture_seconds": post_capture_seconds,
        "samples": {
            "consumer": len(consumer_samples),
            "dashboard": len(dashboard_samples),
        },
        "consumer_latency_ms": {
            "avg": round(statistics.fmean(consumer_samples), 2) if consumer_samples else 0.0,
            "p50": round(percentile(consumer_samples, 0.50), 2),
            "p95": round(percentile(consumer_samples, 0.95), 2),
            "max": round(max(consumer_samples), 2) if consumer_samples else 0.0,
        },
        "event_to_dashboard_latency_ms": {
            "avg": round(statistics.fmean(dashboard_samples), 2) if dashboard_samples else 0.0,
            "p50": round(percentile(dashboard_samples, 0.50), 2),
            "p95": round(percentile(dashboard_samples, 0.95), 2),
            "max": round(max(dashboard_samples), 2) if dashboard_samples else 0.0,
        },
    }

    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark de latencia realtime do DijkFood")
    parser.add_argument(
        "--scenario",
        choices=["teste", "normal", "peak", "special"],
        default="teste",
        help="Cenario a executar no simulador",
    )
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=1.0,
        help="Intervalo entre leituras de /metrics (segundos)",
    )
    parser.add_argument(
        "--post-capture-seconds",
        type=float,
        default=15.0,
        help="Tempo extra de captura apos fim da simulacao (segundos)",
    )
    parser.add_argument(
        "--export-json",
        action="store_true",
        help="Exporta resultado em simulator/simulator_output",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_benchmark(
        scenario=args.scenario,
        sample_interval_seconds=args.sample_interval,
        post_capture_seconds=args.post_capture_seconds,
    )

    print("\n===== LATENCY BENCHMARK =====")
    print(json.dumps(result, indent=2))

    if args.export_json:
        output_dir = Path(__file__).resolve().parent / "simulator_output"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"latency_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        output_file.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(f"[latency] report exported: {output_file}")


if __name__ == "__main__":
    main()
