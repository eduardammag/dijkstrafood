from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

import boto3


class AthenaAnalyticsClient:
    def __init__(self) -> None:
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self.database = os.getenv("ATHENA_DATABASE", "dijkfood_demo_analytics")
        self.table = os.getenv("ATHENA_TABLE", "order_events")
        self.output_location = os.getenv("ATHENA_OUTPUT_LOCATION", "").strip()
        self.workgroup = os.getenv("ATHENA_WORKGROUP", "primary")
        self.timeout_seconds = float(os.getenv("ATHENA_QUERY_TIMEOUT_SECONDS", "25"))
        self.cache_ttl_seconds = float(os.getenv("ATHENA_CACHE_TTL_SECONDS", "30"))
        self.enabled = os.getenv("ATHENA_ANALYTICS_ENABLED", "true").lower() == "true"

        self._client = boto3.client("athena", region_name=self.region)
        self._cache: dict[str, Any] | None = None
        self._cache_at = 0.0

    def snapshot(self) -> dict[str, Any]:
        now = time.time()
        if self._cache and now - self._cache_at < self.cache_ttl_seconds:
            return self._cache

        if not self.enabled:
            return self._empty("Athena analytics disabled")
        if not self.output_location:
            return self._empty("ATHENA_OUTPUT_LOCATION is not configured")

        try:
            snapshot = self._query_snapshot()
        except Exception as exc:
            snapshot = self._empty(str(exc))

        self._cache = snapshot
        self._cache_at = now
        return snapshot

    def _query_snapshot(self) -> dict[str, Any]:
        today = datetime.now(timezone.utc)
        year = f"{today.year:04d}"
        month = f"{today.month:02d}"
        day = f"{today.day:02d}"

        where = f"year = '{year}' AND month = '{month}' AND day = '{day}'"
        base = f'"{self.database}"."{self.table}"'

        total_events_rows = self._run_query(
            f"""
            SELECT count(*) AS total_events
            FROM {base}
            WHERE {where}
            """
        )

        latest_orders_cte = f"""
            WITH latest_orders AS (
              SELECT order_id, status
              FROM (
                SELECT
                  order_id,
                  coalesce(event_status, to_status, from_status, 'UNKNOWN') AS status,
                  row_number() OVER (
                    PARTITION BY order_id
                    ORDER BY
                      coalesce(
                        try(from_iso8601_timestamp(created_at)),
                        from_iso8601_timestamp('1970-01-01T00:00:00+00:00')
                      ) DESC,
                      coalesce(try_cast(event_id AS bigint), 0) DESC
                  ) AS rn
                FROM {base}
                WHERE {where}
                  AND order_id IS NOT NULL
              )
              WHERE rn = 1
            )
        """

        order_summary_rows = self._run_query(
            latest_orders_cte
            + """
            SELECT
              count(*) AS total_orders,
              sum(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) AS orders_delivered,
              sum(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS orders_cancelled,
              sum(CASE WHEN status NOT IN ('DELIVERED', 'CANCELLED') THEN 1 ELSE 0 END) AS orders_open
            FROM latest_orders
            """
        )

        current_status_rows = self._run_query(
            latest_orders_cte
            + """
            SELECT status, count(*) AS total
            FROM latest_orders
            GROUP BY status
            ORDER BY total DESC
            """
        )

        type_rows = self._run_query(
            f"""
            SELECT event_type, count(*) AS total
            FROM {base}
            WHERE {where}
            GROUP BY event_type
            ORDER BY total DESC
            """
        )

        hourly_rows = self._run_query(
            f"""
            SELECT hour, count(*) AS total_events, count(DISTINCT order_id) AS total_orders
            FROM {base}
            WHERE {where}
            GROUP BY hour
            ORDER BY hour
            """
        )

        total_events = _to_int((total_events_rows[0] if total_events_rows else {}).get("total_events"))
        order_summary = order_summary_rows[0] if order_summary_rows else {}
        total_orders = _to_int(order_summary.get("total_orders"))
        delivered = _to_int(order_summary.get("orders_delivered"))
        cancelled = _to_int(order_summary.get("orders_cancelled"))
        open_orders = _to_int(order_summary.get("orders_open"))
        current_status = [
            {"label": row.get("status") or "UNKNOWN", "value": _to_int(row.get("total"))}
            for row in current_status_rows
        ]

        return {
            "source": "athena",
            "database": self.database,
            "table": self.table,
            "partition": {"year": year, "month": month, "day": day},
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "kpis": {
                "total_events": total_events,
                "total_orders": total_orders,
                "orders_open": open_orders,
                "orders_delivered": delivered,
                "orders_cancelled": cancelled,
                "delivery_rate_pct": round(delivered / total_orders * 100, 1) if total_orders else 0.0,
                "cancel_rate_pct": round(cancelled / total_orders * 100, 1) if total_orders else 0.0,
            },
            "current_status": current_status,
            "by_status": current_status,
            "by_type": [
                {"label": row.get("event_type") or "UNKNOWN", "value": _to_int(row.get("total"))}
                for row in type_rows
            ],
            "events_by_hour": [
                {
                    "hour": row.get("hour"),
                    "total_events": _to_int(row.get("total_events")),
                    "total_orders": _to_int(row.get("total_orders")),
                }
                for row in hourly_rows
            ],
            "error": None,
        }

    def _run_query(self, query: str) -> list[dict[str, str | None]]:
        params: dict[str, Any] = {
            "QueryString": query,
            "QueryExecutionContext": {"Database": self.database},
            "ResultConfiguration": {"OutputLocation": self.output_location},
            "WorkGroup": self.workgroup,
        }
        execution_id = self._client.start_query_execution(**params)["QueryExecutionId"]
        deadline = time.time() + self.timeout_seconds

        while time.time() < deadline:
            execution = self._client.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]
            state = execution["Status"]["State"]
            if state == "SUCCEEDED":
                return self._read_results(execution_id)
            if state in {"FAILED", "CANCELLED"}:
                reason = execution["Status"].get("StateChangeReason", state)
                raise RuntimeError(reason)
            time.sleep(0.7)

        raise TimeoutError(f"Athena query timed out: {execution_id}")

    def _read_results(self, execution_id: str) -> list[dict[str, str | None]]:
        paginator = self._client.get_paginator("get_query_results")
        rows: list[list[str | None]] = []

        for page in paginator.paginate(QueryExecutionId=execution_id):
            for row in page["ResultSet"].get("Rows", []):
                rows.append([cell.get("VarCharValue") for cell in row.get("Data", [])])

        if not rows:
            return []

        headers = [header or "" for header in rows[0]]
        return [dict(zip(headers, row)) for row in rows[1:]]

    def _empty(self, error: str | None = None) -> dict[str, Any]:
        return {
            "source": "athena",
            "database": self.database,
            "table": self.table,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "kpis": {
                "total_events": 0,
                "total_orders": 0,
                "orders_open": 0,
                "orders_delivered": 0,
                "orders_cancelled": 0,
                "delivery_rate_pct": 0.0,
                "cancel_rate_pct": 0.0,
            },
            "current_status": [],
            "by_status": [],
            "by_type": [],
            "events_by_hour": [],
            "error": error,
        }


def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
