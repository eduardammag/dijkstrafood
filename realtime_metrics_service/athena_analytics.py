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

        state_timing_rows = self._run_query(
            f"""
            WITH status_events AS (
              SELECT
                order_id,
                coalesce(to_status, event_status, from_status) AS status,
                try(from_iso8601_timestamp(created_at)) AS created_ts,
                lead(try(from_iso8601_timestamp(created_at))) OVER (
                  PARTITION BY order_id
                  ORDER BY
                    try(from_iso8601_timestamp(created_at)),
                    coalesce(try_cast(event_id AS bigint), 0)
                ) AS next_ts
              FROM {base}
              WHERE {where}
                AND order_id IS NOT NULL
                AND coalesce(to_status, event_status, from_status) IS NOT NULL
                AND try(from_iso8601_timestamp(created_at)) IS NOT NULL
            )
            SELECT
              status,
              count(*) AS transitions,
              round(avg(date_diff('second', created_ts, next_ts)), 1) AS avg_seconds
            FROM status_events
            WHERE next_ts IS NOT NULL
            GROUP BY status
            ORDER BY avg_seconds DESC
            """
        )

        heatmap_rows = self._run_query(
            f"""
            SELECT
              day_of_week(try(from_iso8601_timestamp(created_at))) AS dow_num,
              CASE day_of_week(try(from_iso8601_timestamp(created_at)))
                WHEN 1 THEN 'Seg'
                WHEN 2 THEN 'Ter'
                WHEN 3 THEN 'Qua'
                WHEN 4 THEN 'Qui'
                WHEN 5 THEN 'Sex'
                WHEN 6 THEN 'Sab'
                WHEN 7 THEN 'Dom'
                ELSE 'N/A'
              END AS dow_label,
              hour(try(from_iso8601_timestamp(created_at))) AS hour_utc,
              count(DISTINCT order_id) AS total_orders
            FROM {base}
            WHERE {where}
              AND order_id IS NOT NULL
              AND try(from_iso8601_timestamp(created_at)) IS NOT NULL
            GROUP BY 1, 2, 3
            ORDER BY 1, 3
            """
        )

        delivery_histogram_rows = self._run_query(
            f"""
            WITH order_times AS (
              SELECT
                order_id,
                min(try(from_iso8601_timestamp(created_at))) AS created_ts,
                max(
                  CASE
                    WHEN coalesce(to_status, event_status, from_status) = 'DELIVERED'
                    THEN try(from_iso8601_timestamp(created_at))
                  END
                ) AS delivered_ts
              FROM {base}
              WHERE {where}
                AND order_id IS NOT NULL
                AND try(from_iso8601_timestamp(created_at)) IS NOT NULL
              GROUP BY order_id
            ),
            delivered_orders AS (
              SELECT
                order_id,
                date_diff('minute', created_ts, delivered_ts) AS delivery_minutes
              FROM order_times
              WHERE created_ts IS NOT NULL
                AND delivered_ts IS NOT NULL
                AND delivered_ts >= created_ts
            )
            SELECT
              bucket,
              sort_key,
              count(*) AS total_orders
            FROM (
              SELECT
                CASE
                  WHEN delivery_minutes <= 10 THEN '00-10 min'
                  WHEN delivery_minutes <= 20 THEN '11-20 min'
                  WHEN delivery_minutes <= 30 THEN '21-30 min'
                  WHEN delivery_minutes <= 45 THEN '31-45 min'
                  WHEN delivery_minutes <= 60 THEN '46-60 min'
                  ELSE '60+ min'
                END AS bucket,
                CASE
                  WHEN delivery_minutes <= 10 THEN 1
                  WHEN delivery_minutes <= 20 THEN 2
                  WHEN delivery_minutes <= 30 THEN 3
                  WHEN delivery_minutes <= 45 THEN 4
                  WHEN delivery_minutes <= 60 THEN 5
                  ELSE 6
                END AS sort_key
              FROM delivered_orders
            )
            GROUP BY bucket, sort_key
            ORDER BY sort_key
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
            "avg_time_by_status": [
                {
                    "label": row.get("status") or "UNKNOWN",
                    "value": _to_float(row.get("avg_seconds")),
                    "transitions": _to_int(row.get("transitions")),
                }
                for row in state_timing_rows
            ],
            "demand_heatmap": [
                {
                    "day_of_week": row.get("dow_label") or "N/A",
                    "day_of_week_num": _to_int(row.get("dow_num")),
                    "hour_utc": _to_int(row.get("hour_utc")),
                    "total_orders": _to_int(row.get("total_orders")),
                }
                for row in heatmap_rows
            ],
            "delivery_time_histogram": [
                {
                    "label": row.get("bucket") or "N/A",
                    "value": _to_int(row.get("total_orders")),
                }
                for row in delivery_histogram_rows
            ],
            "compatibility": {
                "implemented": [
                    "volume_de_pedidos_no_tempo",
                    "tempo_medio_em_cada_estado",
                    "heatmap_demanda_por_horario_e_dia_da_semana",
                    "histograma_do_tempo_total_de_entrega",
                ],
                "blocked_by_schema": [
                    "distribuicao_de_pedidos_por_regiao",
                    "top_10_restaurantes_por_volume",
                ],
            },
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
            "avg_time_by_status": [],
            "demand_heatmap": [],
            "delivery_time_histogram": [],
            "compatibility": {
                "implemented": [],
                "blocked_by_schema": [],
            },
            "error": error,
        }


def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
