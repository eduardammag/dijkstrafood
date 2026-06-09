import os
import threading
import time
import json
import urllib.request

import boto3
from botocore.exceptions import ClientError

from event_parser import parse_event_bytes
from metrics_state import MetricsState


class KinesisConsumer(threading.Thread):
    def __init__(self, state: MetricsState):
        super().__init__(daemon=True)
        self.state = state
        self._stop_event = threading.Event()

        self.region = os.getenv("AWS_REGION", "us-east-1")
        self.stream_name = os.getenv("KINESIS_STREAM_NAME", "")
        self.endpoint_url = os.getenv("KINESIS_ENDPOINT_URL", "").strip() or None
        self.iterator_type = os.getenv("KINESIS_ITERATOR_TYPE", "LATEST")
        self.poll_interval = float(os.getenv("KINESIS_POLL_INTERVAL_SECONDS", "1.0"))
        self.records_limit = int(os.getenv("KINESIS_RECORDS_LIMIT", "500"))

        self._client = None
        self._shard_iterators: dict[str, str] = {}

    def stop(self):
        self._stop_event.set()

    def run(self):
        if not self.stream_name:
            print("[realtime-metrics] KINESIS_STREAM_NAME is empty; consumer disabled")
            return

        client_kwargs = {"service_name": "kinesis", "region_name": self.region}
        if self.endpoint_url:
            client_kwargs["endpoint_url"] = self.endpoint_url
        self._client = boto3.client(**client_kwargs)
        print(f"[realtime-metrics] consuming stream={self.stream_name} region={self.region}")

        while not self._stop_event.is_set():
            try:
                self._ensure_shard_iterators()
                any_record = self._poll_once()

                if not any_record:
                    time.sleep(self.poll_interval)

            except ClientError as exc:
                print(f"[realtime-metrics] kinesis client error: {exc}")
                time.sleep(2.0)
            except Exception as exc:
                print(f"[realtime-metrics] unexpected consumer error: {exc}")
                time.sleep(2.0)

    def _ensure_shard_iterators(self):
        assert self._client is not None

        response = self._client.list_shards(StreamName=self.stream_name)
        shards = response.get("Shards", [])

        current_shards = {shard["ShardId"] for shard in shards}
        stale_shards = set(self._shard_iterators.keys()) - current_shards
        for shard_id in stale_shards:
            del self._shard_iterators[shard_id]

        for shard in shards:
            shard_id = shard["ShardId"]
            if shard_id in self._shard_iterators:
                continue

            iterator_response = self._client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=shard_id,
                ShardIteratorType=self.iterator_type,
            )
            self._shard_iterators[shard_id] = iterator_response["ShardIterator"]

    def _poll_once(self) -> bool:
        assert self._client is not None

        any_record = False
        for shard_id, iterator in list(self._shard_iterators.items()):
            if not iterator:
                continue

            try:
                response = self._client.get_records(
                    ShardIterator=iterator,
                    Limit=self.records_limit,
                )
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code in {"ExpiredIteratorException", "ResourceNotFoundException"}:
                    self._reset_iterator(shard_id)
                    continue
                raise

            self._shard_iterators[shard_id] = response.get("NextShardIterator")
            records = response.get("Records", [])

            if records:
                any_record = True

            for record in records:
                raw_data = record.get("Data")
                if raw_data is None:
                    continue

                parsed_events = parse_event_bytes(raw_data)
                for event in parsed_events:
                    self.state.apply(event)

        return any_record

    def _reset_iterator(self, shard_id: str):
        assert self._client is not None

        try:
            iterator_response = self._client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=shard_id,
                ShardIteratorType=self.iterator_type,
            )
            self._shard_iterators[shard_id] = iterator_response["ShardIterator"]
        except Exception as exc:
            print(f"[realtime-metrics] failed to reset iterator for {shard_id}: {exc}")


class ApiInventoryPoller(threading.Thread):
    def __init__(self, state: MetricsState):
        super().__init__(daemon=True)
        self.state = state
        self._stop_event = threading.Event()
        self.api_url = os.getenv("API_URL", "").rstrip("/")
        self.poll_interval = float(os.getenv("API_INVENTORY_POLL_INTERVAL_SECONDS", "5.0"))
        self.request_timeout = float(os.getenv("API_INVENTORY_REQUEST_TIMEOUT_SECONDS", "5.0"))

    def stop(self):
        self._stop_event.set()

    def run(self):
        if not self.api_url:
            print("[realtime-metrics] API_URL is empty; inventory poller disabled")
            return

        print(f"[realtime-metrics] polling courier inventory from {self.api_url}")

        while not self._stop_event.is_set():
            try:
                total_registered = self._fetch_count("/couriers", "couriers")
                available = self._fetch_count("/couriers/available", "couriers")
                self.state.update_courier_inventory(
                    total_registered=total_registered,
                    available=available,
                )
            except Exception as exc:
                print(f"[realtime-metrics] inventory poll failed: {exc}")

            time.sleep(self.poll_interval)

    def _fetch_count(self, path: str, key: str) -> int:
        url = f"{self.api_url}{path}"
        with urllib.request.urlopen(url, timeout=self.request_timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        records = payload.get(key, [])
        if not isinstance(records, list):
            return 0
        return len(records)
