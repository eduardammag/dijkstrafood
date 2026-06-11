from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from typing import Any


class RedisSnapshotStore:
    def __init__(self) -> None:
        self.url = os.getenv("REDIS_URL", "").strip()
        self.key_prefix = os.getenv("REDIS_KEY_PREFIX", "dijkfood:realtime").strip(":")
        self.ttl_seconds = int(os.getenv("REDIS_SNAPSHOT_TTL_SECONDS", "120"))
        self.channel_prefix = os.getenv("REDIS_CHANNEL_PREFIX", self.key_prefix).strip(":")
        self._client = None
        self._last_error: str | None = None

        if not self.url:
            return

        self._connect()

    @property
    def enabled(self) -> bool:
        return bool(self.url)

    @property
    def available(self) -> bool:
        return self._client is not None

    def _connect(self) -> None:
        if not self.url:
            return

        try:
            import redis

            self._client = redis.Redis.from_url(
                self.url,
                socket_connect_timeout=1,
                socket_timeout=1,
                decode_responses=True,
            )
            self._client.ping()
        except Exception as exc:
            self._last_error = str(exc)
            self._client = None

    def status(self) -> dict[str, Any]:
        if self.enabled and self._client is None:
            self._connect()

        return {
            "enabled": self.enabled,
            "available": self.available,
            "url_configured": bool(self.url),
            "key_prefix": self.key_prefix,
            "channel_prefix": self.channel_prefix,
            "ttl_seconds": self.ttl_seconds,
            "last_error": self._last_error,
        }

    def write_snapshot(self, name: str, payload: dict[str, Any]) -> None:
        if self._client is None:
            self._connect()
        if self._client is None:
            return

        key = self._key(name)
        envelope = {
            "name": name,
            "source": "redis",
            "stored_at": time.time(),
            "payload": payload,
        }

        try:
            self._client.setex(key, self.ttl_seconds, json.dumps(envelope, default=str))
        except Exception as exc:
            self._last_error = str(exc)
            self._client = None

    def publish_snapshot(self, name: str, payload: dict[str, Any]) -> None:
        if self._client is None:
            self._connect()
        if self._client is None:
            return

        message = {
            "name": name,
            "source": "redis-pubsub",
            "published_at": time.time(),
            "payload": payload,
        }

        try:
            self._client.publish(self._channel(name), json.dumps(message, default=str))
        except Exception as exc:
            self._last_error = str(exc)
            self._client = None

    def read_snapshot(self, name: str) -> dict[str, Any] | None:
        if self._client is None:
            self._connect()
        if self._client is None:
            return None

        try:
            raw = self._client.get(self._key(name))
            if raw is None:
                return None
            envelope = json.loads(raw)
            payload = envelope.get("payload")
            return payload if isinstance(payload, dict) else None
        except Exception as exc:
            self._last_error = str(exc)
            self._client = None
            return None

    def _key(self, name: str) -> str:
        return f"{self.key_prefix}:{name}"

    def _channel(self, name: str) -> str:
        return f"{self.channel_prefix}:{name}:pubsub"


class RedisSnapshotSubscriber:
    def __init__(self) -> None:
        self.store = RedisSnapshotStore()
        self._stopped = False

    def stop(self) -> None:
        self._stopped = True

    def listen(self, name: str, on_payload: Callable[[dict[str, Any]], None]) -> None:
        if not self.store.enabled:
            return

        while not self._stopped:
            if self.store._client is None:
                self.store._connect()
            if self.store._client is None:
                time.sleep(2.0)
                continue

            pubsub = None
            try:
                pubsub = self.store._client.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self.store._channel(name))
                for message in pubsub.listen():
                    if self._stopped:
                        break
                    if message.get("type") != "message":
                        continue
                    envelope = json.loads(message.get("data") or "{}")
                    payload = envelope.get("payload")
                    if isinstance(payload, dict):
                        on_payload(payload)
            except Exception as exc:
                self.store._last_error = str(exc)
                self.store._client = None
                time.sleep(2.0)
            finally:
                if pubsub is not None:
                    try:
                        pubsub.close()
                    except Exception:
                        pass
