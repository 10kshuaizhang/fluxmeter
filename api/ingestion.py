"""HTTP-to-Kafka custody boundary for usage events."""

from __future__ import annotations

import os
import threading
import time
import hashlib
import json
import uuid


class KafkaUnavailableError(RuntimeError):
    """Kafka did not acknowledge custody before the configured deadline."""


EVENT_ID_TTL_SECONDS = int(os.getenv("EVENT_ID_TTL_SECONDS", str(30 * 24 * 60 * 60)))
EVENT_ID_PENDING_TTL_SECONDS = int(os.getenv("EVENT_ID_PENDING_TTL_SECONDS", "60"))


def canonical_payload_hash(payload: dict) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def event_identity_status(redis_client, event_id: str, payload_hash: str) -> str:
    """Atomically claim an ID and return owner, pending, same, or conflict."""
    result = redis_client.eval(
        """
        local current = redis.call('GET', KEYS[1])
        if not current then
          redis.call('SET', KEYS[1], 'pending:' .. ARGV[1], 'EX', ARGV[2])
          return 'owner'
        end
        if current == 'accepted:' .. ARGV[1] then return 'same' end
        if current == 'pending:' .. ARGV[1] then return 'pending' end
        return 'conflict'
        """,
        1,
        f"ingest:event:{event_id}",
        payload_hash,
        str(EVENT_ID_PENDING_TTL_SECONDS),
    )
    return str(result)


def remember_event_identity(redis_client, event_id: str, payload_hash: str) -> None:
    redis_client.eval(
        """
        if redis.call('GET', KEYS[1]) == 'pending:' .. ARGV[1] then
          redis.call('SET', KEYS[1], 'accepted:' .. ARGV[1], 'EX', ARGV[2])
          return 1
        end
        return 0
        """,
        1,
        f"ingest:event:{event_id}",
        payload_hash,
        str(EVENT_ID_TTL_SECONDS),
    )


def release_event_identity(redis_client, event_id: str, payload_hash: str) -> None:
    redis_client.eval(
        """
        if redis.call('GET', KEYS[1]) == 'pending:' .. ARGV[1] then
          return redis.call('DEL', KEYS[1])
        end
        return 0
        """,
        1,
        f"ingest:event:{event_id}",
        payload_hash,
    )


def trusted_envelope(
    payload: dict,
    *,
    tenant_id: str | None,
    api_key_id: str | None,
    received_at: int,
    source: str = "http",
    trace_id: str | None = None,
    reservation_id: str | None = None,
    reserved_usd: float = 0.0,
) -> dict:
    envelope = {
        "envelopeVersion": 1,
        "source": source,
        "payload": payload,
        "auth": {
            "tenantId": tenant_id,
            "customerId": payload["customerId"],
            "apiKeyId": api_key_id,
        },
        "receipt": {
            "receivedAt": received_at,
            "traceId": trace_id or str(uuid.uuid4()),
        },
    }
    if reservation_id:
        envelope["reservation"] = {
            "reservationId": reservation_id,
            "reservedUsd": max(0.0, reserved_usd),
        }
    return envelope


def publish_with_ack(
    producer,
    *,
    topic: str,
    key: bytes,
    value: bytes,
    timeout_seconds: float,
) -> None:
    """Publish one record and return only after its delivery callback succeeds."""
    delivered = threading.Event()
    delivery_error: list[object] = []

    def on_delivery(error, _message) -> None:
        if error is not None:
            delivery_error.append(error)
        delivered.set()

    try:
        producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
    except Exception as exc:
        raise KafkaUnavailableError(str(exc)) from exc

    deadline = time.monotonic() + timeout_seconds
    while not delivered.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise KafkaUnavailableError("Kafka acknowledgement timed out")
        producer.poll(min(remaining, 0.05))

    if delivery_error:
        raise KafkaUnavailableError(str(delivery_error[0]))
