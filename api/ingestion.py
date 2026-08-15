"""Token Event Custody — deep accept / accept_many over Kafka + identity."""

from __future__ import annotations

import os
import threading
import time
import hashlib
import json
import uuid
from typing import Any, Callable, Literal


class KafkaUnavailableError(RuntimeError):
    """Kafka did not acknowledge custody before the configured deadline."""


EVENT_ID_TTL_SECONDS = int(os.getenv("EVENT_ID_TTL_SECONDS", str(30 * 24 * 60 * 60)))
EVENT_ID_PENDING_TTL_SECONDS = int(os.getenv("EVENT_ID_PENDING_TTL_SECONDS", "60"))
EVENT_MAX_AGE_SECONDS = int(os.getenv("EVENT_MAX_AGE_SECONDS", str(24 * 60 * 60)))
EVENT_MAX_FUTURE_SECONDS = int(os.getenv("EVENT_MAX_FUTURE_SECONDS", str(5 * 60)))

BufferPublish = Callable[..., bool]
OnKafkaDown = Literal["fail", "buffer"]

CLAIM_EVENT_ID_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('SET', KEYS[1], 'pending:' .. ARGV[1], 'EX', ARGV[2])
  return 'owner'
end
if current == 'accepted:' .. ARGV[1] then return 'same' end
if current == 'pending:' .. ARGV[1] then return 'pending' end
return 'conflict'
"""

ACCEPT_EVENT_ID_SCRIPT = """
if redis.call('GET', KEYS[1]) == 'pending:' .. ARGV[1] then
  redis.call('SET', KEYS[1], 'accepted:' .. ARGV[1], 'EX', ARGV[2])
  return 1
end
return 0
"""

RELEASE_EVENT_ID_SCRIPT = """
if redis.call('GET', KEYS[1]) == 'pending:' .. ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""

BATCH_CLAIM_EVENT_IDS_SCRIPT = """
local count = #KEYS
local ttl = ARGV[count + 1]
local results = {}
for index = 1, count do
  local payload_hash = ARGV[index]
  local current = redis.call('GET', KEYS[index])
  if not current then
    redis.call('SET', KEYS[index], 'pending:' .. payload_hash, 'EX', ttl)
    results[index] = 'owner'
  elseif current == 'accepted:' .. payload_hash then
    results[index] = 'same'
  elseif current == 'pending:' .. payload_hash then
    results[index] = 'pending'
  else
    results[index] = 'conflict'
  end
end
return results
"""

BATCH_ACCEPT_EVENT_IDS_SCRIPT = """
local count = #KEYS
local ttl = ARGV[count + 1]
for index = 1, count do
  local payload_hash = ARGV[index]
  if redis.call('GET', KEYS[index]) == 'pending:' .. payload_hash then
    redis.call('SET', KEYS[index], 'accepted:' .. payload_hash, 'EX', ttl)
  end
end
return count
"""

BATCH_RELEASE_EVENT_IDS_SCRIPT = """
for index = 1, #KEYS do
  local payload_hash = ARGV[index]
  if redis.call('GET', KEYS[index]) == 'pending:' .. payload_hash then
    redis.call('DEL', KEYS[index])
  end
end
return #KEYS
"""


def canonical_payload_hash(payload: dict) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def event_identity_status(redis_client, event_id: str, payload_hash: str) -> str:
    """Atomically claim an ID and return owner, pending, same, or conflict."""
    result = redis_client.eval(
        CLAIM_EVENT_ID_SCRIPT,
        1,
        f"ingest:event:{event_id}",
        payload_hash,
        str(EVENT_ID_PENDING_TTL_SECONDS),
    )
    return str(result)


def event_identity_status_batch(redis_client, identities: list[tuple[str, str]]) -> list[str]:
    """Claim unique event IDs in one Redis command, preserving input order."""
    if not identities:
        return []
    keys = [f"ingest:event:{event_id}" for event_id, _ in identities]
    hashes = [payload_hash for _, payload_hash in identities]
    results = redis_client.eval(
        BATCH_CLAIM_EVENT_IDS_SCRIPT,
        len(keys),
        *keys,
        *hashes,
        str(EVENT_ID_PENDING_TTL_SECONDS),
    )
    return [str(result) for result in results]


def remember_event_identity(redis_client, event_id: str, payload_hash: str) -> None:
    redis_client.eval(
        ACCEPT_EVENT_ID_SCRIPT,
        1,
        f"ingest:event:{event_id}",
        payload_hash,
        str(EVENT_ID_TTL_SECONDS),
    )


def remember_event_identities(redis_client, identities: list[tuple[str, str]]) -> None:
    if not identities:
        return
    keys = [f"ingest:event:{event_id}" for event_id, _ in identities]
    hashes = [payload_hash for _, payload_hash in identities]
    redis_client.eval(
        BATCH_ACCEPT_EVENT_IDS_SCRIPT,
        len(keys),
        *keys,
        *hashes,
        str(EVENT_ID_TTL_SECONDS),
    )


def release_event_identity(redis_client, event_id: str, payload_hash: str) -> None:
    redis_client.eval(
        RELEASE_EVENT_ID_SCRIPT,
        1,
        f"ingest:event:{event_id}",
        payload_hash,
    )


def release_event_identities(redis_client, identities: list[tuple[str, str]]) -> None:
    if not identities:
        return
    keys = [f"ingest:event:{event_id}" for event_id, _ in identities]
    hashes = [payload_hash for _, payload_hash in identities]
    redis_client.eval(BATCH_RELEASE_EVENT_IDS_SCRIPT, len(keys), *keys, *hashes)


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


def publish_batch_with_ack(
    producer,
    messages: list[tuple[str, bytes, bytes]],
    *,
    timeout_seconds: float,
) -> list[KafkaUnavailableError | None]:
    """Enqueue a whole batch, then await each broker acknowledgement concurrently."""
    if not messages:
        return []

    pending = object()
    outcomes: list[object] = [pending] * len(messages)
    remaining = len(messages)
    deadline = time.monotonic() + timeout_seconds

    def callback_for(index: int):
        def on_delivery(error, _message) -> None:
            nonlocal remaining
            if outcomes[index] is not pending:
                return
            outcomes[index] = None if error is None else KafkaUnavailableError(str(error))
            remaining -= 1

        return on_delivery

    for index, (topic, key, value) in enumerate(messages):
        while True:
            try:
                producer.produce(
                    topic,
                    key=key,
                    value=value,
                    on_delivery=callback_for(index),
                )
                break
            except BufferError:
                remaining_time = deadline - time.monotonic()
                if remaining_time <= 0:
                    outcomes[index] = KafkaUnavailableError("Kafka producer queue remained full")
                    remaining -= 1
                    break
                producer.poll(min(remaining_time, 0.01))
            except Exception as exc:
                outcomes[index] = KafkaUnavailableError(str(exc))
                remaining -= 1
                break

    while remaining > 0:
        remaining_time = deadline - time.monotonic()
        if remaining_time <= 0:
            break
        producer.poll(min(remaining_time, 0.05))

    if remaining > 0:
        for index, outcome in enumerate(outcomes):
            if outcome is pending:
                outcomes[index] = KafkaUnavailableError("Kafka acknowledgement timed out")

    return [outcome if isinstance(outcome, KafkaUnavailableError) else None for outcome in outcomes]


def resolve_custody_event_id(
    event_dict: dict,
    *,
    reservation_id: str | None = None,
) -> str:
    """Stable eventId: reservation-derived for Gateway, else explicit required."""
    if reservation_id:
        return f"res:{reservation_id}"
    existing = event_dict.get("eventId")
    if existing:
        return str(existing)
    raise ValueError("eventId required when reservation_id is absent")


def _prepare_payload(event_dict: dict, *, reservation_id: str | None = None) -> tuple[dict, str]:
    payload = dict(event_dict)
    payload["eventId"] = resolve_custody_event_id(payload, reservation_id=reservation_id)
    # Identity hash is retry-stable: hash before injecting server clock.
    payload_hash = canonical_payload_hash(payload)
    if "timestamp" not in payload:
        payload["timestamp"] = int(time.time() * 1000)
    return payload, payload_hash


def _suspicious_time(timestamp_ms: int, received_at: int, *, max_age: int, max_future: int) -> bool:
    return (
        timestamp_ms < received_at - max_age * 1000
        or timestamp_ms > received_at + max_future * 1000
    )


def accept(
    redis_client,
    producer,
    event_dict: dict,
    *,
    tenant_id: str | None,
    api_key_id: str | None,
    topic: str,
    quarantine_topic: str,
    timeout_seconds: float,
    on_kafka_down: OnKafkaDown = "fail",
    source: str = "http",
    reservation_id: str | None = None,
    reserved_usd: float = 0.0,
    max_age_seconds: int = EVENT_MAX_AGE_SECONDS,
    max_future_seconds: int = EVENT_MAX_FUTURE_SECONDS,
    buffer_publish: BufferPublish | None = None,
) -> dict[str, Any]:
    """Deep Custody interface for one Token Event.

    Returns status in:
    accepted | quarantined | pending | conflict | buffered | unavailable
    plus optional idempotent=True for identical replay.
    """
    payload, payload_hash = _prepare_payload(event_dict, reservation_id=reservation_id)
    event_id = payload["eventId"]
    identity = event_identity_status(redis_client, event_id, payload_hash)
    if identity == "same":
        return {"status": "accepted", "eventId": event_id, "idempotent": True}
    if identity == "pending":
        return {"status": "pending", "eventId": event_id, "retryable": True}
    if identity == "conflict":
        return {"status": "conflict", "eventId": event_id, "retryable": False}

    received_at = int(time.time() * 1000)
    suspicious = _suspicious_time(
        int(payload["timestamp"]),
        received_at,
        max_age=max_age_seconds,
        max_future=max_future_seconds,
    )
    envelope = trusted_envelope(
        payload,
        tenant_id=tenant_id,
        api_key_id=api_key_id,
        received_at=received_at,
        source=source,
        reservation_id=reservation_id,
        reserved_usd=reserved_usd,
    )
    if suspicious:
        envelope["quarantine"] = {"reason": "event_time_out_of_range"}
    dest_topic = quarantine_topic if suspicious else topic

    if on_kafka_down == "buffer":
        if buffer_publish is None:
            raise ValueError("buffer_publish required when on_kafka_down='buffer'")
        published = buffer_publish(
            redis_client, producer, dest_topic, envelope, timeout_seconds
        )
        # Outbox store is durable custody — mark identity accepted either way.
        remember_event_identity(redis_client, event_id, payload_hash)
        if published:
            return {
                "status": "quarantined" if suspicious else "accepted",
                "eventId": event_id,
                "idempotent": False,
            }
        return {"status": "buffered", "eventId": event_id, "retryable": True}

    try:
        publish_with_ack(
            producer,
            topic=dest_topic,
            key=str(payload["customerId"]).encode("utf-8"),
            value=json.dumps(envelope).encode("utf-8"),
            timeout_seconds=timeout_seconds,
        )
    except KafkaUnavailableError:
        release_event_identity(redis_client, event_id, payload_hash)
        return {"status": "unavailable", "eventId": event_id, "retryable": True}

    remember_event_identity(redis_client, event_id, payload_hash)
    return {
        "status": "quarantined" if suspicious else "accepted",
        "eventId": event_id,
        "idempotent": False,
    }


def accept_many(
    redis_client,
    producer,
    events: list[dict],
    *,
    tenant_id: str | None,
    api_key_id: str | None,
    topic: str,
    quarantine_topic: str,
    timeout_seconds: float,
    source: str = "http",
    max_age_seconds: int = EVENT_MAX_AGE_SECONDS,
    max_future_seconds: int = EVENT_MAX_FUTURE_SECONDS,
) -> list[dict[str, Any]]:
    """Batch custody with the same per-event semantics as accept()."""
    prepared: list[tuple[dict, str]] = []
    for event_dict in events:
        prepared.append(_prepare_payload(event_dict))

    results: list[dict | None] = [None] * len(prepared)
    unique: list[tuple[int, dict, str]] = []
    aliases: dict[int, list[int]] = {}
    seen_in_batch: dict[str, tuple[str, int]] = {}

    for index, (payload, payload_hash) in enumerate(prepared):
        event_id = payload["eventId"]
        previous = seen_in_batch.get(event_id)
        if previous is None:
            seen_in_batch[event_id] = (payload_hash, index)
            unique.append((index, payload, payload_hash))
            aliases[index] = []
        elif previous[0] == payload_hash:
            aliases[previous[1]].append(index)
        else:
            results[index] = {"eventId": event_id, "status": "conflict", "retryable": False}

    identities = [(item[1]["eventId"], item[2]) for item in unique]
    identity_states = event_identity_status_batch(redis_client, identities)
    owned: list[tuple[int, dict, str, bool, dict]] = []

    for (index, payload, payload_hash), identity in zip(unique, identity_states):
        event_id = payload["eventId"]
        if identity == "same":
            duplicate = {"eventId": event_id, "status": "accepted", "idempotent": True}
            results[index] = duplicate
            for alias_index in aliases[index]:
                results[alias_index] = dict(duplicate)
            continue
        if identity == "pending":
            pending = {"eventId": event_id, "status": "pending", "retryable": True}
            results[index] = pending
            for alias_index in aliases[index]:
                results[alias_index] = dict(pending)
            continue
        if identity == "conflict":
            conflict = {"eventId": event_id, "status": "conflict", "retryable": False}
            results[index] = conflict
            for alias_index in aliases[index]:
                results[alias_index] = dict(conflict)
            continue

        received_at = int(time.time() * 1000)
        suspicious = _suspicious_time(
            int(payload["timestamp"]),
            received_at,
            max_age=max_age_seconds,
            max_future=max_future_seconds,
        )
        envelope = trusted_envelope(
            payload,
            tenant_id=tenant_id,
            api_key_id=api_key_id,
            received_at=received_at,
            source=source,
        )
        if suspicious:
            envelope["quarantine"] = {"reason": "event_time_out_of_range"}
        owned.append((index, payload, payload_hash, suspicious, envelope))

    messages = [
        (
            quarantine_topic if suspicious else topic,
            payload["customerId"].encode("utf-8"),
            json.dumps(envelope).encode("utf-8"),
        )
        for _, payload, _, suspicious, envelope in owned
    ]
    outcomes = publish_batch_with_ack(
        producer, messages, timeout_seconds=timeout_seconds
    )

    accepted_identities: list[tuple[str, str]] = []
    failed_identities: list[tuple[str, str]] = []
    for (index, payload, payload_hash, suspicious, _), outcome in zip(owned, outcomes):
        event_id = payload["eventId"]
        if outcome is None:
            accepted_identities.append((event_id, payload_hash))
            result = {
                "eventId": event_id,
                "status": "quarantined" if suspicious else "accepted",
                "idempotent": False,
            }
            results[index] = result
            for alias_index in aliases[index]:
                results[alias_index] = {
                    "eventId": event_id,
                    "status": result["status"],
                    "idempotent": True,
                }
        else:
            failed_identities.append((event_id, payload_hash))
            result = {"eventId": event_id, "status": "unavailable", "retryable": True}
            results[index] = result
            for alias_index in aliases[index]:
                results[alias_index] = dict(result)

    remember_event_identities(redis_client, accepted_identities)
    release_event_identities(redis_client, failed_identities)

    if any(result is None for result in results):
        raise RuntimeError("batch custody left an event unresolved")
    return [result for result in results if result is not None]
