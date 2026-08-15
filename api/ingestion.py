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
