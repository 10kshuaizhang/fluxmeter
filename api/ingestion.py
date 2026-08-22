"""Token Event Custody — deep accept / accept_many over Kafka + identity."""

from __future__ import annotations

import asyncio
import os
import threading
import time
import hashlib
import json
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Literal


class CustodyMetrics:
    """Low-cardinality in-process counters; never label by tenant or event ID."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._outcomes: dict[str, int] = defaultdict(int)
        self._stage_count: dict[str, int] = defaultdict(int)
        self._stage_sum: dict[str, float] = defaultdict(float)

    def outcome(self, status: str, count: int = 1) -> None:
        with self._lock:
            self._outcomes[status] += count

    def observe(self, stage: str, seconds: float) -> None:
        with self._lock:
            self._stage_count[stage] += 1
            self._stage_sum[stage] += seconds

    def prometheus(self) -> str:
        with self._lock:
            lines = ["# TYPE fluxmeter_custody_outcomes_total counter"]
            for status, value in sorted(self._outcomes.items()):
                lines.append(f'fluxmeter_custody_outcomes_total{{outcome="{status}"}} {value}')
            lines.append("# TYPE fluxmeter_custody_stage_seconds summary")
            for stage in sorted(self._stage_count):
                lines.append(f'fluxmeter_custody_stage_seconds_count{{stage="{stage}"}} {self._stage_count[stage]}')
                lines.append(f'fluxmeter_custody_stage_seconds_sum{{stage="{stage}"}} {self._stage_sum[stage]:.9f}')
            return "\n".join(lines) + "\n"


CUSTODY_METRICS = CustodyMetrics()
RECONCILE_EXECUTOR = ThreadPoolExecutor(
    max_workers=int(os.getenv("INGEST_RECONCILE_WORKERS", "4")),
    thread_name_prefix="fluxmeter-custody-reconcile",
)


def _dispatch_reconciliation(callback: Callable[[bool], None], succeeded: bool) -> None:
    RECONCILE_EXECUTOR.submit(callback, succeeded)


class KafkaUnavailableError(RuntimeError):
    """Kafka did not acknowledge custody before the configured deadline."""

    def __init__(self, message: str, *, uncertain: bool = False):
        super().__init__(message)
        self.uncertain = uncertain


class CustodyOverloadedError(RuntimeError):
    """The bounded in-process custody queue has no remaining capacity."""


class KafkaProducerDispatcher:
    """One polling thread per producer; request threads wait on delivery events."""

    def __init__(self, producer, *, max_inflight: int = 20_000):
        self._producer = producer
        self._slots = threading.BoundedSemaphore(max_inflight)
        self._produce_lock = threading.Lock()
        self._closed = threading.Event()
        self._thread = threading.Thread(
            target=self._poll_loop,
            name="fluxmeter-kafka-dispatcher",
            daemon=True,
        )
        self._thread.start()

    def __getattr__(self, name: str):
        return getattr(self._producer, name)

    def _poll_loop(self) -> None:
        while not self._closed.is_set():
            self._producer.poll(0.05)

    def close(self) -> None:
        self._closed.set()
        self._thread.join(timeout=1)

    def publish_and_wait(
        self,
        *,
        topic: str,
        key: bytes,
        value: bytes,
        timeout_seconds: float,
        on_late_delivery: Callable[[bool], None] | None = None,
    ) -> None:
        if not self._slots.acquire(blocking=False):
            raise CustodyOverloadedError("custody in-flight limit reached")
        delivered = threading.Event()
        delivery_error: list[object] = []
        timed_out = threading.Event()
        reconciliation_lock = threading.Lock()
        reconciliation_dispatched = False

        def dispatch_late(succeeded: bool) -> None:
            nonlocal reconciliation_dispatched
            if on_late_delivery is None:
                return
            with reconciliation_lock:
                if reconciliation_dispatched:
                    return
                reconciliation_dispatched = True
            _dispatch_reconciliation(on_late_delivery, succeeded)

        def on_delivery(error, _message) -> None:
            if error is not None:
                delivery_error.append(error)
            delivered.set()
            self._slots.release()
            if timed_out.is_set():
                dispatch_late(error is None)

        try:
            with self._produce_lock:
                self._producer.produce(
                    topic, key=key, value=value, on_delivery=on_delivery
                )
        except BufferError as exc:
            self._slots.release()
            raise CustodyOverloadedError("Kafka producer queue is full") from exc
        except Exception as exc:
            self._slots.release()
            raise KafkaUnavailableError(str(exc)) from exc

        if not delivered.wait(timeout_seconds):
            timed_out.set()
            if delivered.is_set():
                dispatch_late(not delivery_error)
            raise KafkaUnavailableError("Kafka acknowledgement timed out", uncertain=True)
        if delivery_error:
            raise KafkaUnavailableError(str(delivery_error[0]))

    def publish_batch_and_wait(
        self,
        messages: list[tuple[str, bytes, bytes]],
        *,
        timeout_seconds: float,
        late_callbacks: list[Callable[[bool], None] | None] | None = None,
    ) -> list[KafkaUnavailableError | CustodyOverloadedError | None]:
        outcomes: list[KafkaUnavailableError | CustodyOverloadedError | None] = [None] * len(messages)
        delivered = [threading.Event() for _ in messages]
        timed_out = [threading.Event() for _ in messages]
        reconciliation_locks = [threading.Lock() for _ in messages]
        reconciliation_dispatched = [False] * len(messages)

        def dispatch_late(index: int, succeeded: bool) -> None:
            callback = late_callbacks[index] if late_callbacks else None
            if callback is None:
                return
            with reconciliation_locks[index]:
                if reconciliation_dispatched[index]:
                    return
                reconciliation_dispatched[index] = True
            _dispatch_reconciliation(callback, succeeded)

        def callback_for(index: int):
            def on_delivery(error, _message) -> None:
                if error is not None:
                    outcomes[index] = KafkaUnavailableError(str(error))
                delivered[index].set()
                self._slots.release()
                if timed_out[index].is_set():
                    dispatch_late(index, error is None)
            return on_delivery

        for index, (topic, key, value) in enumerate(messages):
            if not self._slots.acquire(blocking=False):
                outcomes[index] = CustodyOverloadedError("custody in-flight limit reached")
                delivered[index].set()
                continue
            try:
                with self._produce_lock:
                    self._producer.produce(
                        topic, key=key, value=value, on_delivery=callback_for(index)
                    )
            except BufferError:
                self._slots.release()
                outcomes[index] = CustodyOverloadedError("Kafka producer queue is full")
                delivered[index].set()
            except Exception as exc:
                self._slots.release()
                outcomes[index] = KafkaUnavailableError(str(exc))
                delivered[index].set()

        deadline = time.monotonic() + timeout_seconds
        for index, event in enumerate(delivered):
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not event.wait(remaining):
                timed_out[index].set()
                delivered_in_race = event.is_set()
                late_succeeded = outcomes[index] is None
                outcomes[index] = KafkaUnavailableError(
                    "Kafka acknowledgement timed out", uncertain=True
                )
                if delivered_in_race:
                    dispatch_late(index, late_succeeded)
        return outcomes


EVENT_ID_TTL_SECONDS = int(os.getenv("EVENT_ID_TTL_SECONDS", str(30 * 24 * 60 * 60)))
EVENT_ID_PENDING_TTL_SECONDS = int(os.getenv("EVENT_ID_PENDING_TTL_SECONDS", "120"))
EVENT_ID_UNCERTAIN_TTL_SECONDS = int(os.getenv("EVENT_ID_UNCERTAIN_TTL_SECONDS", "600"))
EVENT_IDENTITY_SHARDS = int(os.getenv("EVENT_IDENTITY_SHARDS", "8"))
EVENT_IDENTITY_CLEANUP_LIMIT = int(os.getenv("EVENT_IDENTITY_CLEANUP_LIMIT", "64"))
EVENT_ID_FINALIZE_RETRIES = int(os.getenv("EVENT_ID_FINALIZE_RETRIES", "3"))
EVENT_MAX_AGE_SECONDS = int(os.getenv("EVENT_MAX_AGE_SECONDS", str(24 * 60 * 60)))
EVENT_MAX_FUTURE_SECONDS = int(os.getenv("EVENT_MAX_FUTURE_SECONDS", str(5 * 60)))

BufferPublish = Callable[..., bool]
OnKafkaDown = Literal["fail", "buffer"]


@dataclass(frozen=True)
class CustodyConfig:
    topic: str
    quarantine_topic: str
    timeout_seconds: float
    max_age_seconds: int = EVENT_MAX_AGE_SECONDS
    max_future_seconds: int = EVENT_MAX_FUTURE_SECONDS


@dataclass(frozen=True)
class CustodyContext:
    tenant_id: str | None = None
    api_key_id: str | None = None
    source: str = "http"
    reservation_id: str | None = None
    reserved_usd: float = 0.0
    on_kafka_down: OnKafkaDown = "fail"
    buffer_publish: BufferPublish | None = None


class TokenEventCustody:
    """Small custody interface; transport, TTL, envelope, and failure states stay inside."""

    def __init__(self, redis_client, producer, config: CustodyConfig) -> None:
        self._redis = redis_client
        self._producer = producer
        self._config = config

    def accept(
        self, event: dict, context: CustodyContext | None = None
    ) -> dict[str, Any]:
        ctx = context or CustodyContext()
        return _accept_impl(
            self._redis,
            self._producer,
            event,
            tenant_id=ctx.tenant_id,
            api_key_id=ctx.api_key_id,
            topic=self._config.topic,
            quarantine_topic=self._config.quarantine_topic,
            timeout_seconds=self._config.timeout_seconds,
            on_kafka_down=ctx.on_kafka_down,
            source=ctx.source,
            reservation_id=ctx.reservation_id,
            reserved_usd=ctx.reserved_usd,
            max_age_seconds=self._config.max_age_seconds,
            max_future_seconds=self._config.max_future_seconds,
            buffer_publish=ctx.buffer_publish,
        )

    def accept_many(
        self, events: list[dict], context: CustodyContext | None = None
    ) -> list[dict[str, Any]]:
        ctx = context or CustodyContext()
        if ctx.on_kafka_down != "fail" or ctx.buffer_publish is not None:
            raise ValueError("batch custody does not support Gateway outbox mode")
        return _accept_many_impl(
            self._redis,
            self._producer,
            events,
            tenant_id=ctx.tenant_id,
            api_key_id=ctx.api_key_id,
            topic=self._config.topic,
            quarantine_topic=self._config.quarantine_topic,
            timeout_seconds=self._config.timeout_seconds,
            source=ctx.source,
            max_age_seconds=self._config.max_age_seconds,
            max_future_seconds=self._config.max_future_seconds,
        )


@dataclass
class _QueuedCustodyRequest:
    event: dict
    context: CustodyContext
    result: asyncio.Future


class TokenEventCustodyBatcher:
    """Coalesce concurrent single-event custody calls behind one async seam."""

    def __init__(
        self,
        custody: TokenEventCustody,
        *,
        max_batch_size: int = 64,
        max_wait_seconds: float = 0.001,
        max_queue_size: int = 20_000,
    ) -> None:
        if max_batch_size < 1:
            raise ValueError("max_batch_size must be positive")
        if max_wait_seconds < 0:
            raise ValueError("max_wait_seconds must not be negative")
        if max_queue_size < max_batch_size:
            raise ValueError("max_queue_size must be at least max_batch_size")
        self._custody = custody
        self._max_batch_size = max_batch_size
        self._max_wait_seconds = max_wait_seconds
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        self._worker: asyncio.Task | None = None
        self._closed = False
        self._stop = object()

    async def accept(
        self, event: dict, context: CustodyContext | None = None
    ) -> dict[str, Any]:
        if self._closed:
            raise RuntimeError("custody batcher is closed")
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        request = _QueuedCustodyRequest(event, context or CustodyContext(), future)
        try:
            self._queue.put_nowait(request)
        except asyncio.QueueFull as exc:
            raise CustodyOverloadedError("custody batch queue is full") from exc
        if self._worker is None:
            self._worker = asyncio.create_task(self._run())
        return await future

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._worker is not None:
            await self._queue.put(self._stop)
            await self._worker

    async def _run(self) -> None:
        while True:
            first = await self._queue.get()
            if first is self._stop:
                self._queue.task_done()
                return
            requests = [first]
            deadline = asyncio.get_running_loop().time() + self._max_wait_seconds
            while len(requests) < self._max_batch_size:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    request = await asyncio.wait_for(self._queue.get(), timeout=remaining)
                except TimeoutError:
                    break
                if request is self._stop:
                    self._queue.task_done()
                    self._closed = True
                    break
                requests.append(request)
            await self._flush(requests)
            for _ in requests:
                self._queue.task_done()
            if self._closed:
                return

    async def _flush(self, requests: list[_QueuedCustodyRequest]) -> None:
        groups: dict[CustodyContext, list[_QueuedCustodyRequest]] = defaultdict(list)
        for request in requests:
            groups[request.context].append(request)
        for context, group in groups.items():
            try:
                results = await asyncio.to_thread(
                    self._custody.accept_many,
                    [request.event for request in group],
                    context,
                )
                if len(results) != len(group):
                    raise RuntimeError("batch custody returned the wrong number of results")
                for request, result in zip(group, results):
                    if not request.result.done():
                        request.result.set_result(result)
            except Exception as exc:
                for request in group:
                    if not request.result.done():
                        request.result.set_exception(exc)

CLAIM_EVENT_IDS_SCRIPT = """
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, ARGV[4])
for _, field in ipairs(expired) do
  redis.call('HDEL', KEYS[1], field)
  redis.call('ZREM', KEYS[2], field)
end
local count = tonumber(ARGV[5])
local results = {}
for index = 1, count do
  local field = ARGV[4 + index * 2]
  local fingerprint = ARGV[5 + index * 2]
  local current = redis.call('HGET', KEYS[1], field)
  local current_expiry = redis.call('ZSCORE', KEYS[2], field)
  if current and current_expiry and tonumber(current_expiry) <= tonumber(ARGV[1]) then
    redis.call('HDEL', KEYS[1], field)
    redis.call('ZREM', KEYS[2], field)
    current = false
  end
  if not current then
    redis.call('HSET', KEYS[1], field, 'pending:' .. fingerprint)
    redis.call('ZADD', KEYS[2], ARGV[2], field)
    results[index] = 'owner'
  elseif current == 'accepted:' .. fingerprint then
    results[index] = 'same'
  elseif current == 'pending:' .. fingerprint then
    results[index] = 'pending'
  elseif current == 'uncertain:' .. fingerprint then
    results[index] = 'uncertain'
  else
    results[index] = 'conflict'
  end
end
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('EXPIRE', KEYS[2], ARGV[3])
return results
"""

TRANSITION_EVENT_IDS_SCRIPT = """
local target = ARGV[1]
local expiry = ARGV[2]
local bucket_ttl = ARGV[3]
local count = tonumber(ARGV[4])
local matched = 0
for index = 1, count do
  local field = ARGV[3 + index * 2]
  local fingerprint = ARGV[4 + index * 2]
  local current = redis.call('HGET', KEYS[1], field)
  local allowed = current == 'pending:' .. fingerprint
  if target == 'accepted' then
    allowed = allowed or current == 'uncertain:' .. fingerprint
  end
  if allowed then
    redis.call('HSET', KEYS[1], field, target .. ':' .. fingerprint)
    redis.call('ZADD', KEYS[2], expiry, field)
    matched = matched + 1
  elseif current == target .. ':' .. fingerprint then
    matched = matched + 1
  end
end
redis.call('EXPIRE', KEYS[1], bucket_ttl)
redis.call('EXPIRE', KEYS[2], bucket_ttl)
return matched
"""

RELEASE_EVENT_IDS_SCRIPT = """
local count = tonumber(ARGV[1])
local changed = 0
for index = 1, count do
  local field = ARGV[index * 2]
  local fingerprint = ARGV[index * 2 + 1]
  local current = redis.call('HGET', KEYS[1], field)
  if current == 'pending:' .. fingerprint or current == 'uncertain:' .. fingerprint then
    redis.call('HDEL', KEYS[1], field)
    redis.call('ZREM', KEYS[2], field)
    changed = changed + 1
  end
end
return changed
"""


def canonical_payload_hash(payload: dict, *, tenant_id: str | None = None) -> str:
    identity = {"tenantId": tenant_id or "default", "payload": payload}
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _identity_location(tenant_id: str | None, event_id: str) -> tuple[str, str, str]:
    tenant_tag = hashlib.sha256((tenant_id or "default").encode()).hexdigest()[:16]
    event_digest = hashlib.sha256(event_id.encode()).hexdigest()
    shard = f"{int(event_digest[:8], 16) % EVENT_IDENTITY_SHARDS:02x}"
    return (
        f"ingest:id:{{{tenant_tag}}}:{shard}",
        f"ingest:id-expiry:{{{tenant_tag}}}:{shard}",
        event_digest,
    )


def _group_identities(
    tenant_id: str | None, identities: list[tuple[str, str]]
) -> dict[tuple[str, str], list[tuple[int, str, str]]]:
    groups: dict[tuple[str, str], list[tuple[int, str, str]]] = defaultdict(list)
    for index, (event_id, fingerprint) in enumerate(identities):
        state_key, expiry_key, field = _identity_location(tenant_id, event_id)
        groups[(state_key, expiry_key)].append((index, field, fingerprint))
    return groups


def event_identity_status_batch(
    redis_client, tenant_id: str | None, identities: list[tuple[str, str]]
) -> list[str]:
    """Claim tenant-scoped IDs in compact shard buckets, preserving order."""
    if not identities:
        return []
    results = [""] * len(identities)
    now = int(time.time())
    bucket_ttl = EVENT_ID_TTL_SECONDS + 24 * 60 * 60
    grouped = list(_group_identities(tenant_id, identities).items())
    pipe = redis_client.pipeline(transaction=False)
    for (state_key, expiry_key), members in grouped:
        args: list[str] = [
            str(now),
            str(now + EVENT_ID_PENDING_TTL_SECONDS),
            str(bucket_ttl),
            str(EVENT_IDENTITY_CLEANUP_LIMIT),
            str(len(members)),
        ]
        for _, field, fingerprint in members:
            args.extend((field, fingerprint))
        pipe.eval(CLAIM_EVENT_IDS_SCRIPT, 2, state_key, expiry_key, *args)
    grouped_states = pipe.execute()
    for ((_, _), members), states in zip(grouped, grouped_states):
        for (index, _, _), state in zip(members, states):
            results[index] = str(state)
    return results


def event_identity_status(
    redis_client, tenant_id: str | None, event_id: str, payload_hash: str
) -> str:
    return event_identity_status_batch(
        redis_client, tenant_id, [(event_id, payload_hash)]
    )[0]


def _transition_event_identities(
    redis_client,
    tenant_id: str | None,
    identities: list[tuple[str, str]],
    *,
    target: Literal["accepted", "uncertain"],
) -> int:
    if not identities:
        return 0
    now = int(time.time())
    expiry = now + (
        EVENT_ID_TTL_SECONDS if target == "accepted" else EVENT_ID_UNCERTAIN_TTL_SECONDS
    )
    bucket_ttl = EVENT_ID_TTL_SECONDS + 24 * 60 * 60
    changed = 0
    grouped = list(_group_identities(tenant_id, identities).items())
    pipe = redis_client.pipeline(transaction=False)
    for (state_key, expiry_key), members in grouped:
        args: list[str] = [target, str(expiry), str(bucket_ttl), str(len(members))]
        for _, field, fingerprint in members:
            args.extend((field, fingerprint))
        pipe.eval(TRANSITION_EVENT_IDS_SCRIPT, 2, state_key, expiry_key, *args)
    for result in pipe.execute():
        changed += int(result)
    return changed


def remember_event_identities(
    redis_client, tenant_id: str | None, identities: list[tuple[str, str]]
) -> bool:
    for attempt in range(EVENT_ID_FINALIZE_RETRIES):
        try:
            return _transition_event_identities(
                redis_client, tenant_id, identities, target="accepted"
            ) == len(identities)
        except Exception:
            if attempt + 1 == EVENT_ID_FINALIZE_RETRIES:
                return False
            time.sleep(0.01 * (attempt + 1))
    return False


def remember_event_identity(
    redis_client, tenant_id: str | None, event_id: str, payload_hash: str
) -> bool:
    return remember_event_identities(redis_client, tenant_id, [(event_id, payload_hash)])


def mark_event_identities_uncertain(
    redis_client, tenant_id: str | None, identities: list[tuple[str, str]]
) -> None:
    try:
        _transition_event_identities(
            redis_client, tenant_id, identities, target="uncertain"
        )
    except Exception:
        pass


def release_event_identities(
    redis_client, tenant_id: str | None, identities: list[tuple[str, str]]
) -> None:
    if not identities:
        return
    grouped = list(_group_identities(tenant_id, identities).items())
    pipe = redis_client.pipeline(transaction=False)
    for (state_key, expiry_key), members in grouped:
        args: list[str] = [str(len(members))]
        for _, field, fingerprint in members:
            args.extend((field, fingerprint))
        pipe.eval(RELEASE_EVENT_IDS_SCRIPT, 2, state_key, expiry_key, *args)
    pipe.execute()


def release_event_identity(
    redis_client, tenant_id: str | None, event_id: str, payload_hash: str
) -> None:
    release_event_identities(redis_client, tenant_id, [(event_id, payload_hash)])


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
    on_late_delivery: Callable[[bool], None] | None = None,
) -> None:
    """Publish one record and return only after its delivery callback succeeds."""
    if hasattr(producer, "publish_and_wait"):
        producer.publish_and_wait(
            topic=topic,
            key=key,
            value=value,
            timeout_seconds=timeout_seconds,
            on_late_delivery=on_late_delivery,
        )
        return
    delivered = threading.Event()
    delivery_error: list[object] = []
    timed_out = threading.Event()
    reconciliation_lock = threading.Lock()
    reconciliation_dispatched = False

    def dispatch_late(succeeded: bool) -> None:
        nonlocal reconciliation_dispatched
        if on_late_delivery is None:
            return
        with reconciliation_lock:
            if reconciliation_dispatched:
                return
            reconciliation_dispatched = True
        _dispatch_reconciliation(on_late_delivery, succeeded)

    def on_delivery(error, _message) -> None:
        if error is not None:
            delivery_error.append(error)
        delivered.set()
        if timed_out.is_set():
            dispatch_late(error is None)

    try:
        producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
    except BufferError as exc:
        raise CustodyOverloadedError("Kafka producer queue is full") from exc
    except Exception as exc:
        raise KafkaUnavailableError(str(exc)) from exc

    deadline = time.monotonic() + timeout_seconds
    while not delivered.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            timed_out.set()
            if delivered.is_set():
                dispatch_late(not delivery_error)
            raise KafkaUnavailableError("Kafka acknowledgement timed out", uncertain=True)
        producer.poll(min(remaining, 0.05))

    if delivery_error:
        raise KafkaUnavailableError(str(delivery_error[0]))


def publish_batch_with_ack(
    producer,
    messages: list[tuple[str, bytes, bytes]],
    *,
    timeout_seconds: float,
    late_callbacks: list[Callable[[bool], None] | None] | None = None,
) -> list[KafkaUnavailableError | CustodyOverloadedError | None]:
    """Enqueue a whole batch, then await each broker acknowledgement concurrently."""
    if not messages:
        return []
    if hasattr(producer, "publish_batch_and_wait"):
        return producer.publish_batch_and_wait(
            messages,
            timeout_seconds=timeout_seconds,
            late_callbacks=late_callbacks,
        )

    pending = object()
    outcomes: list[object] = [pending] * len(messages)
    remaining = len(messages)
    deadline = time.monotonic() + timeout_seconds
    timed_out = [False] * len(messages)

    def callback_for(index: int):
        def on_delivery(error, _message) -> None:
            nonlocal remaining
            if outcomes[index] is not pending:
                callback = late_callbacks[index] if late_callbacks else None
                if timed_out[index] and callback is not None:
                    _dispatch_reconciliation(callback, error is None)
                return
            outcomes[index] = None if error is None else KafkaUnavailableError(str(error))
            remaining -= 1
            callback = late_callbacks[index] if late_callbacks else None
            if timed_out[index] and callback is not None:
                _dispatch_reconciliation(callback, error is None)

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
                outcomes[index] = CustodyOverloadedError("Kafka producer queue is full")
                remaining -= 1
                break
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
                timed_out[index] = True
                outcomes[index] = KafkaUnavailableError(
                    "Kafka acknowledgement timed out", uncertain=True
                )

    return [
        outcome
        if isinstance(outcome, (KafkaUnavailableError, CustodyOverloadedError))
        else None
        for outcome in outcomes
    ]


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


def _prepare_payload(
    event_dict: dict,
    *,
    tenant_id: str | None,
    reservation_id: str | None = None,
) -> tuple[dict, str]:
    payload = dict(event_dict)
    payload["eventId"] = resolve_custody_event_id(payload, reservation_id=reservation_id)
    # Identity hash is retry-stable: hash before injecting server clock.
    payload_hash = canonical_payload_hash(payload, tenant_id=tenant_id)
    if "timestamp" not in payload:
        payload["timestamp"] = int(time.time() * 1000)
    return payload, payload_hash


def _suspicious_time(timestamp_ms: int, received_at: int, *, max_age: int, max_future: int) -> bool:
    return (
        timestamp_ms < received_at - max_age * 1000
        or timestamp_ms > received_at + max_future * 1000
    )


def _accept_impl(
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
    payload, payload_hash = _prepare_payload(
        event_dict, tenant_id=tenant_id, reservation_id=reservation_id
    )
    event_id = payload["eventId"]
    stage_started = time.monotonic()
    identity = event_identity_status(redis_client, tenant_id, event_id, payload_hash)
    CUSTODY_METRICS.observe("identity_claim", time.monotonic() - stage_started)
    if identity == "same":
        return {"status": "accepted", "eventId": event_id, "idempotent": True}
    if identity == "pending":
        return {"status": "pending", "eventId": event_id, "retryable": True}
    if identity == "uncertain":
        return {"status": "uncertain", "eventId": event_id, "retryable": True}
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

    def reconcile_late_delivery(succeeded: bool) -> None:
        if succeeded:
            if remember_event_identity(
                redis_client, tenant_id, event_id, payload_hash
            ):
                CUSTODY_METRICS.outcome("reconciled_accepted")
            else:
                CUSTODY_METRICS.outcome("reconcile_finalize_failed")
        else:
            release_event_identity(redis_client, tenant_id, event_id, payload_hash)
            CUSTODY_METRICS.outcome("reconciled_released")

    if on_kafka_down == "buffer":
        if buffer_publish is None:
            raise ValueError("buffer_publish required when on_kafka_down='buffer'")
        published = buffer_publish(
            redis_client, producer, dest_topic, envelope, timeout_seconds
        )
        # Outbox store is durable custody — mark identity accepted either way.
        finalized = remember_event_identity(
            redis_client, tenant_id, event_id, payload_hash
        )
        if not finalized:
            mark_event_identities_uncertain(
                redis_client, tenant_id, [(event_id, payload_hash)]
            )
            return {"status": "buffered", "eventId": event_id, "retryable": True}
        if published:
            return {
                "status": "quarantined" if suspicious else "accepted",
                "eventId": event_id,
                "idempotent": False,
            }
        return {"status": "buffered", "eventId": event_id, "retryable": True}

    try:
        stage_started = time.monotonic()
        publish_with_ack(
            producer,
            topic=dest_topic,
            key=str(payload["customerId"]).encode("utf-8"),
            value=json.dumps(envelope).encode("utf-8"),
            timeout_seconds=timeout_seconds,
            on_late_delivery=reconcile_late_delivery,
        )
        CUSTODY_METRICS.observe("kafka_ack", time.monotonic() - stage_started)
    except CustodyOverloadedError:
        release_event_identity(redis_client, tenant_id, event_id, payload_hash)
        return {"status": "overloaded", "eventId": event_id, "retryable": True}
    except KafkaUnavailableError as exc:
        if exc.uncertain:
            mark_event_identities_uncertain(
                redis_client, tenant_id, [(event_id, payload_hash)]
            )
            return {"status": "uncertain", "eventId": event_id, "retryable": True}
        release_event_identity(redis_client, tenant_id, event_id, payload_hash)
        return {"status": "unavailable", "eventId": event_id, "retryable": True}

    stage_started = time.monotonic()
    finalized = remember_event_identity(redis_client, tenant_id, event_id, payload_hash)
    CUSTODY_METRICS.observe("identity_finalize", time.monotonic() - stage_started)
    if not finalized:
        mark_event_identities_uncertain(
            redis_client, tenant_id, [(event_id, payload_hash)]
        )
        return {"status": "uncertain", "eventId": event_id, "retryable": True}
    return {
        "status": "quarantined" if suspicious else "accepted",
        "eventId": event_id,
        "idempotent": False,
    }


def _accept_many_impl(
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
        prepared.append(_prepare_payload(event_dict, tenant_id=tenant_id))

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
    stage_started = time.monotonic()
    identity_states = event_identity_status_batch(redis_client, tenant_id, identities)
    CUSTODY_METRICS.observe("identity_claim", time.monotonic() - stage_started)
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
        if identity == "uncertain":
            uncertain = {"eventId": event_id, "status": "uncertain", "retryable": True}
            results[index] = uncertain
            for alias_index in aliases[index]:
                results[alias_index] = dict(uncertain)
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
    late_callbacks = []
    for _, payload, payload_hash, _, _ in owned:
        event_id = payload["eventId"]
        def reconcile_late_delivery(
            succeeded: bool,
            event_id: str = event_id,
            payload_hash: str = payload_hash,
        ) -> None:
            if succeeded:
                if remember_event_identity(
                    redis_client, tenant_id, event_id, payload_hash
                ):
                    CUSTODY_METRICS.outcome("reconciled_accepted")
                else:
                    CUSTODY_METRICS.outcome("reconcile_finalize_failed")
            else:
                release_event_identity(
                    redis_client, tenant_id, event_id, payload_hash
                )
                CUSTODY_METRICS.outcome("reconciled_released")
        late_callbacks.append(reconcile_late_delivery)
    stage_started = time.monotonic()
    outcomes = publish_batch_with_ack(
        producer,
        messages,
        timeout_seconds=timeout_seconds,
        late_callbacks=late_callbacks,
    )
    CUSTODY_METRICS.observe("kafka_ack", time.monotonic() - stage_started)

    accepted: list[tuple[int, str, str, bool]] = []
    failed_identities: list[tuple[str, str]] = []
    uncertain_identities: list[tuple[str, str]] = []
    for (index, payload, payload_hash, suspicious, _), outcome in zip(owned, outcomes):
        event_id = payload["eventId"]
        if outcome is None:
            accepted.append((index, event_id, payload_hash, suspicious))
        elif isinstance(outcome, CustodyOverloadedError):
            failed_identities.append((event_id, payload_hash))
            result = {"eventId": event_id, "status": "overloaded", "retryable": True}
            results[index] = result
            for alias_index in aliases[index]:
                results[alias_index] = dict(result)
        elif outcome.uncertain:
            uncertain_identities.append((event_id, payload_hash))
            result = {"eventId": event_id, "status": "uncertain", "retryable": True}
            results[index] = result
            for alias_index in aliases[index]:
                results[alias_index] = dict(result)
        else:
            failed_identities.append((event_id, payload_hash))
            result = {"eventId": event_id, "status": "unavailable", "retryable": True}
            results[index] = result
            for alias_index in aliases[index]:
                results[alias_index] = dict(result)

    accepted_identities = [(event_id, fingerprint) for _, event_id, fingerprint, _ in accepted]
    stage_started = time.monotonic()
    finalized = remember_event_identities(redis_client, tenant_id, accepted_identities)
    CUSTODY_METRICS.observe("identity_finalize", time.monotonic() - stage_started)
    if finalized:
        for index, event_id, _, suspicious in accepted:
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
        uncertain_identities.extend(accepted_identities)
        for index, event_id, _, _ in accepted:
            result = {"eventId": event_id, "status": "uncertain", "retryable": True}
            results[index] = result
            for alias_index in aliases[index]:
                results[alias_index] = dict(result)
    mark_event_identities_uncertain(redis_client, tenant_id, uncertain_identities)
    release_event_identities(redis_client, tenant_id, failed_identities)

    if any(result is None for result in results):
        raise RuntimeError("batch custody left an event unresolved")
    return [result for result in results if result is not None]
