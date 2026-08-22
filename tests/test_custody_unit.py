"""Custody module seam — accept / accept_many contract from architecture grill."""

from __future__ import annotations

import asyncio
import json
import sys
import time

import fakeredis
import pytest

sys.path.insert(0, "api")

from ingestion import (
    CustodyConfig,
    CustodyContext,
    KafkaUnavailableError,
    TokenEventCustody,
    TokenEventCustodyBatcher,
    _identity_location,
    event_identity_status,
    resolve_custody_event_id,
)


def accept(redis_client, producer, event, **kwargs):
    config = CustodyConfig(
        topic=kwargs.pop("topic"),
        quarantine_topic=kwargs.pop("quarantine_topic"),
        timeout_seconds=kwargs.pop("timeout_seconds"),
        max_age_seconds=kwargs.pop("max_age_seconds", 24 * 60 * 60),
        max_future_seconds=kwargs.pop("max_future_seconds", 5 * 60),
    )
    context = CustodyContext(
        tenant_id=kwargs.pop("tenant_id", None),
        api_key_id=kwargs.pop("api_key_id", None),
        source=kwargs.pop("source", "http"),
        reservation_id=kwargs.pop("reservation_id", None),
        reserved_usd=kwargs.pop("reserved_usd", 0.0),
        on_kafka_down=kwargs.pop("on_kafka_down", "fail"),
        buffer_publish=kwargs.pop("buffer_publish", None),
    )
    assert not kwargs
    return TokenEventCustody(redis_client, producer, config).accept(event, context)


def accept_many(redis_client, producer, events, **kwargs):
    config = CustodyConfig(
        topic=kwargs.pop("topic"),
        quarantine_topic=kwargs.pop("quarantine_topic"),
        timeout_seconds=kwargs.pop("timeout_seconds"),
    )
    context = CustodyContext(
        tenant_id=kwargs.pop("tenant_id", None),
        api_key_id=kwargs.pop("api_key_id", None),
        source=kwargs.pop("source", "http"),
    )
    assert not kwargs
    return TokenEventCustody(redis_client, producer, config).accept_many(events, context)


class FakeProducer:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.messages: list[dict] = []

    def produce(self, topic, *, key, value, on_delivery=None):
        self.messages.append({"topic": topic, "key": key, "value": value})
        if on_delivery:
            on_delivery(RuntimeError("broker down") if self.fail else None, None)

    def poll(self, timeout):
        return 0


def _event(**overrides):
    base = {
        "customerId": "cust_1",
        "modelId": "gpt-4o-mini",
        "inputTokens": 10,
        "outputTokens": 5,
        "eventId": "evt-1",
        "timestamp": int(time.time() * 1000),
    }
    base.update(overrides)
    return base


def test_resolve_event_id_from_reservation():
    assert resolve_custody_event_id({}, reservation_id="r1") == "res:r1"


def test_resolve_event_id_requires_explicit_without_reservation():
    with pytest.raises(ValueError):
        resolve_custody_event_id({"customerId": "c"}, reservation_id=None)


def test_accept_idempotent_replay_same_payload():
    r = fakeredis.FakeRedis(decode_responses=True)
    producer = FakeProducer()
    event = _event()

    first = accept(
        r,
        producer,
        event,
        tenant_id="t1",
        api_key_id="k1",
        topic="token-events",
        quarantine_topic="token-events-quarantine",
        timeout_seconds=1.0,
    )
    second = accept(
        r,
        producer,
        dict(event),
        tenant_id="t1",
        api_key_id="k1",
        topic="token-events",
        quarantine_topic="token-events-quarantine",
        timeout_seconds=1.0,
    )

    assert first["status"] == "accepted"
    assert second["status"] == "accepted"
    assert second.get("idempotent") is True
    assert len(producer.messages) == 1


def test_claim_expires_requested_identity_even_behind_cleanup_backlog():
    r = fakeredis.FakeRedis(decode_responses=True)
    tenant_id = "t1"
    event_id = "evt-expired-behind-backlog"
    fingerprint = "fingerprint"
    state_key, expiry_key, field = _identity_location(tenant_id, event_id)
    now = int(time.time())

    # More expired identities than one bounded cleanup pass can remove. The
    # requested identity sorts after that backlog, reproducing the production
    # state where retries remained pending for minutes after their own TTL.
    for index in range(65):
        stale_field = f"stale-{index:03d}"
        r.hset(state_key, stale_field, "pending:stale")
        r.zadd(expiry_key, {stale_field: now - 1_000 + index})
    r.hset(state_key, field, f"pending:{fingerprint}")
    r.zadd(expiry_key, {field: now - 1})

    assert event_identity_status(r, tenant_id, event_id, fingerprint) == "owner"
    assert r.hget(state_key, field) == f"pending:{fingerprint}"
    assert r.zscore(expiry_key, field) > now


def test_accept_conflict_on_payload_mismatch():
    r = fakeredis.FakeRedis(decode_responses=True)
    producer = FakeProducer()
    accept(
        r,
        producer,
        _event(inputTokens=10),
        tenant_id=None,
        api_key_id=None,
        topic="token-events",
        quarantine_topic="q",
        timeout_seconds=1.0,
    )
    result = accept(
        r,
        producer,
        _event(inputTokens=99),
        tenant_id=None,
        api_key_id=None,
        topic="token-events",
        quarantine_topic="q",
        timeout_seconds=1.0,
    )
    assert result["status"] == "conflict"


def test_accept_kafka_fail_returns_unavailable():
    r = fakeredis.FakeRedis(decode_responses=True)
    producer = FakeProducer(fail=True)
    result = accept(
        r,
        producer,
        _event(eventId="evt-fail"),
        tenant_id=None,
        api_key_id=None,
        topic="token-events",
        quarantine_topic="q",
        timeout_seconds=1.0,
        on_kafka_down="fail",
    )
    assert result["status"] == "unavailable"
    state_key, _, field = _identity_location(None, "evt-fail")
    assert r.hget(state_key, field) is None


def test_same_event_id_is_isolated_by_tenant():
    r = fakeredis.FakeRedis(decode_responses=True)
    producer = FakeProducer()
    event = _event(eventId="shared-id")

    first = accept(
        r, producer, event, tenant_id="tenant-a", api_key_id="k1",
        topic="token-events", quarantine_topic="q", timeout_seconds=1.0,
    )
    second = accept(
        r, producer, event, tenant_id="tenant-b", api_key_id="k2",
        topic="token-events", quarantine_topic="q", timeout_seconds=1.0,
    )

    assert first["status"] == second["status"] == "accepted"
    assert len(producer.messages) == 2


def test_ack_timeout_enters_uncertain_instead_of_releasing_claim():
    class TimeoutProducer(FakeProducer):
        def produce(self, topic, *, key, value, on_delivery=None):
            self.messages.append({"topic": topic, "key": key, "value": value})

    r = fakeredis.FakeRedis(decode_responses=True)
    producer = TimeoutProducer()
    event = _event(eventId="evt-timeout")
    first = accept(
        r, producer, event, tenant_id="t1", api_key_id="k1",
        topic="token-events", quarantine_topic="q", timeout_seconds=0.01,
    )
    second = accept(
        r, producer, event, tenant_id="t1", api_key_id="k1",
        topic="token-events", quarantine_topic="q", timeout_seconds=0.01,
    )

    assert first["status"] == "uncertain"
    assert second["status"] == "uncertain"
    assert len(producer.messages) == 1


def test_late_ack_reconciles_uncertain_identity_to_accepted():
    class LateProducer(FakeProducer):
        def __init__(self):
            super().__init__()
            self.callback = None

        def produce(self, topic, *, key, value, on_delivery=None):
            self.messages.append({"topic": topic, "key": key, "value": value})
            self.callback = on_delivery

    r = fakeredis.FakeRedis(decode_responses=True)
    producer = LateProducer()
    event = _event(eventId="evt-late-ack")

    first = accept(
        r, producer, event, tenant_id="t1", api_key_id="k1",
        topic="token-events", quarantine_topic="q", timeout_seconds=0.01,
    )
    assert first["status"] == "uncertain"

    producer.callback(None, None)
    state_key, _, field = _identity_location("t1", "evt-late-ack")
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        if str(r.hget(state_key, field)).startswith("accepted:"):
            break
        time.sleep(0.01)

    replay = accept(
        r, producer, event, tenant_id="t1", api_key_id="k1",
        topic="token-events", quarantine_topic="q", timeout_seconds=0.01,
    )
    assert replay == {"status": "accepted", "eventId": "evt-late-ack", "idempotent": True}
    assert len(producer.messages) == 1


def test_finalize_failure_does_not_report_accepted(monkeypatch):
    import ingestion

    r = fakeredis.FakeRedis(decode_responses=True)
    producer = FakeProducer()
    monkeypatch.setattr(ingestion, "remember_event_identity", lambda *_args, **_kwargs: False)

    result = accept(
        r, producer, _event(eventId="evt-finalize"), tenant_id="t1", api_key_id="k1",
        topic="token-events", quarantine_topic="q", timeout_seconds=1.0,
    )

    assert result == {"status": "uncertain", "eventId": "evt-finalize", "retryable": True}
    assert len(producer.messages) == 1


def test_producer_queue_full_returns_overloaded_and_releases_claim():
    class FullProducer(FakeProducer):
        def produce(self, topic, *, key, value, on_delivery=None):
            raise BufferError("full")

    r = fakeredis.FakeRedis(decode_responses=True)
    event = _event(eventId="evt-full")
    result = accept(
        r, FullProducer(), event, tenant_id="t1", api_key_id="k1",
        topic="token-events", quarantine_topic="q", timeout_seconds=1.0,
    )
    state_key, _, field = _identity_location("t1", "evt-full")

    assert result["status"] == "overloaded"
    assert r.hget(state_key, field) is None


def test_batch_queue_full_preserves_per_item_overloaded_status():
    class FullProducer(FakeProducer):
        def produce(self, topic, *, key, value, on_delivery=None):
            raise BufferError("full")

    outcomes = accept_many(
        fakeredis.FakeRedis(decode_responses=True),
        FullProducer(),
        [_event(eventId="batch-full")],
        tenant_id="t1",
        api_key_id="k1",
        topic="token-events",
        quarantine_topic="q",
        timeout_seconds=1.0,
    )

    assert outcomes == [
        {"eventId": "batch-full", "status": "overloaded", "retryable": True}
    ]


def test_accept_kafka_fail_buffers_for_gateway():
    r = fakeredis.FakeRedis(decode_responses=True)
    producer = FakeProducer(fail=True)
    buffered = []

    def buffer_publish(redis_client, prod, topic, envelope, timeout):
        buffered.append(envelope)
        return False

    result = accept(
        r,
        producer,
        _event(eventId=None),
        tenant_id="t1",
        api_key_id="k1",
        topic="token-events",
        quarantine_topic="q",
        timeout_seconds=1.0,
        on_kafka_down="buffer",
        source="gateway",
        reservation_id="resv-9",
        reserved_usd=0.1,
        buffer_publish=buffer_publish,
    )

    assert result["status"] == "buffered"
    assert result["eventId"] == "res:resv-9"
    assert len(buffered) == 1
    assert buffered[0]["reservation"]["reservationId"] == "resv-9"


def test_accept_many_matches_single_semantics():
    r = fakeredis.FakeRedis(decode_responses=True)
    producer = FakeProducer()
    first = _event(eventId="a")
    events = [first, _event(eventId="b", customerId="cust_2")]

    outcomes = accept_many(
        r,
        producer,
        events,
        tenant_id="t1",
        api_key_id="k1",
        topic="token-events",
        quarantine_topic="q",
        timeout_seconds=1.0,
    )

    assert [o["status"] for o in outcomes] == ["accepted", "accepted"]
    assert len(producer.messages) == 2
    again = accept_many(
        r,
        producer,
        [dict(first)],
        tenant_id="t1",
        api_key_id="k1",
        topic="token-events",
        quarantine_topic="q",
        timeout_seconds=1.0,
    )
    assert again[0]["idempotent"] is True
    assert json.loads(producer.messages[0]["value"])["auth"]["tenantId"] == "t1"


def test_batcher_coalesces_concurrent_single_event_requests_in_order():
    class RecordingCustody:
        def __init__(self):
            self.calls: list[tuple[list[dict], CustodyContext]] = []

        def accept_many(self, events, context):
            self.calls.append((events, context))
            return [
                {"status": "accepted", "eventId": event["eventId"], "idempotent": False}
                for event in events
            ]

    async def scenario():
        custody = RecordingCustody()
        batcher = TokenEventCustodyBatcher(
            custody,
            max_batch_size=64,
            max_wait_seconds=0.01,
            max_queue_size=128,
        )
        context = CustodyContext(tenant_id="t1", api_key_id="k1")
        try:
            first, second = await asyncio.gather(
                batcher.accept(_event(eventId="batch-a"), context),
                batcher.accept(_event(eventId="batch-b"), context),
            )
        finally:
            await batcher.close()
        return custody, first, second

    custody, first, second = asyncio.run(scenario())

    assert len(custody.calls) == 1
    assert [event["eventId"] for event in custody.calls[0][0]] == ["batch-a", "batch-b"]
    assert custody.calls[0][1] == CustodyContext(tenant_id="t1", api_key_id="k1")
    assert first["eventId"] == "batch-a"
    assert second["eventId"] == "batch-b"


def test_batcher_never_mixes_tenant_contexts():
    class RecordingCustody:
        def __init__(self):
            self.calls = []

        def accept_many(self, events, context):
            self.calls.append((events, context))
            return [
                {"status": "accepted", "eventId": event["eventId"]}
                for event in events
            ]

    async def scenario():
        custody = RecordingCustody()
        batcher = TokenEventCustodyBatcher(
            custody, max_batch_size=64, max_wait_seconds=0.01, max_queue_size=128
        )
        try:
            await asyncio.gather(
                batcher.accept(
                    _event(eventId="tenant-a-event"), CustodyContext(tenant_id="a")
                ),
                batcher.accept(
                    _event(eventId="tenant-b-event"), CustodyContext(tenant_id="b")
                ),
            )
        finally:
            await batcher.close()
        return custody.calls

    calls = asyncio.run(scenario())

    assert len(calls) == 2
    assert {(call[1].tenant_id, call[0][0]["eventId"]) for call in calls} == {
        ("a", "tenant-a-event"),
        ("b", "tenant-b-event"),
    }
