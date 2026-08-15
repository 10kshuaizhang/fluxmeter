"""Custody module seam — accept / accept_many contract from architecture grill."""

from __future__ import annotations

import json
import sys
import time

import fakeredis
import pytest

sys.path.insert(0, "api")

from ingestion import (
    KafkaUnavailableError,
    accept,
    accept_many,
    resolve_custody_event_id,
)


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
    assert r.get("ingest:event:evt-fail") is None


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
