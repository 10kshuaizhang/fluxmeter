"""Public HTTP ingestion contract for the single Kafka/Flink path."""

from __future__ import annotations

import json
import time

import fakeredis
import pytest
import redis
from fastapi.testclient import TestClient


class BoundaryKafkaProducer:
    """Controllable stand-in for the external Kafka broker boundary."""

    def __init__(self, *, error: Exception | None = None):
        self.error = error
        self.delivery_errors: list[Exception | None] = []
        self.metadata_error: Exception | None = None
        self.messages: list[dict] = []
        self.defer_delivery = False
        self.pending_callbacks: list[tuple[object, object]] = []
        self.produced_before_first_poll: int | None = None

    def produce(self, topic, *, key, value, on_delivery=None):
        self.messages.append({"topic": topic, "key": key, "value": value})
        error = self.delivery_errors.pop(0) if self.delivery_errors else self.error
        if on_delivery:
            if self.defer_delivery:
                self.pending_callbacks.append((on_delivery, error))
            else:
                on_delivery(error, None)

    def poll(self, timeout):
        if self.produced_before_first_poll is None:
            self.produced_before_first_poll = len(self.messages)
        pending, self.pending_callbacks = self.pending_callbacks, []
        for callback, error in pending:
            callback(error, None)
        return 0

    def flush(self, timeout):
        return 0 if self.error is None else 1

    def list_topics(self, timeout):
        if self.metadata_error:
            raise self.metadata_error
        return object()


@pytest.fixture
def ingestion_api(monkeypatch):
    import main
    import auth

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    producer = BoundaryKafkaProducer()
    monkeypatch.setattr(auth, "_redis", lambda: redis_client)
    monkeypatch.setattr(main, "get_redis", lambda: redis_client)
    monkeypatch.setattr(main, "get_kafka_producer", lambda: producer)
    monkeypatch.setattr(main, "_custody_batcher", None)
    with TestClient(main.app) as client:
        yield client, redis_client, producer


def test_health_is_mode_free_liveness(ingestion_api):
    client, _, _ = ingestion_api

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_watermark_heartbeat_uses_dedicated_topic_and_single_publisher(ingestion_api):
    import main

    _, redis_client, producer = ingestion_api

    assert main.publish_watermark_heartbeat(redis_client, producer) is True
    assert main.publish_watermark_heartbeat(redis_client, producer) is False
    assert len(producer.messages) == 1
    message = producer.messages[0]
    assert message["topic"] == main.WATERMARK_TOPIC
    envelope = json.loads(message["value"])
    assert envelope["payload"]["metadata"] == {
        "_heartbeat": "true",
        "_watermark": "true",
    }


def test_ingest_returns_retryable_503_without_kafka_custody(ingestion_api):
    client, _, producer = ingestion_api
    producer.error = RuntimeError("broker unavailable")

    response = client.post(
        "/ingest",
        json={
            "customerId": "cust_1",
            "modelId": "gpt-4o-mini",
            "inputTokens": 10,
            "outputTokens": 5,
            "eventId": "evt-retryable",
        },
    )

    assert response.status_code == 503
    assert response.headers["Retry-After"] == "1"
    assert response.json()["detail"]["code"] == "kafka_unavailable"


def test_ingest_publishes_versioned_trusted_envelope(ingestion_api, monkeypatch):
    import main

    client, _, producer = ingestion_api
    monkeypatch.setattr(main, "resolve_key_context", lambda _key: ("cust_1", "key_7"))
    monkeypatch.setattr(main, "resolve_tenant_from_key", lambda _key: "tenant_trusted")

    response = client.post(
        "/ingest",
        headers={"X-API-Key": "fm_live_test"},
        json={
            "customerId": "cust_1",
            "tenantId": "tenant_forged",
            "modelId": "gpt-4o-mini",
            "inputTokens": 10,
            "eventId": "evt-envelope",
        },
    )

    assert response.status_code == 202
    envelope = json.loads(producer.messages[0]["value"])
    assert envelope["envelopeVersion"] == 1
    assert envelope["source"] == "http"
    assert envelope["auth"] == {
        "tenantId": "tenant_trusted",
        "customerId": "cust_1",
        "apiKeyId": "key_7",
    }
    assert envelope["payload"]["eventId"] == "evt-envelope"
    assert "tenantId" not in envelope["payload"]
    assert envelope["receipt"]["receivedAt"] > 0
    assert envelope["receipt"]["traceId"]


def test_single_ingest_uses_shared_custody_batcher(ingestion_api, monkeypatch):
    import main

    class RecordingBatcher:
        def __init__(self):
            self.calls = []

        async def accept(self, event, context):
            self.calls.append((event, context))
            return {
                "status": "accepted",
                "eventId": event["eventId"],
                "idempotent": False,
            }

    batcher = RecordingBatcher()
    monkeypatch.setattr(main, "get_custody_batcher", lambda: batcher)
    client, _, producer = ingestion_api

    response = client.post(
        "/ingest",
        json={
            "customerId": "cust_batcher",
            "modelId": "gpt-4o-mini",
            "inputTokens": 10,
            "eventId": "evt-batcher-seam",
        },
    )

    assert response.status_code == 202
    assert len(batcher.calls) == 1
    assert batcher.calls[0][0]["eventId"] == "evt-batcher-seam"
    assert batcher.calls[0][1].source == "http"
    assert producer.messages == []


def test_ingest_routes_authenticate_without_fastapi_dependency_graph(
    ingestion_api, monkeypatch
):
    import main

    ingest_routes = {
        route.path: route
        for route in main.app.routes
        if getattr(route, "path", None) in {"/ingest", "/ingest/batch"}
    }
    assert ingest_routes["/ingest"].dependant.dependencies == []
    assert ingest_routes["/ingest/batch"].dependant.dependencies == []

    calls = []

    async def reject(key):
        calls.append(key)
        raise main.HTTPException(status_code=401, detail="auth probe")

    monkeypatch.setattr(main, "require_api_key", reject)
    client, _, producer = ingestion_api
    response = client.post(
        "/ingest",
        headers={"X-API-Key": "probe-key"},
        json={"customerId": "cust_auth", "modelId": "m", "eventId": "evt-auth"},
    )

    assert response.status_code == 401
    assert calls == ["probe-key"]
    assert producer.messages == []


def test_identical_retry_is_accepted_without_republishing(ingestion_api):
    client, _, producer = ingestion_api
    event = {
        "customerId": "cust_retry",
        "modelId": "gpt-4o-mini",
        "inputTokens": 10,
        "eventId": "evt-same",
    }

    first = client.post("/ingest", json=event)
    second = client.post("/ingest", json=event)

    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json()["idempotent"] is True
    assert len(producer.messages) == 1


def test_event_id_reuse_with_different_payload_is_conflict(ingestion_api):
    client, _, producer = ingestion_api
    first = {
        "customerId": "cust_conflict",
        "modelId": "gpt-4o-mini",
        "inputTokens": 10,
        "eventId": "evt-conflict",
    }
    changed = {**first, "inputTokens": 11}

    assert client.post("/ingest", json=first).status_code == 202
    response = client.post("/ingest", json=changed)

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "event_id_conflict"
    assert len(producer.messages) == 1


def test_identity_store_failure_is_retryable_503(ingestion_api, monkeypatch):
    import main

    class RedisFailingBatcher:
        async def accept(self, *_args, **_kwargs):
            raise redis.ConnectionError("identity store down")

    client, _, producer = ingestion_api
    monkeypatch.setattr(main, "get_custody_batcher", lambda: RedisFailingBatcher())
    response = client.post(
        "/ingest",
        json={"customerId": "cust_1", "modelId": "m", "eventId": "evt-redis-down"},
    )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "identity_store_unavailable"
    assert producer.messages == []


def test_batch_rejects_invalid_item_without_blocking_valid_event(ingestion_api):
    client, _, producer = ingestion_api

    response = client.post(
        "/ingest/batch",
        json=[
            {"customerId": "cust_batch", "modelId": "m", "eventId": "evt-ok"},
            {
                "customerId": "cust_batch",
                "modelId": "m",
                "eventId": "evt-bad",
                "metadata": {"not_allowed": "x"},
            },
        ],
    )

    assert response.status_code == 207
    assert len(producer.messages) == 1
    assert response.json()["results"] == [
        {"eventId": "evt-ok", "status": "accepted", "idempotent": False},
        {
            "eventId": "evt-bad",
            "status": "rejected",
            "retryable": False,
            "message": "metadata key 'not_allowed' not in whitelist ['feature', 'room_id']",
        },
    ]


def test_batch_rejects_schema_invalid_item_without_blocking_valid_event(ingestion_api):
    client, _, producer = ingestion_api
    response = client.post(
        "/ingest/batch",
        json=[
            {"customerId": "cust_batch", "modelId": "m", "eventId": "evt-ok-schema"},
            {"modelId": "m", "eventId": "evt-missing-customer"},
        ],
    )

    assert response.status_code == 207
    assert len(producer.messages) == 1
    assert [item["status"] for item in response.json()["results"]] == ["accepted", "rejected"]


def test_metrics_are_low_cardinality_and_exclude_tenant_ids(ingestion_api, monkeypatch):
    import main

    client, _, _ = ingestion_api
    monkeypatch.setattr(main, "resolve_tenant_from_key", lambda _key: "tenant-secret")
    assert client.post(
        "/ingest",
        json={"customerId": "cust_1", "modelId": "m", "eventId": "evt-metrics"},
    ).status_code == 202

    metrics = client.get("/metrics").text
    assert "fluxmeter_custody_outcomes_total" in metrics
    assert "identity_claim" in metrics
    assert "tenant-secret" not in metrics


def test_batch_reports_mixed_kafka_custody(ingestion_api):
    client, _, producer = ingestion_api
    producer.delivery_errors = [None, RuntimeError("broker unavailable")]

    response = client.post(
        "/ingest/batch",
        json=[
            {"customerId": "cust_batch", "modelId": "m", "eventId": "evt-1"},
            {"customerId": "cust_batch", "modelId": "m", "eventId": "evt-2"},
        ],
    )

    assert response.status_code == 207
    assert response.json() == {
        "status": "partial",
        "results": [
            {"eventId": "evt-1", "status": "accepted", "idempotent": False},
            {"eventId": "evt-2", "status": "failed", "retryable": True},
        ],
    }


def test_batch_enqueues_every_owned_event_before_waiting_for_acks(ingestion_api):
    client, _, producer = ingestion_api
    producer.defer_delivery = True

    response = client.post(
        "/ingest/batch",
        json=[
            {"customerId": "cust_batch", "modelId": "m", "eventId": f"evt-{index}"}
            for index in range(5)
        ],
    )

    assert response.status_code == 202
    assert producer.produced_before_first_poll == 5
    assert len(response.json()["results"]) == 5


def test_batch_collapses_identical_event_ids_without_republishing(ingestion_api):
    client, _, producer = ingestion_api
    event = {"customerId": "cust_batch", "modelId": "m", "eventId": "evt-same-batch"}

    response = client.post("/ingest/batch", json=[event, event])

    assert response.status_code == 202
    assert len(producer.messages) == 1
    assert response.json()["results"] == [
        {"eventId": "evt-same-batch", "status": "accepted", "idempotent": False},
        {"eventId": "evt-same-batch", "status": "accepted", "idempotent": True},
    ]


def test_batch_reports_in_request_event_id_conflict_in_input_order(ingestion_api):
    client, _, producer = ingestion_api
    first = {"customerId": "cust_batch", "modelId": "m", "eventId": "evt-conflict-batch"}

    response = client.post("/ingest/batch", json=[first, {**first, "inputTokens": 1}])

    assert response.status_code == 207
    assert len(producer.messages) == 1
    assert response.json()["results"] == [
        {"eventId": "evt-conflict-batch", "status": "accepted", "idempotent": False},
        {"eventId": "evt-conflict-batch", "status": "conflict", "retryable": False},
    ]


def test_ready_requires_flink_to_consume_causal_probe(ingestion_api, monkeypatch):
    import main

    client, redis_client, _ = ingestion_api
    original_publish = main.publish_with_ack

    def consume_probe(producer, **kwargs):
        original_publish(producer, **kwargs)
        envelope = json.loads(kwargs["value"])
        redis_client.set("flink:probe:" + envelope["payload"]["eventId"], "1")

    monkeypatch.setattr(main, "publish_with_ack", consume_probe)

    ready = client.get("/ready")
    monkeypatch.setattr(main, "publish_with_ack", original_publish)
    monkeypatch.setattr(main, "FLINK_READY_PROBE_TIMEOUT_SECONDS", 0.01)
    stale = client.get("/ready")

    assert ready.status_code == 200
    assert ready.json() == {"status": "ready"}
    assert stale.status_code == 503
    assert stale.json()["detail"]["code"] == "flink_stale"


def test_suspicious_event_time_is_durably_quarantined(ingestion_api):
    client, _, producer = ingestion_api

    response = client.post(
        "/ingest",
        json={
            "customerId": "cust_skew",
            "modelId": "m",
            "eventId": "evt-skew",
            "timestamp": 1,
        },
    )

    assert response.status_code == 202
    assert response.json()["status"] == "quarantined"
    assert producer.messages[0]["topic"] == "token-events-quarantine"


def test_batch_quarantines_only_suspicious_events(ingestion_api):
    client, _, producer = ingestion_api

    response = client.post(
        "/ingest/batch",
        json=[
            {"customerId": "cust_batch", "modelId": "m", "eventId": "evt-current"},
            {
                "customerId": "cust_batch",
                "modelId": "m",
                "eventId": "evt-old",
                "timestamp": 1,
            },
        ],
    )

    assert response.status_code == 202
    assert [message["topic"] for message in producer.messages] == [
        "token-events",
        "token-events-quarantine",
    ]
    assert response.json()["results"][1]["status"] == "quarantined"


def test_all_conflict_batch_is_not_reported_as_retryable_kafka_failure(ingestion_api):
    client, _, _ = ingestion_api
    original = {"customerId": "cust", "modelId": "m", "eventId": "same", "inputTokens": 1}
    assert client.post("/ingest", json=original).status_code == 202

    response = client.post("/ingest/batch", json=[{**original, "inputTokens": 2}])

    assert response.status_code == 409
    assert "Retry-After" not in response.headers
