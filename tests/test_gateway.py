"""Gateway proxy tests — mock upstream, no live OpenAI."""

from __future__ import annotations

import json
import os
import sys
import time
from types import SimpleNamespace

import fakeredis
import pytest

os.environ["FLUXMETER_AUTH_OPTIONAL"] = "true"
os.environ["GATEWAY_UPSTREAM_API_KEY"] = "sk-test-upstream"
os.environ["GATEWAY_OUTBOX_WORKER"] = "false"

sys.path.insert(0, "api")

from fastapi.testclient import TestClient

UPSTREAM_CALLS = {"n": 0}


def _reset_upstream():
    UPSTREAM_CALLS["n"] = 0


def _setup_customer(r, customer_id: str, balance: float):
    r.set(f"budget:{customer_id}:balance_usd", str(balance))
    r.set(f"budget:{customer_id}:held_usd", "0")


class _MockStreamResponse:
    def __init__(self, chunks: list[bytes], status_code: int = 200, body: bytes = b""):
        self.status_code = status_code
        self._chunks = chunks
        self._body = body

    async def __aenter__(self):
        UPSTREAM_CALLS["n"] += 1
        return self

    async def __aexit__(self, *args):
        return False

    async def aiter_bytes(self):
        for chunk in self._chunks:
            yield chunk

    async def aread(self):
        return self._body


class _MockAsyncClient:
    def __init__(self, **kwargs):
        self._stream_chunks = kwargs.pop("_stream_chunks", [])
        self._json_response = kwargs.pop("_json_response", None)
        self._stream_status = kwargs.pop("_stream_status", 200)
        self._stream_body = kwargs.pop("_stream_body", b"")

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def post(self, url, **kwargs):
        UPSTREAM_CALLS["n"] += 1
        body = self._json_response or {
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "choices": [{"message": {"role": "assistant", "content": "hi"}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30},
        }

        class Resp:
            status_code = 200

            @staticmethod
            def json():
                return body

        return Resp()

    def stream(self, method, url, **kwargs):
        return _MockStreamResponse(
            self._stream_chunks,
            status_code=self._stream_status,
            body=self._stream_body,
        )


class _KafkaProducer:
    def __init__(self):
        self.error = None
        self.messages = []

    def produce(self, topic, *, key, value, on_delivery=None):
        self.messages.append({"topic": topic, "key": key, "value": value})
        if on_delivery:
            on_delivery(self.error, None)

    def poll(self, timeout):
        return 0


@pytest.fixture
def gw(monkeypatch):
    _reset_upstream()
    fake = fakeredis.FakeRedis(decode_responses=True)
    producer = _KafkaProducer()
    import gateway.deps as deps
    import auth

    deps._kafka_producer = producer
    monkeypatch.setattr(auth, "_redis", lambda: fake)
    monkeypatch.setattr(deps, "get_redis", lambda: fake)
    monkeypatch.setattr("gateway.routes.get_redis", lambda: fake)

    def client_factory(**kwargs):
        return _MockAsyncClient(**kwargs)

    monkeypatch.setattr("gateway.proxy.httpx.AsyncClient", client_factory)
    from gateway_app import app

    return TestClient(app), fake, producer


def test_check_denies_before_upstream(gw):
    client, r, _ = gw
    _setup_customer(r, "cust_deny", balance=0.0)

    resp = client.post(
        "/v1/chat/completions",
        headers={"X-FluxMeter-Customer-Id": "cust_deny"},
        json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert resp.status_code == 402
    assert UPSTREAM_CALLS["n"] == 0
    body = resp.json()
    assert body["error"]["code"] == "budget_exceeded"


def test_non_stream_ingests_usage(gw, monkeypatch):
    client, r, producer = gw
    _setup_customer(r, "cust_ok", balance=10.0)

    monkeypatch.setattr(
        "gateway.proxy.httpx.AsyncClient",
        lambda **kw: _MockAsyncClient(
            _json_response={
                "id": "x",
                "choices": [{"message": {"content": "ok"}}],
                "usage": {"prompt_tokens": 100, "completion_tokens": 50},
            }
        ),
    )

    resp = client.post(
        "/v1/chat/completions",
        headers={
            "X-FluxMeter-Customer-Id": "cust_ok",
            "Authorization": "Bearer sk-live",
        },
        json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert resp.status_code == 200
    assert UPSTREAM_CALLS["n"] == 1
    envelope = json.loads(producer.messages[0]["value"])
    assert envelope["source"] == "gateway"
    assert envelope["payload"]["inputTokens"] == 100
    assert envelope["payload"]["outputTokens"] == 50
    assert envelope["auth"]["customerId"] == "cust_ok"
    assert envelope["reservation"]["reservationId"]
    assert envelope["reservation"]["reservedUsd"] > 0
    assert resp.headers["X-FluxMeter-Reservation-Id"] == envelope["reservation"]["reservationId"]
    assert float(resp.headers["X-FluxMeter-Reserved-Usd"]) > 0
    assert float(r.get("budget:cust_ok:held_usd")) > 0


def test_stream_kill_under_1s(gw, monkeypatch):
    client, r, producer = gw
    _setup_customer(r, "cust_kill", balance=10.0)

    chunk = json.dumps({"choices": [{"delta": {"content": "x" * 80}}]})
    stream_chunks = [
        f"data: {chunk}\n\n".encode() for _ in range(30)
    ] + [b"data: [DONE]\n\n"]

    monkeypatch.setattr(
        "gateway.proxy.httpx.AsyncClient",
        lambda **kw: _MockAsyncClient(_stream_chunks=stream_chunks),
    )
    monkeypatch.setattr(
        "gateway.proxy.get_catalog",
        lambda: SimpleNamespace(estimate_completion_usd=lambda *a, **k: 0.00001),
    )

    t0 = time.monotonic()
    resp = client.post(
        "/v1/chat/completions",
        headers={
            "X-FluxMeter-Customer-Id": "cust_kill",
            "Authorization": "Bearer sk-live",
        },
        json={
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": True,
        },
    )
    elapsed = time.monotonic() - t0
    assert resp.status_code == 200
    text = resp.text
    assert "stream_killed" in text or "fluxmeter_budget" in text
    assert '"output_tokens"' in text
    assert '"metered_usd"' in text
    assert resp.headers["X-FluxMeter-Reservation-Id"]
    assert float(resp.headers["X-FluxMeter-Reserved-Usd"]) > 0
    assert elapsed < 1.0
    assert UPSTREAM_CALLS["n"] == 1
    assert producer.messages


def test_proxy_only_no_track_sdk(gw, monkeypatch):
    """Usage recorded via Gateway only — no SDK track call."""
    client, r, producer = gw
    _setup_customer(r, "cust_proxy", balance=5.0)

    monkeypatch.setattr(
        "gateway.proxy.httpx.AsyncClient",
        lambda **kw: _MockAsyncClient(
            _json_response={
                "choices": [{"message": {"content": "proxy"}}],
                "usage": {"prompt_tokens": 5, "completion_tokens": 15},
            }
        ),
    )

    resp = client.post(
        "/v1/chat/completions",
        headers={
            "X-FluxMeter-Customer-Id": "cust_proxy",
            "Authorization": "Bearer sk-live",
        },
        json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "x"}]},
    )
    assert resp.status_code == 200
    assert producer.messages


def test_gateway_keeps_usage_in_durable_outbox_when_kafka_fails(gw, monkeypatch):
    client, r, producer = gw
    _setup_customer(r, "cust_outbox", balance=5.0)
    producer.error = RuntimeError("down")
    monkeypatch.setattr(
        "gateway.proxy.httpx.AsyncClient",
        lambda **kw: _MockAsyncClient(
            _json_response={
                "choices": [{"message": {"content": "proxy"}}],
                "usage": {"prompt_tokens": 5, "completion_tokens": 15},
            }
        ),
    )

    response = client.post(
        "/v1/chat/completions",
        headers={
            "X-FluxMeter-Customer-Id": "cust_outbox",
            "Authorization": "Bearer sk-live",
        },
        json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "x"}]},
    )

    assert response.status_code == 200
    assert r.zcard("gateway:outbox:pending") == 1


def test_non_stream_zero_usage_still_reconciles_reservation_via_flink(gw, monkeypatch):
    client, r, producer = gw
    _setup_customer(r, "cust_zero", balance=5.0)
    monkeypatch.setattr(
        "gateway.proxy.httpx.AsyncClient",
        lambda **kw: _MockAsyncClient(
            _json_response={"choices": [{"message": {"content": "ok"}}], "usage": {}}
        ),
    )

    response = client.post(
        "/v1/chat/completions",
        headers={
            "X-FluxMeter-Customer-Id": "cust_zero",
            "Authorization": "Bearer sk-live",
        },
        json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "x"}]},
    )

    assert response.status_code == 200
    assert len(producer.messages) == 1
    assert json.loads(producer.messages[0]["value"])["reservation"]["reservedUsd"] > 0


def test_stream_upstream_error_settles_reservation(gw, monkeypatch):
    client, r, producer = gw
    _setup_customer(r, "cust_stream_err", balance=10.0)
    monkeypatch.setattr(
        "gateway.proxy.httpx.AsyncClient",
        lambda **kw: _MockAsyncClient(
            _stream_status=500,
            _stream_body=b'{"error":"upstream"}',
        ),
    )

    resp = client.post(
        "/v1/chat/completions",
        headers={
            "X-FluxMeter-Customer-Id": "cust_stream_err",
            "Authorization": "Bearer sk-live",
        },
        json={
            "model": "gpt-4o-mini",
            "stream": True,
            "messages": [{"role": "user", "content": "hi"}],
        },
    )
    assert resp.status_code == 200  # SSE passthrough of error body
    assert float(r.get("budget:cust_stream_err:held_usd") or 0) == 0.0
    assert r.zcard("gateway:reservations:pending") == 0
    assert len(producer.messages) == 0


def test_health(gw):
    client, _, _ = gw
    assert client.get("/health").json()["status"] == "ok"


def test_poison_outbox_record_does_not_block_following_events(gw):
    _, r, producer = gw
    from gateway.outbox import PENDING_KEY, flush_once

    r.set("gateway:outbox:event:bad", "not-json")
    r.zadd(PENDING_KEY, {"bad": 1})
    assert flush_once(r, producer, "token-events", 1) is True
    assert r.zcard(PENDING_KEY) == 0
    assert r.llen("gateway:outbox:dead") == 1
