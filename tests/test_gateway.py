"""Gateway proxy tests — mock upstream, no live OpenAI."""

from __future__ import annotations

import json
import os
import sys
import time

import fakeredis
import pytest

os.environ["FLUXMETER_AUTH_OPTIONAL"] = "true"
os.environ["FLUXMETER_LITE_MODE"] = "true"
os.environ["GATEWAY_UPSTREAM_API_KEY"] = "sk-test-upstream"

sys.path.insert(0, "api")

from fastapi.testclient import TestClient

UPSTREAM_CALLS = {"n": 0}


def _reset_upstream():
    UPSTREAM_CALLS["n"] = 0


def _setup_customer(r, customer_id: str, balance: float):
    r.set(f"budget:{customer_id}:balance_usd", str(balance))
    r.set(f"budget:{customer_id}:held_usd", "0")


class _MockStreamResponse:
    status_code = 200

    def __init__(self, chunks: list[bytes]):
        self._chunks = chunks

    async def __aenter__(self):
        UPSTREAM_CALLS["n"] += 1
        return self

    async def __aexit__(self, *args):
        return False

    async def aiter_bytes(self):
        for chunk in self._chunks:
            yield chunk

    async def aread(self):
        return b""


class _MockAsyncClient:
    def __init__(self, **kwargs):
        self._stream_chunks = kwargs.pop("_stream_chunks", [])
        self._json_response = kwargs.pop("_json_response", None)

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
        return _MockStreamResponse(self._stream_chunks)


@pytest.fixture
def gw(monkeypatch):
    _reset_upstream()
    fake = fakeredis.FakeRedis(decode_responses=True)
    import gateway.deps as deps

    deps._lite_aggregator = None
    monkeypatch.setattr(deps, "get_redis", lambda: fake)
    monkeypatch.setattr("gateway.routes.get_redis", lambda: fake)

    def client_factory(**kwargs):
        return _MockAsyncClient(**kwargs)

    monkeypatch.setattr("gateway.proxy.httpx.AsyncClient", client_factory)
    from gateway_app import app

    return TestClient(app), fake


def test_check_denies_before_upstream(gw):
    client, r = gw
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
    client, r = gw
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
    cost = float(r.get("customer:cust_ok:cost_usd") or 0)
    assert cost > 0
    assert int(r.get("customer:cust_ok:input_tokens") or 0) == 100
    assert int(r.get("customer:cust_ok:output_tokens") or 0) == 50


def test_stream_kill_under_1s(gw, monkeypatch):
    client, r = gw
    _setup_customer(r, "cust_kill", balance=10.0)

    chunk = json.dumps({"choices": [{"delta": {"content": "x" * 80}}]})
    stream_chunks = [
        f"data: {chunk}\n\n".encode() for _ in range(30)
    ] + [b"data: [DONE]\n\n"]

    monkeypatch.setattr(
        "gateway.proxy.httpx.AsyncClient",
        lambda **kw: _MockAsyncClient(_stream_chunks=stream_chunks),
    )
    monkeypatch.setattr("gateway.proxy.estimate_request_cost", lambda *a, **k: 0.00001)

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
    assert elapsed < 1.0
    assert UPSTREAM_CALLS["n"] == 1
    cost = float(r.get("customer:cust_kill:cost_usd") or 0)
    assert cost > 0


def test_proxy_only_no_track_sdk(gw, monkeypatch):
    """Usage recorded via Gateway only — no SDK track call."""
    client, r = gw
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
    assert r.get("customer:cust_proxy:event_count") is not None


def test_health(gw):
    client, _ = gw
    assert client.get("/health").json()["status"] == "ok"
