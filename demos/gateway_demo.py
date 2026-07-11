#!/usr/bin/env python3
"""Phase G Gateway demo: proxy-only ingest, budget deny, mid-stream kill.

Self-check (mock upstream, no stack)::

    PYTHONPATH=api python demos/gateway_demo.py

Live Lite stack (optional)::

    make demo
    OPENAI_API_KEY=sk-... PYTHONPATH=api python demos/gateway_demo.py --live
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "api"))

os.environ.setdefault("FLUXMETER_AUTH_OPTIONAL", "true")
os.environ.setdefault("FLUXMETER_LITE_MODE", "true")


def _self_check_deny() -> None:
    import fakeredis
    from fastapi.testclient import TestClient
    import gateway.deps as deps

    fake = fakeredis.FakeRedis(decode_responses=True)
    import gateway.routes as routes

    deps._lite_aggregator = None
    deps.get_redis = lambda: fake
    routes.get_redis = lambda: fake
    fake.set("budget:cust_demo:balance_usd", "0")
    fake.set("budget:cust_demo:held_usd", "0")

    calls = {"n": 0}

    class _NoUpstream:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, *a, **k):
            calls["n"] += 1
            raise AssertionError("upstream should not be called")

    import gateway.proxy as proxy

    proxy.httpx.AsyncClient = lambda **kw: _NoUpstream()

    from gateway_app import app

    client = TestClient(app)
    resp = client.post(
        "/v1/chat/completions",
        headers={"X-FluxMeter-Customer-Id": "cust_demo", "Authorization": "Bearer sk-x"},
        json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert resp.status_code == 402, resp.text
    assert calls["n"] == 0
    print("ok  budget deny blocks upstream")


def _self_check_ingest() -> None:
    import fakeredis
    from fastapi.testclient import TestClient
    import gateway.deps as deps

    fake = fakeredis.FakeRedis(decode_responses=True)
    import gateway.routes as routes

    deps._lite_aggregator = None
    deps.get_redis = lambda: fake
    routes.get_redis = lambda: fake
    fake.set("budget:cust_demo:balance_usd", "10")
    fake.set("budget:cust_demo:held_usd", "0")

    class _MockClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, *a, **k):
            class R:
                status_code = 200

                @staticmethod
                def json():
                    return {
                        "choices": [{"message": {"content": "ok"}}],
                        "usage": {"prompt_tokens": 12, "completion_tokens": 8},
                    }

            return R()

    import gateway.proxy as proxy

    proxy.httpx.AsyncClient = lambda **kw: _MockClient()

    from gateway_app import app

    client = TestClient(app)
    resp = client.post(
        "/v1/chat/completions",
        headers={"X-FluxMeter-Customer-Id": "cust_demo", "Authorization": "Bearer sk-x"},
        json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert resp.status_code == 200
    assert fake.get("customer:cust_demo:event_count") is not None
    print("ok  proxy-only ingest (no SDK track)")


def _live(api_base: str) -> None:
    import httpx

    key = os.getenv("OPENAI_API_KEY")
    if not key:
        print("Set OPENAI_API_KEY for --live", file=sys.stderr)
        sys.exit(1)
    gw = os.getenv("FLUXMETER_GATEWAY", "http://127.0.0.1:8080")
    resp = httpx.post(
        f"{gw}/v1/chat/completions",
        headers={
            "X-FluxMeter-Customer-Id": "cust_demo",
            "Authorization": f"Bearer {key}",
        },
        json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "Say hi in 3 words"}]},
        timeout=60.0,
    )
    print("status", resp.status_code)
    print(json.dumps(resp.json(), indent=2)[:500])
    usage = httpx.get(f"{api_base}/usage/cust_demo", timeout=10.0)
    print("usage", usage.json())


def main() -> None:
    parser = argparse.ArgumentParser(description="FluxMeter Gateway demo")
    parser.add_argument("--live", action="store_true", help="Call live gateway + OpenAI")
    parser.add_argument("--api", default=os.getenv("FLUXMETER_API", "http://127.0.0.1:8000"))
    args = parser.parse_args()

    if args.live:
        _live(args.api)
        return

    t0 = time.monotonic()
    _self_check_deny()
    _self_check_ingest()
    print(f"ok  gateway self-check ({time.monotonic() - t0:.2f}s)")


if __name__ == "__main__":
    main()
