#!/usr/bin/env python3
"""Reseller MVP: per-downstream-customer usage / cost / period via curl-able API.

Path-3 wedge demo for AI API resellers / token gateways.

Self-check (no Docker stack)::

    PYTHONPATH=api python demos/reseller_usage_demo.py

Live Lite stack::

    make demo
    PYTHONPATH=api python demos/reseller_usage_demo.py --live
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "api"))

os.environ.setdefault("FLUXMETER_AUTH_OPTIONAL", "true")
os.environ.setdefault("FLUXMETER_LITE_MODE", "true")
os.environ.setdefault(
    "FLUXMETER_PRICING_CONFIG",
    os.path.join(ROOT, "config", "pricing.json"),
)

# Three downstream customers on a fictional reseller
DOWNSTREAM = (
    ("downstream_alice", "gpt-4o-mini", 1200, 400),
    ("downstream_bob", "gpt-4o-mini", 800, 200),
    ("downstream_carol", "gpt-4o", 500, 150),
)


def _ingest_body(customer_id: str, model_id: str, input_tokens: int, output_tokens: int) -> dict:
    return {
        "eventId": f"evt_{uuid.uuid4().hex}",
        "customerId": customer_id,
        "modelId": model_id,
        "provider": "openai",
        "inputTokens": input_tokens,
        "outputTokens": output_tokens,
        "timestamp": int(time.time() * 1000),
    }


def _self_check() -> None:
    import fakeredis
    from fastapi.testclient import TestClient

    import main as api_main
    from lite_aggregate_lua import LiteAggregator
    from pricing_loader import PricingCatalog, billing_period_month
    from rollup_worker import rollup_customer_minute

    fake = fakeredis.FakeRedis(decode_responses=True)
    catalog = PricingCatalog.load_from_file(
        os.environ["FLUXMETER_PRICING_CONFIG"]
    )
    agg = LiteAggregator(fake, catalog=catalog)

    api_main.LITE_MODE = True
    api_main._lite_aggregator = agg
    api_main.get_redis = lambda: fake  # type: ignore[assignment]
    api_main.get_lite_aggregator = lambda: agg  # type: ignore[assignment]

    client = TestClient(api_main.app)
    now_ms = int(time.time() * 1000)
    period = billing_period_month(now_ms)

    print("=== Reseller usage MVP (self-check) ===")
    print(f"period={period}")
    print()

    lines: list[dict] = []
    for cid, model, inp, out in DOWNSTREAM:
        resp = client.post("/ingest", json=_ingest_body(cid, model, inp, out))
        assert resp.status_code == 202, resp.text
        body = resp.json()
        assert body.get("status") == "ok", body
        rollup_customer_minute(fake, cid, int(time.time()))

        lifetime = client.get(f"/usage/customer/{cid}")
        assert lifetime.status_code == 200, lifetime.text
        period_resp = client.get(f"/usage/customer/{cid}/period/{period}")
        assert period_resp.status_code == 200, period_resp.text

        lt = lifetime.json()
        pb = period_resp.json()
        assert lt["total_tokens"] == inp + out
        assert pb["total_tokens"] == inp + out
        assert pb["event_count"] == 1
        assert lt["cost_usd"] > 0
        lines.append(
            {
                "customer_id": cid,
                "model": model,
                "period": period,
                "total_tokens": pb["total_tokens"],
                "cost_usd": round(pb["cost_usd"], 6),
                "event_count": pb["event_count"],
            }
        )

    print("Invoice-style lines (GET /usage/customer/{id}/period/{YYYY-MM}):")
    print(json.dumps(lines, indent=2))
    print()
    print("ok  three downstream customers — lifetime + period cost queryable")


def _live(api_base: str) -> None:
    import httpx

    from pricing_loader import billing_period_month

    period = billing_period_month(int(time.time() * 1000))
    print("=== Reseller usage MVP (live) ===")
    print(f"api={api_base} period={period}")
    print()

    lines: list[dict] = []
    for cid, model, inp, out in DOWNSTREAM:
        # unique ids so re-runs don't collide on idempotency
        live_cid = f"{cid}_{uuid.uuid4().hex[:6]}"
        resp = httpx.post(
            f"{api_base}/ingest",
            json=_ingest_body(live_cid, model, inp, out),
            timeout=15.0,
        )
        resp.raise_for_status()
        # wait briefly for rollup worker (60s interval) — flush via admin not available;
        # lifetime is immediate; period may 404 until rollup. Show lifetime always.
        lifetime = httpx.get(f"{api_base}/usage/customer/{live_cid}", timeout=10.0)
        lifetime.raise_for_status()
        lt = lifetime.json()
        period_resp = httpx.get(
            f"{api_base}/usage/customer/{live_cid}/period/{period}",
            timeout=10.0,
        )
        period_payload = period_resp.json() if period_resp.status_code == 200 else None
        lines.append(
            {
                "customer_id": live_cid,
                "lifetime_tokens": lt["total_tokens"],
                "lifetime_cost_usd": lt["cost_usd"],
                "period": period,
                "period_usage": period_payload,
                "note": None
                if period_payload
                else "period 404 until rollup worker flushes (~60s); lifetime is immediate",
            }
        )

    print(json.dumps(lines, indent=2))
    print()
    print("ok  live ingest + lifetime usage (period after rollup)")


def main() -> None:
    parser = argparse.ArgumentParser(description="FluxMeter reseller usage demo")
    parser.add_argument("--live", action="store_true", help="Hit a running Lite API")
    parser.add_argument("--api", default=os.getenv("FLUXMETER_API", "http://127.0.0.1:8000"))
    args = parser.parse_args()

    if args.live:
        _live(args.api)
        return

    t0 = time.monotonic()
    _self_check()
    print(f"done in {time.monotonic() - t0:.2f}s")


if __name__ == "__main__":
    main()
