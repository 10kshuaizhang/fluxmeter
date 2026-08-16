#!/usr/bin/env python3
"""FluxMeter full demo: metering via Custody, Intelligence, Gateway, wrap.

Self-check (no stack)::

    make demo-run
    # or: PYTHONPATH=api:sdk/python python demos/full_demo.py

Live tour (requires ``make demo``)::

    make demo-run-live
    # optional: OPENAI_API_KEY=sk-... for gateway live call
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "api"))
sys.path.insert(0, os.path.join(ROOT, "sdk", "python"))

os.environ.setdefault("FLUXMETER_AUTH_OPTIONAL", "true")

SECTIONS = ("metering", "intel", "gateway", "wrap", "all")


def _seed_intelligence_data(r, customer_id: str, period: str, baseline: str) -> None:
    from usage_buckets import model_period_key, period_customers_index, rollup_day_key, rollup_month_key

    for p, cost in [(baseline, "100"), (period, "140")]:
        r.hset(
            rollup_month_key(customer_id, p),
            mapping={
                "cost_usd": cost,
                "event_count": "10",
                "total_tokens": "50000",
                "input_tokens": "30000",
                "output_tokens": "20000",
            },
        )
        r.sadd(period_customers_index(p), customer_id)
    r.hset(
        model_period_key(customer_id, "gpt-4o", period),
        mapping={
            "cost_usd": "100",
            "event_count": "5",
            "total_tokens": "25000",
            "input_tokens": "15000",
            "output_tokens": "10000",
        },
    )
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    r.hset(
        rollup_day_key(customer_id, today),
        mapping={
            "cost_usd": "12.5",
            "event_count": "2",
            "total_tokens": "5000",
            "input_tokens": "3000",
            "output_tokens": "2000",
        },
    )


def self_check_metering() -> None:
    """Custody accept path with fake Redis + fake Kafka producer."""
    import fakeredis
    from ingestion import accept
    from pricing_loader import PricingCatalog, reload_catalog

    class _Prod:
        def produce(self, topic, *, key, value, on_delivery=None):
            if on_delivery:
                on_delivery(None, None)

        def poll(self, timeout):
            return 0

    r = fakeredis.FakeRedis(decode_responses=True)
    reload_catalog(PricingCatalog.load_from_file())
    cid = f"demo_{uuid.uuid4().hex[:8]}"
    event = {
        "customerId": cid,
        "modelId": "gpt-4o-mini",
        "inputTokens": 1200,
        "outputTokens": 450,
        "sessionId": "sess_demo",
        "parentSpanId": "span_demo",
        "eventId": str(uuid.uuid4()),
    }
    result = accept(
        r,
        _Prod(),
        event,
        tenant_id=None,
        api_key_id=None,
        topic="token-events",
        quarantine_topic="token-events-quarantine",
        timeout_seconds=1.0,
    )
    assert result["status"] == "accepted", result
    assert r.get(f"ingest:event:{event['eventId']}").startswith("accepted:")
    print("ok  metering: Custody accept claims event identity")


def self_check_intel() -> None:
    import fakeredis
    from intelligence.forecast import compute_forecast
    from intelligence.root_cause import analyze_root_cause

    r = fakeredis.FakeRedis(decode_responses=True)
    period = datetime.now(timezone.utc).strftime("%Y-%m")
    baseline = "2026-06"
    cid = "demo_intel"
    _seed_intelligence_data(r, cid, period, baseline)
    r.set(f"budget:{cid}:initial_balance_usd", "200")

    fc = compute_forecast(r, period=period, scope=f"customer:{cid}")
    assert fc.mtd_cost_usd >= 0
    report = analyze_root_cause(r, period=period, baseline_period=baseline)
    assert report is not None
    print("ok  intel: forecast + root-cause on seeded rollups")


def self_check_gateway() -> None:
    from gateway.pricing_estimate import estimate_request_cost

    cost = estimate_request_cost("gpt-4o-mini", 256)
    assert cost > 0
    print("ok  gateway: catalog-backed estimate")


def self_check_wrap() -> None:
    print("ok  wrap: skipped in offline self-check (needs live API)")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--section", choices=SECTIONS, default="all")
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    if args.live:
        print("live mode: use make demo-run-live against a running stack")
        return
    section = args.section
    if section in ("metering", "all"):
        self_check_metering()
    if section in ("intel", "all"):
        self_check_intel()
    if section in ("gateway", "all"):
        self_check_gateway()
    if section in ("wrap", "all"):
        self_check_wrap()


if __name__ == "__main__":
    main()
