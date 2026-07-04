"""Tier pricing E2E — Lite ingest path (Redis required).

Run: PRICING_FILE=contrib/pricing/tiered-example.json pytest tests/test_tier_e2e.py -v
For HTTP path, API must be started with same PRICING_FILE.
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import redis

sys.path.insert(0, "api")

from lite_aggregate_lua import LiteAggregator  # noqa: E402
from pricing_loader import PricingCatalog, period_volume_key, reload_catalog  # noqa: E402

API = os.getenv("FLUXMETER_API", "http://127.0.0.1:8000")
TIERED = Path("contrib/pricing/tiered-example.json")


@pytest.fixture
def r():
    try:
        conn = redis.Redis(host="localhost", port=6379, decode_responses=True)
        conn.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not available")
    return conn


@pytest.fixture
def tiered_catalog():
    catalog = PricingCatalog(json.loads(TIERED.read_text()))
    reload_catalog(catalog)
    return catalog


def _utc_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


class TestTierPricingE2E:
    """Integration: multi-event volume tier via LiteAggregator (Flink uses same algebra)."""

    def teardown_method(self):
        reload_catalog(PricingCatalog.load_from_file())

    def test_two_events_cross_volume_tier(self, r, tiered_catalog):
        agg = LiteAggregator(r, catalog=tiered_catalog)
        cid = f"e2e_vol_{uuid.uuid4().hex[:8]}"
        ts = _utc_ms()
        period_key = period_volume_key(None, cid, "gpt-4o-mini", ts)

        # Event 1: 9M tokens → tier-1 pricing
        e1 = {
            "customerId": cid,
            "modelId": "gpt-4o-mini",
            "inputTokens": 9_000_000,
            "outputTokens": 0,
            "eventId": str(uuid.uuid4()),
            "timestamp": ts,
        }
        r1 = agg.aggregate(e1)
        assert r1["status"] == "ok"
        assert r1["cost_usd"] == pytest.approx(1.35, rel=1e-4)  # 9M × 0.15/M

        # Event 2: 2M tokens → starts at 9M → tier-1 for whole event (volume mode)
        e2 = {
            "customerId": cid,
            "modelId": "gpt-4o-mini",
            "inputTokens": 2_000_000,
            "outputTokens": 0,
            "eventId": str(uuid.uuid4()),
            "timestamp": ts,
        }
        r2 = agg.aggregate(e2)
        assert r2["status"] == "ok"
        assert r2["cost_usd"] == pytest.approx(0.30, rel=1e-4)
        assert int(r.get(period_key) or 0) == 11_000_000

    @pytest.mark.skipif(
        os.getenv("FLUXMETER_LITE_API") != "1",
        reason="Set FLUXMETER_LITE_API=1 with API on tiered PRICING_FILE",
    )
    def test_http_ingest_tier_cost(self):
        """Optional: HTTP ingest when API runs with tiered catalog."""
        cid = f"http_tier_{uuid.uuid4().hex[:8]}"
        event = {
            "customerId": cid,
            "modelId": "gpt-4o-mini",
            "inputTokens": 1_000_000,
            "outputTokens": 0,
            "eventId": str(uuid.uuid4()),
        }
        resp = httpx.post(f"{API}/ingest", json=event, timeout=5.0)
        assert resp.status_code == 202
        body = resp.json()
        assert body.get("status") == "ok"
