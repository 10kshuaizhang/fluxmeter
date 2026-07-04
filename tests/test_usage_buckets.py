"""Usage bucket + session aggregation tests (requires local Redis)."""

from __future__ import annotations

import sys
import time
import uuid

import pytest
import redis

sys.path.insert(0, "api")

from pricing_loader import billing_period_day, billing_period_month
from usage_buckets import (
    increment_session,
    read_session,
    read_usage_bucket,
    rollup_day_key,
    rollup_month_key,
)


@pytest.fixture
def r():
    try:
        conn = redis.Redis(host="localhost", port=6379, decode_responses=True)
        conn.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not available on localhost:6379")
    yield conn


class TestBillingPeriodDay:
    def test_day_format(self):
        # 2026-07-05 00:00 UTC
        ts = 1783209600000 - (1783209600000 % 86400000)  # fallback: use known
        assert billing_period_day(int(time.time() * 1000)).count("-") == 2
        assert len(billing_period_month(int(time.time() * 1000))) == 7


class TestUsageBuckets:
    def test_read_empty_bucket(self, r):
        assert read_usage_bucket(r, f"rollup:missing:{uuid.uuid4().hex}:d:2099-01-01") is None

    def test_session_increment_and_read(self, r):
        sid = f"sess_{uuid.uuid4().hex[:8]}"
        cid = f"cust_{uuid.uuid4().hex[:8]}"
        increment_session(
            r, cid, sid,
            input_tokens=100, output_tokens=50, total_tokens=150, cost_usd=0.01,
        )
        data = read_session(r, sid)
        assert data is not None
        assert data["customer_id"] == cid
        assert data["total_tokens"] == 150
        assert data["event_count"] == 1
        assert abs(data["cost_usd"] - 0.01) < 1e-6

    def test_rollup_keys(self):
        assert rollup_month_key("u1", "2026-07") == "rollup:u1:period:2026-07"
        assert rollup_day_key("u1", "2026-07-05") == "rollup:u1:d:2026-07-05"
