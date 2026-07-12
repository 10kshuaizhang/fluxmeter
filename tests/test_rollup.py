"""Rollup worker tests — verify counter compaction logic.

Run with: pytest tests/test_rollup.py -v
Requires: Redis running on localhost:6379
"""

import time
import uuid

import pytest
import redis


@pytest.fixture
def r():
    try:
        conn = redis.Redis(host="localhost", port=6379, decode_responses=True)
        conn.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not available on localhost:6379")
    yield conn


def _set_buf(customer_key: str, r: redis.Redis, **fields) -> None:
    for name, val in fields.items():
        r.set(f"{customer_key}:buf:{name}", str(val))


class TestRollupLogic:
    """Test the rollup compaction logic directly."""

    def test_minute_rollup_sums_correctly(self, r):
        """Per-customer buffer counters roll into minute buckets."""
        cid = f"test_rollup_{uuid.uuid4().hex[:8]}"
        customer_key = f"customer:{cid}"

        _set_buf(
            customer_key,
            r,
            input_tokens=5000,
            output_tokens=2000,
            total_tokens=7000,
            event_count=10,
            cost_usd=0.5,
        )

        import sys
        sys.path.insert(0, "api")
        from rollup_worker import rollup_customer_minute

        minute_key = rollup_customer_minute(r, cid, int(time.time()))

        assert r.hget(minute_key, "input_tokens") == "5000"
        assert r.hget(minute_key, "output_tokens") == "2000"
        assert r.hget(minute_key, "event_count") == "10"

    def test_rollup_preserves_lifetime_counters(self, r):
        """After rollup, lifetime counters are preserved; buf is zeroed."""
        cid = f"test_reset_{uuid.uuid4().hex[:8]}"
        customer_key = f"customer:{cid}"

        r.set(f"{customer_key}:input_tokens", "3000")
        r.set(f"{customer_key}:event_count", "5")
        _set_buf(customer_key, r, input_tokens=3000, event_count=5)

        import sys
        sys.path.insert(0, "api")
        from rollup_worker import rollup_customer_minute

        rollup_customer_minute(r, cid, int(time.time()))

        assert int(r.get(f"{customer_key}:input_tokens") or 0) == 3000
        assert int(r.get(f"{customer_key}:event_count") or 0) == 5
        assert int(r.get(f"{customer_key}:buf:input_tokens") or 0) == 0
        assert int(r.get(f"{customer_key}:buf:event_count") or 0) == 0

    def test_rollup_double_interval(self, r):
        """Two rollup intervals accumulate month bucket and lifetime totals."""
        cid = f"test_double_{uuid.uuid4().hex[:8]}"
        customer_key = f"customer:{cid}"

        import sys
        sys.path.insert(0, "api")
        from pricing_loader import billing_period_month
        from rollup_worker import rollup_customer_minute
        from usage_buckets import read_usage_bucket, rollup_month_key

        r.set(f"{customer_key}:input_tokens", "1000")
        r.set(f"{customer_key}:event_count", "1")
        _set_buf(customer_key, r, input_tokens=1000, total_tokens=1000, event_count=1, cost_usd=0.1)

        now = int(time.time())
        rollup_customer_minute(r, cid, now)

        r.incrby(f"{customer_key}:input_tokens", 2000)
        r.incrby(f"{customer_key}:event_count", 2)
        _set_buf(customer_key, r, input_tokens=2000, total_tokens=2000, event_count=2, cost_usd=0.2)

        rollup_customer_minute(r, cid, now + 60)

        month = billing_period_month(now * 1000)
        data = read_usage_bucket(r, rollup_month_key(cid, month))
        assert data is not None
        assert data["input_tokens"] == 3000
        assert data["event_count"] == 3
        assert int(r.get(f"{customer_key}:input_tokens") or 0) == 3000
        assert int(r.get(f"{customer_key}:event_count") or 0) == 3

    def test_minute_buckets_have_ttl(self, r):
        """Minute buckets expire after 24 hours."""
        cid = f"test_ttl_{uuid.uuid4().hex[:8]}"
        customer_key = f"customer:{cid}"
        _set_buf(customer_key, r, input_tokens=1000, total_tokens=1000, event_count=1)

        import sys
        sys.path.insert(0, "api")
        from rollup_worker import rollup_customer_minute

        minute_key = rollup_customer_minute(r, cid, int(time.time()))

        ttl = r.ttl(minute_key)
        assert 86000 < ttl <= 86400

    def test_day_rollup_bucket(self, r):
        """Daily rollup hash is populated alongside minute/month."""
        cid = f"test_day_{uuid.uuid4().hex[:8]}"
        customer_key = f"customer:{cid}"
        _set_buf(
            customer_key,
            r,
            input_tokens=2000,
            total_tokens=2000,
            event_count=2,
            cost_usd=0.2,
        )

        import sys
        sys.path.insert(0, "api")
        from rollup_worker import rollup_customer_minute
        from usage_buckets import read_usage_bucket, rollup_day_key
        from pricing_loader import billing_period_day

        rollup_customer_minute(r, cid, int(time.time()))
        day = billing_period_day(int(time.time() * 1000))
        data = read_usage_bucket(r, rollup_day_key(cid, day))
        assert data is not None
        assert data["input_tokens"] == 2000
        assert data["event_count"] == 2

    def test_month_rollup_bucket(self, r):
        """Monthly rollup hash is populated alongside minute/day."""
        cid = f"test_month_{uuid.uuid4().hex[:8]}"
        customer_key = f"customer:{cid}"
        _set_buf(
            customer_key,
            r,
            input_tokens=3000,
            output_tokens=1000,
            total_tokens=4000,
            event_count=3,
            cost_usd=0.3,
        )

        import sys
        sys.path.insert(0, "api")
        from rollup_worker import rollup_customer_minute
        from usage_buckets import read_usage_bucket, rollup_month_key
        from pricing_loader import billing_period_month

        rollup_customer_minute(r, cid, int(time.time()))
        month = billing_period_month(int(time.time() * 1000))
        data = read_usage_bucket(r, rollup_month_key(cid, month))
        assert data is not None
        assert data["input_tokens"] == 3000
        assert data["event_count"] == 3

    def test_legacy_pending_drain(self, r):
        """Pre-buffer deploy pending slice rolls once without clearing lifetime."""
        cid = f"test_legacy_{uuid.uuid4().hex[:8]}"
        customer_key = f"customer:{cid}"

        r.set(f"{customer_key}:input_tokens", "1500")
        r.set(f"{customer_key}:event_count", "2")

        import sys
        sys.path.insert(0, "api")
        from rollup_worker import rollup_customer_minute
        from usage_buckets import read_usage_bucket, rollup_month_key
        from pricing_loader import billing_period_month

        rollup_customer_minute(r, cid, int(time.time()))

        month = billing_period_month(int(time.time() * 1000))
        data = read_usage_bucket(r, rollup_month_key(cid, month))
        assert data is not None
        assert data["input_tokens"] == 1500
        assert int(r.get(f"{customer_key}:input_tokens") or 0) == 1500
