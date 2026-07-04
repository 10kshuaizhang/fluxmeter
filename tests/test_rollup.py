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


class TestRollupLogic:
    """Test the rollup compaction logic directly."""

    def test_minute_rollup_sums_correctly(self, r):
        """Per-customer counters roll into minute buckets."""
        cid = f"test_rollup_{uuid.uuid4().hex[:8]}"
        customer_key = f"customer:{cid}"

        # Simulate accumulated counters
        r.set(f"{customer_key}:input_tokens", "5000")
        r.set(f"{customer_key}:output_tokens", "2000")
        r.set(f"{customer_key}:total_tokens", "7000")
        r.set(f"{customer_key}:event_count", "10")
        r.set(f"{customer_key}:cost_usd", "0.5")

        # Import and run rollup
        import sys
        sys.path.insert(0, "api")
        from rollup_worker import rollup_customer_minute

        minute_key = rollup_customer_minute(r, cid, int(time.time()))

        # Minute bucket exists with correct values
        assert r.hget(minute_key, "input_tokens") == "5000"
        assert r.hget(minute_key, "output_tokens") == "2000"
        assert r.hget(minute_key, "event_count") == "10"

    def test_rollup_resets_live_counters(self, r):
        """After rollup, live counters are zeroed."""
        cid = f"test_reset_{uuid.uuid4().hex[:8]}"
        customer_key = f"customer:{cid}"

        r.set(f"{customer_key}:input_tokens", "3000")
        r.set(f"{customer_key}:event_count", "5")

        import sys
        sys.path.insert(0, "api")
        from rollup_worker import rollup_customer_minute

        rollup_customer_minute(r, cid, int(time.time()))

        # Live counters zeroed (new events start from 0)
        assert int(r.get(f"{customer_key}:input_tokens") or 0) == 0
        assert int(r.get(f"{customer_key}:event_count") or 0) == 0

    def test_minute_buckets_have_ttl(self, r):
        """Minute buckets expire after 24 hours."""
        cid = f"test_ttl_{uuid.uuid4().hex[:8]}"
        customer_key = f"customer:{cid}"
        r.set(f"{customer_key}:input_tokens", "1000")
        r.set(f"{customer_key}:event_count", "1")

        import sys
        sys.path.insert(0, "api")
        from rollup_worker import rollup_customer_minute

        minute_key = rollup_customer_minute(r, cid, int(time.time()))

        ttl = r.ttl(minute_key)
        assert 86000 < ttl <= 86400  # ~24h
