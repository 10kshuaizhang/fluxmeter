"""Tests for metadata dimension counters."""

from __future__ import annotations

import sys

import fakeredis
import pytest

sys.path.insert(0, "api")

from billing_dims import increment_dims, read_dim_usage, validate_metadata


@pytest.fixture
def r():
    return fakeredis.FakeRedis(decode_responses=True)


def test_validate_metadata_whitelist():
    with pytest.raises(ValueError, match="whitelist"):
        validate_metadata({"unknown": "x"})


def test_increment_and_read_dim(r):
    meta = validate_metadata({"room_id": "room-42"})
    increment_dims(r, meta, cost_usd=1.25, event_ts_ms=1700000000000)
    data = read_dim_usage(r, "room_id", "room-42")
    assert data is not None
    assert data["cost_usd"] == pytest.approx(1.25)
    assert data["event_count"] == 1
