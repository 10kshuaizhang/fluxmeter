"""Tests for per-key API key daily/monthly budgets."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, "api")

from auth import check_api_key_budget, set_api_key_budget


def test_daily_cap_denies():
    r = MagicMock()
    r.get.side_effect = lambda k: {
        "apikey:meta:key1": '{"customer_id":"c1","daily_budget_usd":1.0}',
        "apikey:key1:spent:d:2026-07-11": "0.8",
    }.get(k)

    import auth as auth_mod
    import pricing_loader as pl

    old_day = pl.billing_period_day
    pl.billing_period_day = lambda _ms: "2026-07-11"
    try:
        deny = check_api_key_budget(r, "key1", 0.3)
    finally:
        pl.billing_period_day = old_day

    assert deny is not None
    assert deny["reason"] == "api_key_daily_budget"


def test_set_api_key_budget_updates_meta():
    r = MagicMock()
    r.get.return_value = '{"customer_id":"c1","revoked":false}'

    import auth as auth_mod

    old_redis = auth_mod._redis
    auth_mod._redis = lambda: r
    try:
        out = set_api_key_budget("key1", daily_budget_usd=5.0, monthly_budget_usd=100.0)
    finally:
        auth_mod._redis = old_redis

    assert out["daily_budget_usd"] == 5.0
    r.set.assert_called()
