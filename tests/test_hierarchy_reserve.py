"""Unit tests for parent span reserve-confirm (Phase 4 hierarchy budgets)."""

from __future__ import annotations

import sys

import fakeredis
import pytest

sys.path.insert(0, "api")

from budget_ops import reconcile_hold, reserve_hold


@pytest.fixture
def r():
    return fakeredis.FakeRedis(decode_responses=True)


def _setup_customer(r, customer_id: str, balance: float = 10.0):
    r.set(f"budget:{customer_id}:balance_usd", str(balance))
    r.set(f"budget:{customer_id}:held_usd", "0")


def _setup_span_cap(r, span_id: str, max_cost: float, spent: float = 0.0):
    r.set(f"span:{span_id}:max_cost_usd", str(max_cost))
    r.set(f"span:{span_id}:cost_usd", str(spent))
    r.set(f"span:{span_id}:held_usd", "0")


class TestHierarchyReserve:
    def test_second_reserve_denied_when_over_parent_cap(self, r):
        _setup_customer(r, "cust_1", balance=10.0)
        _setup_span_cap(r, "span_parent", max_cost=1.0, spent=0.0)

        first = reserve_hold(r, "cust_1", 0.60, parent_span_id="span_parent")
        assert first["allowed"] is True

        second = reserve_hold(r, "cust_1", 0.60, parent_span_id="span_parent")
        assert second["allowed"] is False
        assert second["reason"] == "hierarchy_reserve"
        assert second["scope"] == "span"

    def test_reconcile_then_reserve_succeeds(self, r):
        _setup_customer(r, "cust_1", balance=10.0)
        _setup_span_cap(r, "span_parent", max_cost=1.0)

        reserve_hold(r, "cust_1", 0.60, parent_span_id="span_parent")
        reconcile_hold(r, "cust_1", 0.60, parent_span_id="span_parent")

        third = reserve_hold(r, "cust_1", 0.60, parent_span_id="span_parent")
        assert third["allowed"] is True

    def test_no_parent_span_id_unchanged(self, r):
        _setup_customer(r, "cust_1", balance=5.0)

        result = reserve_hold(r, "cust_1", 1.0)
        assert result["allowed"] is True
        assert "span_held_usd" not in result

    def test_parent_span_without_cap_skips_span_hold(self, r):
        _setup_customer(r, "cust_1", balance=5.0)
        r.set("span:orphan:cost_usd", "0")

        result = reserve_hold(r, "cust_1", 1.0, parent_span_id="orphan")
        assert result["allowed"] is True
        assert r.get("span:orphan:held_usd") is None
