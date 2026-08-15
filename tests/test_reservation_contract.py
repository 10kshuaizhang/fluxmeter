"""Reservation contract vectors + tenant budget key seam."""

from __future__ import annotations

import json
from pathlib import Path

import fakeredis
import pytest

from budget_ops import (
    expire_reservations,
    register_gateway_reservation,
    reserve_hold,
    settle_gateway_reservation,
)
from budget_gate import run_budget_check
from tenant_keys import budget_prefix, budget_prefix_for_read, budget_prefix_for_write


VECTORS = Path("docs/contracts/reservation-vectors.json")


def _held_key(customer_id: str, tenant_id: str | None = None) -> str:
    return f"{budget_prefix(tenant_id, customer_id)}:held_usd"


def test_reservation_contract_vectors():
    vectors = json.loads(VECTORS.read_text())
    for case in vectors:
        r = fakeredis.FakeRedis(decode_responses=True)
        for step in case["steps"]:
            op = step["op"]
            if op == "set_held":
                r.set(
                    _held_key(step["customer_id"], step.get("tenant_id")),
                    str(step["held_usd"]),
                )
            elif op == "open":
                register_gateway_reservation(
                    r,
                    step["reservation_id"],
                    customer_id=step["customer_id"],
                    reserved_usd=step["reserved_usd"],
                    parent_span_id=None,
                    expires_at=step.get("expires_at"),
                    tenant_id=step.get("tenant_id"),
                )
            elif op == "expire":
                assert expire_reservations(r, now=step["now"]) == step["expect_released"]
            elif op == "settle":
                assert settle_gateway_reservation(r, step["reservation_id"]) == pytest.approx(
                    step["expect_released"]
                )
            elif op == "assert_held":
                assert float(r.get(_held_key(step["customer_id"], step.get("tenant_id")))) == pytest.approx(
                    step["held_usd"]
                )
            else:
                raise AssertionError(f"unknown op {op}")


def test_python_budget_prefix_matches_java_strings():
    assert budget_prefix(None, "cust_1") == "budget:cust_1"
    assert budget_prefix("t1", "cust_1") == "tenant:t1:budget:cust_1"


def test_tenants_do_not_share_budget_or_gate():
    r = fakeredis.FakeRedis(decode_responses=True)
    r.set("tenant:t1:budget:cust_x:balance_usd", "10")
    r.set("tenant:t1:budget:cust_x:held_usd", "0")
    r.set("tenant:t2:budget:cust_x:balance_usd", "1")
    r.set("tenant:t2:budget:cust_x:held_usd", "0")

    gate_t1 = run_budget_check(r, "cust_x", 5.0, tenant_id="t1", increment_rate_limit=False)
    gate_t2 = run_budget_check(r, "cust_x", 5.0, tenant_id="t2", increment_rate_limit=False)
    assert gate_t1["allowed"] is True
    assert gate_t2["allowed"] is False

    hold = reserve_hold(r, "cust_x", 2.0, tenant_id="t1")
    assert hold["allowed"] is True
    assert float(r.get("tenant:t1:budget:cust_x:held_usd")) == pytest.approx(2.0)
    assert float(r.get("tenant:t2:budget:cust_x:held_usd") or 0) == 0.0


def test_read_fallback_to_legacy_budget_key():
    r = fakeredis.FakeRedis(decode_responses=True)
    r.set("budget:legacy:balance_usd", "42")
    assert budget_prefix_for_read(r, "tenant_a", "legacy") == "budget:legacy"
    assert budget_prefix_for_write("tenant_a", "legacy") == "tenant:tenant_a:budget:legacy"
