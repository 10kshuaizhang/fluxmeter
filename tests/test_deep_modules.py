"""Interface tests for the deep Budget and Reservation modules."""

from __future__ import annotations

import sys

import fakeredis
import pytest
import redis

sys.path.insert(0, "api")

from budget import Budget
from reservation import Reservation


class BrokenRedis:
    def __getattr__(self, _name):
        def unavailable(*_args, **_kwargs):
            raise redis.ConnectionError("offline")

        return unavailable


def test_budget_cache_and_rate_limit_are_tenant_scoped():
    r = fakeredis.FakeRedis(decode_responses=True)
    cache = {}
    clock = lambda: 120.0
    tenant_a = Budget(r, "tenant-a", cache=cache, clock=clock)
    tenant_b = Budget(r, "tenant-b", cache=cache, clock=clock)
    tenant_a.configure("same-customer", 10, max_rpm=2)
    tenant_b.configure("same-customer", 1, max_rpm=2)

    assert tenant_a.check("same-customer", 5)["allowed"] is True
    assert tenant_b.check("same-customer", 5)["allowed"] is False
    assert r.get("tenant:tenant-a:ratelimit:same-customer:2") == "1"
    assert r.get("tenant:tenant-b:ratelimit:same-customer:2") is None

    offline_a = Budget(BrokenRedis(), "tenant-a", cache=cache, clock=clock)
    offline_b = Budget(BrokenRedis(), "tenant-b", cache=cache, clock=clock)
    assert offline_a.check("same-customer", 5)["allowed"] is True
    assert offline_b.check("same-customer", 5)["allowed"] is False


def test_budget_hierarchy_check_counts_existing_holds():
    r = fakeredis.FakeRedis(decode_responses=True)
    budget = Budget(r, "t1", cache={})
    budget.configure("c1", 10)
    r.set("tenant:t1:span:run-1:max_cost_usd", "1")
    r.set("tenant:t1:span:run-1:cost_usd", "0.4")
    r.set("tenant:t1:span:run-1:held_usd", "0.5")

    decision = budget.check(
        "c1", 0.2, parent_span_id="run-1", count_request=False
    )
    assert decision["allowed"] is False
    assert decision["reason"] == "hierarchy_cap"


def test_budget_package_is_tenant_scoped():
    r = fakeredis.FakeRedis(decode_responses=True)
    Budget(r, "t1", cache={}).set_package("same", 100)
    Budget(r, "t2", cache={}).set_package("same", 7)

    assert Budget(r, "t1", cache={}).package_balance("same")["tokens_remaining"] == 100
    assert Budget(r, "t2", cache={}).package_balance("same")["tokens_remaining"] == 7


def test_reservation_open_atomically_holds_and_registers():
    r = fakeredis.FakeRedis(decode_responses=True)
    Budget(r, "t1", cache={}).configure("c1", 10)
    reservations = Reservation(r, "t1", clock=lambda: 100)

    opened = reservations.open(
        "r1", customer_id="c1", estimated_cost_usd=2, expires_at=200
    )
    assert opened["allowed"] is True
    assert float(r.get("tenant:t1:budget:c1:held_usd")) == 2
    assert r.hget("reservation:r1", "held_key") == "tenant:t1:budget:c1:held_usd"
    assert r.zscore("gateway:reservations:pending", "r1") == 200

    replay = reservations.open(
        "r1", customer_id="c1", estimated_cost_usd=2, expires_at=300
    )
    assert replay["idempotent"] is True
    assert float(r.get("tenant:t1:budget:c1:held_usd")) == 2

    conflict = reservations.open(
        "r1", customer_id="c1", estimated_cost_usd=3, expires_at=300
    )
    assert conflict == {
        "allowed": False,
        "reason": "reservation_conflict",
        "conflict": True,
    }


def test_reservation_settle_and_expire_are_idempotent():
    r = fakeredis.FakeRedis(decode_responses=True)
    Budget(r, None, cache={}).configure("c1", 5)
    reservations = Reservation(r, None, clock=lambda: 100)
    reservations.open("settle", customer_id="c1", estimated_cost_usd=1)
    assert reservations.settle("settle") == pytest.approx(1)
    assert reservations.settle("settle") == 0

    reservations.open(
        "expire", customer_id="c1", estimated_cost_usd=0.5, expires_at=50
    )
    assert reservations.expire_due(now=60) == 1
    assert reservations.expire_due(now=60) == 0
    assert float(r.get("budget:c1:held_usd")) == 0


def test_ingested_reservation_is_not_released_by_reconcile():
    r = fakeredis.FakeRedis(decode_responses=True)
    Budget(r, None, cache={}).configure("c1", 1.0)
    reservations = Reservation(r, None, clock=lambda: 100)
    opened = reservations.open("hold-1", customer_id="c1", estimated_cost_usd=0.30)
    assert opened["allowed"] is True
    assert float(r.get("budget:c1:held_usd")) == pytest.approx(0.30)

    assert reservations.mark_ingested("hold-1") is True
    result = reservations.reconcile("c1", 0.30, reservation_id="hold-1")
    assert result["released_usd"] == pytest.approx(0.0)
    assert result.get("deferred") is True
    assert float(r.get("budget:c1:held_usd")) == pytest.approx(0.30)
    assert r.exists("reservation:hold-1")

    amount = reservations.reconcile("c1", 0.30)
    assert amount["released_usd"] == pytest.approx(0.0)
    assert float(r.get("budget:c1:held_usd")) == pytest.approx(0.30)


def test_unused_named_hold_still_releases_on_reconcile():
    r = fakeredis.FakeRedis(decode_responses=True)
    Budget(r, None, cache={}).configure("c1", 1.0)
    reservations = Reservation(r, None, clock=lambda: 100)
    reservations.open("unused", customer_id="c1", estimated_cost_usd=0.30)
    result = reservations.reconcile("c1", 0.30, reservation_id="unused")
    assert result["released_usd"] == pytest.approx(0.30)
    assert float(r.get("budget:c1:held_usd")) == pytest.approx(0.0)


def test_named_holds_cap_a_runaway_reserve_loop():
    r = fakeredis.FakeRedis(decode_responses=True)
    Budget(r, None, cache={}).configure("runaway", 0.30)
    reservations = Reservation(r, None, clock=lambda: 100)
    admitted = 0
    for i in range(50):
        hold = reservations.open(
            f"loop-{i}", customer_id="runaway", estimated_cost_usd=0.03
        )
        if not hold.get("allowed"):
            break
        admitted += 1
    assert 9 <= admitted <= 10
    assert hold["reason"] == "insufficient_balance"
    assert admitted * 0.03 <= 0.30 + 1e-9
    assert float(r.get("budget:runaway:held_usd")) <= 0.30 + 1e-9
