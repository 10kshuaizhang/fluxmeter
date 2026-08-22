"""Unit tests for span/session hierarchy caps at /check."""

from __future__ import annotations

import fakeredis

from budget import Budget


def test_hierarchy_cap_denies_when_over_span():
    r = fakeredis.FakeRedis(decode_responses=True)
    Budget(r).configure("c1", 10)
    r.set("span:job1:max_cost_usd", "1.0")
    r.set("span:job1:cost_usd", "0.9")
    deny = Budget(r).check(
        "c1", 0.2, parent_span_id="job1", count_request=False
    )
    assert deny is not None
    assert deny["allowed"] is False
    assert deny["reason"] == "hierarchy_cap"
    assert deny["scope"] == "span"


def test_hierarchy_cap_allows_under_budget():
    r = fakeredis.FakeRedis(decode_responses=True)
    Budget(r).configure("c1", 10)
    r.set("span:job1:max_cost_usd", "1.0")
    r.set("span:job1:cost_usd", "0.1")
    assert Budget(r).check(
        "c1", 0.2, parent_span_id="job1", count_request=False
    )["allowed"] is True


def test_no_cap_configured():
    r = fakeredis.FakeRedis(decode_responses=True)
    Budget(r).configure("c1", 10)
    assert Budget(r).check(
        "c1", 9.0, parent_span_id="job1", session_id="s1", count_request=False
    )["allowed"] is True
