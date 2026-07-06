"""Unit tests for span/session hierarchy caps at /check."""

from __future__ import annotations

from unittest.mock import MagicMock

from main import _check_hierarchy_cap


def test_hierarchy_cap_denies_when_over_span():
    r = MagicMock()
    r.get.side_effect = lambda k: {
        "span:job1:max_cost_usd": "1.0",
        "span:job1:cost_usd": "0.9",
    }.get(k)
    deny = _check_hierarchy_cap(
        r, parent_span_id="job1", session_id=None, estimated_cost_usd=0.2
    )
    assert deny is not None
    assert deny["allowed"] is False
    assert deny["reason"] == "hierarchy_cap"
    assert deny["scope"] == "span"


def test_hierarchy_cap_allows_under_budget():
    r = MagicMock()
    r.get.side_effect = lambda k: {
        "span:job1:max_cost_usd": "1.0",
        "span:job1:cost_usd": "0.1",
    }.get(k)
    assert _check_hierarchy_cap(
        r, parent_span_id="job1", session_id=None, estimated_cost_usd=0.2
    ) is None


def test_no_cap_configured():
    r = MagicMock()
    r.get.return_value = None
    assert _check_hierarchy_cap(
        r, parent_span_id="job1", session_id="s1", estimated_cost_usd=9.0
    ) is None
