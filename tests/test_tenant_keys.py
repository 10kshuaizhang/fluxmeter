"""Unit tests for tenant_keys — no Redis required."""

from __future__ import annotations

import sys

sys.path.insert(0, "api")

from tenant_keys import budget_prefix, customer_prefix, global_key, has_tenant  # noqa: E402


def test_has_tenant():
    assert not has_tenant(None)
    assert not has_tenant("")
    assert not has_tenant("   ")
    assert has_tenant("tenant_abc")


def test_single_tenant_keys():
    assert customer_prefix(None, "cust_1") == "customer:cust_1"
    assert budget_prefix(None, "cust_1") == "budget:cust_1"
    assert global_key(None, "total_tokens") == "global:total_tokens"


def test_multi_tenant_keys():
    tid = "tenant_xyz"
    assert customer_prefix(tid, "cust_1") == "tenant:tenant_xyz:customer:cust_1"
    assert budget_prefix(tid, "cust_1") == "tenant:tenant_xyz:budget:cust_1"
    assert global_key(tid, "total_tokens") == "tenant:tenant_xyz:global:total_tokens"
