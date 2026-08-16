"""UsageQuery + tenant lifetime dual-read."""

from __future__ import annotations

import sys

import fakeredis

sys.path.insert(0, "api")

from tenant_keys import (  # noqa: E402
    customer_prefix,
    customer_prefix_for_read,
    global_ns_for_read,
)
from usage_query import (  # noqa: E402
    apply_model_cost_adjustment,
    get_customer,
    get_global,
    iter_model_usage_rows,
    list_customer_spans,
)


def test_customer_prefix_for_read_prefers_tenant():
    r = fakeredis.FakeRedis(decode_responses=True)
    tid, cid = "t1", "c1"
    preferred = customer_prefix(tid, cid)
    r.set(f"{preferred}:total_tokens", "10")
    r.set(f"customer:{cid}:total_tokens", "99")
    assert customer_prefix_for_read(r, tid, cid) == preferred
    assert get_customer(r, tid, cid)["total_tokens"] == 10


def test_customer_prefix_for_read_legacy_fallback():
    r = fakeredis.FakeRedis(decode_responses=True)
    tid, cid = "t1", "legacy"
    r.set(f"customer:{cid}:total_tokens", "42")
    r.set(f"customer:{cid}:cost_usd", "1.5")
    assert customer_prefix_for_read(r, tid, cid) == f"customer:{cid}"
    assert get_customer(r, tid, cid)["cost_usd"] == 1.5


def test_global_ns_legacy_fallback():
    r = fakeredis.FakeRedis(decode_responses=True)
    r.set("global:total_events", "7")
    assert global_ns_for_read(r, "t1") == "global"
    assert get_global(r, "t1")["total_events"] == 7


def test_spans_tenant_key():
    r = fakeredis.FakeRedis(decode_responses=True)
    prefix = customer_prefix("t1", "c1")
    r.zadd(f"{prefix}:spans", {"span_a": 3.0})
    rows = list_customer_spans(r, "t1", "c1", limit=5)
    assert rows == [{"span_id": "span_a", "cost_usd": 3.0}]


def test_iter_model_prefers_tenant_over_legacy():
    r = fakeredis.FakeRedis(decode_responses=True)
    tid, cid, mid = "t1", "c1", "gpt-4o-mini"
    legacy = f"customer:{cid}:model:{mid}"
    tenant = f"tenant:{tid}:customer:{cid}:model:{mid}"
    r.set(f"{legacy}:input_tokens", "100")
    r.set(f"{legacy}:output_tokens", "10")
    r.set(f"{tenant}:input_tokens", "200")
    r.set(f"{tenant}:output_tokens", "20")
    rows = list(iter_model_usage_rows(r, tid, mid))
    assert len(rows) == 1
    assert rows[0]["input_tokens"] == 200
    assert rows[0]["tenant_scoped"] is True


def test_apply_adjustment_writes_scan_family():
    r = fakeredis.FakeRedis(decode_responses=True)
    tid, cid, mid = "t1", "c1", "gpt-4o-mini"
    model_key = f"tenant:{tid}:customer:{cid}:model:{mid}"
    cust = f"tenant:{tid}:customer:{cid}"
    r.set(f"{model_key}:cost_usd", "1.0")
    r.set(f"{cust}:cost_usd", "1.0")
    r.set(f"tenant:{tid}:global:total_cost_usd", "1.0")
    apply_model_cost_adjustment(
        r,
        tid,
        customer_id=cid,
        model_key=model_key,
        customer_prefix=cust,
        adjustment=0.5,
    )
    assert float(r.get(f"{model_key}:cost_usd")) == 1.5
    assert float(r.get(f"tenant:{tid}:global:total_cost_usd")) == 1.5
