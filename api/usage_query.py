"""Lifetime UsageQuery — tenant-aware reads of Flink lifetime counters."""

from __future__ import annotations

from typing import Any, Iterator, Optional

from tenant_keys import (
    customer_prefix_for_read,
    global_key_for_write,
    global_ns_for_read,
    has_tenant,
)


def get_global(redis_client, tenant_id: str | None = None) -> dict[str, Any]:
    ns = global_ns_for_read(redis_client, tenant_id)
    return {
        "total_events": int(redis_client.get(f"{ns}:total_events") or 0),
        "total_tokens": int(redis_client.get(f"{ns}:total_tokens") or 0),
        "input_tokens": int(redis_client.get(f"{ns}:input_tokens") or 0),
        "output_tokens": int(redis_client.get(f"{ns}:output_tokens") or 0),
        "total_cost_usd": float(redis_client.get(f"{ns}:total_cost_usd") or 0),
        "last_window_end": _int_or_none(redis_client.get(f"{ns}:last_window_end")),
    }


def get_customer(
    redis_client, tenant_id: str | None, customer_id: str
) -> Optional[dict[str, Any]]:
    key = customer_prefix_for_read(redis_client, tenant_id, customer_id)
    total_tokens = redis_client.get(f"{key}:total_tokens")
    if total_tokens is None:
        return None
    return {
        "customer_id": customer_id,
        "total_tokens": int(total_tokens),
        "input_tokens": int(redis_client.get(f"{key}:input_tokens") or 0),
        "output_tokens": int(redis_client.get(f"{key}:output_tokens") or 0),
        "cache_read_tokens": int(redis_client.get(f"{key}:cache_read_tokens") or 0),
        "reasoning_tokens": int(redis_client.get(f"{key}:reasoning_tokens") or 0),
        "event_count": int(redis_client.get(f"{key}:event_count") or 0),
        "cost_usd": float(redis_client.get(f"{key}:cost_usd") or 0),
    }


def customer_counters(
    redis_client, tenant_id: str | None, customer_id: str
) -> dict[str, Any]:
    """Lifetime fields with zero defaults (billing export); no 404 semantics."""
    key = customer_prefix_for_read(redis_client, tenant_id, customer_id)
    return {
        "event_count": int(redis_client.get(f"{key}:event_count") or 0),
        "input_tokens": int(redis_client.get(f"{key}:input_tokens") or 0),
        "output_tokens": int(redis_client.get(f"{key}:output_tokens") or 0),
        "cost_usd": float(redis_client.get(f"{key}:cost_usd") or 0),
    }


def customer_cost_usd(redis_client, tenant_id: str | None, customer_id: str) -> float:
    key = customer_prefix_for_read(redis_client, tenant_id, customer_id)
    return float(redis_client.get(f"{key}:cost_usd") or 0)


def get_customer_model(
    redis_client, tenant_id: str | None, customer_id: str, model_id: str
) -> Optional[dict[str, Any]]:
    prefix = customer_prefix_for_read(redis_client, tenant_id, customer_id)
    key = f"{prefix}:model:{model_id}"
    total_tokens = redis_client.get(f"{key}:total_tokens")
    if total_tokens is None:
        return None
    return {
        "model_id": model_id,
        "total_tokens": int(total_tokens),
        "input_tokens": int(redis_client.get(f"{key}:input_tokens") or 0),
        "output_tokens": int(redis_client.get(f"{key}:output_tokens") or 0),
        "cost_usd": float(redis_client.get(f"{key}:cost_usd") or 0),
    }


def list_customer_spans(
    redis_client, tenant_id: str | None, customer_id: str, limit: int = 10
) -> list[dict[str, Any]]:
    prefix = customer_prefix_for_read(redis_client, tenant_id, customer_id)
    spans = redis_client.zrevrange(f"{prefix}:spans", 0, limit - 1, withscores=True)
    if not spans:
        return []
    return [{"span_id": span_id, "cost_usd": score} for span_id, score in spans]


def iter_model_usage_rows(
    redis_client, tenant_id: str | None, model_id: str
) -> Iterator[dict[str, Any]]:
    """Yield per-customer model token rows. Dual-scan when tenant set; prefer tenant keys."""
    # customer_id -> row; tenant rows overwrite legacy
    by_cid: dict[str, dict[str, Any]] = {}

    patterns: list[tuple[str, bool]] = []
    if has_tenant(tenant_id):
        patterns.append(
            (f"tenant:{tenant_id}:customer:*:model:{model_id}:input_tokens", True)
        )
    patterns.append((f"customer:*:model:{model_id}:input_tokens", False))

    for pattern, is_tenant_pattern in patterns:
        for key in redis_client.scan_iter(match=pattern, count=1000):
            parsed = _parse_model_input_key(key, model_id)
            if parsed is None:
                continue
            cid, model_key, cust_prefix = parsed
            if cid in by_cid and not is_tenant_pattern:
                continue
            input_tokens = int(redis_client.get(f"{model_key}:input_tokens") or 0)
            output_tokens = int(redis_client.get(f"{model_key}:output_tokens") or 0)
            by_cid[cid] = {
                "customer_id": cid,
                "model_key": model_key,
                "customer_prefix": cust_prefix,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "tenant_scoped": is_tenant_pattern,
            }

    yield from by_cid.values()


def apply_model_cost_adjustment(
    redis_client,
    tenant_id: str | None,
    *,
    customer_id: str,
    model_key: str,
    customer_prefix: str,
    adjustment: float,
) -> None:
    """Adjust model/customer/global cost on the scanned key family.

    Global write uses canonical tenant global when tenant_id is set.
    """
    _ = customer_id  # retained for call-site clarity
    pipe = redis_client.pipeline()
    pipe.incrbyfloat(f"{model_key}:cost_usd", adjustment)
    pipe.incrbyfloat(f"{customer_prefix}:cost_usd", adjustment)
    pipe.incrbyfloat(global_key_for_write(tenant_id, "total_cost_usd"), adjustment)
    pipe.execute()


def _parse_model_input_key(
    key: str, model_id: str
) -> Optional[tuple[str, str, str]]:
    """Return (customer_id, model_key, customer_prefix) or None."""
    parts = key.split(":")
    if len(parts) >= 6 and parts[0] == "tenant" and parts[2] == "customer":
        # tenant:{tid}:customer:{cid}:model:{mid}:input_tokens
        cid = parts[3]
        cust_prefix = f"tenant:{parts[1]}:customer:{cid}"
        model_key = f"{cust_prefix}:model:{model_id}"
        return cid, model_key, cust_prefix
    if len(parts) >= 4 and parts[0] == "customer":
        cid = parts[1]
        cust_prefix = f"customer:{cid}"
        model_key = f"{cust_prefix}:model:{model_id}"
        return cid, model_key, cust_prefix
    return None


def _int_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    return int(value)
