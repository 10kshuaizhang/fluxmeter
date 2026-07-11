"""Shared pre-request budget gate for /check and Gateway."""

from __future__ import annotations

import os
import time
from typing import Optional

import redis

from auth import check_api_key_budget
from budget_ops import get_effective_balance

CACHE_TTL_SEC = 30
_budget_cache: dict[str, dict] = {}


def cache_get(customer_id: str) -> Optional[dict]:
    entry = _budget_cache.get(customer_id)
    if entry and (time.time() - entry["ts"]) < CACHE_TTL_SEC:
        return entry
    return None


def cache_set(customer_id: str, balance: float, max_rpm: int = 0, held: float = 0.0) -> None:
    _budget_cache[customer_id] = {
        "balance": balance,
        "held": held,
        "max_rpm": max_rpm,
        "ts": time.time(),
    }


def check_hierarchy_cap(
    r: redis.Redis,
    *,
    parent_span_id: Optional[str],
    session_id: Optional[str],
    estimated_cost_usd: float,
) -> Optional[dict]:
    """Return deny payload if span/session cap would be exceeded, else None."""
    checks: list[tuple[str, str]] = []
    if parent_span_id:
        checks.append(("span", parent_span_id))
    if session_id:
        checks.append(("session", session_id))
    for kind, scope_id in checks:
        max_raw = r.get(f"{kind}:{scope_id}:max_cost_usd")
        if max_raw is None:
            continue
        try:
            max_cost = float(max_raw)
        except (TypeError, ValueError):
            continue
        spent = float(r.get(f"{kind}:{scope_id}:cost_usd") or 0)
        if spent + max(estimated_cost_usd, 0.0) > max_cost:
            return {
                "allowed": False,
                "reason": "hierarchy_cap",
                "scope": kind,
                "scope_id": scope_id,
                "spent_usd": spent,
                "max_cost_usd": max_cost,
            }
    return None


def run_budget_check(
    r: redis.Redis,
    customer_id: str,
    estimated_cost_usd: float = 0.0,
    *,
    parent_span_id: Optional[str] = None,
    session_id: Optional[str] = None,
    key_id: Optional[str] = None,
    increment_rate_limit: bool = True,
) -> dict:
    """Pre-request guardrail gate (Redis-first, cache fallback, fail policy)."""
    try:
        budget_key = f"budget:{customer_id}"
        rate_limit_key = f"ratelimit:{customer_id}:{int(time.time()) // 60}"
        requests_this_minute = int(r.get(rate_limit_key) or 0)
        max_rpm = r.get(f"budget:{customer_id}:max_rpm")
        max_rpm_val = int(max_rpm) if max_rpm else 0

        if max_rpm_val > 0 and requests_this_minute >= max_rpm_val:
            return {
                "allowed": False,
                "balance_usd": None,
                "reason": "rate_limited",
                "requests_this_minute": requests_this_minute,
                "max_rpm": max_rpm_val,
                "source": "redis",
            }

        balance = r.get(f"{budget_key}:balance_usd")
        if balance is None:
            if increment_rate_limit:
                pipe = r.pipeline()
                pipe.incr(rate_limit_key)
                pipe.expire(rate_limit_key, 120)
                pipe.execute()
            return {
                "allowed": True,
                "balance_usd": None,
                "reason": "no_budget_configured",
                "requests_this_minute": requests_this_minute + (1 if increment_rate_limit else 0),
                "source": "redis",
            }

        balance_val, held_val, effective = get_effective_balance(r, customer_id)
        cache_set(customer_id, balance_val, max_rpm_val, held_val)

        if effective <= 0:
            return {
                "allowed": False,
                "balance_usd": balance_val,
                "held_usd": held_val,
                "effective_balance_usd": effective,
                "reason": "budget_exhausted",
                "requests_this_minute": requests_this_minute,
                "source": "redis",
            }

        if estimated_cost_usd > 0 and effective < estimated_cost_usd:
            return {
                "allowed": False,
                "balance_usd": balance_val,
                "held_usd": held_val,
                "effective_balance_usd": effective,
                "reason": "insufficient_balance",
                "requests_this_minute": requests_this_minute,
                "source": "redis",
            }

        hierarchy = check_hierarchy_cap(
            r,
            parent_span_id=parent_span_id,
            session_id=session_id,
            estimated_cost_usd=estimated_cost_usd,
        )
        if hierarchy is not None:
            hierarchy.update({
                "balance_usd": balance_val,
                "held_usd": held_val,
                "effective_balance_usd": effective,
                "requests_this_minute": requests_this_minute,
                "source": "redis",
            })
            return hierarchy

        if key_id:
            key_deny = check_api_key_budget(r, key_id, estimated_cost_usd)
            if key_deny is not None:
                key_deny.update({
                    "balance_usd": balance_val,
                    "held_usd": held_val,
                    "effective_balance_usd": effective,
                    "requests_this_minute": requests_this_minute,
                    "source": "redis",
                })
                return key_deny

        if increment_rate_limit:
            pipe = r.pipeline()
            pipe.incr(rate_limit_key)
            pipe.expire(rate_limit_key, 120)
            pipe.execute()

        return {
            "allowed": True,
            "balance_usd": balance_val,
            "held_usd": held_val,
            "effective_balance_usd": effective,
            "reason": "ok",
            "requests_this_minute": requests_this_minute + (1 if increment_rate_limit else 0),
            "source": "redis",
        }

    except Exception:
        cached = cache_get(customer_id)
        if cached:
            balance_val = cached["balance"]
            held_val = cached.get("held", 0.0)
            effective = balance_val - held_val
            if effective <= 0:
                return {
                    "allowed": False,
                    "balance_usd": balance_val,
                    "held_usd": held_val,
                    "effective_balance_usd": effective,
                    "reason": "budget_exhausted",
                    "source": "cache",
                }
            if estimated_cost_usd > 0 and effective < estimated_cost_usd:
                return {
                    "allowed": False,
                    "balance_usd": balance_val,
                    "held_usd": held_val,
                    "effective_balance_usd": effective,
                    "reason": "insufficient_balance",
                    "source": "cache",
                }
            return {
                "allowed": True,
                "balance_usd": balance_val,
                "held_usd": held_val,
                "effective_balance_usd": effective,
                "reason": "ok",
                "source": "cache",
            }

        fail_policy = os.getenv("BUDGET_FAIL_POLICY", "closed")
        if fail_policy == "closed":
            return {
                "allowed": False,
                "balance_usd": None,
                "reason": "redis_unavailable_fail_closed",
                "source": "policy",
            }
        return {
            "allowed": True,
            "balance_usd": None,
            "reason": "redis_unavailable_fail_open",
            "source": "policy",
        }
