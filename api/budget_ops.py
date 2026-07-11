"""Atomic budget hold operations (reserve/reconcile) via Redis Lua."""

from __future__ import annotations

import redis

from usage_buckets import SPAN_TTL_SEC

RESERVE_SCRIPT = """
local balance = tonumber(redis.call('GET', KEYS[1]) or '0')
local held = tonumber(redis.call('GET', KEYS[2]) or '0')
local estimate = tonumber(ARGV[1])
local effective = balance - held
if effective < estimate then
  return {0, balance, held, effective}
end
redis.call('INCRBYFLOAT', KEYS[2], estimate)
local new_held = held + estimate
return {1, balance, new_held, effective - estimate}
"""

RESERVE_SPAN_SCRIPT = """
local balance = tonumber(redis.call('GET', KEYS[1]) or '0')
local held = tonumber(redis.call('GET', KEYS[2]) or '0')
local estimate = tonumber(ARGV[1])
local effective = balance - held
if effective < estimate then
  return {0, balance, held, effective, 'insufficient_balance', 0, 0}
end

local span_held = tonumber(redis.call('GET', KEYS[3]) or '0')
local span_spent = tonumber(redis.call('GET', KEYS[4]) or '0')
local span_max_raw = redis.call('GET', KEYS[5])
if span_max_raw then
  local span_max = tonumber(span_max_raw)
  if span_spent + span_held + estimate > span_max then
    return {0, balance, held, effective, 'hierarchy_reserve', span_held, span_spent}
  end
  redis.call('INCRBYFLOAT', KEYS[3], estimate)
  local ttl = tonumber(ARGV[2])
  if ttl and ttl > 0 then
    redis.call('EXPIRE', KEYS[3], ttl)
  end
  span_held = span_held + estimate
end

redis.call('INCRBYFLOAT', KEYS[2], estimate)
return {1, balance, held + estimate, effective - estimate, 'reserved', span_held, span_spent}
"""

RECONCILE_SCRIPT = """
local held = tonumber(redis.call('GET', KEYS[1]) or '0')
local reserved = tonumber(ARGV[1])
if held < reserved then
  reserved = held
end
redis.call('INCRBYFLOAT', KEYS[1], -reserved)
local balance = tonumber(redis.call('GET', KEYS[2]) or '0')
return {balance, held - reserved, reserved}
"""

RECONCILE_SPAN_SCRIPT = """
local held = tonumber(redis.call('GET', KEYS[1]) or '0')
local reserved = tonumber(ARGV[1])
if held < reserved then
  reserved = held
end
redis.call('INCRBYFLOAT', KEYS[1], -reserved)

local span_held_key = KEYS[3]
if span_held_key and span_held_key ~= '' then
  local span_held = tonumber(redis.call('GET', span_held_key) or '0')
  local span_release = reserved
  if span_held < span_release then
    span_release = span_held
  end
  if span_release > 0 then
    redis.call('INCRBYFLOAT', span_held_key, -span_release)
  end
end

local balance = tonumber(redis.call('GET', KEYS[2]) or '0')
return {balance, held - reserved, reserved}
"""


def reserve_hold(
    r: redis.Redis,
    customer_id: str,
    estimated_cost_usd: float,
    *,
    parent_span_id: str | None = None,
) -> dict:
    """Increase held_usd without changing balance_usd (Sink is sole balance deductor)."""
    budget_key = f"budget:{customer_id}"

    if parent_span_id:
        span_id = parent_span_id
        result = r.eval(
            RESERVE_SPAN_SCRIPT,
            5,
            f"{budget_key}:balance_usd",
            f"{budget_key}:held_usd",
            f"span:{span_id}:held_usd",
            f"span:{span_id}:cost_usd",
            f"span:{span_id}:max_cost_usd",
            str(estimated_cost_usd),
            str(SPAN_TTL_SEC),
        )
        allowed = int(result[0])
        balance_val = float(result[1])
        held_val = float(result[2])
        effective_after = float(result[3])
        reason = result[4] if len(result) > 4 else "reserved"
        span_held = float(result[5]) if len(result) > 5 else 0.0
        span_spent = float(result[6]) if len(result) > 6 else 0.0

        if allowed == 0:
            payload = {
                "allowed": False,
                "balance_usd": balance_val,
                "held_usd": held_val,
                "effective_balance_usd": effective_after,
                "reason": reason,
            }
            if reason == "hierarchy_reserve":
                payload.update({
                    "scope": "span",
                    "scope_id": span_id,
                    "span_held_usd": span_held,
                    "span_spent_usd": span_spent,
                })
            return payload

        out = {
            "allowed": True,
            "balance_usd": balance_val,
            "held_usd": held_val,
            "effective_balance_usd": balance_val - held_val,
            "reserved_usd": estimated_cost_usd,
            "reason": "reserved",
        }
        if span_held > 0:
            out["span_held_usd"] = span_held
            out["parent_span_id"] = span_id
        return out

    result = r.eval(
        RESERVE_SCRIPT,
        2,
        f"{budget_key}:balance_usd",
        f"{budget_key}:held_usd",
        str(estimated_cost_usd),
    )
    allowed, balance, held, effective_after = result
    balance_val = float(balance)
    held_val = float(held)
    if int(allowed) == 0:
        return {
            "allowed": False,
            "balance_usd": balance_val,
            "held_usd": held_val,
            "effective_balance_usd": float(effective_after),
            "reason": "insufficient_balance",
        }
    return {
        "allowed": True,
        "balance_usd": balance_val,
        "held_usd": held_val,
        "effective_balance_usd": balance_val - held_val,
        "reserved_usd": estimated_cost_usd,
        "reason": "reserved",
    }


def reconcile_hold(
    r: redis.Redis,
    customer_id: str,
    reserved_usd: float,
    *,
    parent_span_id: str | None = None,
) -> dict:
    """Release hold after streaming completes. Balance unchanged (Sink deducted actual)."""
    budget_key = f"budget:{customer_id}"
    span_held_key = f"span:{parent_span_id}:held_usd" if parent_span_id else ""

    if parent_span_id:
        result = r.eval(
            RECONCILE_SPAN_SCRIPT,
            3,
            f"{budget_key}:held_usd",
            f"{budget_key}:balance_usd",
            span_held_key,
            str(reserved_usd),
        )
    else:
        result = r.eval(
            RECONCILE_SCRIPT,
            2,
            f"{budget_key}:held_usd",
            f"{budget_key}:balance_usd",
            str(reserved_usd),
        )

    balance_val = float(result[0])
    held_val = float(result[1])
    released = float(result[2])
    out = {
        "balance_usd": balance_val,
        "held_usd": held_val,
        "released_usd": released,
        "reserved_usd": reserved_usd,
    }
    if parent_span_id:
        out["parent_span_id"] = parent_span_id
    return out


def get_effective_balance(r: redis.Redis, customer_id: str) -> tuple[float, float, float]:
    """Return (balance, held, effective)."""
    budget_key = f"budget:{customer_id}"
    balance = float(r.get(f"{budget_key}:balance_usd") or 0)
    held = float(r.get(f"{budget_key}:held_usd") or 0)
    return balance, held, balance - held
