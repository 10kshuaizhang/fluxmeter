"""Atomic budget hold operations (reserve/reconcile) via Redis Lua."""

from __future__ import annotations

import redis

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


def reserve_hold(r: redis.Redis, customer_id: str, estimated_cost_usd: float) -> dict:
    """Increase held_usd without changing balance_usd (Sink is sole balance deductor)."""
    budget_key = f"budget:{customer_id}"
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
            "effective_balance_usd": float(result[3]),
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


def reconcile_hold(r: redis.Redis, customer_id: str, reserved_usd: float) -> dict:
    """Release hold after streaming completes. Balance unchanged (Sink deducted actual)."""
    budget_key = f"budget:{customer_id}"
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
    return {
        "balance_usd": balance_val,
        "held_usd": held_val,
        "released_usd": released,
        "reserved_usd": reserved_usd,
    }


def get_effective_balance(r: redis.Redis, customer_id: str) -> tuple[float, float, float]:
    """Return (balance, held, effective)."""
    budget_key = f"budget:{customer_id}"
    balance = float(r.get(f"{budget_key}:balance_usd") or 0)
    held = float(r.get(f"{budget_key}:held_usd") or 0)
    return balance, held, balance - held
