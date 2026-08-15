"""Atomic budget hold operations (reserve/reconcile) via Redis Lua."""

from __future__ import annotations

import json
import time

import redis

from usage_buckets import SPAN_TTL_SEC
from tenant_keys import budget_prefix_for_read, budget_prefix_for_write

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

EXPIRE_RESERVATION_SCRIPT = """
local customer = redis.call('HGET', KEYS[1], 'customer_id')
if not customer then redis.call('ZREM', KEYS[2], ARGV[1]); return {} end
local reserved = tonumber(redis.call('HGET', KEYS[1], 'reserved_usd') or '0')
local parent = redis.call('HGET', KEYS[1], 'parent_span_id') or ''
local held_key = KEYS[3]
local held = tonumber(redis.call('GET', held_key) or '0')
local release = math.min(held, reserved)
if release > 0 then redis.call('INCRBYFLOAT', held_key, -release) end
if parent ~= '' then
  local span_key = 'span:' .. parent .. ':held_usd'
  local span_held = tonumber(redis.call('GET', span_key) or '0')
  if span_held > 0 then redis.call('INCRBYFLOAT', span_key, -math.min(span_held, release)) end
end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return {customer, tostring(release)}
"""


def _reservation_held_key(r: redis.Redis, reservation_id: str) -> str:
    key = f"reservation:{reservation_id}"
    customer_id = r.hget(key, "customer_id") or ""
    tenant_raw = r.hget(key, "tenant_id") or ""
    tenant_id = tenant_raw.strip() or None
    return f"{budget_prefix_for_write(tenant_id, customer_id)}:held_usd"


def register_gateway_reservation(
    r: redis.Redis,
    reservation_id: str,
    *,
    customer_id: str,
    reserved_usd: float,
    parent_span_id: str | None,
    expires_at: float | None = None,
    tenant_id: str | None = None,
) -> None:
    """Persist a Gateway hold until Flink reconciles it or expire() releases it."""
    deadline = expires_at if expires_at is not None else time.time() + 900
    key = f"reservation:{reservation_id}"
    pipe = r.pipeline()
    pipe.hset(
        key,
        mapping={
            "customer_id": customer_id,
            "tenant_id": tenant_id or "",
            "reserved_usd": str(max(0.0, reserved_usd)),
            "parent_span_id": parent_span_id or "",
        },
    )
    pipe.expire(key, 86400)
    pipe.zadd("gateway:reservations:pending", {reservation_id: deadline})
    pipe.execute()


def refresh_gateway_reservation(
    r: redis.Redis,
    reservation_id: str,
    *,
    expires_at: float | None = None,
) -> None:
    """Extend an active long-stream reservation without changing its amount."""
    if not r.exists(f"reservation:{reservation_id}"):
        return
    deadline = expires_at if expires_at is not None else time.time() + 900
    pipe = r.pipeline()
    pipe.expire(f"reservation:{reservation_id}", 86400)
    pipe.zadd("gateway:reservations:pending", {reservation_id: deadline})
    pipe.execute()


def expire_reservations(r: redis.Redis, *, now: float | None = None) -> int:
    """Sole Reservation expire entry — workers must call this, not invent a second path."""
    cutoff = time.time() if now is None else now
    expired = r.zrangebyscore("gateway:reservations:pending", 0, cutoff)
    released = 0
    for reservation_id in expired:
        held_key = _reservation_held_key(r, reservation_id)
        result = r.eval(
            EXPIRE_RESERVATION_SCRIPT,
            3,
            f"reservation:{reservation_id}",
            "gateway:reservations:pending",
            held_key,
            reservation_id,
        )
        if not result:
            continue
        customer_id = str(result[0])
        released_usd = float(result[1])
        r.rpush(
            "gateway:reservation:alerts",
            json.dumps(
                {
                    "type": "RESERVATION_EXPIRED",
                    "reservationId": reservation_id,
                    "customerId": customer_id,
                    "releasedUsd": released_usd,
                    "timestamp": int(cutoff * 1000),
                }
            ),
        )
        released += 1
    return released


# ponytail: alias kept for older call sites / tests
reap_expired_reservations = expire_reservations


def settle_gateway_reservation(r: redis.Redis, reservation_id: str) -> float:
    """Atomically release and remove a reservation when no billable usage exists."""
    held_key = _reservation_held_key(r, reservation_id)
    result = r.eval(
        EXPIRE_RESERVATION_SCRIPT,
        3,
        f"reservation:{reservation_id}",
        "gateway:reservations:pending",
        held_key,
        reservation_id,
    )
    return float(result[1]) if result else 0.0


def reserve_hold(
    r: redis.Redis,
    customer_id: str,
    estimated_cost_usd: float,
    *,
    parent_span_id: str | None = None,
    tenant_id: str | None = None,
) -> dict:
    """Increase held_usd without changing balance_usd (Sink is sole balance deductor)."""
    # Operate on the key that currently holds balance (may be legacy during cutover).
    budget_key = budget_prefix_for_read(r, tenant_id, customer_id)

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
    tenant_id: str | None = None,
) -> dict:
    """Release hold after streaming completes. Balance unchanged (Sink deducted actual)."""
    budget_key = budget_prefix_for_read(r, tenant_id, customer_id)
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


def get_effective_balance(
    r: redis.Redis,
    customer_id: str,
    *,
    tenant_id: str | None = None,
) -> tuple[float, float, float]:
    """Return (balance, held, effective)."""
    budget_key = budget_prefix_for_read(r, tenant_id, customer_id)
    balance = float(r.get(f"{budget_key}:balance_usd") or 0)
    held = float(r.get(f"{budget_key}:held_usd") or 0)
    return balance, held, balance - held
