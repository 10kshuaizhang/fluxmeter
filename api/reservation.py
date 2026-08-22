"""Reservation deep module — atomic holds with one lifecycle interface."""

from __future__ import annotations

import json
import time
from typing import Any, Callable

import redis

from tenant_keys import budget_prefix_for_read, scope_prefix_for_read
from usage_buckets import SPAN_TTL_SEC

PENDING_KEY = "gateway:reservations:pending"
RECORD_TTL_SECONDS = 24 * 60 * 60

OPEN_SCRIPT = """
local existing_customer = redis.call('HGET', KEYS[6], 'customer_id')
if existing_customer then
  local same = existing_customer == ARGV[4]
    and (redis.call('HGET', KEYS[6], 'tenant_id') or '') == ARGV[5]
    and tonumber(redis.call('HGET', KEYS[6], 'reserved_usd') or '-1') == tonumber(ARGV[1])
  if same then return {2, 'existing'} end
  return {-1, 'reservation_conflict'}
end
local balance_raw = redis.call('GET', KEYS[1])
if not balance_raw then return {0, 'no_budget'} end
local balance = tonumber(balance_raw)
local held = tonumber(redis.call('GET', KEYS[2]) or '0')
local estimate = tonumber(ARGV[1])
if balance - held < estimate then return {0, 'insufficient_balance', balance, held} end

local span_held = 0
local span_spent = 0
if ARGV[6] ~= '' then
  span_held = tonumber(redis.call('GET', KEYS[3]) or '0')
  span_spent = tonumber(redis.call('GET', KEYS[4]) or '0')
  local span_max_raw = redis.call('GET', KEYS[5])
  if span_max_raw and span_spent + span_held + estimate > tonumber(span_max_raw) then
    return {0, 'hierarchy_reserve', balance, held, span_held, span_spent}
  end
  if span_max_raw then
    redis.call('INCRBYFLOAT', KEYS[3], estimate)
    redis.call('EXPIRE', KEYS[3], ARGV[2])
    span_held = span_held + estimate
  end
end

redis.call('INCRBYFLOAT', KEYS[2], estimate)
redis.call('HSET', KEYS[6],
  'customer_id', ARGV[4], 'tenant_id', ARGV[5], 'reserved_usd', ARGV[1],
  'parent_span_id', ARGV[6], 'held_key', KEYS[2],
  'span_held_key', ARGV[9])
redis.call('EXPIRE', KEYS[6], ARGV[8])
redis.call('ZADD', KEYS[7], ARGV[7], ARGV[3])
return {1, 'opened', balance, held + estimate, span_held, span_spent}
"""

RESERVE_SCRIPT = """
local balance_raw = redis.call('GET', KEYS[1])
if not balance_raw then return {-1, 0, 0, 0, 'no_budget'} end
local balance = tonumber(balance_raw)
local held = tonumber(redis.call('GET', KEYS[2]) or '0')
local estimate = tonumber(ARGV[1])
local effective = balance - held
if effective < estimate then return {0, balance, held, effective, 'insufficient_balance'} end
local span_held = 0
local span_spent = 0
if ARGV[2] ~= '' then
  span_held = tonumber(redis.call('GET', KEYS[3]) or '0')
  span_spent = tonumber(redis.call('GET', KEYS[4]) or '0')
  local span_max_raw = redis.call('GET', KEYS[5])
  if span_max_raw and span_spent + span_held + estimate > tonumber(span_max_raw) then
    return {0, balance, held, effective, 'hierarchy_reserve', span_held, span_spent}
  end
  if span_max_raw then
    redis.call('INCRBYFLOAT', KEYS[3], estimate)
    redis.call('EXPIRE', KEYS[3], ARGV[3])
    span_held = span_held + estimate
  end
end
redis.call('INCRBYFLOAT', KEYS[2], estimate)
return {1, balance, held + estimate, effective - estimate, 'reserved', span_held, span_spent}
"""

RECONCILE_SCRIPT = """
local held = tonumber(redis.call('GET', KEYS[1]) or '0')
local requested = tonumber(ARGV[1])
local released = math.min(held, requested)
if released > 0 then redis.call('INCRBYFLOAT', KEYS[1], -released) end
if KEYS[3] ~= 'noop' then
  local span_held = tonumber(redis.call('GET', KEYS[3]) or '0')
  if span_held > 0 then redis.call('INCRBYFLOAT', KEYS[3], -math.min(span_held, released)) end
end
local balance = tonumber(redis.call('GET', KEYS[2]) or '0')
return {balance, held - released, released}
"""

CLOSE_SCRIPT = """
local customer = redis.call('HGET', KEYS[1], 'customer_id')
if not customer then redis.call('ZREM', KEYS[2], ARGV[1]); return {} end
local reserved = tonumber(redis.call('HGET', KEYS[1], 'reserved_usd') or '0')
local held = tonumber(redis.call('GET', KEYS[3]) or '0')
local released = math.min(held, reserved)
if released > 0 then redis.call('INCRBYFLOAT', KEYS[3], -released) end
if KEYS[4] ~= 'noop' then
  local span_held = tonumber(redis.call('GET', KEYS[4]) or '0')
  if span_held > 0 then redis.call('INCRBYFLOAT', KEYS[4], -math.min(span_held, released)) end
end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return {customer, tostring(released)}
"""


class Reservation:
    """Owns hold creation and every terminal/expiry transition."""

    def __init__(
        self,
        redis_client: redis.Redis,
        tenant_id: str | None = None,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._redis = redis_client
        self.tenant_id = tenant_id.strip() if tenant_id and tenant_id.strip() else None
        self._clock = clock

    def open(
        self,
        reservation_id: str,
        *,
        customer_id: str,
        estimated_cost_usd: float,
        parent_span_id: str | None = None,
        expires_at: float | None = None,
    ) -> dict[str, Any]:
        if estimated_cost_usd <= 0:
            raise ValueError("estimated_cost_usd must be positive")
        budget_key = budget_prefix_for_read(self._redis, self.tenant_id, customer_id)
        span_key = self._scope("span", parent_span_id) if parent_span_id else None
        deadline = expires_at if expires_at is not None else self._clock() + 900
        result = self._redis.eval(
            OPEN_SCRIPT,
            7,
            f"{budget_key}:balance_usd",
            f"{budget_key}:held_usd",
            f"{span_key}:held_usd" if span_key else "noop",
            f"{span_key}:cost_usd" if span_key else "noop",
            f"{span_key}:max_cost_usd" if span_key else "noop",
            self._key(reservation_id),
            PENDING_KEY,
            str(estimated_cost_usd),
            str(SPAN_TTL_SEC),
            reservation_id,
            customer_id,
            self.tenant_id or "",
            parent_span_id or "",
            str(deadline),
            str(RECORD_TTL_SECONDS),
            f"{span_key}:held_usd" if span_key else "",
        )
        code = int(result[0])
        reason = str(result[1])
        if code == -1:
            return {"allowed": False, "reason": reason, "conflict": True}
        if code == 0:
            payload: dict[str, Any] = {"allowed": False, "reason": reason}
            if len(result) > 2:
                payload.update(
                    balance_usd=float(result[2]),
                    held_usd=float(result[3]),
                    effective_balance_usd=float(result[2]) - float(result[3]),
                )
            if reason == "hierarchy_reserve":
                payload.update(scope="span", scope_id=parent_span_id)
            return payload
        if code == 2:
            return {
                "allowed": True,
                "reason": "existing",
                "reservation_id": reservation_id,
                "reserved_usd": estimated_cost_usd,
                "idempotent": True,
            }
        return {
            "allowed": True,
            "reason": "reserved",
            "reservation_id": reservation_id,
            "reserved_usd": estimated_cost_usd,
            "balance_usd": float(result[2]),
            "held_usd": float(result[3]),
            "effective_balance_usd": float(result[2]) - float(result[3]),
            "idempotent": False,
        }

    def reserve(
        self,
        customer_id: str,
        estimated_cost_usd: float,
        *,
        parent_span_id: str | None = None,
    ) -> dict[str, Any]:
        if estimated_cost_usd <= 0:
            raise ValueError("estimated_cost_usd must be positive")
        budget_key = budget_prefix_for_read(self._redis, self.tenant_id, customer_id)
        span_key = self._scope("span", parent_span_id) if parent_span_id else None
        result = self._redis.eval(
            RESERVE_SCRIPT,
            5,
            f"{budget_key}:balance_usd",
            f"{budget_key}:held_usd",
            f"{span_key}:held_usd" if span_key else "noop",
            f"{span_key}:cost_usd" if span_key else "noop",
            f"{span_key}:max_cost_usd" if span_key else "noop",
            str(estimated_cost_usd),
            parent_span_id or "",
            str(SPAN_TTL_SEC),
        )
        allowed = int(result[0])
        reason = str(result[4])
        if allowed < 0:
            return {"allowed": False, "reason": "no_budget"}
        payload = {
            "allowed": allowed == 1,
            "balance_usd": float(result[1]),
            "held_usd": float(result[2]),
            "effective_balance_usd": float(result[3]),
            "reason": reason,
        }
        if allowed == 1:
            payload["reserved_usd"] = estimated_cost_usd
        if reason == "hierarchy_reserve":
            payload.update(scope="span", scope_id=parent_span_id)
        return payload

    def reconcile(
        self,
        customer_id: str,
        reserved_usd: float,
        *,
        parent_span_id: str | None = None,
    ) -> dict[str, Any]:
        budget_key = budget_prefix_for_read(self._redis, self.tenant_id, customer_id)
        span_key = self._scope("span", parent_span_id) if parent_span_id else None
        result = self._redis.eval(
            RECONCILE_SCRIPT,
            3,
            f"{budget_key}:held_usd",
            f"{budget_key}:balance_usd",
            f"{span_key}:held_usd" if span_key else "noop",
            str(max(0.0, reserved_usd)),
        )
        return {
            "balance_usd": float(result[0]),
            "held_usd": float(result[1]),
            "released_usd": float(result[2]),
            "reserved_usd": reserved_usd,
            **({"parent_span_id": parent_span_id} if parent_span_id else {}),
        }

    def refresh(self, reservation_id: str, *, expires_at: float | None = None) -> bool:
        if not self._redis.exists(self._key(reservation_id)):
            return False
        deadline = expires_at if expires_at is not None else self._clock() + 900
        pipe = self._redis.pipeline()
        pipe.expire(self._key(reservation_id), RECORD_TTL_SECONDS)
        pipe.zadd(PENDING_KEY, {reservation_id: deadline})
        pipe.execute()
        return True

    def settle(self, reservation_id: str) -> float:
        result = self._close(reservation_id)
        return float(result[1]) if result else 0.0

    def expire_due(self, *, now: float | None = None) -> int:
        cutoff = self._clock() if now is None else now
        expired = self._redis.zrangebyscore(PENDING_KEY, 0, cutoff)
        released = 0
        for reservation_id in expired:
            result = self._close(reservation_id)
            if not result:
                continue
            self._redis.rpush(
                "gateway:reservation:alerts",
                json.dumps(
                    {
                        "type": "RESERVATION_EXPIRED",
                        "reservationId": reservation_id,
                        "customerId": str(result[0]),
                        "releasedUsd": float(result[1]),
                        "timestamp": int(cutoff * 1000),
                    }
                ),
            )
            released += 1
        return released

    def _close(self, reservation_id: str):
        key = self._key(reservation_id)
        held_key = self._redis.hget(key, "held_key")
        if not held_key:
            customer = self._redis.hget(key, "customer_id") or ""
            tenant = self._redis.hget(key, "tenant_id") or ""
            held_key = f"{budget_prefix_for_read(self._redis, tenant or None, customer)}:held_usd"
        span_held_key = self._redis.hget(key, "span_held_key") or "noop"
        return self._redis.eval(
            CLOSE_SCRIPT,
            4,
            key,
            PENDING_KEY,
            held_key,
            span_held_key,
            reservation_id,
        )

    def _scope(self, kind: str, scope_id: str | None) -> str:
        assert scope_id is not None
        return scope_prefix_for_read(self._redis, self.tenant_id, kind, scope_id)

    @staticmethod
    def _key(reservation_id: str) -> str:
        return f"reservation:{reservation_id}"
