"""Budget deep module — configure, top up, inspect, and authorize spend."""

from __future__ import annotations

import os
import time
from typing import Any, Callable, MutableMapping

import redis

from auth import check_api_key_budget
from tenant_keys import (
    budget_prefix_for_read,
    budget_prefix_for_write,
    package_key,
    rate_limit_key,
    scope_prefix_for_read,
)

CACHE_TTL_SECONDS = 30
_SHARED_CACHE: dict[tuple[str, str], dict[str, float]] = {}


class BudgetNotFound(KeyError):
    pass


class InvalidBudget(ValueError):
    pass


class Budget:
    """Tenant-scoped budget interface; Redis keys and fallback policy stay internal."""

    def __init__(
        self,
        redis_client: redis.Redis,
        tenant_id: str | None = None,
        *,
        cache: MutableMapping[tuple[str, str], dict[str, float]] | None = None,
        clock: Callable[[], float] = time.time,
        fail_policy: str | None = None,
    ) -> None:
        self._redis = redis_client
        self.tenant_id = tenant_id.strip() if tenant_id and tenant_id.strip() else None
        self._cache = _SHARED_CACHE if cache is None else cache
        self._clock = clock
        self._fail_policy = fail_policy or os.getenv("BUDGET_FAIL_POLICY", "closed")

    def configure(
        self,
        customer_id: str,
        balance_usd: float,
        *,
        alert_threshold_usd: float | None = None,
        max_rpm: int | None = None,
    ) -> dict[str, Any]:
        if balance_usd < 0:
            raise InvalidBudget("balance_usd must be >= 0")
        if alert_threshold_usd is not None and alert_threshold_usd < 0:
            raise InvalidBudget("alert_threshold_usd must be >= 0")
        if max_rpm is not None and max_rpm < 0:
            raise InvalidBudget("max_rpm must be >= 0")

        key = budget_prefix_for_write(self.tenant_id, customer_id)
        mapping = {
            "balance_usd": balance_usd,
            "initial_balance_usd": balance_usd,
            "held_usd": 0,
            "debt_usd": 0,
            "total_deducted_usd": 0,
            "total_topup_usd": 0,
        }
        pipe = self._redis.pipeline(transaction=True)
        for suffix, value in mapping.items():
            pipe.set(f"{key}:{suffix}", str(value))
        pipe.delete(f"{key}:webhook_low_sent")
        for pct in (70, 90):
            pipe.delete(f"{key}:webhook_warn_{pct}_sent")
        if alert_threshold_usd is None:
            pipe.delete(f"{key}:alert_threshold_usd")
        else:
            pipe.set(f"{key}:alert_threshold_usd", str(alert_threshold_usd))
        if max_rpm is None:
            pipe.delete(f"{key}:max_rpm")
        else:
            pipe.set(f"{key}:max_rpm", str(max_rpm))
        pipe.execute()
        self._cache.pop(self._cache_key(customer_id), None)
        return self.snapshot(customer_id) or {}

    def top_up(self, customer_id: str, amount_usd: float) -> dict[str, float | str]:
        if amount_usd <= 0:
            raise InvalidBudget("amount_usd must be positive")
        read_key = budget_prefix_for_read(self._redis, self.tenant_id, customer_id)
        write_key = budget_prefix_for_write(self.tenant_id, customer_id)
        balance = self._redis.get(f"{read_key}:balance_usd")
        if balance is None:
            raise BudgetNotFound(customer_id)

        pipe = self._redis.pipeline(transaction=True)
        if read_key != write_key and not self._redis.exists(f"{write_key}:balance_usd"):
            for suffix in (
                "balance_usd",
                "held_usd",
                "debt_usd",
                "initial_balance_usd",
                "total_topup_usd",
                "total_deducted_usd",
                "alert_threshold_usd",
                "max_rpm",
            ):
                legacy = self._redis.get(f"{read_key}:{suffix}")
                if legacy is not None:
                    pipe.set(f"{write_key}:{suffix}", legacy)
        pipe.incrbyfloat(f"{write_key}:balance_usd", amount_usd)
        pipe.incrbyfloat(f"{write_key}:total_topup_usd", amount_usd)
        results = pipe.execute()
        new_balance = float(results[-2])
        self._cache.pop(self._cache_key(customer_id), None)
        return {
            "customer_id": customer_id,
            "new_balance_usd": new_balance,
            "added_usd": amount_usd,
        }

    def snapshot(self, customer_id: str) -> dict[str, Any] | None:
        key = budget_prefix_for_read(self._redis, self.tenant_id, customer_id)
        balance = self._redis.get(f"{key}:balance_usd")
        if balance is None:
            return None
        balance_value = float(balance)
        held = float(self._redis.get(f"{key}:held_usd") or 0)
        return {
            "customer_id": customer_id,
            "balance_usd": balance_value,
            "held_usd": held,
            "effective_balance_usd": balance_value - held,
            "debt_usd": float(self._redis.get(f"{key}:debt_usd") or 0),
            "alert_threshold_usd": _float_or_none(
                self._redis.get(f"{key}:alert_threshold_usd")
            ),
            "is_exhausted": balance_value <= 0,
        }

    def set_package(self, customer_id: str, tokens: int) -> dict[str, int | str]:
        if tokens < 0:
            raise InvalidBudget("tokens must be >= 0")
        self._redis.set(package_key(self.tenant_id, customer_id), str(tokens))
        return {"customer_id": customer_id, "tokens_remaining": tokens}

    def package_balance(self, customer_id: str) -> dict[str, int | str]:
        remaining = int(
            self._redis.get(package_key(self.tenant_id, customer_id)) or 0
        )
        return {"customer_id": customer_id, "tokens_remaining": remaining}

    def check(
        self,
        customer_id: str,
        estimated_cost_usd: float = 0.0,
        *,
        parent_span_id: str | None = None,
        session_id: str | None = None,
        key_id: str | None = None,
        count_request: bool = True,
    ) -> dict[str, Any]:
        estimate = max(0.0, estimated_cost_usd)
        try:
            return self._check_redis(
                customer_id,
                estimate,
                parent_span_id=parent_span_id,
                session_id=session_id,
                key_id=key_id,
                count_request=count_request,
            )
        except redis.RedisError:
            return self._check_fallback(customer_id, estimate)

    def _check_redis(
        self,
        customer_id: str,
        estimate: float,
        *,
        parent_span_id: str | None,
        session_id: str | None,
        key_id: str | None,
        count_request: bool,
    ) -> dict[str, Any]:
        budget_key = budget_prefix_for_read(self._redis, self.tenant_id, customer_id)
        write_key = budget_prefix_for_write(self.tenant_id, customer_id)
        minute = int(self._clock()) // 60
        rpm_key = rate_limit_key(self.tenant_id, customer_id, minute)
        requests = int(self._redis.get(rpm_key) or 0)
        max_rpm = self._redis.get(f"{write_key}:max_rpm")
        if max_rpm is None and budget_key != write_key:
            max_rpm = self._redis.get(f"{budget_key}:max_rpm")
        max_rpm_value = int(max_rpm) if max_rpm else 0

        if max_rpm_value > 0 and requests >= max_rpm_value:
            return self._decision(
                False,
                reason="rate_limited",
                balance=None,
                requests=requests,
                max_rpm=max_rpm_value,
            )

        snapshot = self.snapshot(customer_id)
        if snapshot is None:
            if count_request:
                self._increment_rate_limit(rpm_key)
            return {
                "allowed": True,
                "balance_usd": None,
                "reason": "no_budget_configured",
                "requests_this_minute": requests + (1 if count_request else 0),
                "source": "redis",
            }

        balance = float(snapshot["balance_usd"])
        held = float(snapshot["held_usd"])
        effective = float(snapshot["effective_balance_usd"])
        self._cache[self._cache_key(customer_id)] = {
            "balance": balance,
            "held": held,
            "ts": self._clock(),
        }
        common = {
            "balance_usd": balance,
            "held_usd": held,
            "effective_balance_usd": effective,
            "requests_this_minute": requests,
            "source": "redis",
        }
        if effective <= 0:
            return {"allowed": False, "reason": "budget_exhausted", **common}
        if estimate > effective:
            return {"allowed": False, "reason": "insufficient_balance", **common}

        hierarchy = self._hierarchy_denial(
            parent_span_id=parent_span_id,
            session_id=session_id,
            estimated_cost_usd=estimate,
        )
        if hierarchy:
            return {**hierarchy, **common}
        if key_id:
            key_denial = check_api_key_budget(self._redis, key_id, estimate)
            if key_denial:
                return {**key_denial, **common}

        if count_request:
            self._increment_rate_limit(rpm_key)
        return {
            "allowed": True,
            "reason": "ok",
            **common,
            "requests_this_minute": requests + (1 if count_request else 0),
        }

    def _hierarchy_denial(
        self,
        *,
        parent_span_id: str | None,
        session_id: str | None,
        estimated_cost_usd: float,
    ) -> dict[str, Any] | None:
        scopes = (("span", parent_span_id), ("session", session_id))
        for kind, scope_id in scopes:
            if not scope_id:
                continue
            prefix = scope_prefix_for_read(
                self._redis, self.tenant_id, kind, scope_id
            )
            maximum = self._redis.get(f"{prefix}:max_cost_usd")
            if maximum is None:
                continue
            try:
                max_cost = float(maximum)
            except (TypeError, ValueError):
                continue
            spent = float(self._redis.get(f"{prefix}:cost_usd") or 0)
            held = float(self._redis.get(f"{prefix}:held_usd") or 0)
            if spent + held + estimated_cost_usd > max_cost:
                return {
                    "allowed": False,
                    "reason": "hierarchy_cap",
                    "scope": kind,
                    "scope_id": scope_id,
                    "spent_usd": spent,
                    "scope_held_usd": held,
                    "max_cost_usd": max_cost,
                }
        return None

    def _check_fallback(self, customer_id: str, estimate: float) -> dict[str, Any]:
        cached = self._cache.get(self._cache_key(customer_id))
        if cached and self._clock() - cached["ts"] < CACHE_TTL_SECONDS:
            balance = cached["balance"]
            held = cached.get("held", 0.0)
            effective = balance - held
            allowed = effective > 0 and estimate <= effective
            reason = "ok" if allowed else (
                "budget_exhausted" if effective <= 0 else "insufficient_balance"
            )
            return {
                "allowed": allowed,
                "balance_usd": balance,
                "held_usd": held,
                "effective_balance_usd": effective,
                "reason": reason,
                "source": "cache",
            }
        closed = self._fail_policy == "closed"
        return {
            "allowed": not closed,
            "balance_usd": None,
            "reason": f"redis_unavailable_fail_{'closed' if closed else 'open'}",
            "source": "policy",
        }

    def _increment_rate_limit(self, key: str) -> None:
        pipe = self._redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, 120)
        pipe.execute()

    def _cache_key(self, customer_id: str) -> tuple[str, str]:
        return self.tenant_id or "", customer_id

    @staticmethod
    def _decision(
        allowed: bool,
        *,
        reason: str,
        balance: float | None,
        requests: int,
        max_rpm: int,
    ) -> dict[str, Any]:
        return {
            "allowed": allowed,
            "balance_usd": balance,
            "reason": reason,
            "requests_this_minute": requests,
            "max_rpm": max_rpm,
            "source": "redis",
        }


def _float_or_none(value: Any) -> float | None:
    return None if value is None else float(value)
