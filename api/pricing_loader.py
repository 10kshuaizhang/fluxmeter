"""Pricing catalog loader — Python mirror of io.fluxmeter.pricing.PricingCatalog."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from tenant_keys import customer_prefix

PRICING_FILE = os.getenv(
    "PRICING_FILE",
    os.path.join(os.path.dirname(__file__), "..", "config", "pricing.json"),
)

MAX_TIER_END = 9223372036854775807  # Lua math.max integer safe bound


@dataclass(frozen=True)
class Tier:
    up_to_tokens_m: int | None
    input_per_m: float
    output_per_m: float
    embedding_per_m: float


@dataclass(frozen=True)
class ModelPricing:
    pricing_mode: str  # flat | volume | graduated
    input_per_m: float
    output_per_m: float
    embedding_per_m: float
    tiers: tuple[Tier, ...]

    @classmethod
    def from_json(cls, node: dict[str, Any]) -> ModelPricing:
        input_p = float(node.get("input_per_m", 1.0))
        output_p = float(node.get("output_per_m", 3.0))
        embed_p = float(node.get("embedding_per_m", 0.10))
        raw_tiers = node.get("tiers") or []
        tiers: list[Tier] = []
        for t in raw_tiers:
            up = t.get("up_to_tokens_m")
            tiers.append(
                Tier(
                    up_to_tokens_m=int(up) if up is not None else None,
                    input_per_m=float(t.get("input_per_m", input_p)),
                    output_per_m=float(t.get("output_per_m", output_p)),
                    embedding_per_m=float(t.get("embedding_per_m", embed_p)),
                )
            )
        has_tiers = bool(tiers)
        mode = node.get("pricing_mode")
        if not mode:
            mode = "volume" if has_tiers else "flat"
        if mode == "flat" and has_tiers:
            raise ValueError("pricing_mode=flat cannot have tiers")
        if mode in ("volume", "graduated") and not has_tiers:
            raise ValueError(f"pricing_mode={mode} requires tiers")
        return cls(mode, input_p, output_p, embed_p, tuple(tiers))

    def flat_tier(self) -> Tier:
        return Tier(None, self.input_per_m, self.output_per_m, self.embedding_per_m)

    def tier_at_token(self, token_index: int) -> Tier:
        if not self.tiers:
            return self.flat_tier()
        tokens_m = token_index // 1_000_000
        for tier in self.tiers:
            if tier.up_to_tokens_m is None or tokens_m < tier.up_to_tokens_m:
                return tier
        return self.tiers[-1]

    @staticmethod
    def tier_end_tokens(tier: Tier) -> int:
        if tier.up_to_tokens_m is None:
            return MAX_TIER_END
        return tier.up_to_tokens_m * 1_000_000

    def to_lua_spec(self) -> str:
        """Compact spec for Redis Lua: mode,I,O,E|up:I:O:E,..."""
        header = f"{self.pricing_mode},{self.input_per_m},{self.output_per_m},{self.embedding_per_m}"
        if not self.tiers:
            return header + "|"
        parts = []
        for t in self.tiers:
            up = "null" if t.up_to_tokens_m is None else str(t.up_to_tokens_m)
            parts.append(f"{up}:{t.input_per_m}:{t.output_per_m}:{t.embedding_per_m}")
        return header + "|" + ",".join(parts)


class PricingCatalog:
    def __init__(self, root: dict[str, Any]):
        self.version = str(root.get("version", "1"))
        self.cache_read_multiplier = float(root.get("cache_read_multiplier", 0.5))
        self.volume_scope = root.get("volume_scope", "customer_model")
        self.billing_period = root.get("billing_period", "calendar_month")
        defaults_node = root.get("defaults") or {}
        self.defaults = ModelPricing.from_json(defaults_node)
        self.models: dict[str, ModelPricing] = {
            k: ModelPricing.from_json(v) for k, v in (root.get("models") or {}).items()
        }
        self.prefix_models: list[str] = list(root.get("prefix_models") or [])

    @classmethod
    def load_from_file(cls, path: str | None = None) -> PricingCatalog:
        with open(path or PRICING_FILE, encoding="utf-8") as f:
            return cls(json.load(f))

    @classmethod
    def load_from_redis(cls, r) -> PricingCatalog:
        snap = r.get("pricing:current")
        if snap:
            return cls(json.loads(snap))
        return cls.load_from_file()

    def normalize_model_id(self, model: str) -> str:
        if not model:
            return "unknown"
        if model in self.models:
            return model
        for prefix in self.prefix_models:
            if model.startswith(prefix):
                return prefix
        return model

    def model_pricing(self, model: str) -> ModelPricing:
        return self.models.get(self.normalize_model_id(model), self.defaults)

    def pricing_spec(self, model: str) -> str:
        return self.model_pricing(model).to_lua_spec()

    def calculate_cost_micro(self, event: dict[str, Any], monthly_tokens_before: int = 0) -> int:
        pricing = self.model_pricing(event.get("modelId", "unknown"))
        cache_mult = self.cache_read_multiplier
        if pricing.pricing_mode == "graduated":
            return self._cost_graduated(event, pricing, monthly_tokens_before, cache_mult)
        tier = pricing.tier_at_token(monthly_tokens_before)
        return self._cost_at_tier(event, tier, cache_mult)

    def _cost_at_tier(self, event: dict[str, Any], tier: Tier, cache_mult: float) -> int:
        cost = 0.0
        cost += round(event.get("inputTokens", 0) * tier.input_per_m)
        cost += round(event.get("outputTokens", 0) * tier.output_per_m)
        cost += round(event.get("cacheReadTokens", 0) * tier.input_per_m * cache_mult)
        cost += round(event.get("reasoningTokens", 0) * tier.output_per_m)
        cost += round(event.get("cacheWriteTokens", 0) * tier.input_per_m)
        cost += round(event.get("embeddingTokens", 0) * tier.embedding_per_m)
        return int(cost)

    def _cost_graduated(
        self,
        event: dict[str, Any],
        pricing: ModelPricing,
        monthly_before: int,
        cache_mult: float,
    ) -> int:
        cursor = monthly_before
        cost = 0
        pairs = (
            ("inputTokens", "input"),
            ("outputTokens", "output"),
            ("cacheReadTokens", "cache_read"),
            ("reasoningTokens", "reasoning"),
            ("cacheWriteTokens", "cache_write"),
            ("embeddingTokens", "embedding"),
        )
        for field, category in pairs:
            tokens = int(event.get(field, 0))
            chunk_cost, cursor = self._cost_category_graduated(
                tokens, cursor, pricing, category, cache_mult
            )
            cost += chunk_cost
        return cost

    def _cost_category_graduated(
        self,
        tokens: int,
        cursor: int,
        pricing: ModelPricing,
        category: str,
        cache_mult: float,
    ) -> tuple[int, int]:
        remaining = tokens
        cost = 0
        while remaining > 0:
            tier = pricing.tier_at_token(cursor)
            tier_end = pricing.tier_end_tokens(tier)
            capacity = remaining if tier_end == MAX_TIER_END else min(remaining, tier_end - cursor)
            if capacity <= 0:
                tier = pricing.tiers[-1]
                capacity = remaining
            rate = self._rate_for(tier, category, cache_mult)
            cost += round(capacity * rate)
            cursor += capacity
            remaining -= capacity
        return cost, cursor

    @staticmethod
    def _rate_for(tier: Tier, category: str, cache_mult: float) -> float:
        if category == "input":
            return tier.input_per_m
        if category in ("output", "reasoning"):
            return tier.output_per_m
        if category == "cache_read":
            return tier.input_per_m * cache_mult
        if category == "cache_write":
            return tier.input_per_m
        return tier.embedding_per_m


def billing_period_month(timestamp_ms: int) -> str:
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m")


def billing_period_day(timestamp_ms: int) -> str:
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def period_volume_key(
    tenant_id: str | None,
    customer_id: str,
    model_id: str,
    timestamp_ms: int,
) -> str:
    """customer_model scope monthly volume counter (UTC calendar month)."""
    period = billing_period_month(timestamp_ms)
    base = customer_prefix(tenant_id, customer_id)
    return f"{base}:model:{model_id}:period:{period}:volume_tokens"


# Module-level catalog; LiteAggregator reloads on init.
_catalog: PricingCatalog | None = None


def get_catalog() -> PricingCatalog:
    global _catalog
    if _catalog is None:
        _catalog = PricingCatalog.load_from_file()
    return _catalog


def reload_catalog(catalog: PricingCatalog | None = None, redis_client=None) -> PricingCatalog:
    global _catalog
    if catalog is not None:
        _catalog = catalog
    elif redis_client is not None:
        _catalog = PricingCatalog.load_from_redis(redis_client)
    else:
        _catalog = PricingCatalog.load_from_file()
    return _catalog


def normalize_model_id(model: str) -> str:
    return get_catalog().normalize_model_id(model)


def calculate_cost_micro(event: dict[str, Any], monthly_tokens_before: int = 0) -> int:
    return get_catalog().calculate_cost_micro(event, monthly_tokens_before)
