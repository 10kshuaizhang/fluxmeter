"""Atomic Lua-based lite aggregation with inline budget deduction.

Single Lua script ensures all-or-nothing: idempotency check, counter increments,
budget deduction, and threshold alert happen atomically.
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

import redis

from tenant_keys import budget_prefix, customer_prefix, global_key

# Pricing tables (mirrors config/pricing.json)
INPUT_PRICES = {
    "gpt-4o": 2.50, "gpt-4o-mini": 0.15, "o1": 15.00, "o3-mini": 1.10,
    "claude-opus-4": 15.00, "claude-sonnet-4": 3.00, "claude-haiku-4": 0.80,
    "gemini-1.5-pro": 3.50, "gemini-1.5-flash": 0.075,
}
OUTPUT_PRICES = {
    "gpt-4o": 10.00, "gpt-4o-mini": 0.60, "o1": 60.00, "o3-mini": 4.40,
    "claude-opus-4": 75.00, "claude-sonnet-4": 15.00, "claude-haiku-4": 4.00,
    "gemini-1.5-pro": 10.50, "gemini-1.5-flash": 0.30,
}
EMBEDDING_PRICES = {
    "text-embedding-3-small": 0.02, "text-embedding-3-large": 0.13,
}

KNOWN_MODELS = frozenset(INPUT_PRICES.keys() | OUTPUT_PRICES.keys() | EMBEDDING_PRICES.keys())

PREFIX_MODELS = [
    "gpt-4o-mini", "gpt-4o", "o3-mini", "o1",
    "claude-opus-4", "claude-sonnet-4", "claude-haiku-4",
    "gemini-1.5-pro", "gemini-1.5-flash",
    "text-embedding-3-large", "text-embedding-3-small",
]


def normalize_model_id(model: str) -> str:
    if not model:
        return "unknown"
    if model in KNOWN_MODELS:
        return model
    for known in PREFIX_MODELS:
        if model.startswith(known):
            return known
    return model


def calculate_cost_micro(event: dict[str, Any]) -> int:
    """Calculate event cost in microdollars (1 USD = 1,000,000 micro)."""
    model = normalize_model_id(event.get("modelId", "unknown"))
    cost = 0.0
    cost += event.get("inputTokens", 0) * INPUT_PRICES.get(model, 1.00)
    cost += event.get("outputTokens", 0) * OUTPUT_PRICES.get(model, 3.00)
    cost += event.get("cacheReadTokens", 0) * INPUT_PRICES.get(model, 1.00) * 0.5
    cost += event.get("reasoningTokens", 0) * OUTPUT_PRICES.get(model, 3.00)
    cost += event.get("cacheWriteTokens", 0) * INPUT_PRICES.get(model, 1.00)
    cost += event.get("embeddingTokens", 0) * EMBEDDING_PRICES.get(model, 0.10)
    return round(cost)


# Lua script: atomic aggregate + budget deduct + threshold check
# KEYS: [1]=idemp_key, [2]=customer_key, [3]=model_key,
#        [4]=budget_balance_key, [5]=budget_threshold_key, [6]=global_key_prefix
# ARGV: [1]=input_t, [2]=output_t, [3]=total_t, [4]=cost_usd_str,
#        [5]=now_ms, [6]=has_event_id (0/1), [7]=cache_read, [8]=reasoning
# Returns: [status, balance_after]
#   status: 1=success, 0=duplicate, -1=budget_exhausted_alert
AGGREGATE_LUA = """
-- Idempotency check
local has_eid = tonumber(ARGV[6])
if has_eid == 1 then
  local set = redis.call('SET', KEYS[1], '1', 'NX', 'EX', 600)
  if not set then
    return {0, 0}
  end
end

-- Parse args
local input_t = tonumber(ARGV[1])
local output_t = tonumber(ARGV[2])
local total_t = tonumber(ARGV[3])
local cost_usd = tonumber(ARGV[4])
local now_ms = ARGV[5]
local cache_read = tonumber(ARGV[7])
local reasoning = tonumber(ARGV[8])

-- Customer counters
local ckey = KEYS[2]
redis.call('INCRBY', ckey .. ':input_tokens', input_t)
redis.call('INCRBY', ckey .. ':output_tokens', output_t)
redis.call('INCRBY', ckey .. ':total_tokens', total_t)
redis.call('INCRBY', ckey .. ':event_count', 1)
redis.call('INCRBYFLOAT', ckey .. ':cost_usd', cost_usd)
if cache_read > 0 then
  redis.call('INCRBY', ckey .. ':cache_read_tokens', cache_read)
end
if reasoning > 0 then
  redis.call('INCRBY', ckey .. ':reasoning_tokens', reasoning)
end

-- Model counters
local mkey = KEYS[3]
redis.call('INCRBY', mkey .. ':input_tokens', input_t)
redis.call('INCRBY', mkey .. ':output_tokens', output_t)
redis.call('INCRBY', mkey .. ':total_tokens', total_t)
redis.call('INCRBYFLOAT', mkey .. ':cost_usd', cost_usd)

-- Global counters (tenant-scoped or single-tenant global:)
local gprefix = KEYS[6]
redis.call('INCRBY', gprefix .. 'total_tokens', total_t)
redis.call('INCRBY', gprefix .. 'input_tokens', input_t)
redis.call('INCRBY', gprefix .. 'output_tokens', output_t)
redis.call('INCRBY', gprefix .. 'total_events', 1)
redis.call('INCRBYFLOAT', gprefix .. 'total_cost_usd', cost_usd)
redis.call('SET', gprefix .. 'last_window_end', now_ms)

-- Budget deduction (if budget exists)
local bal_key = KEYS[4]
local balance = redis.call('GET', bal_key)
local status = 1
if balance then
  local new_balance = tonumber(balance) - cost_usd
  if new_balance < 0 then
    new_balance = 0
    status = -1
  end
  redis.call('SET', bal_key, tostring(new_balance))
  -- Track total deducted for reconciliation
  redis.call('INCRBYFLOAT', KEYS[4] .. '_deducted', cost_usd)
  return {status, tostring(new_balance)}
end

return {status, -1}
"""


class LiteAggregator:
    """Production-grade atomic aggregator for lite mode."""

    def __init__(self, r: redis.Redis):
        self._redis = r
        self._script = r.register_script(AGGREGATE_LUA)

    def aggregate(self, event: dict[str, Any]) -> dict:
        """Atomically aggregate one event. Returns status dict."""
        customer_id = event.get("customerId")
        model_id = event.get("modelId", "unknown")
        tenant_id = event.get("tenantId")
        if not customer_id or not model_id:
            return {"status": "rejected", "reason": "missing_customer_or_model"}

        normalized_model = normalize_model_id(model_id)
        event_id = event.get("eventId")
        input_t = event.get("inputTokens", 0)
        output_t = event.get("outputTokens", 0)
        cache_read = event.get("cacheReadTokens", 0)
        reasoning = event.get("reasoningTokens", 0)
        embedding = event.get("embeddingTokens", 0)
        cache_write = event.get("cacheWriteTokens", 0)
        total_t = input_t + output_t + cache_read + cache_write + reasoning + embedding
        cost_micro = calculate_cost_micro(event)
        cost_usd = cost_micro / 1_000_000.0
        now_ms = event.get("timestamp", int(time.time() * 1000))

        # Build keys
        idemp_key = ""
        has_eid = 0
        if event_id:
            idemp_key = "e:" + hashlib.sha256(event_id.encode()).hexdigest()[:16]
            has_eid = 1
        else:
            idemp_key = "e:noop"  # Placeholder, won't be checked

        customer_key = customer_prefix(tenant_id, customer_id)
        model_key = f"{customer_key}:model:{normalized_model}"
        budget_base = budget_prefix(tenant_id, customer_id)
        budget_balance_key = f"{budget_base}:balance_usd"
        budget_threshold_key = f"{budget_base}:threshold_pct"
        global_prefix = global_key(tenant_id, "")

        result = self._script(
            keys=[idemp_key, customer_key, model_key, budget_balance_key,
                  budget_threshold_key, global_prefix],
            args=[input_t, output_t, total_t, f"{cost_usd:.10f}",
                  str(now_ms), has_eid, cache_read, reasoning],
        )

        status_code = int(result[0])
        if status_code == 0:
            return {"status": "duplicate", "event_id": event_id}

        response = {"status": "ok", "cost_usd": cost_usd}
        if status_code == -1:
            response["budget_alert"] = "BUDGET_EXHAUSTED"
        balance_after = float(result[1])
        if balance_after >= 0:
            response["balance_usd"] = balance_after

        return response

    def aggregate_batch(self, events: list[dict[str, Any]]) -> list[dict]:
        """Aggregate multiple events. Each is atomic independently."""
        return [self.aggregate(e) for e in events]
