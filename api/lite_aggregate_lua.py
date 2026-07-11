"""Atomic Lua-based lite aggregation with inline budget deduction.

Single Lua script: idempotency, period volume read, tier-aware cost, counters,
budget deduction — all atomic.
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

import redis

from pricing_loader import (
    PricingCatalog,
    billing_period_month,
    calculate_cost_micro,
    normalize_model_id,
    period_volume_key,
    reload_catalog,
)
from tenant_keys import budget_prefix, customer_prefix, global_key
from usage_buckets import DAY_BUCKET_TTL, increment_session, increment_span, model_period_key

# KEYS: [1]=idemp, [2]=customer, [3]=model, [4]=budget_bal, [5]=budget_thresh,
#       [6]=global_prefix, [7]=period_volume, [8]=package_tokens
# ARGV: [1]=input, [2]=output, [3]=total, [4]=now_ms, [5]=has_eid,
#       [6]=cache_read, [7]=reasoning, [8]=cache_write, [9]=embedding,
#       [10]=pricing_spec, [11]=cache_read_multiplier
AGGREGATE_LUA = """
local function round(x)
  if x >= 0 then return math.floor(x + 0.5) end
  return math.ceil(x - 0.5)
end

local function parse_tiers(tier_str)
  local tiers = {}
  if tier_str == nil or tier_str == '' then return tiers end
  for part in string.gmatch(tier_str, '[^,]+') do
    local up, inp, out, emb = string.match(part, '([^:]*):([^:]*):([^:]*):([^:]*)')
    local up_num = nil
    if up ~= nil and up ~= '' and up ~= 'null' then up_num = tonumber(up) end
    tiers[#tiers + 1] = {
      up_to = up_num,
      input = tonumber(inp),
      output = tonumber(out),
      embed = tonumber(emb)
    }
  end
  return tiers
end

local function tier_end(up_to)
  if up_to == nil then return 9223372036854775807 end
  return up_to * 1000000
end

local function tier_at_token(token_index, tiers, def_in, def_out, def_emb)
  if #tiers == 0 then
    return { up_to = nil, input = def_in, output = def_out, embed = def_emb }
  end
  local tokens_m = math.floor(token_index / 1000000)
  for i = 1, #tiers do
    local t = tiers[i]
    if t.up_to == nil or tokens_m < t.up_to then return t end
  end
  return tiers[#tiers]
end

local function rate_for(tier, category, cache_mult)
  if category == 'input' then return tier.input end
  if category == 'output' or category == 'reasoning' then return tier.output end
  if category == 'cache_read' then return tier.input * cache_mult end
  if category == 'cache_write' then return tier.input end
  return tier.embed
end

local function cost_at_tier(input_t, output_t, cache_read, reasoning, cache_write, embed, tier, cache_mult)
  local c = 0
  c = c + round(input_t * tier.input)
  c = c + round(output_t * tier.output)
  c = c + round(cache_read * tier.input * cache_mult)
  c = c + round(reasoning * tier.output)
  c = c + round(cache_write * tier.input)
  c = c + round(embed * tier.embed)
  return c
end

local function cost_category_graduated(tokens, cursor, tiers, def_in, def_out, def_emb, category, cache_mult)
  local remaining = tokens
  local cost = 0
  while remaining > 0 do
    local tier = tier_at_token(cursor, tiers, def_in, def_out, def_emb)
    local tend = tier_end(tier.up_to)
    local capacity = remaining
    if tend < 9223372036854775807 then
      capacity = math.min(remaining, tend - cursor)
    end
    if capacity <= 0 then
      tier = tiers[#tiers]
      capacity = remaining
    end
    local rate = rate_for(tier, category, cache_mult)
    cost = cost + round(capacity * rate)
    cursor = cursor + capacity
    remaining = remaining - capacity
  end
  return cost, cursor
end

local function compute_cost_micro(mode, def_in, def_out, def_emb, tiers, monthly_before,
    input_t, output_t, cache_read, reasoning, cache_write, embed, cache_mult)
  if mode == 'graduated' then
    local cursor = monthly_before
    local total = 0
    local c
    c, cursor = cost_category_graduated(input_t, cursor, tiers, def_in, def_out, def_emb, 'input', cache_mult)
    total = total + c
    c, cursor = cost_category_graduated(output_t, cursor, tiers, def_in, def_out, def_emb, 'output', cache_mult)
    total = total + c
    c, cursor = cost_category_graduated(cache_read, cursor, tiers, def_in, def_out, def_emb, 'cache_read', cache_mult)
    total = total + c
    c, cursor = cost_category_graduated(reasoning, cursor, tiers, def_in, def_out, def_emb, 'reasoning', cache_mult)
    total = total + c
    c, cursor = cost_category_graduated(cache_write, cursor, tiers, def_in, def_out, def_emb, 'cache_write', cache_mult)
    total = total + c
    c, cursor = cost_category_graduated(embed, cursor, tiers, def_in, def_out, def_emb, 'embedding', cache_mult)
    total = total + c
    return total
  end
  local tier = tier_at_token(monthly_before, tiers, def_in, def_out, def_emb)
  return cost_at_tier(input_t, output_t, cache_read, reasoning, cache_write, embed, tier, cache_mult)
end

-- Idempotency
local has_eid = tonumber(ARGV[5])
if has_eid == 1 then
  local set = redis.call('SET', KEYS[1], '1', 'NX', 'EX', 600)
  if not set then return {0, 0, 0, 0} end
end

local input_t = tonumber(ARGV[1])
local output_t = tonumber(ARGV[2])
local total_t = tonumber(ARGV[3])
local now_ms = ARGV[4]
local cache_read = tonumber(ARGV[6])
local reasoning = tonumber(ARGV[7])
local cache_write = tonumber(ARGV[8])
local embed = tonumber(ARGV[9])
local spec = ARGV[10]
local cache_mult = tonumber(ARGV[11])

-- Prepaid token package drawdown (optional KEYS[8])
local pkg_key = KEYS[8]
if pkg_key and pkg_key ~= '' and pkg_key ~= 'noop' then
  local rem = tonumber(redis.call('GET', pkg_key) or '0')
  if total_t > rem then
    return {-2, 0, 0, 0}
  end
  redis.call('DECRBY', pkg_key, total_t)
end

local mode, def_in, def_out, def_emb, tier_part = string.match(spec, '([^,]+),([^,]+),([^,]+),([^|]+)|(.*)')
mode = mode or 'flat'
def_in = tonumber(def_in) or 1.0
def_out = tonumber(def_out) or 3.0
def_emb = tonumber(def_emb) or 0.1
local tiers = parse_tiers(tier_part)

local period_key = KEYS[7]
local monthly_before = tonumber(redis.call('INCRBY', period_key, 0) or '0')

local cost_micro = compute_cost_micro(mode, def_in, def_out, def_emb, tiers, monthly_before,
  input_t, output_t, cache_read, reasoning, cache_write, embed, cache_mult)
local cost_usd = cost_micro / 1000000.0

local ckey = KEYS[2]
redis.call('INCRBY', ckey .. ':input_tokens', input_t)
redis.call('INCRBY', ckey .. ':output_tokens', output_t)
redis.call('INCRBY', ckey .. ':total_tokens', total_t)
redis.call('INCRBY', ckey .. ':event_count', 1)
redis.call('INCRBYFLOAT', ckey .. ':cost_usd', cost_usd)
if cache_read > 0 then redis.call('INCRBY', ckey .. ':cache_read_tokens', cache_read) end
if reasoning > 0 then redis.call('INCRBY', ckey .. ':reasoning_tokens', reasoning) end

local mkey = KEYS[3]
redis.call('INCRBY', mkey .. ':input_tokens', input_t)
redis.call('INCRBY', mkey .. ':output_tokens', output_t)
redis.call('INCRBY', mkey .. ':total_tokens', total_t)
redis.call('INCRBYFLOAT', mkey .. ':cost_usd', cost_usd)

local gprefix = KEYS[6]
redis.call('INCRBY', gprefix .. 'total_tokens', total_t)
redis.call('INCRBY', gprefix .. 'input_tokens', input_t)
redis.call('INCRBY', gprefix .. 'output_tokens', output_t)
redis.call('INCRBY', gprefix .. 'total_events', 1)
redis.call('INCRBYFLOAT', gprefix .. 'total_cost_usd', cost_usd)
redis.call('SET', gprefix .. 'last_window_end', now_ms)

redis.call('INCRBY', period_key, total_t)

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
  redis.call('INCRBYFLOAT', KEYS[4] .. '_deducted', cost_usd)
  return {status, tostring(new_balance), monthly_before, cost_micro}
end

return {status, -1, monthly_before, cost_micro}
"""


class LiteAggregator:
    """Production-grade atomic aggregator for lite mode."""

    def __init__(self, r: redis.Redis, catalog: PricingCatalog | None = None):
        self._redis = r
        self._catalog = catalog or reload_catalog(redis_client=r)
        self._script = r.register_script(AGGREGATE_LUA)

    def aggregate(self, event: dict[str, Any]) -> dict:
        """Atomically aggregate one event. Returns status dict."""
        customer_id = event.get("customerId")
        model_id = event.get("modelId", "unknown")
        tenant_id = event.get("tenantId")
        if not customer_id or not model_id:
            return {"status": "rejected", "reason": "missing_customer_or_model"}

        catalog = self._catalog
        normalized_model = catalog.normalize_model_id(model_id)
        event_id = event.get("eventId")
        input_t = event.get("inputTokens", 0)
        output_t = event.get("outputTokens", 0)
        cache_read = event.get("cacheReadTokens", 0)
        reasoning = event.get("reasoningTokens", 0)
        embedding = event.get("embeddingTokens", 0)
        cache_write = event.get("cacheWriteTokens", 0)
        total_t = input_t + output_t + cache_read + cache_write + reasoning + embedding
        now_ms = event.get("timestamp", int(time.time() * 1000))

        idemp_key = ""
        has_eid = 0
        if event_id:
            idemp_key = "e:" + hashlib.sha256(event_id.encode()).hexdigest()[:16]
            has_eid = 1
        else:
            idemp_key = "e:noop"

        customer_key = customer_prefix(tenant_id, customer_id)
        model_key = f"{customer_key}:model:{normalized_model}"
        budget_base = budget_prefix(tenant_id, customer_id)
        budget_balance_key = f"{budget_base}:balance_usd"
        budget_threshold_key = f"{budget_base}:threshold_pct"
        global_prefix = global_key(tenant_id, "")
        period_key = period_volume_key(tenant_id, customer_id, normalized_model, now_ms)
        pricing_spec = catalog.pricing_spec(normalized_model)
        pkg_redis_key = f"package:{customer_id}:tokens_remaining"
        package_key = pkg_redis_key if self._redis.exists(pkg_redis_key) else "noop"

        result = self._script(
            keys=[
                idemp_key,
                customer_key,
                model_key,
                budget_balance_key,
                budget_threshold_key,
                global_prefix,
                period_key,
                package_key,
            ],
            args=[
                input_t,
                output_t,
                total_t,
                str(now_ms),
                has_eid,
                cache_read,
                reasoning,
                cache_write,
                embedding,
                pricing_spec,
                str(catalog.cache_read_multiplier),
            ],
        )

        status_code = int(result[0])
        if status_code == 0:
            return {"status": "duplicate", "event_id": event_id}
        if status_code == -2:
            return {"status": "rejected", "reason": "package_exhausted", "event_id": event_id}

        cost_usd = int(result[3]) / 1_000_000.0

        session_id = event.get("sessionId")
        if session_id:
            increment_session(
                self._redis,
                customer_id,
                session_id,
                input_tokens=input_t,
                output_tokens=output_t,
                total_tokens=total_t,
                cost_usd=cost_usd,
                cache_read_tokens=cache_read,
                reasoning_tokens=reasoning,
            )

        parent_span_id = event.get("parentSpanId")
        if parent_span_id:
            increment_span(
                self._redis,
                tenant_id,
                customer_id,
                parent_span_id,
                total_tokens=total_t,
                cost_usd=cost_usd,
                event_ts_ms=now_ms,
            )

        period = billing_period_month(now_ms)
        mp_key = model_period_key(customer_id, normalized_model, period)
        pipe = self._redis.pipeline()
        pipe.hincrbyfloat(mp_key, "cost_usd", cost_usd)
        pipe.hincrby(mp_key, "event_count", 1)
        pipe.hincrby(mp_key, "total_tokens", total_t)
        pipe.hincrby(mp_key, "input_tokens", input_t)
        pipe.hincrby(mp_key, "output_tokens", output_t)
        pipe.expire(mp_key, DAY_BUCKET_TTL)
        pipe.execute()

        response = {"status": "ok", "cost_usd": cost_usd}
        if status_code == -1:
            response["budget_alert"] = "BUDGET_EXHAUSTED"
        balance_after = float(result[1])
        if balance_after >= 0:
            response["balance_usd"] = balance_after

        return response

    def aggregate_batch(self, events: list[dict[str, Any]]) -> list[dict]:
        return [self.aggregate(e) for e in events]
