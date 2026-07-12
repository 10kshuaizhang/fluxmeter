"""Background rollup worker — compacts buffer counters into time-bucketed summaries.

Runs as asyncio background task inside the API process.
- Every 60s: snapshot customer:{id}:buf:* into minute/month/day hashes, reset buf only
- customer:{id}:* lifetime counters are never reset (matches Full mode + GET /usage/customer)
- Minute hashes have 24h TTL (auto-expire)
"""

from __future__ import annotations

import asyncio
import logging
import time

import redis

from pricing_loader import billing_period_day, billing_period_month
from usage_buckets import BUCKET_FIELDS, DAY_BUCKET_TTL

logger = logging.getLogger(__name__)

ROLLUP_INTERVAL_SEC = 60
MINUTE_BUCKET_TTL = 86400  # 24 hours

# Lua script: read buf counters, store in minute/month/day hashes, reset buf only
ROLLUP_LUA = """
local ckey = KEYS[1]
local minute_key = KEYS[2]
local ttl = tonumber(ARGV[1])

-- Read pending buffer values (not lifetime counters)
local input_t = redis.call('GET', ckey .. ':buf:input_tokens') or '0'
local output_t = redis.call('GET', ckey .. ':buf:output_tokens') or '0'
local total_t = redis.call('GET', ckey .. ':buf:total_tokens') or '0'
local events = redis.call('GET', ckey .. ':buf:event_count') or '0'
local cost = redis.call('GET', ckey .. ':buf:cost_usd') or '0'
local cache_read = redis.call('GET', ckey .. ':buf:cache_read_tokens') or '0'
local reasoning = redis.call('GET', ckey .. ':buf:reasoning_tokens') or '0'

-- Skip if nothing to roll up
if tonumber(events) == 0 then
  return 0
end

-- Store in minute hash (HINCRBY allows multiple rollups into same minute)
redis.call('HINCRBY', minute_key, 'input_tokens', input_t)
redis.call('HINCRBY', minute_key, 'output_tokens', output_t)
redis.call('HINCRBY', minute_key, 'total_tokens', total_t)
redis.call('HINCRBY', minute_key, 'event_count', events)
redis.call('HINCRBYFLOAT', minute_key, 'cost_usd', cost)
if tonumber(cache_read) > 0 then
  redis.call('HINCRBY', minute_key, 'cache_read_tokens', cache_read)
end
if tonumber(reasoning) > 0 then
  redis.call('HINCRBY', minute_key, 'reasoning_tokens', reasoning)
end
redis.call('EXPIRE', minute_key, ttl)

-- Calendar-month bucket (billing period aligned, 400d TTL)
local month_key = KEYS[3]
redis.call('HINCRBY', month_key, 'input_tokens', input_t)
redis.call('HINCRBY', month_key, 'output_tokens', output_t)
redis.call('HINCRBY', month_key, 'total_tokens', total_t)
redis.call('HINCRBY', month_key, 'event_count', events)
redis.call('HINCRBYFLOAT', month_key, 'cost_usd', cost)
if tonumber(cache_read) > 0 then
  redis.call('HINCRBY', month_key, 'cache_read_tokens', cache_read)
end
if tonumber(reasoning) > 0 then
  redis.call('HINCRBY', month_key, 'reasoning_tokens', reasoning)
end
redis.call('EXPIRE', month_key, 34560000)

-- Daily bucket (billing reports, 400d TTL)
local day_key = KEYS[4]
local day_ttl = tonumber(ARGV[2])
redis.call('HINCRBY', day_key, 'input_tokens', input_t)
redis.call('HINCRBY', day_key, 'output_tokens', output_t)
redis.call('HINCRBY', day_key, 'total_tokens', total_t)
redis.call('HINCRBY', day_key, 'event_count', events)
redis.call('HINCRBYFLOAT', day_key, 'cost_usd', cost)
if tonumber(cache_read) > 0 then
  redis.call('HINCRBY', day_key, 'cache_read_tokens', cache_read)
end
if tonumber(reasoning) > 0 then
  redis.call('HINCRBY', day_key, 'reasoning_tokens', reasoning)
end
redis.call('EXPIRE', day_key, day_ttl)

-- Reset buffer only (lifetime customer:{id}:* counters preserved)
redis.call('SET', ckey .. ':buf:input_tokens', '0')
redis.call('SET', ckey .. ':buf:output_tokens', '0')
redis.call('SET', ckey .. ':buf:total_tokens', '0')
redis.call('SET', ckey .. ':buf:event_count', '0')
redis.call('SET', ckey .. ':buf:cost_usd', '0')
redis.call('SET', ckey .. ':buf:cache_read_tokens', '0')
redis.call('SET', ckey .. ':buf:reasoning_tokens', '0')

return 1
"""


def _drain_legacy_pending_buffer(r: redis.Redis, customer_key: str) -> None:
    """ponytail: one-time bridge for pre-buffer deploys; lost lifetime is not recoverable."""
    buf_events = int(r.get(f"{customer_key}:buf:event_count") or 0)
    if buf_events > 0:
        return
    legacy_events = int(r.get(f"{customer_key}:event_count") or 0)
    if legacy_events <= 0:
        return
    for field in BUCKET_FIELDS:
        val = r.get(f"{customer_key}:{field}")
        if val is not None and val not in ("0", "0.0"):
            r.set(f"{customer_key}:buf:{field}", val)


def rollup_customer_minute(r: redis.Redis, customer_id: str, epoch_sec: int) -> str:
    """Roll up buffer counters for one customer into minute + month + day buckets."""
    minute_ts = (epoch_sec // 60) * 60
    customer_key = f"customer:{customer_id}"
    minute_key = f"rollup:{customer_id}:m:{minute_ts}"
    month_key = f"rollup:{customer_id}:period:{billing_period_month(epoch_sec * 1000)}"
    day_key = f"rollup:{customer_id}:d:{billing_period_day(epoch_sec * 1000)}"

    _drain_legacy_pending_buffer(r, customer_key)
    r.eval(ROLLUP_LUA, 4, customer_key, minute_key, month_key, day_key, MINUTE_BUCKET_TTL, DAY_BUCKET_TTL)
    return minute_key


def discover_active_customers(r: redis.Redis) -> list[str]:
    """Find customers with non-zero buf event_count (pending rollup)."""
    customers = []
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match="customer:*:buf:event_count", count=200)
        for key in keys:
            val = r.get(key)
            if val and int(val) > 0:
                # Extract customer_id from "customer:{id}:buf:event_count"
                parts = key.split(":")
                if len(parts) >= 4 and parts[2] == "buf":
                    customers.append(parts[1])
        if cursor == 0:
            break
    return customers


async def rollup_loop(r: redis.Redis):
    """Background loop: roll up all active customer counters every 60s."""
    logger.info("Rollup worker started (interval=%ds)", ROLLUP_INTERVAL_SEC)
    while True:
        try:
            await asyncio.sleep(ROLLUP_INTERVAL_SEC)
            now = int(time.time())
            customers = discover_active_customers(r)
            rolled = 0
            for cid in customers:
                rollup_customer_minute(r, cid, now)
                rolled += 1
            if rolled > 0:
                logger.info("Rolled up %d customers at minute %d", rolled, (now // 60) * 60)
        except Exception as e:
            logger.error("Rollup error: %s", e)
            await asyncio.sleep(5)
