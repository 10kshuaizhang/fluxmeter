"""Background rollup worker — compacts live counters into time-bucketed summaries.

Runs as asyncio background task inside the API process.
- Every 60s: snapshot live counters into per-minute hash, reset live counters
- Minute hashes have 24h TTL (auto-expire)
- /usage/* endpoints read from live counters (current minute) + rolled-up history

This prevents unbounded Redis key growth while preserving time-series granularity.
"""

from __future__ import annotations

import asyncio
import logging
import time

import redis

logger = logging.getLogger(__name__)

ROLLUP_INTERVAL_SEC = 60
MINUTE_BUCKET_TTL = 86400  # 24 hours

# Lua script: atomically read counters, store in minute hash, reset to zero
ROLLUP_LUA = """
local ckey = KEYS[1]
local minute_key = KEYS[2]
local ttl = tonumber(ARGV[1])

-- Read current values
local input_t = redis.call('GET', ckey .. ':input_tokens') or '0'
local output_t = redis.call('GET', ckey .. ':output_tokens') or '0'
local total_t = redis.call('GET', ckey .. ':total_tokens') or '0'
local events = redis.call('GET', ckey .. ':event_count') or '0'
local cost = redis.call('GET', ckey .. ':cost_usd') or '0'
local cache_read = redis.call('GET', ckey .. ':cache_read_tokens') or '0'
local reasoning = redis.call('GET', ckey .. ':reasoning_tokens') or '0'

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

-- Reset live counters (GETDEL pattern via SET 0)
redis.call('SET', ckey .. ':input_tokens', '0')
redis.call('SET', ckey .. ':output_tokens', '0')
redis.call('SET', ckey .. ':total_tokens', '0')
redis.call('SET', ckey .. ':event_count', '0')
redis.call('SET', ckey .. ':cost_usd', '0')
redis.call('SET', ckey .. ':cache_read_tokens', '0')
redis.call('SET', ckey .. ':reasoning_tokens', '0')

return 1
"""


def rollup_customer_minute(r: redis.Redis, customer_id: str, epoch_sec: int) -> str:
    """Roll up live counters for one customer into a minute bucket.

    Returns the minute bucket key.
    """
    minute_ts = (epoch_sec // 60) * 60
    customer_key = f"customer:{customer_id}"
    minute_key = f"rollup:{customer_id}:m:{minute_ts}"

    r.eval(ROLLUP_LUA, 2, customer_key, minute_key, MINUTE_BUCKET_TTL)
    return minute_key


def discover_active_customers(r: redis.Redis) -> list[str]:
    """Find customers with non-zero event_count (active in current interval)."""
    customers = []
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match="customer:*:event_count", count=200)
        for key in keys:
            val = r.get(key)
            if val and int(val) > 0:
                # Extract customer_id from "customer:{id}:event_count"
                parts = key.split(":")
                if len(parts) >= 3:
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
