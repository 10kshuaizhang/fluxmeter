-- Baseline query: poll this every 5s to get "current" usage
-- This simulates what store-then-query architectures do

-- Global totals (equivalent to Flink's global:total_* Redis keys)
SELECT
    sum(total_tokens) AS total_tokens,
    sum(event_count) AS total_events,
    sum(cost_usd) AS total_cost_usd
FROM usage_per_minute;

-- Per-customer totals (equivalent to Flink's customer:* Redis keys)
SELECT
    customer_id,
    sum(total_tokens) AS total_tokens,
    sum(cost_usd) AS cost_usd
FROM usage_per_minute
GROUP BY customer_id
ORDER BY cost_usd DESC
LIMIT 10;

-- Latency measurement: time from newest event to query execution
-- This shows the inherent delay in store-then-query
SELECT
    now() AS query_time,
    max(window_start) AS newest_window,
    dateDiff('second', max(window_start), now()) AS lag_seconds
FROM usage_per_minute;
