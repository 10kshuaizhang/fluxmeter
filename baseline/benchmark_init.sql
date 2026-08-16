-- Benchmark-only derived aggregates (NOT audit SoR). ADR-0: keep out of default init.
CREATE TABLE IF NOT EXISTS fluxmeter.usage_per_minute
(
    window_start DateTime,
    customerId String,
    provider String,
    modelId String,
    input_tokens UInt64,
    output_tokens UInt64,
    cache_read_tokens UInt64,
    reasoning_tokens UInt64,
    total_tokens UInt64,
    event_count UInt64,
    total_latency_ms UInt64,
    cost_usd Float64
)
ENGINE = SummingMergeTree()
ORDER BY (customerId, modelId, window_start);

DROP TABLE IF EXISTS fluxmeter.usage_per_minute_mv;
CREATE MATERIALIZED VIEW fluxmeter.usage_per_minute_mv TO fluxmeter.usage_per_minute AS
SELECT
    window_start,
    customerId,
    provider,
    modelId,
    sum(input_tokens) AS input_tokens,
    sum(output_tokens) AS output_tokens,
    sum(cache_read_tokens) AS cache_read_tokens,
    sum(reasoning_tokens) AS reasoning_tokens,
    sum(total_tokens) AS total_tokens,
    count() AS event_count,
    sum(latency_ms) AS total_latency_ms,
    sum(row_cost_usd) AS cost_usd
FROM
(
    SELECT
        toStartOfMinute(event_time) AS window_start,
        customer_id AS customerId,
        provider,
        model_id AS modelId,
        toUInt64(input_tokens) AS input_tokens,
        toUInt64(output_tokens) AS output_tokens,
        toUInt64(cache_read_tokens) AS cache_read_tokens,
        toUInt64(reasoning_tokens) AS reasoning_tokens,
        toUInt64(input_tokens + output_tokens + cache_read_tokens + cache_write_tokens + reasoning_tokens + embedding_tokens) AS total_tokens,
        toUInt64(latency_ms) AS latency_ms,
        toFloat64(
            (input_tokens / 1000000.0) * multiIf(
                model_id = 'gpt-4o', 2.50, model_id = 'gpt-4o-mini', 0.15,
                model_id = 'o1', 15.00, model_id = 'o3-mini', 1.10,
                model_id = 'claude-opus-4', 15.00, model_id = 'claude-sonnet-4', 3.00,
                model_id = 'claude-haiku-4', 0.80,
                model_id = 'gemini-1.5-pro', 3.50, model_id = 'gemini-1.5-flash', 0.075,
                1.00)
            + (output_tokens / 1000000.0) * multiIf(
                model_id = 'gpt-4o', 10.00, model_id = 'gpt-4o-mini', 0.60,
                model_id = 'o1', 60.00, model_id = 'o3-mini', 4.40,
                model_id = 'claude-opus-4', 75.00, model_id = 'claude-sonnet-4', 15.00,
                model_id = 'claude-haiku-4', 4.00,
                model_id = 'gemini-1.5-pro', 10.50, model_id = 'gemini-1.5-flash', 0.30,
                3.00)
            + (reasoning_tokens / 1000000.0) * multiIf(
                model_id = 'o1', 60.00, model_id = 'o3-mini', 4.40, 3.00)
            + (cache_read_tokens / 1000000.0) * multiIf(
                model_id = 'gpt-4o', 1.25, model_id = 'gpt-4o-mini', 0.075,
                model_id = 'claude-sonnet-4', 1.50, 0.50)
        ) AS row_cost_usd
    FROM fluxmeter.raw_events
)
GROUP BY window_start, customerId, provider, modelId;
