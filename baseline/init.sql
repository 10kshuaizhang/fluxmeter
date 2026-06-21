CREATE DATABASE IF NOT EXISTS fluxmeter;

-- ClickHouse table for raw token events (Kafka engine consumes directly)
-- Field names match JSON camelCase output from Java serializer
CREATE TABLE IF NOT EXISTS fluxmeter.token_events_queue (
    eventId String,
    customerId String,
    requestId Nullable(String),
    spanId Nullable(String),
    provider String,
    modelId String,
    inputTokens UInt32,
    outputTokens UInt32,
    cacheReadTokens UInt32,
    cacheWriteTokens UInt32,
    reasoningTokens UInt32,
    embeddingTokens UInt32,
    `timestamp` UInt64,
    latencyMs UInt32,
    sessionId Nullable(String),
    environment Nullable(String)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'token-events',
    kafka_group_name = 'clickhouse-baseline',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 4,
    kafka_skip_broken_messages = 100;

-- Target table for persisted events
CREATE TABLE IF NOT EXISTS fluxmeter.token_events (
    eventId String,
    customerId String,
    requestId Nullable(String),
    spanId Nullable(String),
    provider String,
    modelId String,
    inputTokens UInt32,
    outputTokens UInt32,
    cacheReadTokens UInt32,
    cacheWriteTokens UInt32,
    reasoningTokens UInt32,
    embeddingTokens UInt32,
    `timestamp` UInt64,
    latencyMs UInt32,
    sessionId Nullable(String),
    environment Nullable(String),
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (customerId, modelId, `timestamp`);

-- Materialized view: auto-insert from Kafka queue into target table
CREATE MATERIALIZED VIEW IF NOT EXISTS fluxmeter.token_events_mv TO fluxmeter.token_events AS
SELECT * FROM fluxmeter.token_events_queue;

-- Pre-aggregated view: per-minute usage summary per customer/model
CREATE TABLE IF NOT EXISTS fluxmeter.usage_per_minute (
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
) ENGINE = SummingMergeTree()
ORDER BY (customerId, modelId, window_start);

-- Materialized view: aggregate into per-minute buckets with multi-category cost
CREATE MATERIALIZED VIEW IF NOT EXISTS fluxmeter.usage_per_minute_mv TO fluxmeter.usage_per_minute AS
SELECT
    toStartOfMinute(fromUnixTimestamp64Milli(`timestamp`)) AS window_start,
    customerId,
    provider,
    modelId,
    sum(inputTokens) AS input_tokens,
    sum(outputTokens) AS output_tokens,
    sum(cacheReadTokens) AS cache_read_tokens,
    sum(reasoningTokens) AS reasoning_tokens,
    sum(inputTokens + outputTokens + cacheReadTokens + cacheWriteTokens + reasoningTokens + embeddingTokens) AS total_tokens,
    count() AS event_count,
    sum(latencyMs) AS total_latency_ms,
    sum(
        (inputTokens / 1000000.0) * multiIf(
            modelId = 'gpt-4o', 2.50, modelId = 'gpt-4o-mini', 0.15,
            modelId = 'o1', 15.00, modelId = 'o3-mini', 1.10,
            modelId = 'claude-opus-4', 15.00, modelId = 'claude-sonnet-4', 3.00,
            modelId = 'claude-haiku-4', 0.80,
            modelId = 'gemini-1.5-pro', 3.50, modelId = 'gemini-1.5-flash', 0.075,
            1.00)
        + (outputTokens / 1000000.0) * multiIf(
            modelId = 'gpt-4o', 10.00, modelId = 'gpt-4o-mini', 0.60,
            modelId = 'o1', 60.00, modelId = 'o3-mini', 4.40,
            modelId = 'claude-opus-4', 75.00, modelId = 'claude-sonnet-4', 15.00,
            modelId = 'claude-haiku-4', 4.00,
            modelId = 'gemini-1.5-pro', 10.50, modelId = 'gemini-1.5-flash', 0.30,
            3.00)
        + (reasoningTokens / 1000000.0) * multiIf(
            modelId = 'o1', 60.00, modelId = 'o3-mini', 4.40, 3.00)
        + (cacheReadTokens / 1000000.0) * multiIf(
            modelId = 'gpt-4o', 1.25, modelId = 'gpt-4o-mini', 0.075,
            modelId = 'claude-sonnet-4', 1.50, 0.50)
    ) AS cost_usd
FROM fluxmeter.token_events_queue
GROUP BY window_start, customerId, provider, modelId;
