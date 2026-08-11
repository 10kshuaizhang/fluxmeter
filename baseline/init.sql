-- ADR-1 auditable cold store (default Full-mode ClickHouse init)
-- Spec: docs/superpowers/specs/2026-08-11-cold-store-audit-design.md
-- NOTE: derived aggregates live in baseline/benchmark_init.sql (benchmark profile only)

CREATE DATABASE IF NOT EXISTS fluxmeter;

-- Drop legacy baseline objects so fresh volumes do not keep the old MergeTree path
DROP TABLE IF EXISTS fluxmeter.token_events_mv;
DROP TABLE IF EXISTS fluxmeter.usage_per_minute_mv;
DROP TABLE IF EXISTS fluxmeter.token_events;
DROP TABLE IF EXISTS fluxmeter.token_events_queue;
DROP TABLE IF EXISTS fluxmeter.usage_per_minute;

DROP TABLE IF EXISTS fluxmeter.raw_events_mv;
DROP TABLE IF EXISTS fluxmeter.raw_events_dlq_mv;
DROP TABLE IF EXISTS fluxmeter.token_events_ingress_mv;
DROP TABLE IF EXISTS fluxmeter.raw_events;
DROP TABLE IF EXISTS fluxmeter.raw_events_dlq;
DROP TABLE IF EXISTS fluxmeter.raw_events_ingress;
DROP TABLE IF EXISTS fluxmeter.token_events_queue;

CREATE TABLE fluxmeter.token_events_queue
(
    eventId String,
    customerId String,
    requestId Nullable(String),
    spanId Nullable(String),
    parentSpanId Nullable(String),
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
    metadata Map(String, String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'token-events',
    kafka_group_name = 'fluxmeter-cold-store',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 4,
    kafka_handle_error_mode = 'stream';

-- Null fanout hub (single read from Kafka engine)
CREATE TABLE fluxmeter.raw_events_ingress
(
    event_id String,
    customer_id String,
    request_id Nullable(String),
    span_id Nullable(String),
    parent_span_id Nullable(String),
    provider String,
    model_id String,
    input_tokens UInt32,
    output_tokens UInt32,
    cache_read_tokens UInt32,
    cache_write_tokens UInt32,
    reasoning_tokens UInt32,
    embedding_tokens UInt32,
    event_time DateTime64(3),
    latency_ms UInt32,
    session_id Nullable(String),
    environment Nullable(String),
    metadata String,
    error_reason String,
    error_detail String,
    raw_payload String
)
ENGINE = Null;

CREATE MATERIALIZED VIEW fluxmeter.token_events_ingress_mv TO fluxmeter.raw_events_ingress AS
SELECT
    if(length(_error) > 0, '', coalesce(eventId, '')) AS event_id,
    coalesce(customerId, '') AS customer_id,
    requestId AS request_id,
    spanId AS span_id,
    parentSpanId AS parent_span_id,
    coalesce(provider, '') AS provider,
    coalesce(modelId, '') AS model_id,
    coalesce(inputTokens, toUInt32(0)) AS input_tokens,
    coalesce(outputTokens, toUInt32(0)) AS output_tokens,
    coalesce(cacheReadTokens, toUInt32(0)) AS cache_read_tokens,
    coalesce(cacheWriteTokens, toUInt32(0)) AS cache_write_tokens,
    coalesce(reasoningTokens, toUInt32(0)) AS reasoning_tokens,
    coalesce(embeddingTokens, toUInt32(0)) AS embedding_tokens,
    if(
        length(_error) > 0 OR timestamp = 0,
        now64(3),
        fromUnixTimestamp64Milli(timestamp)
    ) AS event_time,
    coalesce(latencyMs, toUInt32(0)) AS latency_ms,
    sessionId AS session_id,
    environment AS environment,
    if(length(mapKeys(metadata)) = 0, '{}', toJSONString(metadata)) AS metadata,
    multiIf(
        length(_error) > 0, 'parse_error',
        coalesce(eventId, '') = '', 'missing_event_id',
        ''
    ) AS error_reason,
    if(length(_error) > 0, _error, '') AS error_detail,
    if(length(_error) > 0, _raw_message, '') AS raw_payload
FROM fluxmeter.token_events_queue;

CREATE TABLE fluxmeter.raw_events
(
    event_id String,
    customer_id String,
    request_id Nullable(String),
    span_id Nullable(String),
    parent_span_id Nullable(String),
    provider String,
    model_id String,
    input_tokens UInt32,
    output_tokens UInt32,
    cache_read_tokens UInt32,
    cache_write_tokens UInt32,
    reasoning_tokens UInt32,
    embedding_tokens UInt32,
    event_time DateTime64(3),
    latency_ms UInt32,
    session_id Nullable(String),
    environment Nullable(String),
    metadata String,
    ingested_at DateTime64(3) DEFAULT now64(3),
    partition_date Date MATERIALIZED toDate(event_time),
    PROJECTION proj_customer_time
    (
        SELECT *
        ORDER BY (customer_id, event_time, event_id)
    )
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY partition_date
ORDER BY (event_id)
SETTINGS index_granularity = 8192;

CREATE TABLE fluxmeter.raw_events_dlq
(
    raw_payload String,
    error_reason String,
    error_detail String,
    ingested_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (ingested_at)
TTL toDateTime(ingested_at) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW fluxmeter.raw_events_mv TO fluxmeter.raw_events AS
SELECT
    event_id,
    customer_id,
    request_id,
    span_id,
    parent_span_id,
    provider,
    model_id,
    input_tokens,
    output_tokens,
    cache_read_tokens,
    cache_write_tokens,
    reasoning_tokens,
    embedding_tokens,
    event_time,
    latency_ms,
    session_id,
    environment,
    metadata,
    now64(3) AS ingested_at
FROM fluxmeter.raw_events_ingress
WHERE error_reason = '' AND event_id != '';

CREATE MATERIALIZED VIEW fluxmeter.raw_events_dlq_mv TO fluxmeter.raw_events_dlq AS
SELECT
    if(
        raw_payload != '',
        raw_payload,
        concat(
            '{"customerId":"', customer_id,
            '","modelId":"', model_id,
            '","note":"missing_event_id"}'
        )
    ) AS raw_payload,
    error_reason,
    error_detail,
    now64(3) AS ingested_at
FROM fluxmeter.raw_events_ingress
WHERE error_reason != '';
