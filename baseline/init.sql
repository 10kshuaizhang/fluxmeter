-- ADR-1 / ADR-025 auditable cold store (benchmark overlay ClickHouse)
-- Spec: docs/superpowers/specs/2026-08-11-cold-store-audit-design.md
-- Kafka messages are Trusted Envelopes (envelopeVersion + payload); flat JSON still accepted.
-- Derived aggregates live in baseline/benchmark_init.sql (make benchmark only)

CREATE DATABASE IF NOT EXISTS fluxmeter;

DROP TABLE IF EXISTS fluxmeter.token_events_mv SYNC;
DROP TABLE IF EXISTS fluxmeter.usage_per_minute_mv SYNC;
DROP TABLE IF EXISTS fluxmeter.token_events SYNC;
DROP TABLE IF EXISTS fluxmeter.token_events_queue SYNC;
DROP TABLE IF EXISTS fluxmeter.usage_per_minute SYNC;

DROP TABLE IF EXISTS fluxmeter.raw_events_mv SYNC;
DROP TABLE IF EXISTS fluxmeter.raw_events_dlq_mv SYNC;
DROP TABLE IF EXISTS fluxmeter.token_events_ingress_mv SYNC;
DROP TABLE IF EXISTS fluxmeter.raw_events SYNC;
DROP TABLE IF EXISTS fluxmeter.raw_events_dlq SYNC;
DROP TABLE IF EXISTS fluxmeter.raw_events_ingress SYNC;
DROP TABLE IF EXISTS fluxmeter.token_events_queue SYNC;

-- Whole-message ingest: Trusted Envelope or flat Token Event JSON
CREATE TABLE fluxmeter.token_events_queue
(
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'token-events',
    kafka_group_name = 'fluxmeter-cold-store',
    kafka_format = 'LineAsString',
    kafka_num_consumers = 1;

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

-- ponytail: extract via JSON* on whole message; ceiling = complex nested auth ignored.
CREATE MATERIALIZED VIEW fluxmeter.token_events_ingress_mv TO fluxmeter.raw_events_ingress AS
SELECT
    if(NOT isValidJSON(message), '',
        coalesce(
            nullIf(JSONExtractString(message, 'payload', 'eventId'), ''),
            nullIf(JSONExtractString(message, 'eventId'), ''),
            ''
        )
    ) AS event_id,
    if(NOT isValidJSON(message), '',
        coalesce(
            nullIf(JSONExtractString(message, 'payload', 'customerId'), ''),
            nullIf(JSONExtractString(message, 'customerId'), ''),
            ''
        )
    ) AS customer_id,
    if(NOT isValidJSON(message), CAST(NULL AS Nullable(String)),
        nullIf(coalesce(
            JSONExtractString(message, 'payload', 'requestId'),
            JSONExtractString(message, 'requestId')
        ), '')
    ) AS request_id,
    if(NOT isValidJSON(message), CAST(NULL AS Nullable(String)),
        nullIf(coalesce(
            JSONExtractString(message, 'payload', 'spanId'),
            JSONExtractString(message, 'spanId')
        ), '')
    ) AS span_id,
    if(NOT isValidJSON(message), CAST(NULL AS Nullable(String)),
        nullIf(coalesce(
            JSONExtractString(message, 'payload', 'parentSpanId'),
            JSONExtractString(message, 'parentSpanId')
        ), '')
    ) AS parent_span_id,
    if(NOT isValidJSON(message), '',
        coalesce(
            nullIf(JSONExtractString(message, 'payload', 'provider'), ''),
            nullIf(JSONExtractString(message, 'provider'), ''),
            ''
        )
    ) AS provider,
    if(NOT isValidJSON(message), '',
        coalesce(
            nullIf(JSONExtractString(message, 'payload', 'modelId'), ''),
            nullIf(JSONExtractString(message, 'modelId'), ''),
            ''
        )
    ) AS model_id,
    if(NOT isValidJSON(message), toUInt32(0),
        toUInt32(greatest(
            JSONExtractUInt(message, 'payload', 'inputTokens'),
            JSONExtractUInt(message, 'inputTokens')
        ))
    ) AS input_tokens,
    if(NOT isValidJSON(message), toUInt32(0),
        toUInt32(greatest(
            JSONExtractUInt(message, 'payload', 'outputTokens'),
            JSONExtractUInt(message, 'outputTokens')
        ))
    ) AS output_tokens,
    if(NOT isValidJSON(message), toUInt32(0),
        toUInt32(greatest(
            JSONExtractUInt(message, 'payload', 'cacheReadTokens'),
            JSONExtractUInt(message, 'cacheReadTokens')
        ))
    ) AS cache_read_tokens,
    if(NOT isValidJSON(message), toUInt32(0),
        toUInt32(greatest(
            JSONExtractUInt(message, 'payload', 'cacheWriteTokens'),
            JSONExtractUInt(message, 'cacheWriteTokens')
        ))
    ) AS cache_write_tokens,
    if(NOT isValidJSON(message), toUInt32(0),
        toUInt32(greatest(
            JSONExtractUInt(message, 'payload', 'reasoningTokens'),
            JSONExtractUInt(message, 'reasoningTokens')
        ))
    ) AS reasoning_tokens,
    if(NOT isValidJSON(message), toUInt32(0),
        toUInt32(greatest(
            JSONExtractUInt(message, 'payload', 'embeddingTokens'),
            JSONExtractUInt(message, 'embeddingTokens')
        ))
    ) AS embedding_tokens,
    if(
        NOT isValidJSON(message)
            OR greatest(
                JSONExtractUInt(message, 'payload', 'timestamp'),
                JSONExtractUInt(message, 'timestamp')
            ) = 0,
        -- ponytail: missing ts → fixed epoch so same event_id stays in one partition
        fromUnixTimestamp64Milli(1),
        fromUnixTimestamp64Milli(
            greatest(
                JSONExtractUInt(message, 'payload', 'timestamp'),
                JSONExtractUInt(message, 'timestamp')
            )
        )
    ) AS event_time,
    if(NOT isValidJSON(message), toUInt32(0),
        toUInt32(greatest(
            JSONExtractUInt(message, 'payload', 'latencyMs'),
            JSONExtractUInt(message, 'latencyMs')
        ))
    ) AS latency_ms,
    if(NOT isValidJSON(message), CAST(NULL AS Nullable(String)),
        nullIf(coalesce(
            JSONExtractString(message, 'payload', 'sessionId'),
            JSONExtractString(message, 'sessionId')
        ), '')
    ) AS session_id,
    if(NOT isValidJSON(message), CAST(NULL AS Nullable(String)),
        nullIf(coalesce(
            JSONExtractString(message, 'payload', 'environment'),
            JSONExtractString(message, 'environment')
        ), '')
    ) AS environment,
    if(NOT isValidJSON(message), '{}',
        multiIf(
            JSONHas(message, 'payload', 'metadata'),
                if(JSONExtractRaw(message, 'payload', 'metadata') IN ('', 'null'), '{}',
                   JSONExtractRaw(message, 'payload', 'metadata')),
            JSONHas(message, 'metadata'),
                if(JSONExtractRaw(message, 'metadata') IN ('', 'null'), '{}',
                   JSONExtractRaw(message, 'metadata')),
            '{}'
        )
    ) AS metadata,
    multiIf(
        NOT isValidJSON(message), 'parse_error',
        coalesce(
            nullIf(JSONExtractString(message, 'payload', 'eventId'), ''),
            nullIf(JSONExtractString(message, 'eventId'), ''),
            ''
        ) = '', 'missing_event_id',
        ''
    ) AS error_reason,
    '' AS error_detail,
    message AS raw_payload
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
    raw_payload,
    error_reason,
    error_detail,
    now64(3) AS ingested_at
FROM fluxmeter.raw_events_ingress
WHERE error_reason != '';
