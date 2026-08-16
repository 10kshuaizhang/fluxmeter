# Auditable ClickHouse Cold Store (ADR-1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade the Full-mode ClickHouse Kafka path so every valid `token-events` message with an `eventId` lands in an immutable, dedupe-by-`event_id` `raw_events` table (with DLQ for bad/missing-id messages), without an external `cold_sink/` process and without Lite dual-write.

**Architecture:** Keep ClickHouse Kafka engine as the only cold-store consumer (`kafka_group_name=fluxmeter-cold-store`). Normalize through a Null-engine fanout into `raw_events` (ReplacingMergeTree on `event_id` + customer/time projection) or `raw_events_dlq`. Move `usage_per_minute` aggregates into a benchmark-only init loaded via compose profile. Acceptance is a shell harness (`make test-cold-store`) against kafka + clickhouse.

**Tech Stack:** ClickHouse 24.1 (Kafka engine, `kafka_handle_error_mode=stream`), Kafka topic `token-events`, docker-compose.full.yml, bash + curl for acceptance, existing Makefile targets.

**Spec:** [docs/superpowers/specs/2026-08-11-cold-store-audit-design.md](../specs/2026-08-11-cold-store-audit-design.md)

## Global Constraints

- Identity = `eventId` only (map to `event_id`); never composite replace keys.
- Wide row = one LLM call; align with `spec/schema/token-event-v1.json`.
- Lite path: **out of scope** — do not produce to Kafka from Lite for cold store.
- No Java/Python `cold_sink/` package; no query/aggregate API (ADR-2).
- `raw_events` must not contain `balance` / `held` / `invoice_id` / `aggregated_*` / `cost_usd`.
- Default compose must **not** load derived aggregate MVs; those are benchmark profile only.
- Audit default: **no** silent `kafka_skip_broken_messages > 0`; use `kafka_handle_error_mode='stream'` + DLQ.
- Dedup acceptance is **read-time** (`FINAL` or equivalent); physical duplicates before merge are OK.
- ClickHouse image pin remains `clickhouse/clickhouse-server:24.1`.
- Ship as **4.4.0** (MINOR — new cold-store capability) when implementation lands; update `build.gradle`, `changLog.md`, `progress.md`, `ROADMAP.md` in the final task.

---

## File map

| File | Responsibility |
|------|----------------|
| `baseline/init.sql` | Default cold-store DDL: Kafka queue, Null ingress, `raw_events`, projection, `raw_events_dlq`, MVs |
| `baseline/benchmark_init.sql` | Optional `usage_per_minute` + MV (derived; benchmark only) |
| `baseline/query_audit.sql` | Operator templates: FINAL count, customer range, DLQ inspect |
| `baseline/query.sql` | Keep benchmark aggregate queries; header note that table is not audit SoR |
| `baseline/benchmark.sh` | Ensure benchmark profile tables exist; keep latency comparison |
| `docker-compose.full.yml` | Mount `01_cold_store.sql`; add `benchmark` profile mounting `02_benchmark.sql` |
| `scripts/test-cold-store.sh` | Acceptance A1/A2/A4/A5/A6/A7 (+ optional A3 note) |
| `Makefile` | `test-cold-store` target |
| `docs/runbooks/cold-store-dlq.md` | DLQ inspect / replay rules |
| `docs/disaster-recovery.md` | Point Full recovery at `raw_events`; Lite still has no cold copy |
| `README.md` | Short cold-store + `make test-cold-store` / benchmark profile notes |
| `build.gradle` / `changLog.md` / `progress.md` / `ROADMAP.md` | Version 4.4.0 + checklist |

---

### Task 1: Failing acceptance harness

**Files:**
- Create: `scripts/test-cold-store.sh`
- Modify: `Makefile` (`.PHONY` + `test-cold-store` target)
- Test: `scripts/test-cold-store.sh` (self-validating)

**Interfaces:**
- Consumes: HTTP ClickHouse at `${CLICKHOUSE_URL:-http://localhost:8123}`; Kafka container `fluxmeter-kafka`; topic `token-events`
- Produces: exit 0 only when A1/A2/A4/A5/A6/A7 pass; helpers `ch_query`, `kafka_produce` for later tasks

- [ ] **Step 1: Write the acceptance script (expects new tables — will fail on current baseline)**

```bash
#!/usr/bin/env bash
# scripts/test-cold-store.sh — ADR-1 acceptance (A1,A2,A4,A5,A6,A7)
set -euo pipefail

CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-fluxmeter-kafka}"
TOPIC="${TOPIC:-token-events}"
PREFIX="coldtest-$(date +%s)-$$"
PASS=0
FAIL=0

ch_query() {
  curl -sf "$CLICKHOUSE_URL" --data-binary "$1"
}

kafka_produce() {
  # $1 = single-line JSON
  echo -n "$1" | docker exec -i "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic "$TOPIC" >/dev/null
}

wait_until() {
  local desc="$1" sql="$2" expect="$3" tries="${4:-30}"
  local got=""
  for _ in $(seq 1 "$tries"); do
    got="$(ch_query "$sql" 2>/dev/null || true)"
    got="$(echo -n "$got" | tr -d '[:space:]')"
    if [ "$got" = "$expect" ]; then
      echo "  OK  $desc (got=$got)"
      PASS=$((PASS + 1))
      return 0
    fi
    sleep 1
  done
  echo "  FAIL $desc (expected=$expect got=${got:-<empty>})"
  FAIL=$((FAIL + 1))
  return 1
}

echo "== ADR-1 cold store acceptance =="

# Preconditions
ch_query "SELECT 1" >/dev/null
docker inspect "$KAFKA_CONTAINER" >/dev/null

# A7: no derived columns on raw_events
COLS="$(ch_query "SELECT name FROM system.columns WHERE database='fluxmeter' AND table='raw_events' FORMAT TSV")"
for bad in balance held invoice_id cost_usd aggregated_total; do
  if echo "$COLS" | grep -qi "$bad"; then
    echo "  FAIL A7 column '$bad' present"; FAIL=$((FAIL + 1))
  else
    echo "  OK  A7 no column '$bad'"; PASS=$((PASS + 1))
  fi
done

# A1: N unique events
N=5
CID="${PREFIX}-cust"
for i in $(seq 1 "$N"); do
  ts=$(( $(date +%s) * 1000 ))
  kafka_produce "{\"eventId\":\"${PREFIX}-e${i}\",\"customerId\":\"${CID}\",\"modelId\":\"gpt-4o\",\"provider\":\"openai\",\"inputTokens\":10,\"outputTokens\":5,\"timestamp\":${ts}}"
done
wait_until "A1 count FINAL == $N" \
  "SELECT count() FROM fluxmeter.raw_events FINAL WHERE customer_id='${CID}'" \
  "$N" || true

# A2: replay same eventId three times — count stays N
for _ in 1 2 3; do
  ts=$(( $(date +%s) * 1000 ))
  kafka_produce "{\"eventId\":\"${PREFIX}-e1\",\"customerId\":\"${CID}\",\"modelId\":\"gpt-4o\",\"provider\":\"openai\",\"inputTokens\":10,\"outputTokens\":5,\"timestamp\":${ts}}"
done
sleep 3
wait_until "A2 still $N after replay" \
  "SELECT count() FROM fluxmeter.raw_events FINAL WHERE customer_id='${CID}'" \
  "$N" || true

# A4: customer + time range returns wide fields
ROW="$(ch_query "SELECT event_id, customer_id, model_id, input_tokens, output_tokens FROM fluxmeter.raw_events FINAL WHERE customer_id='${CID}' AND event_id='${PREFIX}-e1' FORMAT TSV")"
if echo "$ROW" | grep -q "${PREFIX}-e1"; then
  echo "  OK  A4 wide row query"; PASS=$((PASS + 1))
else
  echo "  FAIL A4 wide row query (got=${ROW})"; FAIL=$((FAIL + 1))
fi

# A5: missing eventId → DLQ, not main
ts=$(( $(date +%s) * 1000 ))
kafka_produce "{\"customerId\":\"${CID}-noid\",\"modelId\":\"gpt-4o\",\"provider\":\"openai\",\"inputTokens\":1,\"outputTokens\":1,\"timestamp\":${ts}}"
sleep 3
MAIN_NOID="$(ch_query "SELECT count() FROM fluxmeter.raw_events FINAL WHERE customer_id='${CID}-noid'" | tr -d '[:space:]')"
DLQ_MISS="$(ch_query "SELECT count() FROM fluxmeter.raw_events_dlq WHERE error_reason='missing_event_id' AND ingested_at > now() - INTERVAL 5 MINUTE" | tr -d '[:space:]')"
if [ "${MAIN_NOID:-0}" = "0" ] && [ "${DLQ_MISS:-0}" != "0" ]; then
  echo "  OK  A5 missing eventId → DLQ"; PASS=$((PASS + 1))
else
  echo "  FAIL A5 main=${MAIN_NOID} dlq_missing=${DLQ_MISS}"; FAIL=$((FAIL + 1))
fi

# A6: illegal JSON → DLQ parse_error
kafka_produce "this is not json ${PREFIX}"
sleep 3
DLQ_PARSE="$(ch_query "SELECT count() FROM fluxmeter.raw_events_dlq WHERE error_reason='parse_error' AND ingested_at > now() - INTERVAL 5 MINUTE" | tr -d '[:space:]')"
if [ "${DLQ_PARSE:-0}" != "0" ]; then
  echo "  OK  A6 parse_error → DLQ"; PASS=$((PASS + 1))
else
  echo "  FAIL A6 no parse_error DLQ rows"; FAIL=$((FAIL + 1))
fi

echo "== results: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
```

- [ ] **Step 2: Wire Makefile target**

```makefile
# Add test-cold-store to .PHONY line

test-cold-store:
	chmod +x scripts/test-cold-store.sh
	./scripts/test-cold-store.sh
```

- [ ] **Step 3: Run harness against current stack (or kafka+ch only) — expect FAIL**

```bash
# If full stack not up:
docker compose -f docker-compose.full.yml up -d kafka kafka-init clickhouse
# Wait for CH ping, then:
chmod +x scripts/test-cold-store.sh
./scripts/test-cold-store.sh
```

Expected: FAIL — `fluxmeter.raw_events` / `raw_events_dlq` missing (or A7/A1 failures on old `token_events` schema).

- [ ] **Step 4: Commit**

```bash
git add scripts/test-cold-store.sh Makefile
git commit -m "test: add failing ADR-1 cold-store acceptance harness"
```

---

### Task 2: Cold-store DDL + Kafka→Null→raw_events/DLQ pipeline

**Files:**
- Modify: `baseline/init.sql` (full rewrite)
- Modify: `docker-compose.full.yml` (mount as `01_cold_store.sql` so alphabetical order stays stable when benchmark file is added)
- Test: `scripts/test-cold-store.sh`

**Interfaces:**
- Consumes: Kafka topic `token-events` JSONEachRow camelCase
- Produces: tables `fluxmeter.raw_events`, `fluxmeter.raw_events_dlq`, `fluxmeter.raw_events_ingress` (Null), `fluxmeter.token_events_queue` (Kafka); group `fluxmeter-cold-store`

- [ ] **Step 1: Replace `baseline/init.sql` with audit pipeline**

Write the entire file as:

```sql
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
```

- [ ] **Step 2: Update compose volume mount name**

In `docker-compose.full.yml` under `clickhouse.volumes`, change to:

```yaml
    volumes:
      - ./baseline/init.sql:/docker-entrypoint-initdb.d/01_cold_store.sql
```

- [ ] **Step 3: Recreate ClickHouse volume and re-run acceptance**

```bash
docker compose -f docker-compose.full.yml stop clickhouse
docker compose -f docker-compose.full.yml rm -f clickhouse
# Remove anonymous/named volume if present so initdb re-runs:
docker volume ls | grep -i clickhouse || true
docker compose -f docker-compose.full.yml up -d kafka kafka-init clickhouse
# wait for http://localhost:8123/ping
make test-cold-store
```

Expected: PASS for A1/A2/A4/A5/A6/A7.

If `toJSONString(metadata)` or `Map` defaults fail on empty messages, adjust the ingress SELECT to `coalesce(toJSONString(metadata), '{}')` or omit `metadata` from required JSON (JSONEachRow missing Map → empty map).

- [ ] **Step 4: Commit**

```bash
git add baseline/init.sql docker-compose.full.yml
git commit -m "feat: ClickHouse raw_events cold store with DLQ fanout"
```

---

### Task 3: Benchmark-only aggregates profile

**Files:**
- Create: `baseline/benchmark_init.sql`
- Modify: `baseline/benchmark.sh` (apply init if tables missing; document dependency)
- Modify: `baseline/query.sql` (header warning)
- Test: default CH has no `usage_per_minute`; profile creates it

**Interfaces:**
- Consumes: same `token_events_queue` Kafka table (second MV for aggregates is OK on Kafka in CH 24.x when attached alongside ingress MV — if dual-MV on Kafka proves unsafe in practice, benchmark_init must SELECT from `raw_events` instead; **prefer aggregating from `raw_events`** to avoid a second Kafka consumer)
- Produces: `fluxmeter.usage_per_minute` only under profile `benchmark`

- [ ] **Step 1: Write `baseline/benchmark_init.sql` reading from `raw_events` (not a second Kafka group)**

```sql
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

-- Refresh-style: for local benchmark, a MV from raw_events is enough for demo traffic.
DROP TABLE IF EXISTS fluxmeter.usage_per_minute_mv;
CREATE MATERIALIZED VIEW fluxmeter.usage_per_minute_mv TO fluxmeter.usage_per_minute AS
SELECT
    toStartOfMinute(event_time) AS window_start,
    customer_id AS customerId,
    provider,
    model_id AS modelId,
    sum(input_tokens) AS input_tokens,
    sum(output_tokens) AS output_tokens,
    sum(cache_read_tokens) AS cache_read_tokens,
    sum(reasoning_tokens) AS reasoning_tokens,
    sum(input_tokens + output_tokens + cache_read_tokens + cache_write_tokens + reasoning_tokens + embedding_tokens) AS total_tokens,
    count() AS event_count,
    sum(latency_ms) AS total_latency_ms,
    sum(
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
    ) AS cost_usd
FROM fluxmeter.raw_events
GROUP BY window_start, customerId, provider, modelId;
```

- [ ] **Step 2: Apply benchmark SQL at runtime from `benchmark.sh` (keep default compose cold-store-only)**

Default `docker-compose.full.yml` must continue to mount **only** `01_cold_store.sql` from Task 2. Do **not** add `benchmark_init.sql` to the clickhouse service volumes (that would put derived `cost_usd` into the default audit stack).

Near the top of `baseline/benchmark.sh` (after `CLICKHOUSE_URL=`):

```bash
if ! curl -sf "$CLICKHOUSE_URL" --data-binary "EXISTS TABLE fluxmeter.usage_per_minute" | grep -q 1; then
  echo "Applying baseline/benchmark_init.sql (benchmark-only derived tables)..."
  curl -sf "$CLICKHOUSE_URL" --data-binary @baseline/benchmark_init.sql >/dev/null
fi
```

- [ ] **Step 3: Prefix `baseline/query.sql` with warning**

```sql
-- WARNING: usage_per_minute is a DERIVED benchmark table (cost_usd etc).
-- Audit source of truth is fluxmeter.raw_events (see query_audit.sql).
-- Requires: ./baseline/benchmark.sh or manual apply of benchmark_init.sql
```

- [ ] **Step 4: Verify default vs benchmark**

```bash
# Default after cold-store-only init:
curl -s http://localhost:8123 --data-binary "EXISTS TABLE fluxmeter.usage_per_minute"
# Expected: 0

make test-cold-store
# Expected: PASS

# Apply benchmark path:
curl -sf http://localhost:8123 --data-binary @baseline/benchmark_init.sql
curl -s http://localhost:8123 --data-binary "EXISTS TABLE fluxmeter.usage_per_minute"
# Expected: 1
```

- [ ] **Step 5: Commit**

```bash
git add baseline/benchmark_init.sql baseline/benchmark.sh baseline/query.sql
git commit -m "feat: move ClickHouse aggregates to benchmark-only init"
```

---

### Task 4: Audit query templates + isolation note (A3)

**Files:**
- Create: `baseline/query_audit.sql`
- Modify: `scripts/test-cold-store.sh` (print A3 manual checklist; optional automated group check)
- Test: SQL templates runnable via curl

**Interfaces:**
- Consumes: `fluxmeter.raw_events` / `raw_events_dlq`
- Produces: documented queries for runbook + operators

- [ ] **Step 1: Create `baseline/query_audit.sql`**

```sql
-- Auditable cold store queries (ADR-1)

-- A1/A2 style count (read-time dedup)
SELECT count()
FROM fluxmeter.raw_events FINAL
WHERE customer_id = 'CUST'
  AND event_time BETWEEN toDateTime64('2026-08-11 00:00:00', 3) AND toDateTime64('2026-08-12 00:00:00', 3);

-- A4 customer range (projection-friendly)
SELECT *
FROM fluxmeter.raw_events FINAL
WHERE customer_id = 'CUST'
  AND event_time BETWEEN toDateTime64('2026-08-11 00:00:00', 3) AND toDateTime64('2026-08-12 00:00:00', 3)
ORDER BY event_time, event_id;

-- DLQ inspect
SELECT error_reason, count() AS n, max(ingested_at) AS last_seen
FROM fluxmeter.raw_events_dlq
GROUP BY error_reason
ORDER BY n DESC;

-- Consumer group identity (engine settings)
SELECT
    name,
    engine_full
FROM system.tables
WHERE database = 'fluxmeter' AND name = 'token_events_queue';
```

- [ ] **Step 2: Extend harness with A3 soft check (group name present)**

Append before the final summary in `scripts/test-cold-store.sh`:

```bash
# A3 precondition: cold store uses independent group name (isolation from Flink)
ENGINE_FULL="$(ch_query "SELECT engine_full FROM system.tables WHERE database='fluxmeter' AND name='token_events_queue' FORMAT TSV")"
if echo "$ENGINE_FULL" | grep -q "fluxmeter-cold-store"; then
  echo "  OK  A3 group fluxmeter-cold-store configured"
  PASS=$((PASS + 1))
else
  echo "  FAIL A3 expected kafka_group_name fluxmeter-cold-store in engine_full"
  FAIL=$((FAIL + 1))
fi
echo "  NOTE A3 runtime demo: stop Flink TMs, keep producing to Kafka, watch raw_events FINAL count increase."
```

- [ ] **Step 3: Run templates + harness**

```bash
curl -s http://localhost:8123 --data-binary @baseline/query_audit.sql | head
make test-cold-store
```

Expected: harness PASS including A3 group check.

- [ ] **Step 4: Commit**

```bash
git add baseline/query_audit.sql scripts/test-cold-store.sh
git commit -m "docs: add raw_events audit query templates and A3 group check"
```

---

### Task 5: Runbook + disaster recovery + README

**Files:**
- Create: `docs/runbooks/cold-store-dlq.md`
- Modify: `docs/disaster-recovery.md` (Full cold storage pointer; Lite caveat)
- Modify: `README.md` (cold store + `make test-cold-store` + benchmark note)
- Test: markdown links resolve to existing paths

**Interfaces:**
- Consumes: DLQ table semantics from Task 2
- Produces: operator procedures (no new runtime code)

- [ ] **Step 1: Write `docs/runbooks/cold-store-dlq.md`**

```markdown
# Cold Store DLQ Runbook (`fluxmeter.raw_events_dlq`)

## What lands here

| error_reason | Meaning |
|--------------|---------|
| `parse_error` | Kafka message failed JSONEachRow parse (`_error` / `_raw_message`) |
| `missing_event_id` | Parsed JSON without `eventId` — refused by audit identity rules |

Main table `raw_events` never receives these rows.

## Inspect

```bash
curl -s http://localhost:8123 --data-binary @baseline/query_audit.sql
curl -s 'http://localhost:8123/?query=SELECT%20error_reason,count()%20FROM%20fluxmeter.raw_events_dlq%20GROUP%20BY%20error_reason'
```

## Replay rules

1. Fix the payload (must include a **stable** `eventId`).
2. Produce corrected JSON to `token-events` (same topic).
3. Verify with `SELECT * FROM fluxmeter.raw_events FINAL WHERE event_id = '...'`.
4. Do **not** DELETE/UPDATE historical `raw_events` rows — append-only.

## Lite mode

Lite ingest does **not** write cold store in ADR-1. Missing Lite audit copies are expected until ADR-4.
```

- [ ] **Step 2: Update `docs/disaster-recovery.md`**

In section `## 4. Lite Mode Recovery`, keep the cold-storage caveat.

In Full-mode recovery (near Kafka restore), add:

```markdown
### ClickHouse cold store (Full)

- Audit copy: `fluxmeter.raw_events` (dedupe with `FINAL` by `event_id`).
- Consumer group: `fluxmeter-cold-store` (independent from Flink).
- Bad messages: `fluxmeter.raw_events_dlq` — see [runbooks/cold-store-dlq.md](runbooks/cold-store-dlq.md).
- Recreating the ClickHouse volume re-runs `baseline/init.sql`; offsets for `fluxmeter-cold-store` start fresh (replay from topic retention).
```

- [ ] **Step 3: README — add under Full mode / Make targets**

```markdown
### Auditable cold store (Full / ClickHouse)

Kafka `token-events` → ClickHouse `fluxmeter.raw_events` (immutable, dedupe by `eventId`).
Bad / missing-`eventId` messages → `fluxmeter.raw_events_dlq`.

```bash
make test-cold-store   # ADR-1 acceptance (needs kafka + clickhouse)
make benchmark         # Applies derived usage_per_minute (not audit SoR)
```

Lite mode does not populate cold store until ADR-4.
```

- [ ] **Step 4: Commit**

```bash
git add docs/runbooks/cold-store-dlq.md docs/disaster-recovery.md README.md
git commit -m "docs: cold-store DLQ runbook and recovery notes"
```

---

### Task 6: Version bump + tracking + boundary self-check

**Files:**
- Modify: `build.gradle` (`version = '4.4.0'`)
- Modify: `changLog.md` (new `[4.4.0]` section)
- Modify: `progress.md` (version, ADR-1 row → Done, Recent Activity)
- Modify: `ROADMAP.md` (version + cold store track status)
- Modify: `docs/superpowers/specs/2026-08-11-cold-store-audit-design.md` (Status → APPROVED / implemented)

**Interfaces:**
- Consumes: completed Tasks 1–5
- Produces: SemVer + checklist aligned with CLAUDE.md tracking rules

- [ ] **Step 1: Bump version + changelog**

`build.gradle`:

```gradle
version = '4.4.0'
```

`changLog.md` (top entry):

```markdown
## [4.4.0] — 2026-08-11

### Added
- **ADR-1 auditable cold store**: ClickHouse `raw_events` (ReplacingMergeTree by `event_id`, customer-time projection) + `raw_events_dlq`
- `make test-cold-store` acceptance harness; `docs/runbooks/cold-store-dlq.md`; `baseline/query_audit.sql`

### Changed
- Default `baseline/init.sql` replaces legacy `token_events` MergeTree path; Kafka group `fluxmeter-cold-store`
- Benchmark aggregates moved to `baseline/benchmark_init.sql` (applied by `make benchmark`)

### Notes
- Lite mode still has no cold-store copy (ADR-4)
- Dedup is read-time (`FINAL`); no external `cold_sink/` process
```

- [ ] **Step 2: Update progress / roadmap / spec status**

- `progress.md`: Current version `4.4.0`; ADR-1 row **Done**; Recent Activity line for 4.4.0.
- `ROADMAP.md`: engine version `4.4.0`; ecosystem track cold store marked shipped/spec implemented.
- Spec header: `Status: APPROVED — implemented in 4.4.0`.

- [ ] **Step 3: Boundary self-check (manual)**

Walk spec §9 checklist; confirm:

```bash
make test-cold-store
rg -n "cold_sink|kafka_skip_broken_messages" baseline/init.sql && exit 1 || true
rg -n "cost_usd|balance" baseline/init.sql && exit 1 || true
# cost_usd may exist only in benchmark_init.sql
rg -n "cost_usd" baseline/benchmark_init.sql
```

- [ ] **Step 4: Final commit**

```bash
git add build.gradle changLog.md progress.md ROADMAP.md docs/superpowers/specs/2026-08-11-cold-store-audit-design.md
git commit -m "chore: release 4.4.0 auditable ClickHouse cold store"
```

---

## Self-review (plan vs spec)

| Spec requirement | Task |
|------------------|------|
| DoD #1 N events in `raw_events` FINAL | Task 1 A1, Task 2 |
| DoD #2 replay same eventId → 1 | Task 1 A2, Task 2 ReplacingMergeTree |
| DoD #3 isolation vs Flink | Task 4 A3 group check + note; group name in Task 2 |
| DoD #4 customer+time SQL | Task 1 A4, Task 4 `query_audit.sql`, projection in Task 2 |
| DoD #5 no derived cols | Task 1 A7, Task 2 DDL, Task 6 rg check |
| DoD #6 missing id / bad JSON → DLQ | Task 1 A5/A6, Task 2 fanout |
| No Lite / no `cold_sink/` | Global constraints + Task 5 docs |
| Benchmark aggregates optional | Task 3 |
| Runbook + DR docs | Task 5 |
| Tracking / version | Task 6 |

**Placeholder scan:** none intentional — SQL/scripts inlined.  
**Type consistency:** `event_id` / `customer_id` / `error_reason` values `parse_error`|`missing_event_id` used uniformly across init, harness, runbook.
