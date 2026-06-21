# DLQ Replay Runbook

## When to replay

- Late events routed to `token-events-dlq` after watermark passed window end
- Clock skew caused events to miss their window
- Flink job was down and events aged past allowed lateness

## Preconditions

1. Flink job is **running** (or will run after replay)
2. Identify DLQ volume: `kafka-console-consumer` on `token-events-dlq`
3. **Do not** replay duplicate windows without understanding SET NX idempotency — same `eventId` is safe; replays of already-applied windows are skipped via `applied:{customer|model|windowStart}`

## Steps

### 1. Dry run

```bash
./scripts/replay-dlq.sh --dry-run --max 100
```

### 2. Replay (host → Kafka on 9094)

```bash
export KAFKA_BROKERS=localhost:9094
./scripts/replay-dlq.sh --max 10000 --rate 5000
```

### 3. Verify

- `GET /usage/global` — `last_window_end` advancing
- `GET /usage/customer/{id}` — counters increased once
- Reconciliation job: `GET /admin/reconciliation` — no drift

### 4. If events still late

Consider shifting timestamps (manual edit) or temporarily pausing ingest until watermarks catch up. Shifting timestamps changes billing windows — document for audit.

## Escalation

- Persistent DLQ growth → check Flink lag, watermark idleness (30s), partition skew
- Double-counting → verify `eventId` present on replayed events
- Budget drift → run reconciliation job, check `total_deducted_usd` vs `customer:cost_usd`
