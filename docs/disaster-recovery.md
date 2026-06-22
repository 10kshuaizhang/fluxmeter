# FluxMeter Disaster Recovery Runbook

Operational procedures for recovering billing state after infrastructure failures.

## Failure Scenarios

| Failure | Data at risk | Recovery source |
|---------|--------------|-----------------|
| Redis crash / data loss | Budget balances, aggregated counters | Kafka replay + budget re-seed |
| Flink job failure | In-flight window (≤ `WINDOW_SECONDS`) | Kafka offset rewind + checkpoint |
| Kafka broker loss | Uncommitted events (SDK WAL replays) | SDK WAL on producer side |
| Full cluster loss | All hot state | Kafka retention (30d) + checkpoints + AOF |

## Prerequisites

- Kafka topic `token-events` retention ≥ 7 days (default 30 days in `docker-compose.full.yml`)
- Flink checkpoints enabled (`CHECKPOINT_DIR` set, 30s interval)
- Redis AOF enabled (`appendonly yes`)
- Documented budget seed values per customer (Postgres/control-plane or ops vault)

## 1. Redis Total Loss

**Symptoms:** API returns zero usage; budgets missing; Grafana flatlines.

**Steps:**

1. Stop the Flink job to prevent partial writes during rebuild:
   ```bash
   docker exec fluxmeter-jobmanager flink list
   docker exec fluxmeter-jobmanager flink cancel <job-id>
   ```

2. Flush corrupted Redis (if volume is unrecoverable):
   ```bash
   docker compose -f docker-compose.full.yml stop redis
   docker volume rm fluxmeter_redis-data  # destructive
   docker compose -f docker-compose.full.yml up -d redis
   ```

3. Re-seed customer budgets (required before replay deducts correctly):
   ```bash
   curl -X POST http://localhost:8000/budget/cust_1 \
     -H "Content-Type: application/json" \
     -d '{"balance_usd": 500.0, "threshold_pct": 10}'
   ```

4. Reset Flink consumer to earliest offset and replay from Kafka:
   ```bash
   # Cancel job, then resubmit with reset (dev only):
   docker exec fluxmeter-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 \
     --group fluxmeter-aggregator \
     --reset-offsets --to-earliest --topic token-events --execute
   make submit-job
   ```

5. Verify counters match expected order-of-magnitude:
   ```bash
   curl http://localhost:8000/usage/global
   curl http://localhost:8000/usage/cust_1
   ```

**RTO target:** 15–30 minutes for full replay (depends on Kafka volume).

**Caveat:** Replay re-applies idempotency keys (`applied:*` / `a:*`). If Redis was only partially lost, do **not** reset offsets — mixed state requires manual reconciliation.

## 2. Flink Job Crash (Redis intact)

**Symptoms:** Consumer lag growing; no new `global:last_window_end` updates.

**Steps:**

1. Check Flink UI: http://localhost:8081 — look for failed checkpoints.
2. Restart from last checkpoint (automatic on resubmit if `CHECKPOINT_DIR` is mounted):
   ```bash
   make submit-job
   ```
3. Monitor lag:
   ```bash
   docker exec fluxmeter-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 \
     --group fluxmeter-aggregator --describe
   ```

Late events land in `token-events-dlq`. Replay with:
```bash
./scripts/replay-dlq.sh   # if available, or manual produce to token-events
```

## 3. Kafka Outage (Producers have SDK WAL)

**Symptoms:** Ingest errors; SDK queues events locally.

**Steps:**

1. Restore Kafka: `docker compose -f docker-compose.full.yml up -d kafka`
2. SDK WAL replays automatically on reconnect (see `fluxmeter-client` WAL docs).
3. HTTP `/ingest` callers must retry — use `eventId` for idempotency.

## 4. Lite Mode Recovery (no Flink/Kafka)

Lite mode writes directly to Redis. Recovery options:

1. **Redis AOF replay:** restart Redis with existing volume — AOF rebuilds state.
2. **Full loss:** re-ingest from application logs or cold storage (no Kafka buffer).
3. **Rollup history:** per-minute buckets (`rollup:{customer}:m:{ts}`) survive 24h even if live counters were lost.

## 5. Multi-Tenant (SaaS) Considerations

- Tenant metadata lives in Redis (`cp:tenant:*`) and should be backed up separately.
- Replay uses `tenantId` on events for key isolation (`tenant:{tid}:customer:{cid}:*`).
- Control plane re-provisions API keys; usage counters rebuild from Kafka per tenant.

## 6. Verification Checklist

- [ ] `global:last_window_end` advancing (full mode)
- [ ] Sample customer `event_count` > 0
- [ ] Budget `balance_usd` decreases after test ingest
- [ ] No sustained consumer lag (> 5 min at steady state)
- [ ] Prometheus: Flink `numRecordsInPerSecond` > 0 (see `monitoring/prometheus.yml`)
- [ ] Reconciliation job drift < 1% (`/admin/reconcile` if enabled)

## 7. Prevention

- Redis: AOF + `noeviction` policy (already in compose)
- Flink: externalized checkpoints retained on cancellation
- Kafka: `acks=all` on producers (SDK default)
- Monitor: Prometheus scrapes Flink `:9249`; alert on consumer lag (Helm rules in `deploy/helm/`)

## Related Docs

- [DESIGN.md](DESIGN.md) — durability matrix
- [production-deploy.md](production-deploy.md) — prod overlay
- [load-testing.md](load-testing.md) — throughput validation
