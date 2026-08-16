# Cold Store DLQ Runbook (`fluxmeter.raw_events_dlq`)

## What lands here

| error_reason | Meaning |
|--------------|---------|
| `parse_error` | Kafka line is not valid JSON (`LineAsString` + `isValidJSON`) |
| `missing_event_id` | Parsed JSON without `eventId` / `payload.eventId` — refused by audit identity rules |

Main table `raw_events` never receives these rows. `raw_payload` keeps the original Kafka message body.

## Inspect

```bash
curl -s http://localhost:8123 --data-binary @baseline/query_audit.sql
curl -s 'http://localhost:8123/?query=SELECT%20error_reason,count()%20FROM%20fluxmeter.raw_events_dlq%20GROUP%20BY%20error_reason'
```

## Replay rules

1. Fix the payload (must include a **stable** `eventId`, preferably inside Trusted Envelope `payload`).
2. Produce corrected JSON to `token-events` (same topic).
3. Verify with `SELECT * FROM fluxmeter.raw_events FINAL WHERE event_id = '...'`.
4. Do **not** DELETE/UPDATE historical `raw_events` rows — append-only.

## Runtime notes

- Cold store is on the benchmark overlay (`make start-benchmark`) and consumes the same `token-events` topic as Flink (group `fluxmeter-cold-store`).
- After changing `baseline/init.sql`, re-apply with `FORCE_COLD_STORE_INIT=1 make apply-cold-store-init`.

## Related

- Spec: [docs/superpowers/specs/2026-08-11-cold-store-audit-design.md](../superpowers/specs/2026-08-11-cold-store-audit-design.md)
- ADR: [docs/adr/en/025-auditable-clickhouse-cold-store.md](../adr/en/025-auditable-clickhouse-cold-store.md)
- Apply DDL: `make apply-cold-store-init`
- Acceptance: `make test-cold-store`
