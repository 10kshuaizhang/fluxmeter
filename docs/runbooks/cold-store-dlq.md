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

## Related

- Spec: [docs/superpowers/specs/2026-08-11-cold-store-audit-design.md](../superpowers/specs/2026-08-11-cold-store-audit-design.md)
- Apply DDL: `make apply-cold-store-init` (or `FORCE_COLD_STORE_INIT=1` to recreate)
- Acceptance: `make test-cold-store`
