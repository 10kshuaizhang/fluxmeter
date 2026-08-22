# Tencent CVM fault-injection summary

Date: 2026-08-22  
Host: 16 vCPU / 32 GiB Tencent CVM  
FluxMeter: 4.7.1 working tree

## Kafka broker pause

- Paused `fluxmeter-kafka` before a unique `POST /ingest`.
- During outage: HTTP `503 custody_uncertain`; no false `202`.
- After recovery, retrying the identical payload returned HTTP `202` with
  `idempotent=true`.
- A fresh post-recovery event returned HTTP `202 accepted`.
- ClickHouse contained exactly one row for each event and the Flink job remained
  `RUNNING` with 14/14 tasks.

Result: **PASS** for custody, late-delivery reconciliation, idempotent retry,
audit uniqueness, and Flink continuity.

## Redis freeze

- Paused `fluxmeter-redis` before a unique `POST /ingest`.
- API returned HTTP `503 identity_store_unavailable` after about five seconds;
  no false `202` and no ClickHouse audit row appeared.
- After recovery, the queued Redis claim existed as `pending`, so retries
  returned HTTP `503 event_pending`.
- The pending score expired after 120 seconds, but the event was still blocked.
- Its shard had 203,581 expired identities ahead of it; the target ranked
  203,580 while each claim cleans only 64 expired fields.

Result: **FAIL**. Safety is preserved, but retry liveness is not: high-cardinality
expiry backlog can starve a pending identity far beyond its advertised TTL.

## Flink TaskManager restart

- Accepted one event, restarted `fluxmeter-taskmanager`, and restored checkpoint
  83. The original job returned to `RUNNING` with 14/14 tasks in about 34 seconds.
- Accepted a second event after recovery.
- ClickHouse contained both events exactly once.
- After 150 seconds Redis still showed only the first event. Sending a later
  zero-token event for the same customer advanced event time; within five
  seconds the old window became exactly `event_count=2`, `total_tokens=56`.

Result: **PARTIAL**. Checkpoint recovery and data correctness pass, but an idle
stream can leave a completed event-time window unmaterialized indefinitely until
new traffic advances the watermark.

## Initial v4.7.1 release decision

- Do not claim the Kafka/Redis/Flink fault matrix complete.
- Fix expiry-backlog starvation and idle-window liveness before formal reruns.

## v4.7.2 remediation rerun

- Targeted expiry was exercised with the requested identity ranked 205,310 behind
  expired shard members; the real `/ingest` retry returned `202 accepted`.
- Flink now consumes a dedicated one-partition `metering-watermarks` topic whose
  trusted heartbeats are filtered before billing. With no later business event,
  a unique event materialized in Redis after 17 seconds as exactly one event and
  26 tokens (17 input + 9 output).
- The original low-traffic generator experiments were rejected after system tests
  showed that Kafka split watermark merging could still pin the global watermark;
  the committed design uses an explicit heartbeat stream and observable topic.
- Final combined rerun: the pre-restart event materialized, the TaskManager was
  restarted, the job recovered to 14/14 running tasks, and a post-recovery event
  materialized without a manual business heartbeat after 17 seconds. Redis held
  exactly two events / 56 tokens and ClickHouse held exactly the two audit rows.

Result: **PASS**. The Kafka, Redis, and Flink fault-liveness findings from v4.7.1
are closed by v4.7.2; public throughput gates remain separate and open.
