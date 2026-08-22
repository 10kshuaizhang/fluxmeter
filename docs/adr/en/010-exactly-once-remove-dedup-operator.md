# ADR-010: Exactly-Once Composition and Dedup Removal

| Field | Value |
|-------|-------|
| **Status** | Accepted (reversal) |
| **Date** | 2026-06-20 |
| **Version** | 0.6.2 / 2.7.1 |
| **Lang** | English |

## Context

Week 4 architecture review found three CRITICAL/HIGH issues:
1. Flink `EventDeduplicator` keyed by eventId → 1 key/event → at 500K eps **1.8B keys/hour → guaranteed OOM**.
2. `allowedLateness(30s)` + Sink SET NX → late data retriggers window but NX blocks write → **silent data loss**.
3. Counter increment and budget deduction not atomic → customers get free tokens in crash windows.

## Decision

**Delete > add**:
• **Remove** Flink EventDeduplicator — sink-level SET NX is sufficient.
• **Remove** allowedLateness — late events exclusively → sideOutput → Kafka DLQ.
• Merge counter + budget + idempotency into **single Lua EVAL** (2.7.1 further eliminates pipeline crash window).
• Checkpoint: EXACTLY_ONCE + 10m timeout + `tolerableCheckpointFailureNumber(3)`.

## Consequences

✅ No OOM at production throughput; ✅ late data not silently dropped; ✅ financial atomicity. ❌ DLQ requires ops replay (`scripts/dlq_replay.py`); late data excluded from main windows (by design).

## Evidence

changLog 0.6.2 CRITICAL fixes; changLog 2.7.1; `src/test/java/io/fluxmeter/job/LateDataSideOutputTest.java`.

## Architect's note

One of the most important reversals: adding a dedup operator for EO was more dangerous at production scale than having no dedup at all.

## 2026-08-17 amendment (v4.5.0)

The 30-day per-event Flink registry remains prohibited. v4.5.0 adds back a **strictly bounded 10-minute safety dedup** only for the HTTP Kafka-ACK uncertainty/crash window. Client retry identity lives in compact tenant-sharded Redis custody buckets for 30 days; Flink state is neither authoritative for HTTP retries nor sized as long-term identity storage. This does not reverse the capacity lesson above: the safety TTL and its state-size gate are part of the contract.

---

**中文:** [ADR-010](../zh/010-exactly-once-remove-dedup-operator.md)
