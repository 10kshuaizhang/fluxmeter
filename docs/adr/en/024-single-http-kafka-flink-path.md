# ADR-024: Single HTTP entrance with Kafka/Flink processing

**Status:** Accepted
**Date:** 2026-08-15
**Supersedes:** ADR-013 and ADR-014 for active runtime architecture

## Context

The Lite and Full deployments exposed two ingestion implementations with different acknowledgement, billing, rollup, and failure semantics. SDKs also exposed Kafka directly, making tenant identity and protocol evolution difficult to control.

## Decision

FluxMeter has one public event boundary: `POST /ingest` and its batch form. The API authenticates the caller, builds a versioned trusted envelope, and returns success only after Kafka acknowledges the record **and** the tenant-scoped event identity is finalized. Flink is the sole billing and aggregation engine. Redis remains the custody-identity, query, budget, projection, and Gateway-outbox store.

Customer SDKs are HTTP-only. Kafka ports and schemas are private except for explicitly trusted operator replay and benchmark tools. The default compose file runs the whole architecture; production, SaaS, and benchmark files are overlays rather than modes.

Gateway reservations are asynchronous: the Gateway persists an outbox record before publish, Flink atomically deducts the aggregated cost and releases all holds for that window, and an expiry worker releases abandoned reservations.

## Consequences

- There is one set of ingestion semantics, readiness checks, tests, and deployment commands.
- Kafka/Flink are required in all supported installations.
- HTTP custody is retryable and event IDs are stable across retries.
- Immediate per-event cost and balance responses from the former Lite path are removed; query state converges after Flink processing.
- Historical ADRs remain as records but do not describe the active runtime.

## 2026-08-17 amendment (v4.5.0)

- Identity namespace is `(tenant_id, eventId)` and is stored in compact tenant-sharded Redis hashes with expiry indexes, not one Redis key per event.
- `pending` defaults to 120 seconds; ACK timeout/finalize ambiguity becomes `uncertain` for 600 seconds. A late delivery callback finalizes or releases that identity.
- `202` is never returned after Kafka ACK alone. Redis finalize failure is a retryable `503`.
- `eventId` remains optional for one-shot HTTP convenience, but cross-attempt idempotency exists only when the caller supplies a stable ID. SDKs and Gateway always do so.
- Batch validation and custody outcomes are per item; bounded in-flight saturation returns `429` rather than unbounded queue growth.

## 2026-08-22 amendment (v4.8.2)

- The compact Custody identity is the only client-retry registry and retains the production 30-day horizon.
- Flink's `projection:*` marker is a separate replay/idempotency guard. It defaults to 600 seconds, matching Flink crash-recovery safety, and is configurable with `EVENT_PROJECTION_IDEMPOTENCY_TTL_SECONDS`.
- A per-event projection marker must not inherit the 30-day Custody retention. At 10K events/s, that would imply 25.92 billion standalone Redis keys and duplicate the compact identity design.
