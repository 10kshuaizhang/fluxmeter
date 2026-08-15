# ADR-024: Single HTTP entrance with Kafka/Flink processing

**Status:** Accepted
**Date:** 2026-08-15
**Supersedes:** ADR-013 and ADR-014 for active runtime architecture

## Context

The Lite and Full deployments exposed two ingestion implementations with different acknowledgement, billing, rollup, and failure semantics. SDKs also exposed Kafka directly, making tenant identity and protocol evolution difficult to control.

## Decision

FluxMeter has one public event boundary: `POST /ingest` and its batch form. The API authenticates the caller, builds a versioned trusted envelope, and returns success only after Kafka acknowledges custody. Flink is the sole billing and aggregation engine. Redis remains the query, budget, projection, and Gateway-outbox store.

Customer SDKs are HTTP-only. Kafka ports and schemas are private except for explicitly trusted operator replay and benchmark tools. The default compose file runs the whole architecture; production, SaaS, and benchmark files are overlays rather than modes.

Gateway reservations are asynchronous: the Gateway persists an outbox record before publish, Flink atomically deducts the aggregated cost and releases all holds for that window, and an expiry worker releases abandoned reservations.

## Consequences

- There is one set of ingestion semantics, readiness checks, tests, and deployment commands.
- Kafka/Flink are required in all supported installations.
- HTTP custody is retryable and event IDs are stable across retries.
- Immediate per-event cost and balance responses from the former Lite path are removed; query state converges after Flink processing.
- Historical ADRs remain as records but do not describe the active runtime.
