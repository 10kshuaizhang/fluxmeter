# ADR-003: Window Aggregate Before Redis Writes

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-16 |
| **Version** | 0.1.0 |
| **Lang** | English |

## Context

At 1M raw events/sec, per-event Redis writes overwhelm a single instance. Dimensionality must be reduced in the stream layer.

## Decision

Key by `(customer_id, model_id)` → tumbling window (initially 60s, later **10s** for memory) → **post-aggregate** pipelined Redis writes. 10K customers × 9 models × 10s window ≈ **167 writes/sec**.

## Consequences

✅ Redis becomes a query layer, not an event log; Grafana can poll directly. ❌ Query granularity bounded by window size (later patched by pre-request check).

## Evidence

DESIGN.md Next Steps #5; changLog 0.2.0 window 60s→10s; `src/main/java/io/fluxmeter/sink/RedisSink.java`.

---

**中文:** [ADR-003](../zh/003-window-aggregate-before-redis.md)
