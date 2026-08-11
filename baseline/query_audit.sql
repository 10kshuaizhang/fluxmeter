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
