# Migrating to FluxMeter 4.0

FluxMeter 4.0 has one public event entrance and one processing architecture:

```text
application or Gateway -> HTTP -> Kafka -> Flink -> Redis
```

## Breaking changes

- Replace Python `kafka_brokers`, `topic`, `wal_dir`, and `producer_config` with `api_url` and, when enabled, `api_key`.
- Replace JavaScript `kafkaBrokers` and `topic` with `apiUrl` and `apiKey`.
- Stop producing customer events directly to `token-events`. Only trusted operator recovery and benchmark tools may use the internal broker.
- Remove `FLUXMETER_LITE_MODE`; `/health` now returns only `{"status":"ok"}`.
- Use `docker-compose.yml` for every deployment. `docker-compose.saas.yml`, `docker-compose.prod.yml`, and `docker-compose.benchmark.yml` are overlays. `docker-compose.full.yml` was removed.
- Replace `make demo-full`, `make start-full`, and the Lite aliases with `make demo` or `make start`. The Flink job is submitted automatically.

## Delivery contract

`POST /ingest` returns `202` only after Kafka acknowledges custody. A `503` response is retryable. SDKs retry with one stable `eventId`; identical retries are accepted for 30 days and conflicting payload reuse returns `409`.

Batch ingestion validates the entire request before publishing. It returns `202` when all events have custody, `207` for mixed outcomes, and `503` when none do. Inspect each result before retrying.

Events more than 24 hours old or more than five minutes in the future are acknowledged into the quarantine topic instead of billing state.

## Operational changes

- Gate traffic on `GET /ready`, which verifies Redis and sends a causal Kafka-to-Flink consumer probe.
- Keep `token-events`, `token-events-dlq`, and `token-events-quarantine` private.
- The Gateway writes a durable Redis outbox entry before Kafka publication and holds reservations until Flink reconciles usage or the reservation expires.
