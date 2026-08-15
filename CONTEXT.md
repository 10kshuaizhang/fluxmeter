# FluxMeter domain context

Shared vocabulary for metering, custody, budgets, and reservations.
Avoid synonyms that dilute these terms.

## Glossary

| Term | Meaning |
|------|---------|
| **Token Event** | One metered usage record (customer, model, tokens, optional span/session). |
| **Trusted Envelope** | Versioned wrapper around a Token Event plus auth-derived tenant/apiKey and receipt metadata. |
| **Custody** | Accepting a Token Event into durable processing: identity claim → envelope → Kafka ack (or Gateway outbox buffer). Public interface: `accept` / `accept_many`. |
| **Usage Aggregate** | Windowed sum of Token Events for a customer+model (Flink); sole billing rollup path. |
| **Budget** | Prepaid balance and holds in Redis under `budget_prefix(tenant, customer)`. |
| **Reservation** | Temporary hold on Budget (`held_usd`) tied to a Gateway or SDK reserve, attached to a window, then settled or expired. |
| **Gateway** | Side-track OpenAI-compatible proxy that reserves, calls upstream, then ingests via Custody. |
| **Intelligence** | Read-side analytics over Redis rollups (not a second billing engine). |
| **Pricing Catalog** | Shared JSON model rates; Flink is billing truth, Gateway estimate is advisory. |

## Avoid

- Calling Custody “ingest helpers” or “Kafka publish utils” — those are internals.
- Treating Lite Redis Lua aggregators as current runtime (superseded by ADR-024).
- Inventing a second Reservation expire path outside `expire_reservations`.
