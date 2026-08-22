# FluxMeter domain context

Shared vocabulary for metering, custody, budgets, and reservations.
Avoid synonyms that dilute these terms.

## Glossary

| Term | Meaning |
|------|---------|
| **Token Event** | One metered usage record (customer, model, tokens, optional span/session). |
| **Trusted Envelope** | Versioned wrapper around a Token Event plus auth-derived tenant/apiKey and receipt metadata. |
| **Custody** | Accepting a Token Event into durable processing: identity claim → envelope → Kafka ack (or Gateway outbox buffer). Interface: `TokenEventCustody.accept` / `accept_many` with `CustodyConfig` and `CustodyContext`. |
| **Usage Aggregate** | Windowed sum of Token Events for a customer+model (Flink); sole billing rollup path. |
| **UsageQuery** | Read-side module for lifetime Redis counters (`customer:` / `global:`), tenant-prefixed with legacy dual-read. |
| **Cold Store** | Append-only ClickHouse audit copy of Kafka Token Events (`raw_events` + DLQ); not billing truth. |
| **Pricing** | Validated catalog and the sole Python interface for normalization, exact token-category quotes, and advisory Gateway completion estimates. Java mirrors the same JSON contract for billing truth. |
| **Budget** | Tenant-scoped prepaid balance, token package, and authorization module. Interface: `Budget.configure` / `top_up` / `set_package` / `snapshot` / `check`; cache, rate-limit, and package identities include tenant. |
| **Reservation** | Temporary hold on Budget tied to a Gateway or SDK reserve. `Reservation.open` atomically creates the hold and durable lifecycle record; `settle` / `expire_due` are idempotent terminal paths. |
| **Gateway** | Side-track OpenAI-compatible proxy that reserves, calls upstream, then ingests via Custody. Orchestration is ProxiedCompletion (`run`). |
| **Intelligence** | Read-side analytics over Redis rollups (not a second billing engine). |
| **RollupStore** | Read-side module over Redis period rollups; prefers `idx:period:{YYYY-MM}:customers`, falls back to SCAN. |
| **Pricing Catalog** | Shared JSON model rates; Flink is billing truth, Gateway estimate is advisory (same `cost_micro`, `monthly_before=0`). |

## Avoid

- Calling Custody “ingest helpers” or “Kafka publish utils” — those are internals.
- Treating Lite Redis Lua aggregators as current runtime (superseded by ADR-024).
- Inventing a second Reservation expire path outside `Reservation.expire_due`.
- Calling removed `budget_gate` / `budget_ops` helpers instead of the Budget or Reservation interfaces.
- Quoting Gateway spend outside `PricingCatalog`.
- Reading bare `customer:` / `global:` lifetime keys outside UsageQuery.
- Treating Cold Store (`raw_events`) as the billing ledger — Flink → Redis remains truth.
