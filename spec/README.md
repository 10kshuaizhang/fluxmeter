# FluxMeter Spec — Ecosystem Layer

Open standards for AI token billing events, HTTP API, and provider mappings.
This layer is the **growth engine**: SDKs and integrations target these artifacts, not the Flink engine internals.

## Layout

| Path | Purpose |
|------|---------|
| [`schema/token-event-v1.json`](schema/token-event-v1.json) | Canonical JSON Schema for token usage events |
| [`schema/semantic-conventions.md`](schema/semantic-conventions.md) | Field meanings (like OTel semantic conventions) |
| [`schema/pricing-template.yaml`](schema/pricing-template.yaml) | Pricing table template for contrib/community |
| [`openapi/openapi.yaml`](openapi/openapi.yaml) | HTTP API contract (ingest, usage, budget) |

## SDKs (implementations of this spec)

| SDK | Path | Package |
|-----|------|---------|
| Python | [`../sdk/python/`](../../sdk/python/) | `pip install fluxmeter` |
| JavaScript/TypeScript | [`../sdk/js/`](../../sdk/js/) | `npm install @fluxmeter/client` |

## Community extensions

See [`../contrib/`](../../contrib/) for provider adapters, pricing tables, and connector templates.

## Engine (reference implementation)

The streaming aggregation engine lives at [`../src/`](../../src/) — Apache Flink job, sinks, budget enforcement.
It implements this spec but is **not** required to adopt FluxMeter (any consumer of `token-events` JSON can integrate).

## Validation

```bash
./scripts/validate-spec.sh
```

## Versioning

- **Event schema**: `token-event-v1.json` — bump major when breaking fields change
- **OpenAPI**: version field in `openapi.yaml` — aligned with API releases
