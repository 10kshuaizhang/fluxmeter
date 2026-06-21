# Contributing to FluxMeter Community Extensions

## What belongs in `contrib/`

- Provider field mapping guides (`providers/`)
- Pricing tables (`pricing/`) — copy from `spec/schema/pricing-template.yaml`
- Connector stubs (`connectors/`) — Lago, Stripe usage records, webhooks
- Dashboard exports (`dashboards/`) — Grafana JSON, Datadog monitors

## What belongs in core (`src/`, `api/`)

- Flink aggregation, checkpointing, sink optimizations
- Budget enforcement Lua scripts
- Performance-critical paths

Open a **Discussion** before large contrib PRs so we avoid duplicate adapters.

## PR checklist

1. Event JSON matches `spec/schema/token-event-v1.json` (camelCase keys)
2. Provider doc lists source API field → FluxMeter field
3. Pricing files include `effective_date` and `version`
4. No secrets or API keys in examples

## License

Contributions are Apache 2.0, same as the project.
