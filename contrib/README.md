# Community Extensions

Provider adapters, pricing tables, dashboards, and billing connectors.

**Website:** [fluxmeter.dev](https://fluxmeter.dev) · **PRs welcome here** — lower bar than core engine changes in `src/`.

## Structure

```
contrib/
  providers/     # Field mapping docs (OpenAI, Anthropic, …)
  pricing/       # Model price tables (JSON/YAML)
  connectors/    # Webhook / billing platform stubs
  dashboards/    # Grafana, Datadog export templates
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Official spec

All extensions must align with:

- [`spec/schema/token-event-v1.json`](../spec/schema/token-event-v1.json)
- [`spec/schema/semantic-conventions.md`](../spec/schema/semantic-conventions.md)

## Examples in this repo

| Path | Description |
|------|-------------|
| [providers/openai.md](providers/openai.md) | OpenAI response → TokenEvent |
| [providers/anthropic.md](providers/anthropic.md) | Anthropic response → TokenEvent |
| [pricing/openai-2025-06.json](pricing/openai-2025-06.json) | Reference pricing snapshot |
