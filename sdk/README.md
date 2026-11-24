# FluxMeter SDKs

Client libraries implementing [`spec/schema/token-event-v1.json`](../spec/schema/token-event-v1.json).

**Website:** [fluxmeter.dev](https://fluxmeter.dev) · **Spec:** [OpenAPI](../spec/openapi/openapi.yaml) · **API reference:** [docs/api-reference.md](../docs/api-reference.md)

| Language | Path | Install |
|----------|------|---------|
| Python | [python/](python/) | `pip install "fluxmeter>=2.0.2"` (`DeliveryError` requires >=2.0.2) |
| JavaScript/TypeScript | [js/](js/) | Not on npm yet. From a clone: `cd sdk/js && npm install && npm run build`, then `npm install /path/to/fluxmeter/sdk/js` |

HTTP ingest is the public SDK path. Kafka is internal transport. See each SDK README.
