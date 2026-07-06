# @fluxmeter/client

JavaScript/TypeScript SDK for [FluxMeter](https://fluxmeter.dev) token billing.

**Website:** [fluxmeter.dev](https://fluxmeter.dev) · [GitHub](https://github.com/10kshuaizhang/fluxmeter) · [API reference](../../docs/api-reference.md)

Implements [`spec/schema/token-event-v1.json`](../../spec/schema/token-event-v1.json).

## Install

```bash
npm install @fluxmeter/client
```

## Usage (HTTP — no Kafka required)

```typescript
import { FluxMeter } from "@fluxmeter/client";

const meter = new FluxMeter({ apiUrl: "http://localhost:8000" });

await meter.track("cust_123", "gpt-4o", {
  inputTokens: 500,
  outputTokens: 150,
});

await meter.trackOpenAI("cust_123", openaiResponse, { latencyMs: 1200 });
```

## Kafka mode (optional)

```typescript
const meter = new FluxMeter({
  kafkaBrokers: "localhost:9094",
  topic: "token-events",
});
```

Requires optional dependency `kafkajs`.

## Query usage

Ingest is via the SDK; read billing data via the FluxMeter HTTP API ([docs/api-reference.md](../../docs/api-reference.md)):

```typescript
const res = await fetch("http://localhost:8000/usage/span/span_agent_42", {
  headers: { "X-API-Key": process.env.FLUXMETER_API_KEY! },
});
const taskCost = await res.json();
```

Use `parentSpanId` on `track()` for agent task totals; `sessionId` for project-level totals (lite ingest).

## Build

```bash
npm install
npm run build
```

## Publish (npm)

Package name: `@fluxmeter/client` · current version in `package.json`.

```bash
cd sdk/js
npm install
npm run build
npm pack          # inspect tarball
npm publish --access public
```

Requires an npm account with publish rights to the `@fluxmeter` scope (or unscoped rename). CI can set `NODE_AUTH_TOKEN`.
