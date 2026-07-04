# @fluxmeter/client

JavaScript/TypeScript SDK for [FluxMeter](https://fluxmeter.dev) token billing.

**Docs:** [fluxmeter.dev](https://fluxmeter.dev) · [GitHub](https://github.com/10kshuaizhang/fluxmeter)

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

## Build

```bash
npm install
npm run build
```
