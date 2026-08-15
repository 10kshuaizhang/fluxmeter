# @fluxmeter/client

HTTP-only JavaScript/TypeScript SDK for [FluxMeter](https://fluxmeter.dev) token billing.

## Install

```bash
npm install @fluxmeter/client
```

## Usage

```typescript
import { FluxMeter, DeliveryError } from "@fluxmeter/client";

const meter = new FluxMeter({
  apiUrl: "http://localhost:8000",
  apiKey: process.env.FLUXMETER_API_KEY,
});

try {
  await meter.track("cust_123", "gpt-4o", {
    inputTokens: 500,
    outputTokens: 150,
    parentSpanId: "span_agent_42",
    sessionId: "session_7",
  });
} catch (error) {
  if (error instanceof DeliveryError) {
    // Retry with the same eventId reported by the error.
    console.error(error.eventId);
  }
}
```

The client uses bounded exponential retries and reuses one `eventId` across attempts. FluxMeter returns success only after Kafka acknowledges custody. Broker addresses and topics are intentionally not part of the public SDK.

## Build

```bash
npm install
npm run build
```

See the [API reference](../../docs/api-reference.md) for query and budget endpoints.
