# fluxmeter-client

HTTP-only Python SDK for [FluxMeter](https://fluxmeter.dev) token metering and budget enforcement.

## Install

```bash
pip install fluxmeter-client
```

## Usage

```python
from fluxmeter import DeliveryError, FluxMeter

meter = FluxMeter(
    api_url="http://localhost:8000",
    api_key="fm_live_...",
    environment="production",
)

try:
    event_id = meter.track(
        customer_id="cust_123",
        model_id="gpt-4o",
        input_tokens=500,
        output_tokens=150,
        parent_span_id="span_agent_42",
        session_id="session_7",
    )
except DeliveryError as error:
    # The exception preserves the stable event ID used across bounded retries.
    print(error.event_id)
```

`track_openai`, `track_anthropic`, and `track_google` extract usage from provider responses and send the same HTTP event contract. Delivery failures are never silently swallowed. FluxMeter returns success only after Kafka acknowledges custody.

Direct Kafka configuration and the former local WAL were removed in 2.0.0. Kafka is an internal transport; applications should use `/ingest` or `/ingest/batch`.

See the [API reference](../../docs/api-reference.md) and [v4 migration guide](../../docs/migration-4.0.md).
