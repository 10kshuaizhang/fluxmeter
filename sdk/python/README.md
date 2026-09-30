# FluxMeter Python SDK

HTTP-only Python SDK for [FluxMeter](https://fluxmeter.dev) token metering and budget enforcement.

## Install

```bash
# From the engine repository root; installs this checkout
pip install ./sdk/python
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

`track_openai`, `track_anthropic`, and `track_google` extract usage from provider responses and send the same HTTP event contract. Direct `track*` calls raise typed delivery errors after bounded retries. FluxMeter returns acceptance only after Kafka acknowledgement and identity finalization; this is not settled billing. A quarantine receipt is not billable usage.

Direct Kafka configuration and the former local WAL were removed in 2.0.0. Kafka is an internal transport; applications should use `/ingest` or `/ingest/batch`.

See the [API reference](../../../docs/api-reference.md) and [v4 migration guide](../../../docs/migration-4.0.md).


## OpenAI wrapper: observe metering failures

Python SDK **2.0.1** is versioned separately from engine/API **4.8.4** and JS SDK
**2.0.0**. Repository version does not confirm package-index publication.

```python
import logging
from fluxmeter import wrap

logger = logging.getLogger(__name__)

def metering_failed(error):
    # Send to your application's durable recovery/alerting path.
    # DeliveryError preserves the event_id; retain provider usage in your app.
    logger.error("Metering failure: %s, event_id=%s",
                 type(error).__name__, getattr(error, "event_id", None))

client = wrap(openai_client, meter, customer_id="cust_123",
              fail_open=False, on_metering_error=metering_failed)
```

The callback receives the original exception on non-streaming tracking failures,
stream tracking failures, and stream reconciliation failures. A warning is always
logged. For compatibility, the provider result remains available and callback
exceptions are logged rather than replacing that result. Do not retry a provider
call just because its metering failed: recover delivery of the original usage.
`fail_open` controls admission failure behavior only. Checks create no hold;
streaming guards may estimate tokens and do not guarantee zero overspend.
