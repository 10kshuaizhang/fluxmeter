# FluxMeter Python SDK

Send AI token usage events to FluxMeter for real-time aggregation and billing.

**Website:** [fluxmeter.dev](https://fluxmeter.dev) · [GitHub](https://github.com/10kshuaizhang/fluxmeter) · [API reference](../../docs/api-reference.md)

## Install

```bash
pip install fluxmeter
```

## Quick Start (3 lines)

```python
from fluxmeter import FluxMeter

meter = FluxMeter(kafka_brokers="localhost:9094")
meter.track("cust_123", "gpt-4o", input_tokens=500, output_tokens=150)
```

## OpenAI Integration

```python
import time
from openai import OpenAI
from fluxmeter import FluxMeter

client = OpenAI()
meter = FluxMeter(kafka_brokers="localhost:9094", environment="production")

start = time.time()
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Hello!"}],
)
latency = int((time.time() - start) * 1000)

# One line to meter the usage
meter.track_openai("cust_123", response, latency_ms=latency)
```

## Anthropic Integration

```python
import anthropic
from fluxmeter import FluxMeter

client = anthropic.Anthropic()
meter = FluxMeter(kafka_brokers="localhost:9094")

response = client.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=1024,
    messages=[{"role": "user", "content": "Hello!"}],
)

meter.track_anthropic("cust_123", response)
```

## Manual Tracking (any provider)

```python
meter.track(
    customer_id="cust_123",
    model_id="gemini-1.5-pro",
    provider="google",
    input_tokens=2000,
    output_tokens=500,
    request_id="req_abc123",
    span_id="span_7f3a",          # link to your tracing
    session_id="sess_456",        # group by conversation
    latency_ms=890,
    environment="production",
    metadata={"feature": "code-review", "team": "platform"},
)
```

## Configuration

```python
meter = FluxMeter(
    kafka_brokers="kafka1:9092,kafka2:9092",  # Kafka cluster
    topic="token-events",                       # Topic name (default)
    environment="production",                   # Applied to all events
    producer_config={                           # Extra Kafka producer config
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "...",
        "sasl.password": "...",
    },
)
```

## How It Works

```
Your App  →  meter.track(...)  →  Kafka  →  Flink (real-time aggregation)  →  Redis
                                                                                 ↓
                                                                           Grafana / API
```

Events are batched and compressed (lz4) before sending. The SDK flushes automatically on process exit.

## Requirements

- Python 3.9+
- `confluent-kafka` (librdkafka-based, high performance)
- FluxMeter infrastructure running (Kafka + Flink + Redis)
