"""Tests for FluxMeter client (provider response parsing)."""

from unittest.mock import patch, MagicMock
from fluxmeter.client import FluxMeter


def _mock_meter():
    """Create a FluxMeter with mocked Kafka producer."""
    with patch("confluent_kafka.Producer") as mock_producer_cls:
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer
        meter = FluxMeter(kafka_brokers="localhost:9094", wal_enabled=False)
        return meter, mock_producer


def test_track_basic():
    meter, producer = _mock_meter()
    event = meter.track("cust_1", "gpt-4o", input_tokens=100, output_tokens=50)
    assert event.customer_id == "cust_1"
    assert event.model_id == "gpt-4o"
    assert event.input_tokens == 100
    assert event.output_tokens == 50
    assert producer.produce.called


def test_track_openai_dict_response():
    meter, producer = _mock_meter()
    response = {
        "id": "chatcmpl-abc123",
        "model": "gpt-4o-2024-08-06",
        "usage": {
            "prompt_tokens": 1200,
            "completion_tokens": 350,
            "prompt_tokens_details": {"cached_tokens": 200},
            "completion_tokens_details": {"reasoning_tokens": 0},
        },
    }
    event = meter.track_openai("cust_42", response, latency_ms=1200)
    assert event.customer_id == "cust_42"
    assert event.model_id == "gpt-4o-2024-08-06"
    assert event.provider == "openai"
    assert event.input_tokens == 1200
    assert event.output_tokens == 350
    assert event.cache_read_tokens == 200
    assert event.request_id == "chatcmpl-abc123"
    assert event.latency_ms == 1200


def test_track_anthropic_dict_response():
    meter, producer = _mock_meter()
    response = {
        "id": "msg_abc123",
        "model": "claude-sonnet-4-20250514",
        "usage": {
            "input_tokens": 800,
            "output_tokens": 200,
            "cache_read_input_tokens": 150,
            "cache_creation_input_tokens": 50,
        },
    }
    event = meter.track_anthropic("cust_99", response)
    assert event.customer_id == "cust_99"
    assert event.model_id == "claude-sonnet-4-20250514"
    assert event.provider == "anthropic"
    assert event.input_tokens == 800
    assert event.output_tokens == 200
    assert event.cache_read_tokens == 150
    assert event.cache_write_tokens == 50
    assert event.request_id == "msg_abc123"


def test_track_deepseek_dict_response():
    meter, producer = _mock_meter()
    response = {
        "id": "chatcmpl-ds-001",
        "model": "deepseek-v4-flash",
        "usage": {
            "prompt_tokens": 5000,
            "completion_tokens": 1200,
            "prompt_tokens_details": {"cached_tokens": 800},
        },
    }
    event = meter.track_deepseek("cust_42", response, latency_ms=980)
    assert event.provider == "deepseek"
    assert event.model_id == "deepseek-v4-flash"
    assert event.input_tokens == 5000
    assert event.output_tokens == 1200
    assert event.cache_read_tokens == 800
    assert producer.produce.called


def test_track_qwen_dict_response():
    meter, producer = _mock_meter()
    response = {
        "id": "chatcmpl-qw-001",
        "model": "qwen-plus",
        "usage": {"prompt_tokens": 3200, "completion_tokens": 900},
    }
    event = meter.track_qwen("cust_99", response)
    assert event.provider == "qwen"
    assert event.model_id == "qwen-plus"
    assert event.input_tokens == 3200
    assert event.output_tokens == 900
    assert producer.produce.called
