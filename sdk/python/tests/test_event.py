"""Tests for TokenEvent serialization."""

from fluxmeter.event import TokenEvent


def test_to_dict_camel_case():
    event = TokenEvent(
        customer_id="cust_1",
        model_id="gpt-4o",
        provider="openai",
        input_tokens=100,
        output_tokens=50,
    )
    d = event.to_dict()
    assert d["customerId"] == "cust_1"
    assert d["modelId"] == "gpt-4o"
    assert d["provider"] == "openai"
    assert d["inputTokens"] == 100
    assert d["outputTokens"] == 50
    assert "eventId" in d
    assert "timestamp" in d


def test_total_tokens():
    event = TokenEvent(
        customer_id="cust_1",
        model_id="o1",
        input_tokens=1000,
        output_tokens=500,
        reasoning_tokens=3000,
        cache_read_tokens=200,
    )
    assert event.total_tokens == 4700


def test_optional_fields_excluded():
    event = TokenEvent(customer_id="cust_1", model_id="gpt-4o")
    d = event.to_dict()
    assert "requestId" not in d
    assert "spanId" not in d
    assert "sessionId" not in d
    assert "environment" not in d
    assert "metadata" not in d


def test_optional_fields_included():
    event = TokenEvent(
        customer_id="cust_1",
        model_id="gpt-4o",
        request_id="chatcmpl-abc",
        span_id="span_123",
        session_id="sess_456",
        environment="production",
        metadata={"feature": "chat"},
    )
    d = event.to_dict()
    assert d["requestId"] == "chatcmpl-abc"
    assert d["spanId"] == "span_123"
    assert d["sessionId"] == "sess_456"
    assert d["environment"] == "production"
    assert d["metadata"] == {"feature": "chat"}
