"""Proxy-only usage ingest (Lite or Full)."""

from __future__ import annotations

import time
import uuid
from typing import Any, Optional

import redis

from gateway.deps import KAFKA_ACK_TIMEOUT_SECONDS, KAFKA_TOPIC, get_kafka_producer
from gateway.outbox import publish_envelope
from ingestion import trusted_envelope


def ingest_usage(
    r: redis.Redis,
    *,
    customer_id: str,
    model_id: str,
    input_tokens: int,
    output_tokens: int,
    parent_span_id: Optional[str] = None,
    session_id: Optional[str] = None,
    metadata: Optional[dict[str, str]] = None,
    tenant_id: Optional[str] = None,
    api_key_id: Optional[str] = None,
    reservation_id: Optional[str] = None,
    reserved_usd: float = 0.0,
) -> dict[str, Any]:
    """Record token usage from Gateway (same path as POST /ingest)."""
    now_ms = int(time.time() * 1000)
    event_dict: dict[str, Any] = {
        "customerId": customer_id,
        "modelId": model_id,
        "inputTokens": max(0, input_tokens),
        "outputTokens": max(0, output_tokens),
        "timestamp": now_ms,
    }
    if parent_span_id:
        event_dict["parentSpanId"] = parent_span_id
    if session_id:
        event_dict["sessionId"] = session_id
    if metadata:
        event_dict["metadata"] = metadata

    event_dict["eventId"] = str(uuid.uuid4())
    envelope = trusted_envelope(
        event_dict,
        tenant_id=tenant_id,
        api_key_id=api_key_id,
        received_at=now_ms,
        source="gateway",
        reservation_id=reservation_id,
        reserved_usd=reserved_usd,
    )
    published = publish_envelope(
        r,
        get_kafka_producer(),
        KAFKA_TOPIC,
        envelope,
        KAFKA_ACK_TIMEOUT_SECONDS,
    )
    return {
        "status": "accepted" if published else "buffered",
        "eventId": event_dict["eventId"],
    }
