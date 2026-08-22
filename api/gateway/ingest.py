"""Proxy-only usage ingest via shared Token Event Custody."""

from __future__ import annotations

import time
from typing import Any, Optional

import redis

from gateway.deps import KAFKA_ACK_TIMEOUT_SECONDS, KAFKA_TOPIC, get_kafka_producer
from gateway.outbox import publish_envelope
from ingestion import CustodyConfig, CustodyContext, TokenEventCustody


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
    event_id: Optional[str] = None,
) -> dict[str, Any]:
    """Record token usage from Gateway through the same Custody module as POST /ingest."""
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
    if event_id:
        event_dict["eventId"] = event_id

    custody = TokenEventCustody(
        r,
        get_kafka_producer(),
        CustodyConfig(
            topic=KAFKA_TOPIC,
            quarantine_topic=KAFKA_TOPIC,
            timeout_seconds=KAFKA_ACK_TIMEOUT_SECONDS,
        ),
    )
    result = custody.accept(
        event_dict,
        CustodyContext(
            tenant_id=tenant_id,
            api_key_id=api_key_id,
            source="gateway",
            reservation_id=reservation_id,
            reserved_usd=reserved_usd,
            on_kafka_down="buffer",
            buffer_publish=lambda redis_client, producer, topic, envelope, timeout: publish_envelope(
                redis_client, producer, topic, envelope, timeout
            ),
        ),
    )
    status = result["status"]
    if status in ("accepted", "quarantined") or result.get("idempotent"):
        return {"status": "accepted", "eventId": result["eventId"]}
    if status == "buffered":
        return {"status": "buffered", "eventId": result["eventId"]}
    if status == "conflict":
        return {"status": "conflict", "eventId": result["eventId"]}
    if status == "pending":
        return {"status": "pending", "eventId": result["eventId"]}
    return {"status": status, "eventId": result["eventId"]}
