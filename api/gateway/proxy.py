"""OpenAI-compatible upstream forwarding."""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, AsyncIterator, Optional

import httpx
from fastapi.responses import JSONResponse, StreamingResponse

from budget_ops import (
    refresh_gateway_reservation,
    register_gateway_reservation,
    reserve_hold,
    settle_gateway_reservation,
)
from budget_gate import run_budget_check
from gateway.deps import UPSTREAM_API_KEY, UPSTREAM_BASE
from gateway.ingest import ingest_usage
from gateway.pricing_estimate import estimate_request_cost
from gateway.stream_guard import StreamGuard

logger = logging.getLogger(__name__)


def budget_denied_response(gate: dict) -> JSONResponse:
    reason = gate.get("reason", "budget_exceeded")
    return JSONResponse(
        status_code=402,
        content={
            "error": {
                "message": reason,
                "type": "insufficient_quota",
                "code": "budget_exceeded",
                "fluxmeter": gate,
            }
        },
    )


async def handle_chat_completion(
    r,
    *,
    customer_id: str,
    body: dict[str, Any],
    provider_auth: Optional[str],
    parent_span_id: Optional[str],
    session_id: Optional[str],
    key_id: Optional[str],
    tenant_id: Optional[str],
) -> JSONResponse | StreamingResponse:
    model = str(body.get("model") or "unknown")
    stream = bool(body.get("stream"))
    max_tokens = body.get("max_tokens")
    if isinstance(max_tokens, float):
        max_tokens = int(max_tokens)

    estimated = estimate_request_cost(model, max_tokens if isinstance(max_tokens, int) else None)
    gate = run_budget_check(
        r,
        customer_id,
        estimated,
        parent_span_id=parent_span_id,
        session_id=session_id,
        key_id=key_id,
        tenant_id=tenant_id,
    )
    if not gate.get("allowed", False):
        return budget_denied_response(gate)

    headers = {"Content-Type": "application/json"}
    auth = provider_auth or (f"Bearer {UPSTREAM_API_KEY}" if UPSTREAM_API_KEY else None)
    if not auth:
        return JSONResponse(
            status_code=401,
            content={
                "error": {
                    "message": "Missing provider Authorization header or GATEWAY_UPSTREAM_API_KEY",
                    "type": "authentication_error",
                    "code": "missing_provider_key",
                }
            },
        )
    headers["Authorization"] = auth

    hold = reserve_hold(
        r, customer_id, estimated, parent_span_id=parent_span_id, tenant_id=tenant_id
    )
    if not hold.get("allowed"):
        return budget_denied_response(hold)
    reserved_usd = float(hold.get("reserved_usd") or estimated)
    reservation_id = str(uuid.uuid4())
    register_gateway_reservation(
        r,
        reservation_id,
        customer_id=customer_id,
        reserved_usd=reserved_usd,
        parent_span_id=parent_span_id,
        tenant_id=tenant_id,
    )

    url = f"{UPSTREAM_BASE}/chat/completions"
    payload = json.dumps(body).encode("utf-8")

    if stream:
        return StreamingResponse(
            _stream_response(
                r,
                url=url,
                headers=headers,
                payload=payload,
                customer_id=customer_id,
                model=model,
                reserved_usd=reserved_usd,
                parent_span_id=parent_span_id,
                session_id=session_id,
                key_id=key_id,
                tenant_id=tenant_id,
                reservation_id=reservation_id,
            ),
            media_type="text/event-stream",
        )

    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0)) as client:
        resp = await client.post(url, headers=headers, content=payload)
        if resp.status_code >= 400:
            settle_gateway_reservation(r, reservation_id)
            return JSONResponse(status_code=resp.status_code, content=resp.json())

        data = resp.json()
        usage = data.get("usage") or {}
        input_tokens = int(usage.get("prompt_tokens") or 0)
        output_tokens = int(usage.get("completion_tokens") or 0)
        ingest_usage(
            r,
            customer_id=customer_id,
            model_id=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            parent_span_id=parent_span_id,
            session_id=session_id,
            api_key_id=key_id,
            tenant_id=tenant_id,
            reservation_id=reservation_id,
            reserved_usd=reserved_usd,
        )
        return JSONResponse(status_code=200, content=data)


async def _stream_response(
    r,
    *,
    url: str,
    headers: dict[str, str],
    payload: bytes,
    customer_id: str,
    model: str,
    reserved_usd: float,
    parent_span_id: Optional[str],
    session_id: Optional[str],
    key_id: Optional[str],
    tenant_id: Optional[str],
    reservation_id: Optional[str],
) -> AsyncIterator[bytes]:
    guard = StreamGuard(model=model, reserved_usd=reserved_usd)
    next_refresh = time.monotonic() + 60
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0)) as client:
            async with client.stream("POST", url, headers=headers, content=payload) as resp:
                if resp.status_code >= 400:
                    body = await resp.aread()
                    yield body
                    return
                async for chunk in guard.transform(resp.aiter_bytes()):
                    if reservation_id and time.monotonic() >= next_refresh:
                        refresh_gateway_reservation(r, reservation_id)
                        next_refresh = time.monotonic() + 60
                    yield chunk
    finally:
        usage = guard.usage
        if usage.input_tokens or usage.output_tokens or reserved_usd > 0:
            meta = {"_stream_killed": "true"} if usage.killed else None
            try:
                ingest_usage(
                    r,
                    customer_id=customer_id,
                    model_id=model,
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                    parent_span_id=parent_span_id,
                    session_id=session_id,
                    metadata=meta,
                    api_key_id=key_id,
                    tenant_id=tenant_id,
                    reservation_id=reservation_id,
                    reserved_usd=reserved_usd,
                )
            except Exception as exc:
                logger.debug("ingest failed: %s", exc)
