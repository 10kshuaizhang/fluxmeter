"""Gateway HTTP routes — OpenAI-compatible proxy."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from auth import require_api_key, require_customer_access, resolve_key_context
from gateway.deps import get_redis
from gateway.proxy import handle_chat_completion

router = APIRouter(tags=["gateway"])


@router.get("/health")
def health():
    return {"status": "ok", "service": "fluxmeter-gateway"}


@router.post("/v1/chat/completions")
async def chat_completions(
    request: Request,
    _: None = Depends(require_api_key),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    x_fluxmeter_customer_id: str | None = Header(default=None, alias="X-FluxMeter-Customer-Id"),
    x_fluxmeter_span_id: str | None = Header(default=None, alias="X-FluxMeter-Span-Id"),
    x_fluxmeter_session_id: str | None = Header(default=None, alias="X-FluxMeter-Session-Id"),
    authorization: str | None = Header(default=None, alias="Authorization"),
):
    """OpenAI-compatible chat proxy with FluxMeter check, reserve, kill, ingest."""
    if not x_fluxmeter_customer_id:
        raise HTTPException(
            status_code=400,
            detail="X-FluxMeter-Customer-Id header required",
        )
    require_customer_access(x_fluxmeter_customer_id, x_api_key)

    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON body") from exc

    _, key_id = resolve_key_context(x_api_key)
    r = get_redis()
    return await handle_chat_completion(
        r,
        customer_id=x_fluxmeter_customer_id,
        body=body,
        provider_auth=authorization,
        parent_span_id=x_fluxmeter_span_id,
        session_id=x_fluxmeter_session_id,
        key_id=key_id,
    )
