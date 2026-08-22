"""OpenAI-compatible upstream forwarding — ProxiedCompletion orchestration."""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, AsyncIterator, Optional

import httpx
from fastapi.responses import JSONResponse, StreamingResponse

from budget import Budget
from reservation import Reservation
from gateway.deps import UPSTREAM_API_KEY, UPSTREAM_BASE
from gateway.ingest import ingest_usage
from pricing_loader import get_catalog
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


class ProxiedCompletion:
    """Gateway lifecycle: estimate → gate → reserve → upstream → Custody."""

    def __init__(
        self,
        r,
        *,
        customer_id: str,
        body: dict[str, Any],
        provider_auth: Optional[str],
        parent_span_id: Optional[str],
        session_id: Optional[str],
        key_id: Optional[str],
        tenant_id: Optional[str],
    ):
        self.r = r
        self.customer_id = customer_id
        self.body = body
        self.provider_auth = provider_auth
        self.parent_span_id = parent_span_id
        self.session_id = session_id
        self.key_id = key_id
        self.tenant_id = tenant_id
        self.model = str(body.get("model") or "unknown")
        self.stream = bool(body.get("stream"))
        self.reserved_usd = 0.0
        self.reservation_id: Optional[str] = None

    async def run(self) -> JSONResponse | StreamingResponse:
        max_tokens = self.body.get("max_tokens")
        if isinstance(max_tokens, float):
            max_tokens = int(max_tokens)

        estimated = get_catalog().estimate_completion_usd(
            self.model,
            max_output_tokens=max_tokens if isinstance(max_tokens, int) else None,
        )
        gate = Budget(self.r, self.tenant_id).check(
            self.customer_id,
            estimated,
            parent_span_id=self.parent_span_id,
            session_id=self.session_id,
            key_id=self.key_id,
        )
        if not gate.get("allowed", False):
            return budget_denied_response(gate)

        headers = {"Content-Type": "application/json"}
        auth = self.provider_auth or (
            f"Bearer {UPSTREAM_API_KEY}" if UPSTREAM_API_KEY else None
        )
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

        self.reservation_id = str(uuid.uuid4())
        hold = Reservation(self.r, self.tenant_id).open(
            self.reservation_id,
            customer_id=self.customer_id,
            estimated_cost_usd=estimated,
            parent_span_id=self.parent_span_id,
        )
        if not hold.get("allowed"):
            return budget_denied_response(hold)
        self.reserved_usd = float(hold.get("reserved_usd") or estimated)

        url = f"{UPSTREAM_BASE}/chat/completions"
        payload = json.dumps(self.body).encode("utf-8")

        if self.stream:
            return StreamingResponse(
                self._stream(url=url, headers=headers, payload=payload),
                media_type="text/event-stream",
                headers=self._reservation_headers(),
            )
        return await self._non_stream(url=url, headers=headers, payload=payload)

    def _reservation_headers(self) -> dict[str, str]:
        """Expose the hold receipt without changing the OpenAI response body."""
        if not self.reservation_id:
            return {}
        return {
            "X-FluxMeter-Reservation-Id": self.reservation_id,
            "X-FluxMeter-Reserved-Usd": f"{self.reserved_usd:.6f}",
        }

    def _ingest(
        self,
        *,
        input_tokens: int,
        output_tokens: int,
        metadata: Optional[dict] = None,
    ) -> None:
        ingest_usage(
            self.r,
            customer_id=self.customer_id,
            model_id=self.model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            parent_span_id=self.parent_span_id,
            session_id=self.session_id,
            metadata=metadata,
            api_key_id=self.key_id,
            tenant_id=self.tenant_id,
            reservation_id=self.reservation_id,
            reserved_usd=self.reserved_usd,
        )

    async def _non_stream(
        self, *, url: str, headers: dict[str, str], payload: bytes
    ) -> JSONResponse:
        async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0)) as client:
            resp = await client.post(url, headers=headers, content=payload)
            if resp.status_code >= 400:
                if self.reservation_id:
                    Reservation(self.r, self.tenant_id).settle(self.reservation_id)
                return JSONResponse(status_code=resp.status_code, content=resp.json())

            data = resp.json()
            usage = data.get("usage") or {}
            self._ingest(
                input_tokens=int(usage.get("prompt_tokens") or 0),
                output_tokens=int(usage.get("completion_tokens") or 0),
            )
            return JSONResponse(
                status_code=200,
                content=data,
                headers=self._reservation_headers(),
            )

    async def _stream(
        self, *, url: str, headers: dict[str, str], payload: bytes
    ) -> AsyncIterator[bytes]:
        guard = StreamGuard(model=self.model, reserved_usd=self.reserved_usd)
        next_refresh = time.monotonic() + 60
        settled_on_error = False
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(120.0, connect=10.0)
            ) as client:
                async with client.stream(
                    "POST", url, headers=headers, content=payload
                ) as resp:
                    if resp.status_code >= 400:
                        if self.reservation_id:
                            Reservation(self.r, self.tenant_id).settle(self.reservation_id)
                            settled_on_error = True
                        body = await resp.aread()
                        yield body
                        return
                    async for chunk in guard.transform(resp.aiter_bytes()):
                        if self.reservation_id and time.monotonic() >= next_refresh:
                            Reservation(self.r, self.tenant_id).refresh(self.reservation_id)
                            next_refresh = time.monotonic() + 60
                        yield chunk
        finally:
            if settled_on_error:
                return
            usage = guard.usage
            if usage.input_tokens or usage.output_tokens or self.reserved_usd > 0:
                meta = {"_stream_killed": "true"} if usage.killed else None
                try:
                    self._ingest(
                        input_tokens=usage.input_tokens,
                        output_tokens=usage.output_tokens,
                        metadata=meta,
                    )
                except Exception as exc:
                    logger.debug("ingest failed: %s", exc)


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
    return await ProxiedCompletion(
        r,
        customer_id=customer_id,
        body=body,
        provider_auth=provider_auth,
        parent_span_id=parent_span_id,
        session_id=session_id,
        key_id=key_id,
        tenant_id=tenant_id,
    ).run()
