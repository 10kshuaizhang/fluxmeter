"""Wrap OpenAI-compatible clients with pre-call check + post-call track.

Usage::

    from openai import OpenAI
    from fluxmeter import FluxMeter, wrap

    meter = FluxMeter(api_url="http://localhost:8000", api_key="...")
    client = wrap(OpenAI(), meter, customer_id="cust_1", fail_open=True)
    resp = client.chat.completions.create(model="gpt-4o-mini", messages=[...])
"""

from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class BudgetExceededError(Exception):
    """Raised when FluxMeter denies the call before provider hit."""

    def __init__(self, gate: dict):
        self.gate = gate
        super().__init__(gate.get("reason", "budget_exceeded"))


class StreamKilledError(Exception):
    """Raised mid-stream when estimated cost exceeds the reserved hold."""

    def __init__(self, *, estimated_usd: float, reserved_usd: float, chunks: int):
        self.estimated_usd = estimated_usd
        self.reserved_usd = reserved_usd
        self.chunks = chunks
        super().__init__(
            f"stream killed: est ${estimated_usd:.6f} > reserved ${reserved_usd:.6f}"
        )


def wrap(
    client: Any,
    meter: Any,
    customer_id: str,
    *,
    estimated_cost_usd: float = 0.05,
    fail_open: bool = True,
    parent_span_id: Optional[str] = None,
    session_id: Optional[str] = None,
    cost_per_output_token: float = 1e-5,
    cost_per_input_token: float = 2.5e-6,
):
    """Patch ``client.chat.completions.create`` with check → call → track.

    ``fail_open=True`` (default): if the check API is unreachable, allow the LLM
    call (revenue-preserving). Budget denials (``allowed=False`` with a real
    reason) still raise ``BudgetExceededError``.
    """
    completions = client.chat.completions
    original = completions.create

    def create(*args, **kwargs):
        gate = _safe_check(
            meter,
            customer_id,
            estimated_cost_usd,
            fail_open=fail_open,
            parent_span_id=parent_span_id,
            session_id=session_id,
        )
        if not gate.get("allowed", False):
            raise BudgetExceededError(gate)

        stream = bool(kwargs.get("stream"))
        reserved = 0.0
        if stream and getattr(meter, "_api_url", None):
            try:
                hold = meter.reserve(customer_id, estimated_cost_usd)
                if hold.get("allowed"):
                    reserved = float(hold.get("reserved_usd") or estimated_cost_usd)
                elif not fail_open:
                    raise BudgetExceededError(hold)
            except BudgetExceededError:
                raise
            except Exception as e:
                if not fail_open:
                    raise
                logger.debug("reserve failed (fail_open): %s", e)

        result = original(*args, **kwargs)

        if stream:
            return _KillableStream(
                result,
                meter=meter,
                customer_id=customer_id,
                model_id=kwargs.get("model") or (args[0] if args else "unknown"),
                reserved_usd=reserved or estimated_cost_usd,
                cost_per_output_token=cost_per_output_token,
                cost_per_input_token=cost_per_input_token,
                parent_span_id=parent_span_id,
                session_id=session_id,
            )

        try:
            meter.track_openai(
                customer_id,
                result,
                session_id=session_id,
                parent_span_id=parent_span_id,
            )
        except Exception as e:
            logger.debug("track_openai failed: %s", e)
        return result

    completions.create = create  # type: ignore[method-assign]
    return client


def _safe_check(
    meter,
    customer_id: str,
    estimated_cost_usd: float,
    *,
    fail_open: bool,
    parent_span_id: Optional[str],
    session_id: Optional[str],
) -> dict:
    try:
        return meter.check(
            customer_id,
            estimated_cost_usd,
            parent_span_id=parent_span_id,
            session_id=session_id,
        )
    except Exception as e:
        logger.debug("check failed: %s", e)
        if fail_open:
            return {"allowed": True, "reason": "fail_open", "source": "policy"}
        return {"allowed": False, "reason": "check_unavailable", "source": "policy"}


class _KillableStream:
    """OpenAI stream iterator that aborts when estimated spend exceeds reserve."""

    def __init__(
        self,
        stream,
        *,
        meter,
        customer_id: str,
        model_id: str,
        reserved_usd: float,
        cost_per_output_token: float,
        cost_per_input_token: float,
        parent_span_id: Optional[str],
        session_id: Optional[str],
    ):
        self._stream = iter(stream)
        self._meter = meter
        self._customer_id = customer_id
        self._model_id = model_id
        self._reserved_usd = reserved_usd
        self._cost_out = cost_per_output_token
        self._cost_in = cost_per_input_token
        self._parent_span_id = parent_span_id
        self._session_id = session_id
        self._output_tokens = 0
        self._input_tokens = 0
        self._chunks = 0
        self._closed = False

    def __iter__(self):
        return self

    def __next__(self):
        try:
            chunk = next(self._stream)
        except StopIteration:
            self._finish(killed=False)
            raise

        self._chunks += 1
        self._note_chunk(chunk)
        est = self._input_tokens * self._cost_in + self._output_tokens * self._cost_out
        if self._reserved_usd > 0 and est > self._reserved_usd:
            self._finish(killed=True)
            raise StreamKilledError(
                estimated_usd=est,
                reserved_usd=self._reserved_usd,
                chunks=self._chunks,
            )
        return chunk

    def _note_chunk(self, chunk) -> None:
        if hasattr(chunk, "choices") and chunk.choices:
            delta = getattr(chunk.choices[0], "delta", None)
            content = getattr(delta, "content", None) if delta else None
            if content:
                self._output_tokens += max(1, len(content) // 4)
        if hasattr(chunk, "usage") and chunk.usage:
            usage = chunk.usage
            if getattr(usage, "completion_tokens", None):
                self._output_tokens = usage.completion_tokens
            if getattr(usage, "prompt_tokens", None):
                self._input_tokens = usage.prompt_tokens

    def _finish(self, *, killed: bool) -> None:
        if self._closed:
            return
        self._closed = True
        reserved = self._reserved_usd
        if reserved > 0 and getattr(self._meter, "_api_url", None):
            try:
                self._meter.reconcile(self._customer_id, reserved)
            except Exception as e:
                logger.debug("reconcile failed: %s", e)
        if self._output_tokens > 0 or self._input_tokens > 0:
            try:
                self._meter.track(
                    self._customer_id,
                    self._model_id,
                    input_tokens=self._input_tokens,
                    output_tokens=self._output_tokens,
                    parent_span_id=self._parent_span_id,
                    session_id=self._session_id,
                    metadata={"_stream_killed": "true"} if killed else None,
                )
            except Exception as e:
                logger.debug("track failed: %s", e)
