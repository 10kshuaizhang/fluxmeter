"""Unit tests for wrap() + HTTP-mode FluxMeter."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from fluxmeter import BudgetExceededError, FluxMeter, StreamKilledError, wrap


def test_http_meter_check_and_track():
    meter = FluxMeter(api_url="http://127.0.0.1:8000", api_key="k")
    with patch.object(meter, "_http_json", return_value={"allowed": True, "reason": "ok"}) as http:
        gate = meter.check("c1", 0.01)
        assert gate["allowed"] is True
        http.assert_called()

    with patch.object(meter, "_http_json", return_value={"status": "ok"}) as http:
        meter.track("c1", "gpt-4o-mini", input_tokens=10, output_tokens=5)
        assert meter.events_sent == 1
        assert http.call_args[0][:2] == ("POST", "/ingest")


def test_wrap_denies_when_budget_exhausted():
    meter = MagicMock()
    meter._api_url = "http://x"
    meter.check.return_value = {"allowed": False, "reason": "budget_exhausted"}

    original = MagicMock(return_value={"ok": True})
    client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=original))
    )
    wrap(client, meter, "cust", fail_open=False)

    with pytest.raises(BudgetExceededError) as ei:
        client.chat.completions.create(model="gpt-4o-mini", messages=[])
    assert ei.value.gate["reason"] == "budget_exhausted"
    original.assert_not_called()


def test_wrap_fail_open_on_check_error():
    meter = MagicMock()
    meter._api_url = "http://x"
    meter.check.side_effect = RuntimeError("down")
    meter.track_openai = MagicMock()

    create = MagicMock(return_value=SimpleNamespace(id="1", model="gpt-4o-mini", usage=SimpleNamespace(
        prompt_tokens=1, completion_tokens=1, prompt_tokens_details=None, completion_tokens_details=None
    )))
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    wrap(client, meter, "cust", fail_open=True)
    resp = client.chat.completions.create(model="gpt-4o-mini", messages=[])
    assert resp.id == "1"
    meter.track_openai.assert_called_once()


def test_killable_stream_raises_when_over_reserve():
    meter = MagicMock()
    meter._api_url = "http://x"
    meter.check.return_value = {"allowed": True}
    meter.reserve.return_value = {"allowed": True, "reserved_usd": 0.00001}

    # Long content → many est tokens → exceed tiny reserve
    chunk = SimpleNamespace(
        choices=[SimpleNamespace(delta=SimpleNamespace(content="x" * 400))],
        usage=None,
    )

    def fake_stream(**kwargs):
        yield chunk
        yield chunk

    create = MagicMock(side_effect=lambda **kw: fake_stream())
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    wrap(
        client,
        meter,
        "cust",
        estimated_cost_usd=0.00001,
        cost_per_output_token=1.0,  # $1/token for test
        fail_open=False,
    )

    stream = client.chat.completions.create(model="m", messages=[], stream=True)
    with pytest.raises(StreamKilledError):
        for _ in stream:
            pass
    meter.reconcile.assert_called()
