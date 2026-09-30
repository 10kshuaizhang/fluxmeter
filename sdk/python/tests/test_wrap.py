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


@pytest.mark.parametrize("callback_fails", [False, True])
def test_wrap_warns_and_reports_delivery_error_without_repeating_provider(caplog, callback_fails):
    from fluxmeter import DeliveryError

    response = SimpleNamespace(id="provider-response")
    original = MagicMock(return_value=response)
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=original)))
    meter = MagicMock()
    meter.check.return_value = {"allowed": True}
    error = DeliveryError("stable-event-id", "delivery failed")
    meter.track_openai.side_effect = error
    callback = MagicMock(side_effect=RuntimeError("callback unavailable") if callback_fails else None)
    wrap(client, meter, "customer", fail_open=False, on_metering_error=callback)

    assert client.chat.completions.create(model="m", messages=[]) is response
    original.assert_called_once()
    callback.assert_called_once_with(error)
    assert "FluxMeter metering failed (DeliveryError)" in caplog.text
    if callback_fails:
        assert "callback failed" in caplog.text


def test_wrap_metering_failure_is_visible_without_callback(caplog):
    meter = MagicMock()
    meter.check.return_value = {"allowed": True}
    meter.track_openai.side_effect = RuntimeError("private payload must not be logged")
    original = MagicMock(return_value=object())
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=original)))
    wrap(client, meter, "customer")
    assert client.chat.completions.create() is original.return_value
    assert "metering failed" in caplog.text
    assert "private payload" not in caplog.text


def test_stream_reports_tracking_and_reconciliation_failures_once(caplog):
    meter = MagicMock()
    meter._api_url = "http://x"
    meter.check.return_value = {"allowed": True}
    meter.reserve.return_value = {"allowed": True, "reserved_usd": 1}
    reconcile_error = RuntimeError("reconcile failed")
    track_error = RuntimeError("track failed")
    meter.reconcile.side_effect = reconcile_error
    meter.track.side_effect = track_error
    chunk = SimpleNamespace(choices=[SimpleNamespace(delta=SimpleNamespace(content="hello"))], usage=None)
    original = MagicMock(return_value=iter([chunk]))
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=original)))
    errors = []
    wrap(client, meter, "customer", on_metering_error=errors.append)
    stream = client.chat.completions.create(model="m", stream=True)
    assert list(stream) == [chunk]
    assert list(stream) == []
    assert errors == [reconcile_error, track_error]
    assert caplog.text.count("FluxMeter metering failed") == 2
