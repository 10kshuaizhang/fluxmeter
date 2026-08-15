"""HTTP-only SDK delivery contract."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from fluxmeter import DeliveryError, FluxMeter
from fluxmeter.client import _HTTPStatusError


def test_default_client_uses_http_ingest():
    meter = FluxMeter()

    with patch.object(meter, "_http_json", return_value={"status": "accepted"}) as http:
        event = meter.track("cust_1", "gpt-4o-mini", input_tokens=10)

    assert http.call_args.args[:2] == ("POST", "/ingest")
    assert http.call_args.kwargs["body"]["eventId"] == event.event_id


def test_retries_reuse_event_identity():
    meter = FluxMeter(max_retries=2, retry_base_seconds=0)
    attempts = []

    def flaky(_method, _path, *, body=None, query=None):
        attempts.append(body)
        if len(attempts) < 3:
            raise RuntimeError("temporary failure")
        return {"status": "accepted"}

    with patch.object(meter, "_http_json", side_effect=flaky):
        event = meter.track("cust_retry", "gpt-4o-mini", input_tokens=5)

    assert len(attempts) == 3
    assert {attempt["eventId"] for attempt in attempts} == {event.event_id}


def test_exhausted_retries_raise_typed_delivery_error():
    meter = FluxMeter(max_retries=1, retry_base_seconds=0)

    with patch.object(meter, "_http_json", side_effect=RuntimeError("down")):
        with pytest.raises(DeliveryError) as exc:
            meter.track("cust_fail", "gpt-4o-mini", input_tokens=5)

    assert exc.value.event_id
    assert meter.delivery_errors == 1


def test_permanent_http_error_is_not_retried():
    meter = FluxMeter(max_retries=3, retry_base_seconds=0)

    with patch.object(
        meter,
        "_http_json",
        side_effect=_HTTPStatusError(409, {"code": "event_id_conflict"}),
    ) as http:
        with pytest.raises(DeliveryError):
            meter.track("cust_conflict", "gpt-4o-mini", input_tokens=5)

    assert http.call_count == 1
