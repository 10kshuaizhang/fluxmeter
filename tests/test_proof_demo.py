from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "demos"))

from reserve_meter_kill_audit_demo import parse_sse_error, wait_for


def test_parse_sse_error_extracts_meter_receipt():
    receipt = {
        "error": {
            "code": "stream_killed",
            "fluxmeter": {
                "output_tokens": 300,
                "metered_usd": 0.00018,
                "reserved_usd": 0.000078,
            },
        }
    }
    error = parse_sse_error(
        ["data: " + json.dumps({"choices": []}), "data: " + json.dumps(receipt), "data: [DONE]"]
    )
    assert error["code"] == "stream_killed"
    assert error["fluxmeter"]["metered_usd"] > error["fluxmeter"]["reserved_usd"]


def test_wait_for_returns_first_accepted_value():
    values = iter([None, {"event_id": "evt-proof"}])
    result = wait_for(
        "audit row",
        lambda: next(values),
        lambda row: bool(row and row.get("event_id")),
        timeout=1,
        interval=0,
    )
    assert result == {"event_id": "evt-proof"}
