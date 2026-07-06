#!/usr/bin/env python3
"""Phase 3 path-activation demo: check → deny, wrap, mid-stream kill.

Self-check (no stack)::

    PYTHONPATH=sdk/python python demos/path_activation_demo.py

Live Lite stack (optional)::

    make demo
    FLUXMETER_API=http://127.0.0.1:8000 PYTHONPATH=sdk/python \\
      python demos/path_activation_demo.py --live
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

# Prefer local SDK trees
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "sdk", "python"))

from fluxmeter import BudgetExceededError, FluxMeter, StreamKilledError, wrap  # noqa: E402


def _self_check_wrap_deny() -> None:
    meter = MagicMock()
    meter._api_url = "http://x"
    meter.check.return_value = {"allowed": False, "reason": "budget_exhausted"}
    create = MagicMock()
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    wrap(client, meter, "cust_demo", fail_open=False)
    try:
        client.chat.completions.create(model="gpt-4o-mini", messages=[])
        raise AssertionError("expected BudgetExceededError")
    except BudgetExceededError as e:
        assert e.gate["reason"] == "budget_exhausted"
    assert create.call_count == 0
    print("ok  wrap denies before provider when budget exhausted")


def _self_check_stream_kill() -> None:
    meter = MagicMock()
    meter._api_url = "http://x"
    meter.check.return_value = {"allowed": True}
    meter.reserve.return_value = {"allowed": True, "reserved_usd": 0.00001}

    chunk = SimpleNamespace(
        choices=[SimpleNamespace(delta=SimpleNamespace(content="x" * 80))],
        usage=None,
    )

    def stream(**_kw):
        for _ in range(20):
            yield chunk

    create = MagicMock(side_effect=lambda **kw: stream())
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    wrap(
        client,
        meter,
        "cust_demo",
        estimated_cost_usd=0.00001,
        cost_per_output_token=1.0,
        fail_open=False,
    )
    it = client.chat.completions.create(model="m", messages=[], stream=True)
    killed = False
    try:
        for _ in it:
            pass
    except StreamKilledError:
        killed = True
    assert killed, "expected StreamKilledError"
    meter.reconcile.assert_called()
    print("ok  mid-stream kill when est cost exceeds reserve")


def _live(api: str) -> None:
    import urllib.request

    admin = os.getenv("FLUXMETER_ADMIN_KEY", "")
    api_key = os.getenv("FLUXMETER_API_KEY", admin)
    headers = {"Content-Type": "application/json"}
    if admin:
        headers["X-API-Key"] = admin

    cust = f"demo_{uuid.uuid4().hex[:8]}"

    def post(path: str, body: dict) -> None:
        import json
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            f"{api}{path}", data=data, headers=headers, method="POST"
        )
        urllib.request.urlopen(req, timeout=5)

    post(f"/budget/{cust}", {"balance_usd": 0.0001, "alert_threshold_usd": 0.00005})
    meter = FluxMeter(api_url=api, api_key=api_key or None)
    gate = meter.check(cust, 1.0)
    assert gate.get("allowed") is False, gate
    print(f"ok  live check denies oversized estimate for {cust}")

    # Tiny allowed estimate then burn balance via ingest
    gate_ok = meter.check(cust, 0.0)
    assert gate_ok.get("allowed") is True
    meter.track(cust, "gpt-4o-mini", input_tokens=50_000, output_tokens=50_000)
    time.sleep(0.3)
    gate2 = meter.check(cust, 0.01)
    assert gate2.get("allowed") is False, gate2
    print("ok  live balance exhausted after track → subsequent check denies")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="Hit a running Lite API")
    args = parser.parse_args()

    _self_check_wrap_deny()
    _self_check_stream_kill()
    if args.live:
        api = os.getenv("FLUXMETER_API", "http://127.0.0.1:8000").rstrip("/")
        _live(api)
    print("path activation demo: all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
