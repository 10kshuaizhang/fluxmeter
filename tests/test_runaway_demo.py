"""Unit tests for the runaway wow demo — no Docker, no provider key."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))

from runaway_demo import (  # noqa: E402
    HttpJson,
    INPUT_TOKENS,
    MODEL,
    MockUpstream,
    OUTPUT_TOKENS,
    catalog_cost_usd,
    format_comparison,
    parse_budget_error,
    run_chat_loop,
    run_unguarded_parallel,
    usd,
)
from runaway_demo import LoopResult, Ledger  # noqa: E402


def test_catalog_cost_is_three_cents_for_gpt4o_fixture():
    assert catalog_cost_usd() == pytest.approx(0.03)
    assert catalog_cost_usd(MODEL, INPUT_TOKENS, OUTPUT_TOKENS) == pytest.approx(0.03)


def test_usd_formatting():
    assert usd(0.3) == "$0.30"
    assert usd(55.26) == "$55.26"
    assert usd(None) == "n/a"


def test_parse_budget_error_reads_openai_shape():
    payload = {
        "error": {
            "code": "budget_exceeded",
            "type": "insufficient_quota",
            "fluxmeter": {"reason": "budget_exhausted"},
        }
    }
    assert parse_budget_error(payload) == "budget_exceeded"


def test_format_comparison_is_side_by_side_and_gif_friendly():
    unguarded = LoopResult(
        admitted=2000,
        elapsed_s=3.0,
        stopped=False,
        stop_reason="cap (3.0s / 2500 calls, still burning)",
        spend_usd=60.0,
    )
    guarded = LoopResult(
        admitted=10,
        elapsed_s=0.27,
        stopped=True,
        stop_reason="402 budget_exceeded",
        spend_usd=0.30,
        reserved_usd=0.03,
    )
    ledger = Ledger(
        customer_id="runaway-demo",
        event_count=10,
        cost_usd=0.30,
        debt_usd=0.0,
        held_usd=0.0,
        settled=True,
    )
    text = format_comparison(
        budget_usd=0.30,
        cost_per_call=0.03,
        unguarded=unguarded,
        guarded=guarded,
        ledger=ledger,
        customer_id="runaway-demo",
        wall_s=18.4,
    )
    assert "no FluxMeter" in text
    assert "FluxMeter Gateway" in text
    assert "Calls admitted" in text
    assert "Spend USD" in text
    assert "Overspend / debt" in text
    assert "Time to stop" in text
    assert "did not stop" in text
    assert "0.27s" in text
    assert "Your agent loop just tried to spend $60.00." in text
    assert "FluxMeter stopped it at $0.30." in text
    assert "customer=runaway-demo" in text


class _FakeGateway(BaseHTTPRequestHandler):
    admitted_before_deny = 10

    def log_message(self, *_args):
        return

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length:
            self.rfile.read(length)
        n = getattr(self.server, "n", 0) + 1
        self.server.n = n
        if n > self.admitted_before_deny:
            body = json.dumps(
                {
                    "error": {
                        "message": "budget_exhausted",
                        "type": "insufficient_quota",
                        "code": "budget_exceeded",
                    }
                }
            ).encode()
            self.send_response(402)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        body = b'{"id":"ok","usage":{"prompt_tokens":4000,"completion_tokens":2000}}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-FluxMeter-Reserved-Usd", "0.030000")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def fake_gateway():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FakeGateway)
    server.n = 0
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_unguarded_mock_never_stops_inside_cap():
    with MockUpstream(0) as mock:
        client = HttpJson(f"http://127.0.0.1:{mock.port}")
        try:
            result = run_chat_loop(
                client,
                "/v1/chat/completions",
                headers={"Authorization": "Bearer sk-runaway"},
                cost_per_call=0.03,
                max_seconds=0.4,
                max_calls=50,
                stop_on_non_200=False,
            )
        finally:
            client.close()
    assert result.admitted >= 1
    assert result.stopped is False
    assert "still burning" in result.stop_reason
    assert result.spend_usd == pytest.approx(result.admitted * 0.03)


def test_unguarded_parallel_overshoots_without_stopping():
    with MockUpstream(0) as mock:
        result = run_unguarded_parallel(
            f"http://127.0.0.1:{mock.port}",
            headers={"Authorization": "Bearer sk-runaway"},
            cost_per_call=0.03,
            max_seconds=0.5,
            max_calls=400,
            workers=8,
        )
    assert result.admitted >= 8
    assert result.stopped is False
    assert "still burning" in result.stop_reason
    assert result.spend_usd == pytest.approx(result.admitted * 0.03)


def test_gateway_loop_stops_on_402(fake_gateway):
    client = HttpJson(fake_gateway)
    try:
        result = run_chat_loop(
            client,
            "/v1/chat/completions",
            headers={
                "Authorization": "Bearer sk-runaway",
                "X-FluxMeter-Customer-Id": "runaway-test",
            },
            cost_per_call=0.03,
            max_seconds=5,
            max_calls=100,
            stop_on_non_200=True,
        )
    finally:
        client.close()
    assert result.stopped is True
    assert result.first_error_status == 402
    assert result.admitted == 10
    assert result.reserved_usd == pytest.approx(0.03)
    assert "budget_exceeded" in result.stop_reason
    assert result.spend_usd == pytest.approx(0.30)
