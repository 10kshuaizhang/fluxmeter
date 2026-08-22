#!/usr/bin/env python3
"""Deterministic production-path proof: reserve → meter → kill → audit.

Run with ``make demo-proof``. No provider key is required.
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

import httpx


class MockOpenAIHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:  # noqa: N802 - stdlib callback name
        length = int(self.headers.get("Content-Length", "0"))
        if length:
            self.rfile.read(length)
        if self.path != "/v1/chat/completions":
            self.send_error(404)
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()

        # Headers arrive while the Gateway hold is active. The oversized second
        # chunk intentionally violates max_tokens so StreamGuard must terminate it.
        time.sleep(1.0)
        chunks = (
            {"id": "proof", "choices": [{"delta": {"content": "ok"}}]},
            {"id": "proof", "choices": [{"delta": {"content": "x" * 1200}}]},
        )
        for chunk in chunks:
            try:
                self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
                self.wfile.flush()
                time.sleep(0.1)
            except BrokenPipeError:
                return

    def log_message(self, _format: str, *_args: Any) -> None:
        return


class MockUpstream:
    def __init__(self, port: int) -> None:
        self._server = ThreadingHTTPServer(("0.0.0.0", port), MockOpenAIHandler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self) -> MockUpstream:
        self._thread.start()
        return self

    def __exit__(self, *_args: Any) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)


def wait_for(
    description: str,
    probe: Callable[[], Any],
    accept: Callable[[Any], bool],
    *,
    timeout: float = 45.0,
    interval: float = 0.5,
) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        try:
            last = probe()
            if accept(last):
                return last
        except (httpx.HTTPError, ValueError):
            pass
        time.sleep(interval)
    raise RuntimeError(f"timed out waiting for {description}; last={last!r}")


def api_json(client: httpx.Client, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    response = client.request(method, url, **kwargs)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise RuntimeError(f"{method} {url} failed: {response.status_code} {response.text}") from exc
    return response.json()


def parse_sse_error(lines: list[str]) -> dict[str, Any]:
    for line in lines:
        if not line.startswith("data:") or line.strip() == "data: [DONE]":
            continue
        payload = json.loads(line[5:].strip())
        if "error" in payload:
            return payload["error"]
    raise RuntimeError("Gateway stream ended without a FluxMeter kill error")


def clickhouse_row(client: httpx.Client, clickhouse: str, customer_id: str) -> dict[str, Any] | None:
    sql = (
        "SELECT event_id, customer_id, model_id, input_tokens, output_tokens, metadata "
        "FROM fluxmeter.raw_events FINAL "
        f"WHERE customer_id = '{customer_id}' ORDER BY ingested_at DESC LIMIT 1 FORMAT JSONEachRow"
    )
    response = client.post(clickhouse, content=sql)
    response.raise_for_status()
    text = response.text.strip()
    return json.loads(text) if text else None


def run(api: str, gateway: str, clickhouse: str, mock_port: int) -> None:
    customer_id = f"proof_{uuid.uuid4().hex[:10]}"
    with MockUpstream(mock_port), httpx.Client(timeout=15.0) as client:
        wait_for("API readiness", lambda: client.get(f"{api}/ready"), lambda r: r.status_code == 200)
        wait_for("Gateway readiness", lambda: client.get(f"{gateway}/health"), lambda r: r.status_code == 200)
        wait_for("ClickHouse readiness", lambda: client.get(f"{clickhouse}/ping"), lambda r: r.text.strip() == "Ok.")

        budget = api_json(
            client,
            "POST",
            f"{api}/budget/{customer_id}",
            json={"balance_usd": 0.001, "alert_threshold_usd": 0.0001},
        )
        assert budget["held_usd"] == 0

        with client.stream(
            "POST",
            f"{gateway}/v1/chat/completions",
            headers={
                "Authorization": "Bearer sk-proof",
                "X-FluxMeter-Customer-Id": customer_id,
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4o-mini",
                "stream": True,
                "max_tokens": 1,
                "messages": [{"role": "user", "content": "prove the billing path"}],
            },
        ) as response:
            response.raise_for_status()
            reservation_id = response.headers.get("X-FluxMeter-Reservation-Id")
            reserved_usd = float(response.headers.get("X-FluxMeter-Reserved-Usd", "0"))
            assert reservation_id and reserved_usd > 0
            held = api_json(client, "GET", f"{api}/budget/{customer_id}")
            assert held["held_usd"] > 0, held
            print(f"1 RESERVE  id={reservation_id} held=${held['held_usd']:.6f}")
            error = parse_sse_error(list(response.iter_lines()))

        detail = error.get("fluxmeter") or {}
        assert detail.get("output_tokens", 0) > 0
        assert detail.get("metered_usd", 0) > detail.get("reserved_usd", 0)
        print(
            "2 METER    "
            f"output_tokens={detail['output_tokens']} metered=${detail['metered_usd']:.6f} "
            f"> reserved=${detail['reserved_usd']:.6f}"
        )
        assert error.get("code") == "stream_killed"
        print("3 KILL     Gateway emitted stream_killed and closed the provider stream")

        # Advance event time deterministically so the 10-second Flink window closes.
        api_json(
            client,
            "POST",
            f"{api}/ingest",
            json={
                "eventId": f"watermark-{uuid.uuid4()}",
                "customerId": "_proof_watermark",
                "modelId": "gpt-4o-mini",
                "inputTokens": 1,
                "outputTokens": 0,
                "timestamp": int(time.time() * 1000) + 20_000,
            },
        )
        settled = wait_for(
            "Flink budget settlement",
            lambda: api_json(client, "GET", f"{api}/budget/{customer_id}"),
            lambda row: row["held_usd"] == 0 and row["total_spent_usd"] > 0,
            timeout=35.0,
        )
        print(
            "  SETTLE   "
            f"held=${settled['held_usd']:.6f} spent=${settled['total_spent_usd']:.6f} "
            f"balance=${settled['balance_usd']:.6f}"
        )

        audit = wait_for(
            "ClickHouse raw audit row",
            lambda: clickhouse_row(client, clickhouse, customer_id),
            lambda row: bool(row and row.get("event_id")),
            timeout=45.0,
        )
        metadata = json.loads(audit["metadata"])
        assert metadata.get("_stream_killed") == "true", audit
        assert audit["output_tokens"] == detail["output_tokens"], (audit, detail)
        print(
            "4 AUDIT    "
            f"event={audit['event_id']} raw_tokens={audit['output_tokens']} "
            "metadata._stream_killed=true"
        )
        print("\nPASS reserve → meter → kill → settle → audit used the live FluxMeter path")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://127.0.0.1:8000")
    parser.add_argument("--gateway", default="http://127.0.0.1:8080")
    parser.add_argument("--clickhouse", default="http://127.0.0.1:8123")
    parser.add_argument("--mock-port", type=int, default=18080)
    args = parser.parse_args()
    run(
        args.api.rstrip("/"),
        args.gateway.rstrip("/"),
        args.clickhouse.rstrip("/"),
        args.mock_port,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
