"""Exercise the trial over a real HTTP socket without claiming a Flink E2E run."""

import importlib.util
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("quickstart", Path(__file__).resolve().parents[1] / "demos/quickstart.py")
quickstart = importlib.util.module_from_spec(spec)
spec.loader.exec_module(quickstart)


@pytest.fixture
def trial_server():
    state = {"posts": [], "reads": 0, "ready": 0, "cost": 0.00042,
             "status": "accepted", "fail_first": False}

    class Handler(BaseHTTPRequestHandler):
        def respond(self, code, body):
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def do_GET(self):
            if self.path == "/ready":
                state["ready"] += 1
                if state["ready"] == 1:
                    self.send_response(503)
                    self.end_headers()  # startup proxies may return no JSON
                    return
                return self.respond(200, {"status": "ready"})
            state["reads"] += 1
            if state["reads"] == 1:
                return self.respond(404, {"detail": "not projected"})
            return self.respond(200, {"customer_id": state["posts"][-1]["customerId"],
                                     "event_count": 1, "input_tokens": 1200,
                                     "output_tokens": 400, "total_tokens": 1600,
                                     "cost_usd": state["cost"]})

        def do_POST(self):
            event = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            state["posts"].append(event)
            if state["fail_first"] and len(state["posts"]) == 1:
                return self.respond(503, {"detail": {"code": "custody_uncertain"}})
            return self.respond(202, {"eventId": event["eventId"], "status": state["status"]})

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_trial_waits_for_readiness_and_projection_and_is_repeatable(trial_server):
    url, state = trial_server
    state["fail_first"] = True
    first = quickstart.run(url, timeout=5)
    second = quickstart.run(url, timeout=5)
    assert first["customer_id"] != second["customer_id"]
    assert state["posts"][0] == state["posts"][1]  # retry identity/payload unchanged
    assert state["posts"][1]["eventId"] != state["posts"][2]["eventId"]
    assert all("timestamp" not in event for event in state["posts"])


def test_trial_does_not_treat_202_as_settlement(trial_server):
    url, state = trial_server
    state["ready"] = 1
    state["cost"] = 0.00043
    with pytest.raises(RuntimeError, match="Customer usage timed out"):
        quickstart.run(url, timeout=0.01)


def test_trial_rejects_quarantined_receipt(trial_server):
    url, state = trial_server
    state["ready"] = 1
    state["status"] = "quarantined"
    with pytest.raises(RuntimeError, match="not quarantine"):
        quickstart.run(url, timeout=1)


def test_loopback_does_not_use_system_proxy(trial_server, monkeypatch):
    url, state = trial_server
    state["ready"] = 1
    monkeypatch.setattr("urllib.request.getproxies", lambda: {"http": "http://127.0.0.1:1"})
    monkeypatch.setattr("urllib.request.proxy_bypass", lambda host: False)
    assert quickstart.request(url, "/ready") == (200, {"status": "ready"})
