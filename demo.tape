Output "./demo.gif"

Set Shell "bash"
Set FontSize 14
Set Width 1200
Set Height 720
Set Theme "Catppuccin Mocha"
Set Padding 20
Set TypingSpeed 20ms

Type "# FluxMeter v4.0.0 — one HTTP ingestion path"
Enter
Type "# HTTP → Kafka → Flink → Redis (make demo)"
Enter
Sleep 1.2s

Type ""
Enter
Type "# 1. Prepaid balance"
Enter
Sleep 300ms

Type `curl -s -X POST localhost:8000/budget/cust_demo -H 'Content-Type: application/json' -d '{"balance_usd": 10.0, "alert_threshold_usd": 1.0}' | python3 -m json.tool`
Enter
Sleep 2s

Type ""
Enter
Type "# 2. Pre-request check (<10ms, before LLM call)"
Enter
Sleep 300ms

Type `curl -s 'localhost:8000/budget/cust_demo/check?estimated_cost_usd=0.05' | python3 -m json.tool`
Enter
Sleep 2s

Type ""
Enter
Type "# 3. Ingest usage (session + agent span tags for billing queries)"
Enter
Sleep 300ms

Type `curl -s -X POST localhost:8000/ingest -H 'Content-Type: application/json' -d '{"eventId":"evt-demo-1","customerId":"cust_demo","modelId":"gpt-4o","inputTokens":1200,"outputTokens":450,"sessionId":"sess_demo","parentSpanId":"span_agent_demo"}' | python3 -m json.tool`
Enter
Sleep 2s

Type `curl -s -X POST localhost:8000/ingest -H 'Content-Type: application/json' -d '{"eventId":"evt-demo-2","customerId":"cust_demo","modelId":"claude-sonnet-4","inputTokens":800,"outputTokens":320,"reasoningTokens":150,"sessionId":"sess_demo","parentSpanId":"span_agent_demo"}' | python3 -m json.tool`
Enter
Sleep 2s

Type ""
Enter
Type "# 4. Customer + session billing"
Enter
Sleep 300ms

Type `sleep 15 && echo projection_ready`
Enter
Sleep 1s

Type `curl -s localhost:8000/usage/customer/cust_demo | python3 -m json.tool`
Enter
Sleep 2s

Type `curl -s localhost:8000/usage/session/sess_demo | python3 -m json.tool`
Enter
Sleep 2s

Type ""
Enter
Type "# 5. Flink calendar day/month projections"
Enter
Sleep 300ms

Type `curl -s localhost:8000/usage/customer/cust_demo/period/$(date -u +%Y-%m) | python3 -m json.tool`
Enter
Sleep 2s

Type `curl -s localhost:8000/usage/customer/cust_demo/day/$(date -u +%Y-%m-%d) | python3 -m json.tool`
Enter
Sleep 2s

Type ""
Enter
Type "# 6. Agent task cost (after the 60s inactivity window)"
Enter
Sleep 300ms

Type `sleep 50 && curl -s localhost:8000/usage/span/span_agent_demo | python3 -m json.tool`
Enter
Sleep 2s

Type ""
Enter
Type "# 7. Balance after deduction"
Enter
Sleep 300ms

Type `curl -s localhost:8000/budget/cust_demo | python3 -m json.tool`
Enter
Sleep 2s

Type ""
Enter
Type "# github.com/10kshuaizhang/fluxmeter · make demo"
Enter
Sleep 2.5s
