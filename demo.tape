Output "./demo.gif"

Set Shell "bash"
Set FontSize 14
Set Width 1200
Set Height 700
Set Theme "Catppuccin Mocha"
Set Padding 20
Set TypingSpeed 25ms

Type "# FluxMeter v2.2.1 — real-time AI token metering + budget enforcement"
Enter
Type "# Lite mode: Redis + API (make demo) — no Kafka/Flink required"
Enter
Sleep 1.5s

Type ""
Enter
Type "# 1. Set prepaid balance ($10)"
Enter
Sleep 400ms

Type `curl -s -X POST localhost:8000/budget/cust_demo -H 'Content-Type: application/json' -d '{"balance_usd": 10.0, "alert_threshold_usd": 1.0}' | python3 -m json.tool`
Enter
Sleep 2.5s

Type ""
Enter
Type "# 2. Pre-request budget check (<10ms, before LLM call)"
Enter
Sleep 400ms

Type `curl -s 'localhost:8000/budget/cust_demo/check?estimated_cost_usd=0.05' | python3 -m json.tool`
Enter
Sleep 2.5s

Type ""
Enter
Type "# 3. Ingest token usage — atomic aggregation + inline cost/balance"
Enter
Sleep 400ms

Type `curl -s -X POST localhost:8000/ingest -H 'Content-Type: application/json' -d '{"customerId":"cust_demo","modelId":"gpt-4o","inputTokens":1200,"outputTokens":450}' | python3 -m json.tool`
Enter
Sleep 2.5s

Type ""
Enter
Type `curl -s -X POST localhost:8000/ingest -H 'Content-Type: application/json' -d '{"customerId":"cust_demo","modelId":"claude-sonnet-4","inputTokens":800,"outputTokens":320,"reasoningTokens":150}' | python3 -m json.tool`
Enter
Sleep 2.5s

Type ""
Enter
Type "# 4. Query aggregated usage"
Enter
Sleep 400ms

Type `curl -s localhost:8000/usage/global | python3 -m json.tool`
Enter
Sleep 2.5s

Type ""
Enter
Type `curl -s localhost:8000/usage/customer/cust_demo | python3 -m json.tool`
Enter
Sleep 2.5s

Type ""
Enter
Type "# 5. Remaining balance after deduction"
Enter
Sleep 400ms

Type `curl -s localhost:8000/budget/cust_demo | python3 -m json.tool`
Enter
Sleep 2.5s

Type ""
Enter
Type "# Full stack: 1M+ events/sec via Kafka + Flink (make demo-full)"
Enter
Type "# github.com/10kshuaizhang/fluxmeter"
Enter
Sleep 3s
