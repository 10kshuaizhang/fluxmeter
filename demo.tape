Output "./demo.gif"

Set Shell "bash"
Set FontSize 14
Set Width 1200
Set Height 720
Set Theme "Catppuccin Mocha"
Set Padding 20
Set TypingSpeed 20ms

Type "# FluxMeter v2.6.1 — real-time AI token metering + budget enforcement"
Enter
Type "# Lite: Redis + API (make demo) · Full: Kafka + Flink @ 1M+ eps"
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

Type `curl -s -X POST localhost:8000/ingest -H 'Content-Type: application/json' -d '{"customerId":"cust_demo","modelId":"gpt-4o","inputTokens":1200,"outputTokens":450,"sessionId":"sess_demo","parentSpanId":"span_agent_demo"}' | python3 -m json.tool`
Enter
Sleep 2s

Type `curl -s -X POST localhost:8000/ingest -H 'Content-Type: application/json' -d '{"customerId":"cust_demo","modelId":"claude-sonnet-4","inputTokens":800,"outputTokens":320,"reasoningTokens":150,"sessionId":"sess_demo","parentSpanId":"span_agent_demo"}' | python3 -m json.tool`
Enter
Sleep 2s

Type ""
Enter
Type "# 4. Customer + session billing"
Enter
Sleep 300ms

Type `curl -s localhost:8000/usage/customer/cust_demo | python3 -m json.tool`
Enter
Sleep 2s

Type `curl -s localhost:8000/usage/session/sess_demo | python3 -m json.tool`
Enter
Sleep 2s

Type ""
Enter
Type "# 5. Compact into calendar day/month buckets (lite rollup)"
Enter
Sleep 300ms

Type `python3 -c "import sys,time; sys.path.insert(0,'api'); import redis; from rollup_worker import rollup_customer_minute; rollup_customer_minute(redis.Redis(host='localhost',port=6379,decode_responses=True),'cust_demo',int(time.time()))" && echo rollup_ok`
Enter
Sleep 1.5s

Type `curl -s localhost:8000/usage/customer/cust_demo/period/$(date -u +%Y-%m) | python3 -m json.tool`
Enter
Sleep 2s

Type `curl -s localhost:8000/usage/customer/cust_demo/day/$(date -u +%Y-%m-%d) | python3 -m json.tool`
Enter
Sleep 2s

Type ""
Enter
Type "# 6. Agent task cost (parentSpanId · Full mode Flink; seeded here for demo)"
Enter
Sleep 300ms

Type `python3 -c "import redis; r=redis.Redis(host='localhost',port=6379,decode_responses=True); k='span:span_agent_demo'; r.mset({k+':cost_usd':'0.18',k+':total_tokens':'4200',k+':call_count':'3',k+':duration_ms':'2100',k+':customer_id':'cust_demo'}); print('span_ready')" && curl -s localhost:8000/usage/span/span_agent_demo | python3 -m json.tool`
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
