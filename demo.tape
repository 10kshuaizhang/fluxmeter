Output "./demo.gif"

Set Shell "bash"
Set FontSize 14
Set Width 1200
Set Height 720
Set Theme "Catppuccin Mocha"
Set Padding 20
Set TypingSpeed 20ms

# Prerequisite: make demo (HTTP → Kafka → Flink → Redis)

Type "# FluxMeter v4.0.0 — one HTTP ingestion path"
Enter
Type "# make demo · make demo-run · docs/intelligence-api.md"
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
Type "# 3. Ingest (session + agent span tags)"
Enter
Sleep 300ms

Type `curl -s -X POST localhost:8000/ingest -H 'Content-Type: application/json' -d '{"customerId":"cust_demo","modelId":"gpt-4o","inputTokens":1200,"outputTokens":450,"sessionId":"sess_demo","parentSpanId":"span_agent_demo","eventId":"evt-1"}' | python3 -m json.tool`
Enter
Sleep 2s

Type `curl -s -X POST localhost:8000/ingest -H 'Content-Type: application/json' -d '{"customerId":"cust_demo","modelId":"claude-sonnet-4","inputTokens":800,"outputTokens":320,"reasoningTokens":150,"sessionId":"sess_demo","parentSpanId":"span_agent_demo","eventId":"evt-2"}' | python3 -m json.tool`
Enter
Sleep 2s

Type ""
Enter
Type "# 4. Wait for Flink's eventually consistent projection"
Enter
Sleep 300ms

Type `sleep 15 && echo projection_ready`
Enter
Sleep 1.5s

Type `curl -s localhost:8000/usage/customer/cust_demo | python3 -m json.tool`
Enter
Sleep 2s

Type ""
Enter
Type "# 5. Session + agent span (from ingest, no manual Redis seed)"
Enter
Sleep 300ms

Type `curl -s localhost:8000/usage/session/sess_demo | python3 -m json.tool`
Enter
Sleep 2s

Type `curl -s localhost:8000/usage/span/span_agent_demo | python3 -m json.tool`
Enter
Sleep 2s

Type ""
Enter
Type "# 6. Calendar day/month buckets"
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
Type "# 7. Intelligence — seed rollups + root cause + forecast"
Enter
Sleep 300ms

Type `python3 -c "import sys; sys.path.insert(0,'api'); sys.path.insert(0,'demos'); import redis; from full_demo import _seed_intelligence_data; r=redis.Redis(host='localhost',port=6379,decode_responses=True); p=__import__('datetime').datetime.now(__import__('datetime').timezone.utc).strftime('%Y-%m'); y,m=int(p[:4]),int(p[5:7]); b=f'{y}-{m-1:02d}' if m>1 else f'{y-1}-12'; _seed_intelligence_data(r,'cust_demo',p,b); r.set('budget:cust_demo:initial_balance_usd','200'); print('intel_seed_ok')"`
Enter
Sleep 1.5s

Type `curl -s "localhost:8000/intelligence/root-cause?period=$(date -u +%Y-%m)&baseline_period=$(date -u -v-1m +%Y-%m 2>/dev/null || date -u -d 'last month' +%Y-%m)&scope=customer:cust_demo" | python3 -m json.tool | head -24`
Enter
Sleep 2.5s

Type `curl -s "localhost:8000/intelligence/forecast?period=$(date -u +%Y-%m)&scope=customer:cust_demo" | python3 -m json.tool`
Enter
Sleep 2s

Type ""
Enter
Type "# 8. Balance after deduction"
Enter
Sleep 300ms

Type `curl -s localhost:8000/budget/cust_demo | python3 -m json.tool`
Enter
Sleep 2s

Type ""
Enter
Type "# 9. Gateway — 402 when budget exhausted"
Enter
Sleep 300ms

Type `curl -s -X POST localhost:8000/budget/cust_exhausted -H 'Content-Type: application/json' -d '{"balance_usd":0}' > /dev/null && curl -s -X POST localhost:8080/v1/chat/completions -H 'X-FluxMeter-Customer-Id: cust_exhausted' -H 'Authorization: Bearer sk-demo' -H 'Content-Type: application/json' -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}' -w '\nHTTP %{http_code}\n' | tail -8`
Enter
Sleep 2s

Type ""
Enter
Type "# github.com/10kshuaizhang/fluxmeter · make demo-run-live"
Enter
Sleep 2.5s
