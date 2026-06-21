Output "./demo.gif"

Set Shell "bash"
Set FontSize 14
Set Width 1200
Set Height 650
Set Theme "Catppuccin Mocha"
Set Padding 20
Set TypingSpeed 30ms

Type "# FluxMeter — streaming metering for AI token billing"
Enter
Type "# Infrastructure running: Kafka + Flink + Redis + API"
Enter
Sleep 1.5s

Type ""
Enter
Type "# Generate token events (OpenAI, Anthropic, Google models)"
Enter
Sleep 500ms

Type `KAFKA_BROKERS=localhost:9094 NUM_CUSTOMERS=1000 NUM_THREADS=2 TARGET_EPS=5000 timeout 25 java -cp build/libs/fluxmeter-0.4.0.jar io.fluxmeter.generator.LoadGenerator 2>&1 | grep STATS`
Enter
Sleep 28s

Type ""
Enter
Type "# Query real-time usage via API:"
Enter
Sleep 500ms

Type `curl -s localhost:8000/usage/global | python3 -m json.tool`
Enter
Sleep 2.5s

Type ""
Enter
Type `curl -s localhost:8000/usage/customer/cust_42 | python3 -m json.tool`
Enter
Sleep 2.5s

Type ""
Enter
Type "# Check budget enforcement:"
Enter
Sleep 500ms

Type `curl -s localhost:8000/budget/cust_42 | python3 -m json.tool`
Enter
Sleep 2.5s

Type ""
Enter
Type "# Balance deducted in real time. Alerts emitted to Kafka on exhaustion."
Enter
Type "# github.com/YOUR_USER/fluxmeter"
Enter
Sleep 3s
