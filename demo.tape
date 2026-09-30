Output "./demo.gif"
Set Shell "bash"
Set FontSize 14
Set Width 1200
Set Height 720
Set Theme "Catppuccin Mocha"
Set Padding 20
Set TypingSpeed 20ms

# Prerequisite: make demo. The recording repeats the real HTTP proof with new IDs.
Type "# FluxMeter — HTTP custody to verified customer usage, no provider key"
Enter
Sleep 1s
Type "make demo-verify && echo DEMO_VERIFIED"
Enter
# Wait for the command's real assertions, not a fixed projection-ready delay.
Wait+Screen@10m /(?m)^DEMO_VERIFIED\r?$/
Sleep 3s
Type "# 202 is custody. PASS means customer tokens and cost matched."
Enter
Sleep 2s
Type "# Optional reserve / kill / settle / audit: make demo-proof"
Enter
Sleep 2s
