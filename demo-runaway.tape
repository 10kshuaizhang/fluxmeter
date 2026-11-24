Output "./demo-runaway.gif"
Set Shell "bash"
Set FontSize 14
Set Width 1200
Set Height 720
Set Theme "Catppuccin Mocha"
Set Padding 20
Set TypingSpeed 20ms

# Prerequisite: make demo (warm stack). Recording is the runaway A/B, no provider key.
Type "# FluxMeter — runaway agent vs Gateway budget stop, no provider key"
Enter
Sleep 1s
Type "make demo-runaway && echo RUNAWAY_VERIFIED"
Enter
# Wait for the command's real assertions, not a fixed projection-ready delay.
Wait+Screen@10m /(?m)^RUNAWAY_VERIFIED\r?$/
Sleep 3s
Type "# A kept spending. B stopped with 402 budget_exceeded."
Enter
Sleep 2s
