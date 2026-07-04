#!/usr/bin/env bash
# ponytail: one-shot repo metadata — run after `gh auth login`
set -euo pipefail

repo="${1:-10kshuaizhang/fluxmeter}"

topics=(
  llm-billing
  token-metering
  ai-agents
  llm
  billing
  budget-enforcement
  openai
  self-hosted
  kafka
  flink
  metering
  stream-processing
  ai-infrastructure
)

args=()
for t in "${topics[@]}"; do
  args+=(--add-topic "$t")
done

gh repo edit "$repo" "${args[@]}"
echo "Topics set on https://github.com/$repo"
