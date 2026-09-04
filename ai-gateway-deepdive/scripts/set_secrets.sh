#!/usr/bin/env bash
# Mint a workspace PAT and store it in the demo secret scope. The AI Gateway
# endpoint's databricks-model-serving external entities use this token to reach
# the internal Foundation Model endpoints. No external provider API keys are
# used in this demo.
#
# Usage (run via the session's ! prefix so the token never enters chat/logs):
#   ! bash ai-gateway-deepdive/scripts/set_secrets.sh ai-gateway-deepdive
#
# The token is captured into a shell variable and piped via stdin into the
# secret; it never appears in the process argument list. It is unset on exit.
set -euo pipefail

PROFILE="${1:?pass the databricks CLI profile name as arg 1}"
SCOPE="ai_gateway_demo"
KEY="workspace_pat"
LIFETIME_SECONDS=2592000  # 30 days, matches the workspace TTL

TOKEN="$(databricks --profile "$PROFILE" tokens create \
  --lifetime-seconds "$LIFETIME_SECONDS" \
  --comment "ai-gateway-deepdive gateway->FM" \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["token_value"])')"

databricks --profile "$PROFILE" secrets create-scope "$SCOPE" 2>/dev/null \
  || echo "scope $SCOPE already exists (ok)"

printf '%s' "$TOKEN" \
  | databricks --profile "$PROFILE" secrets put-secret "$SCOPE" "$KEY"

unset TOKEN
echo "minted PAT and stored it as $SCOPE/$KEY"
