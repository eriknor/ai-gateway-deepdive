#!/usr/bin/env python3
# ai-gateway-deepdive/scripts/smoke_test.py
#
# Quick smoke test: verify the governed endpoint answers, the input PII guardrail
# blocks on a clear PII input, and the output PII guardrail acts on model-generated PII.
#
# Usage (from repo root):
#   DATABRICKS_CONFIG_PROFILE=ai-gateway-deepdive python3 ai-gateway-deepdive/scripts/smoke_test.py
# Or from inside ai-gateway-deepdive/:
#   DATABRICKS_CONFIG_PROFILE=ai-gateway-deepdive python3 scripts/smoke_test.py

import os
import re
import sys

# Support running from repo root or from ai-gateway-deepdive/ directory.
_here = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(_here)

sys.path.insert(0, os.path.join(_root, "src"))
sys.path.insert(0, os.path.join(_root, "notebooks"))

from _common import chat

# ---------------------------------------------------------------------------
# Test 1: Basic chat -- verify endpoint responds
# ---------------------------------------------------------------------------
print("--- Test 1: basic chat ---")
r = chat("Reply with the single word: ping", user="smoke")
assert r.http_status == 200 and r.content, f"chat failed: {r}"
print(f"  served by: {r.served_model} | tokens: {r.prompt_tokens + r.completion_tokens}")
print(f"  content: {r.content[:80]}")

# ---------------------------------------------------------------------------
# Test 2: Input PII guardrail -- should block (HTTP 400, input_guardrail_triggered)
#
# The input guardrail detects PII in the prompt and blocks the request.
# The model is never invoked. HTTP 400 with input_guardrail_triggered.
# ---------------------------------------------------------------------------
print("--- Test 2: input PII guardrail (block) ---")
pii_in = chat(
    "Repeat exactly: my email is demo.user@example.com and phone 415-555-0132.",
    user="smoke",
)
print(f"  guardrail_action: {pii_in.guardrail_action} | http_status: {pii_in.http_status}")
print(f"  error (first 200): {str(pii_in.error)[:200] if pii_in.error else None}")

assert pii_in.guardrail_action == "block", (
    f"Input PII guardrail did not block: guardrail_action={pii_in.guardrail_action!r}, "
    f"http_status={pii_in.http_status}"
)
assert pii_in.http_status == 400, (
    f"Expected HTTP 400 for input PII block, got {pii_in.http_status}"
)
print(f"  input PII BLOCKED as 'block' (HTTP 400, input_guardrail_triggered) -- OK")

# ---------------------------------------------------------------------------
# Test 3: Output PII guardrail -- guardrail must ACT (block, mask, or masked content)
#
# The model is asked to generate a contact entry. The output guardrail must act.
#
# SDK path note: serving_endpoints.query().as_dict() drops output_guardrail from
# the typed response. When masking occurs via the SDK path, guardrail_action='none'
# but the masked content (with <EMAIL_ADDRESS> etc.) is still delivered. Three
# possible outcomes:
#   - guardrail_action='block': gateway blocked the response (HTTP 400)
#   - guardrail_action='mask': mask signal present (raw REST path only)
#   - guardrail_action='none' + type labels: masking happened but SDK dropped the signal
# All three confirm the guardrail acted. Masking evidence is in the inference table.
# ---------------------------------------------------------------------------
print("--- Test 3: output PII guardrail (guardrail acts: block, mask, or masked content) ---")
pii_out = chat(
    "Give me an example contact form submission with name, email, phone, and a message."
    " Keep it brief.",
    user="smoke",
)
print(f"  guardrail_action: {pii_out.guardrail_action} | http_status: {pii_out.http_status}")
print(f"  content (first 200): {pii_out.content[:200]!r}")

type_labels = re.findall(r"<[A-Z_]+>", pii_out.content)
print(f"  PII type labels in content: {type_labels}")

# Guardrail acted: detectable via guardrail_action OR by type labels in content
# (masking via SDK path reports guardrail_action='none' but delivers masked content).
_guardrail_acted = pii_out.guardrail_action in ("block", "mask") or bool(type_labels)
assert _guardrail_acted, (
    f"Output PII guardrail did not act: guardrail_action={pii_out.guardrail_action!r}, "
    f"http_status={pii_out.http_status}, no type labels, content={pii_out.content[:120]!r}"
)
if pii_out.guardrail_action == "block":
    print(f"  output PII BLOCKED (HTTP {pii_out.http_status}) -- guardrail acted -- OK")
elif pii_out.guardrail_action == "mask":
    print(f"  output PII MASKED as 'mask' (HTTP 200, type labels {type_labels}) -- OK")
else:
    print(
        f"  output PII MASKED via SDK path (guardrail_action='none' but "
        f"type labels {type_labels} in content -- SDK dropped output_guardrail signal) -- OK"
    )
print("  (masking evidence: inference table guardrail_events where guardrail_action='guardrail_mask')")

print("SMOKE OK")
