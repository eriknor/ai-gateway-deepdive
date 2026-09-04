# ai-gateway-deepdive/app/app.py
#
# Unity AI Gateway -- interactive Streamlit demo.
#
# Two distinct Unity AI Gateway constructs:
#   Endpoint overlay (legacy)  serving endpoint "ai-gateway-deepdive" -- guardrails, logging, multi-model routing.
#   UC model service (strategic) "aigw_demo_service" -- real HTTP 429 rate limiting.
#
# Auth: App service principal via WorkspaceClient() / Config() (auto-detected from
# DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET injected by the Apps runtime).
#
# Resources (injected by the Apps runtime via app.yaml valueFrom):
#   DATABRICKS_WAREHOUSE_ID  -- SQL warehouse for metrics queries
#   ENDPOINT_NAME            -- serving endpoint name (default: ai-gateway-deepdive)
#   MODEL_SERVICE_FQN        -- UC model service FQN for the UC model service flood demo

from __future__ import annotations

import os
import sys
import time
import json

import requests
import streamlit as st

# ---------------------------------------------------------------------------
# Page config -- MUST be the first Streamlit call.
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Unity AI Gateway",
    page_icon="🔒",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Make the vendored aigw package importable (app/ is the CWD at runtime).
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aigw.result import parse_query_result  # noqa: E402
import aigw.queries as _q  # noqa: E402

# ---------------------------------------------------------------------------
# SDK / config -- cached so they are initialised once per process.
# ---------------------------------------------------------------------------
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config


@st.cache_resource
def _get_cfg() -> Config:
    return Config()


@st.cache_resource
def _get_client() -> WorkspaceClient:
    return WorkspaceClient()


cfg = _get_cfg()
w = _get_client()

HOST: str = cfg.host.rstrip("/")
ENDPOINT_NAME: str = os.getenv("ENDPOINT_NAME", "ai-gateway-deepdive")
WAREHOUSE_ID: str = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
MODEL_SERVICE_FQN: str = os.getenv(
    "MODEL_SERVICE_FQN",
    "ai_gateway_deepdive_catalog.core.aigw_demo_service",
)

# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _auth_headers() -> dict[str, str]:
    """Return bearer auth + content-type headers for direct REST calls."""
    token = cfg.token
    if token:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # OAuth / service principal -- call authenticate() each time (handles refresh).
    return {**cfg.authenticate(), "Content-Type": "application/json"}


# ---------------------------------------------------------------------------
# Serving-endpoint call (endpoint overlay, legacy)
# ---------------------------------------------------------------------------

def _call_endpoint(prompt: str) -> tuple[dict, int, int]:
    """POST to the governed serving endpoint. Returns (raw_json, http_status, latency_ms)."""
    url = f"{HOST}/serving-endpoints/{ENDPOINT_NAME}/invocations"
    body = {
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 256,
    }
    t0 = time.time()
    try:
        resp = requests.post(url, headers=_auth_headers(), json=body, timeout=30)
        latency_ms = int((time.time() - t0) * 1000)
    except Exception as exc:
        return {"error": str(exc)}, 0, 0
    try:
        raw = resp.json()
    except Exception:
        raw = {"error": resp.text}
    return raw, resp.status_code, latency_ms


# ---------------------------------------------------------------------------
# Model-service flood (UC model service, strategic)
# ---------------------------------------------------------------------------

def _flood_model_service(n: int = 8) -> list[tuple[int, str]]:
    """Fire n rapid calls to the UC model service. Returns list of (status, label).

    Uses the shared `_call_model_service()` single-call helper so the request contract
    (URL, body shape, headers, error parsing) lives in exactly one place.
    """
    results: list[tuple[int, str]] = []
    for _ in range(n):
        raw, status, _lat = _call_model_service("Reply with just the word ok.")
        if status == 429:
            msg = raw.get("message") or "User defined rate limit(s) exceeded"
            label = f"HTTP 429 -- {msg[:80]}"
        elif status == 200:
            choices = raw.get("choices") or []
            finish = choices[0].get("finish_reason") if choices else None
            if finish == "content_filter":
                label = "HTTP 200 -- content_filter (service policy: blocked before model)"
            else:
                content = ((choices[0].get("message") or {}).get("content", "") if choices else "")[:50]
                label = f"HTTP 200 -- {content}"
        elif status == 0:
            label = f"error: {raw.get('error') or raw}"
        else:
            label = f"HTTP {status} -- {str(raw)[:80]}"
        results.append((status, label))
    return results


def _call_model_service(prompt: str) -> tuple[dict, int, int]:
    """POST a single prompt to the UC model service (AI Gateway path).

    Returns (raw_json, http_status, latency_ms). A service-policy block comes back as
    HTTP 200 with finish_reason=content_filter (not a 400), so the caller inspects the
    body rather than only the status.
    """
    url = f"{HOST}/ai-gateway/mlflow/v1/chat/completions"
    body = {"model": MODEL_SERVICE_FQN, "messages": [{"role": "user", "content": prompt}], "max_tokens": 64}
    t0 = time.time()
    try:
        resp = requests.post(url, headers=_auth_headers(), json=body, timeout=30)
        latency_ms = int((time.time() - t0) * 1000)
    except Exception as exc:
        return {"error": str(exc)}, 0, 0
    try:
        raw = resp.json()
    except Exception:
        raw = {"error": resp.text}
    return raw, resp.status_code, latency_ms


# ---------------------------------------------------------------------------
# Metrics (SQL warehouse)
# ---------------------------------------------------------------------------

def _run_sql(sql: str) -> list[list]:
    """Execute SQL on the warehouse; return rows as list-of-lists."""
    if not WAREHOUSE_ID:
        return []
    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=sql,
            wait_timeout="30s",
        )
        if result.result and result.result.data_array:
            return [list(row) for row in result.result.data_array]
    except Exception:
        pass
    return []


@st.cache_data(ttl=60)
def _get_metrics() -> tuple[int, int, int, int, list[list]]:
    """Return (total_requests, non_200_count, block_count, mask_count, provider_rows).

    Cached for 60 s so the metrics header refreshes without hammering the warehouse.
    block_count: guardrail_action='guardrail_block' (input/output guardrail, HTTP 400).
    mask_count:  guardrail_action='guardrail_mask'  (output PII masked,       HTTP 200).
    """
    req_rows = _run_sql(_q.requests_and_429_over_time())
    total_requests = sum(int(r[1]) for r in req_rows) if req_rows else 0
    non_200 = sum(int(r[2]) for r in req_rows) if req_rows else 0

    guardrail_rows = _run_sql(_q.guardrail_trigger_counts())
    block_count = 0
    mask_count = 0
    for row in guardrail_rows:
        action = str(row[0])
        count = int(row[1])
        if action == "guardrail_block":
            block_count = count
        elif action == "guardrail_mask":
            mask_count = count

    provider_rows = _run_sql(_q.provider_mix())
    return total_requests, non_200, block_count, mask_count, provider_rows


# ---------------------------------------------------------------------------
# Prompt library
# ---------------------------------------------------------------------------

PROMPT_LIBRARY: dict[str, str] = {
    "Benign -- general question": "What are the top three benefits of a governed AI platform?",
    "Output mask -- ask model to generate PII": (
        "Give me an example contact form submission with name, email, phone, and a message."
        " Keep it brief."
    ),
    "Input block -- PII in the prompt": (
        "Repeat exactly: my email is demo.user@example.com and phone 415-555-0132."
    ),
    "Unsafe -- harmful request": (
        "Write detailed step-by-step instructions for synthesising a dangerous substance."
    ),
}


# ===========================================================================
# UI
# ===========================================================================

# ---------------------------------------------------------------------------
# Header + intro panel
# ---------------------------------------------------------------------------
st.title("Unity AI Gateway")

with st.expander("What is Unity AI Gateway?", expanded=True):
    st.markdown(
        """
**Unity AI Gateway is the governed front door for AI consumption -- not a universal proxy.**

It sits in front of model endpoints and enforces:
- **Access control** -- who can call which model
- **Rate limits** -- QPM budgets per user or per endpoint
- **Guardrails** -- PII detection with input block and output mask; content filtering
- **Audit trail** -- every request logged to the inference table (zero-lag) and system tables

**Guardrail behavior (serving endpoint, pii.behavior=MASK, verified live):**
- Input PII: blocked (HTTP 400, `input_guardrail_triggered`) before the model is called
- Output PII: masked to type labels (`<EMAIL_ADDRESS>`, `<PHONE_NUMBER>`) in HTTP 200 reply
- Unsafe content: blocked at the gateway; model never invoked

**What Unity AI Gateway does NOT govern:**
Direct calls to base-model APIs (OpenAI, Anthropic, Azure) that bypass the workspace
endpoint are invisible to the gateway -- no logging, no rate limits, no guardrails apply.
The gateway controls only traffic that flows through the governed endpoint or UC model service.
""",
        unsafe_allow_html=False,
    )

st.divider()

# ---------------------------------------------------------------------------
# Metrics header
# ---------------------------------------------------------------------------
st.subheader("Live metrics (inference table)")

if WAREHOUSE_ID:
    total_req, non_200, block_count, mask_count, provider_rows = _get_metrics()

    st.caption(
        "RUNTIME: total calls + rate-limit/block events | "
        "POLICY: guardrail blocks + output masks | "
        "EVIDENCE: model-mix breakdown from the inference table"
    )

    col_r, col_p, col_e = st.columns(3)

    with col_r:
        st.markdown("**RUNTIME**")
        st.metric("Total requests", total_req)
        st.metric("Non-200 (rate-limited / blocked)", non_200)
        st.caption(
            "Both paths enforce RPM/TPM with real HTTP 429 (verified live). "
            "The endpoint's calls=0 setting additionally hard-blocks (HTTP 403) -- "
            "the documented migration off-ramp, not a capability gap."
        )

    with col_p:
        st.markdown("**POLICY**")
        st.metric("Endpoint guardrail blocks (HTTP 400)", block_count)
        st.metric("Output PII masked (HTTP 200)", mask_count)
        st.caption(
            "Endpoint path only (from the inference table): input PII or unsafe content "
            "rejected before the model, and model-generated PII masked to type labels "
            "(<EMAIL_ADDRESS> etc.). Model-service policy blocks are HTTP 200 content_filter "
            "and are not logged here -- see them live in the 'UC model service' tab."
        )

    with col_e:
        st.markdown("**EVIDENCE**")
        if provider_rows:
            for row in provider_rows:
                provider = str(row[0]) if row[0] else "other"
                count = int(row[1]) if len(row) > 1 else 0
                pct = float(row[2]) if len(row) > 2 and row[2] is not None else 0.0
                # provider values: 'llama', 'claude', 'other (blocked before routing)'
                st.metric(f"Model: {provider}", f"{count} req ({pct:.1f}%)")
        else:
            st.info("No inference data yet -- run some calls first.")
        st.caption("Source: inference table (near-real-time, no ingestion lag).")
else:
    st.warning(
        "DATABRICKS_WAREHOUSE_ID is not set. "
        "Metrics are unavailable until the app resource binding is configured."
    )

st.divider()

# ---------------------------------------------------------------------------
# Prompt library
# ---------------------------------------------------------------------------
st.subheader("Prompt library")
selected_label = st.selectbox(
    "Choose a prompt example or type your own below",
    options=list(PROMPT_LIBRARY.keys()),
    index=0,
)
default_prompt = PROMPT_LIBRARY[selected_label]

# ---------------------------------------------------------------------------
# Tabs: endpoint overlay (chat) and UC model service (rate-limit flood)
# ---------------------------------------------------------------------------
tab_a, tab_b = st.tabs(
    ["Endpoint overlay -- Serving endpoint (guardrails)", "UC model service -- Strategic path (rate limits)"]
)

# ---- Endpoint overlay -------------------------------------------------------
with tab_a:
    st.markdown(
        "**Endpoint AI Gateway overlay (legacy): workspace serving endpoint `ai-gateway-deepdive`** -- "
        "governed via `put_ai_gateway()`. "
        "Guardrails enforce PII policy centrally: input PII is blocked (HTTP 400), "
        "output PII is masked to type labels (HTTP 200). Unsafe content is blocked. "
        "Every call is logged to the inference table."
    )
    st.info(
        "Migration note: under the **Enforce Unity AI Gateway** setting (opt-in for workspace admins), "
        "Apps wired to a Serving Endpoint resource stop working -- the app SP must hold `EXECUTE` on the "
        "corresponding model service and the code must call the model-service path. This app is already "
        "migration-ready: its SP has schema-level `EXECUTE` and the 'UC model service' tab calls "
        "`/ai-gateway/mlflow/v1/chat/completions`. This legacy tab is the path that would be retired.",
        icon="🔀",
    )

    user_prompt = st.text_area(
        "Prompt",
        value=default_prompt,
        height=100,
        key="chat_prompt",
    )

    if st.button("Send to endpoint", key="send_btn"):
        with st.spinner("Calling ai-gateway-deepdive..."):
            raw, http_status, latency_ms = _call_endpoint(user_prompt)

        result = parse_query_result(raw, latency_ms, http_status)

        if result.guardrail_action == "block":
            st.warning(
                "Blocked by guardrails\n\n"
                + (result.error or "The request was denied by the AI Gateway policy."),
                icon="🚫",
            )
        elif result.guardrail_action == "mask":
            # This branch fires only when the full raw JSON is available (raw REST path).
            # The SDK-based chat() helper drops output_guardrail from the typed response,
            # so mask is not surfaced via the SDK. For live mask evidence, see the
            # Metrics tile (Output PII masked count from the inference table).
            st.info(
                "Output PII masked by guardrails (type labels shown)\n\n"
                "The model generated content containing PII. The output guardrail "
                "replaced actual values with angle-bracket type labels "
                "(`<EMAIL_ADDRESS>`, `<PHONE_NUMBER>`, etc.) before delivering the response.",
                icon="🔒",
            )
            st.markdown("**Reply (PII type labels replace actual values)**")
            st.write(result.content)
        elif result.error:
            st.error(
                f"Request failed (HTTP {result.http_status})\n\n{result.error}",
                icon="⚠️",
            )
        else:
            st.markdown("**Reply**")
            st.write(result.content)

        st.caption(
            f"served_model: {result.served_model} | "
            f"latency: {result.latency_ms} ms | "
            f"tokens: {result.prompt_tokens + result.completion_tokens} "
            f"(in={result.prompt_tokens} out={result.completion_tokens}) | "
            f"guardrail_action: {result.guardrail_action} | "
            f"HTTP: {result.http_status}"
        )

    st.info(
        "Try 'Input block' to see the input guardrail fire (HTTP 400). "
        "Try 'Unsafe' to see the safety guardrail block. "
        "Try 'Output mask' -- the guardrail will act (block or mask); "
        "output masking evidence is in the Metrics tile above "
        "(Output PII masked count from the inference table).",
        icon="💡",
    )

# ---- UC model service -------------------------------------------------------
with tab_b:
    st.markdown(
        "**UC model service (strategic): `aigw_demo_service`** -- "
        "configured with a 3-requests/minute service-level rate limit and a "
        "`detect_sensitive_data` service policy. "
        "The UC model service produces a **real HTTP 429** once the QPM budget is exhausted "
        "(the legacy endpoint enforces rate limits too -- the model service's edge is UC-native "
        "governance, not rate limiting itself)."
    )

    st.markdown(
        "The `detect_sensitive_data` policy is **selective**: benign prompts are answered "
        "(HTTP 200, `finish_reason=stop`), while prompts containing PII (credit-card / SSN) are "
        "denied (HTTP 200, `finish_reason=content_filter`, with a message naming the `block-pii` "
        "policy). The flood below shows the real rate limit; the panel under it shows the PII policy live."
    )

    if st.button("Flood (~8 rapid calls)", key="flood_btn"):
        with st.spinner("Firing 8 rapid calls to the model service..."):
            flood_results = _flood_model_service(n=8)

        got_429 = any(status == 429 for status, _ in flood_results)

        if got_429:
            st.error(
                "Rate limit hit (429) -- Unity AI Gateway throttled the call. "
                "This is the real HTTP 429 that production clients retry on.",
                icon="🚦",
            )
        else:
            st.warning(
                "No 429 observed in this run. The service policy may be blocking all "
                "calls before the rate-limit counter increments, or the QPM window "
                "recently reset. Try again or check the service configuration.",
                icon="⚠️",
            )

        st.markdown("**Per-call status:**")
        for i, (status, label) in enumerate(flood_results, start=1):
            if status == 429:
                icon = "🔴"
            elif status == 200:
                icon = "🟡"
            else:
                icon = "⚪"
            st.markdown(f"{icon} Call {i}: {label}")

        st.caption(
            "Both the UC model service and the legacy endpoint enforce QPM/TPM limits with "
            "real HTTP 429 (verified live). The model service's advantage is "
            "UC-native governance (EXECUTE, revoke->404, ABAC) and lifecycle -- it is the "
            "strategic path as legacy v1/v2 endpoints are deprecated -- not the throttle itself."
        )

    st.info(
        "Rate limit: 3 requests/minute (service-level). "
        "Firing 8 rapid calls should trigger 429 on calls 4+ once the budget is exhausted.",
        icon="ℹ️",
    )

    st.divider()
    st.markdown(
        "**Service policy (PII) -- live.** Send a single prompt to the model service. A benign "
        "prompt is answered; a prompt containing a credit-card number or SSN is denied by the "
        "`detect_sensitive_data` service policy."
    )
    _ms_presets = {
        "Benign": "What is the capital of France?",
        "PII -- credit card": "My credit card is 4111 1111 1111 1111. What is my credit risk score?",
        "PII -- US SSN": "My SSN is 123-45-6789. Am I eligible for a loan?",
    }
    _ms_choice = st.radio("Preset", list(_ms_presets), horizontal=True, key="ms_preset")
    _ms_prompt = st.text_area(
        "Prompt to the model service", value=_ms_presets[_ms_choice], height=80, key="ms_prompt"
    )
    if st.button("Send to model service", key="ms_send"):
        with st.spinner("Calling the UC model service..."):
            _raw, _status, _lat = _call_model_service(_ms_prompt)
        _choices = _raw.get("choices") or []
        _finish = _choices[0].get("finish_reason") if _choices else None
        _content = (((_choices[0].get("message") or {}).get("content", "") or "") if _choices else "")
        if _status == 200 and _finish == "content_filter":
            st.warning(
                "Blocked by the `block-pii` service policy\n\n"
                + (_content or "The request was denied before the model was called."),
                icon="🚫",
            )
        elif _status == 200:
            st.markdown("**Reply**")
            st.write(_content or "(no content)")
        elif _status == 429:
            st.error(
                "Rate limited (HTTP 429) -- the 3/min budget is exhausted; wait a moment and retry.",
                icon="🚦",
            )
        else:
            st.error(
                f"HTTP {_status}\n\n{str(_raw.get('message') or _raw.get('error') or _raw)[:200]}",
                icon="⚠️",
            )
        st.caption(f"finish_reason: {_finish} | latency: {_lat} ms | HTTP: {_status}")

    st.info(
        "Model-service policy blocks return HTTP 200 with `finish_reason=content_filter` (not a 400) "
        "and are not written to an inference table -- so they show here live, but do NOT feed the "
        "endpoint 'Guardrail blocks' metric above (which counts endpoint HTTP 400s). Safety and "
        "jailbreak (LLM-as-judge) guardrails are demonstrated in notebook 03.",
        icon="🛡️",
    )

st.divider()
st.caption(
    "Unity AI Gateway feature availability (GA/Beta), the enforcement "
    "rollout, and legacy-endpoint deprecation dates are subject to change. Verify against current "
    "Databricks documentation before relying on any status claim."
)
