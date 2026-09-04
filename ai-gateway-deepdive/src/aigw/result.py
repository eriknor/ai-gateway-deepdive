# ai-gateway-deepdive/src/aigw/result.py  (canonical)
# app/aigw/result.py is a VENDORED COPY kept byte-identical to this file.
# The Databricks App runtime imports from app/aigw/ (self-contained);
# notebooks and tests import from src/aigw/. Both copies MUST stay in sync --
# any change to one must be applied to the other.
import re
from dataclasses import dataclass

# Angle-bracket PII type labels produced by the serving-endpoint output guardrail
# when pii.behavior=MASK is configured and the model generates PII in its response.
# Verified live (2026-08-21): <EMAIL_ADDRESS>, <PHONE_NUMBER> observed in HTTP 200 content.
# Full set includes <SSN>, <CREDIT_CARD>, <IP_ADDRESS>, etc.
# Do NOT use "***" or "[REDACTED]" -- those appear in normal text and false-fire.
_PII_TYPE_LABEL_RE = re.compile(r"<[A-Z_]+>")


@dataclass
class ChatResult:
    content: str
    served_model: str
    prompt_tokens: int
    completion_tokens: int
    latency_ms: int
    http_status: int
    guardrail_action: str  # "none" | "mask" | "block"
    error: str | None


def parse_query_result(raw: dict, latency_ms: int, http_status: int = 200) -> ChatResult:
    # Transport error: connection failed before any response was received.
    # _call_endpoint in app.py returns http_status=0 when requests.post raises an
    # exception (e.g. DNS failure, connection refused, timeout before headers). Return
    # as an explicit error so callers show an error panel rather than a blank reply.
    if http_status == 0:
        return ChatResult(
            content="",
            served_model="",
            prompt_tokens=0,
            completion_tokens=0,
            latency_ms=latency_ms,
            http_status=0,
            guardrail_action="none",
            error=str(raw.get("error") or raw.get("message") or "Transport/connection error"),
        )

    # Guardrail block: gateway returns an error payload instead of choices.
    # Covers both input_guardrail_triggered (HTTP 400, input PII blocked) and
    # output_guardrail_triggered (HTTP 400, output PII blocked when safety category
    # also fires) as well as safety and other gateway blocks.
    err_msg = raw.get("message") or raw.get("error")
    if raw.get("error_code") or (err_msg and "guardrail" in str(err_msg).lower()):
        return ChatResult(
            content="",
            served_model=str(raw.get("model", "")),
            prompt_tokens=0,
            completion_tokens=0,
            latency_ms=latency_ms,
            http_status=http_status,
            guardrail_action="block",
            error=str(err_msg),
        )

    choices = raw.get("choices") or []
    content = ""
    if choices:
        content = (choices[0].get("message") or {}).get("content", "") or ""
    usage = raw.get("usage") or {}

    # Non-guardrail error with no response content: rate-limit (429), auth failure
    # (403), upstream error, etc. Return with error set so the caller shows an
    # error panel rather than an empty successful-looking reply.
    if not choices and err_msg and http_status != 200:
        return ChatResult(
            content="",
            served_model=str(raw.get("model", "")),
            prompt_tokens=0,
            completion_tokens=0,
            latency_ms=latency_ms,
            http_status=http_status,
            guardrail_action="none",
            error=str(err_msg),
        )

    # Detect output PII masking (HTTP 200 pass-through with masked content).
    # When the model generates PII and pii.behavior=MASK is configured on the
    # serving endpoint, the output guardrail replaces actual PII values with
    # angle-bracket type labels before delivering the response.
    # Example: "Email: <EMAIL_ADDRESS>, Phone: <PHONE_NUMBER>" -- HTTP 200, finish_reason stop.
    #
    # IMPORTANT -- SDK path limitation:
    #   serving_endpoints.query().as_dict() (used by _common.chat()) is a TYPED SDK
    #   response; the typed model drops fields it does not know about, including
    #   output_guardrail. As a result, the _pii_detected_api signal below is NEVER
    #   set via the SDK path, and guardrail_action='mask' is never returned by
    #   chat()-based callers. When masking occurs on the SDK path, chat() returns
    #   guardrail_action='none' but the content already has type labels (<EMAIL_ADDRESS>
    #   etc.) -- a false-negative on guardrail_action. The chat() helper can yield:
    #     - 'none' with type labels in content: masking happened, SDK dropped the signal
    #     - 'none' without type labels: clean pass-through
    #     - 'block': guardrail blocked (HTTP 400)
    #   This mask branch IS correct and fires for callers that pass the full raw
    #   JSON (e.g. app.py's _call_endpoint via requests.post, or the unit tests).
    #   Masking IS real and IS recorded in the inference table under
    #   guardrail_action='guardrail_mask'; see the guardrail_events view and dashboard
    #   for evidence that does not rely on the SDK query path.
    #
    # Detection requires BOTH signals to avoid false-fires on ordinary model output
    # like <YOUR_API_KEY>, <BASE_URL>, or <GET> that match the regex but are not
    # guardrail-produced PII labels:
    #   1. output_guardrail[].pii_detection == True in the raw API response dict
    #      (the gateway sets this only when it actually ran PII detection and masked).
    #   2. A <TYPE_LABEL> angle-bracket pattern is present in content.
    # "***" and "[REDACTED]" are NOT used -- they appear in normal markdown and text.
    _pii_detected_api = any(
        bool(og.get("pii_detection"))
        for og in (raw.get("output_guardrail") or [])
        if isinstance(og, dict)
    )
    guardrail_action = (
        "mask"
        if (_pii_detected_api and _PII_TYPE_LABEL_RE.search(content or ""))
        else "none"
    )

    return ChatResult(
        content=content,
        served_model=str(raw.get("model", "")),
        prompt_tokens=int(usage.get("prompt_tokens", 0)),
        completion_tokens=int(usage.get("completion_tokens", 0)),
        latency_ms=latency_ms,
        http_status=http_status,
        guardrail_action=guardrail_action,
        error=None,
    )
