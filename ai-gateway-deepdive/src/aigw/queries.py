# ai-gateway-deepdive/src/aigw/queries.py  (canonical)
# app/aigw/queries.py is a VENDORED COPY kept byte-identical to this file.
#
# Centralized SQL for the AI Gateway deep-dive demo.
# Every function returns a SQL string parameterized by the module-level
# constants below. The Databricks App runtime imports from app/aigw/ (self-contained);
# notebooks and tests import from src/aigw/. Both copies MUST stay in sync --
# any change to one must be applied to the other.
#
# Schema notes (verified live against ai_gateway_deepdive_catalog.core.aigw_payload):
#   - No served_entity_name column exists on the inference table.
#   - Model is derived from get_json_object(response, '$.model').
#   - Tokens live in get_json_object(response, '$.usage.total_tokens').
#   - status_code: 200 = success, 429/403/400/404 = various error/block states.
#   - system.serving.endpoint_usage has ~1-2h ingestion lag; prefer inference table
#     for near-real-time metrics.

CATALOG = "ai_gateway_deepdive_catalog"
SCHEMA = "core"
ENDPOINT = "ai-gateway-deepdive"
INFERENCE_TABLE = f"{CATALOG}.{SCHEMA}.aigw_payload"
GUARDRAIL_VIEW = f"{CATALOG}.{SCHEMA}.guardrail_events"

# Inline CASE expression that maps the raw model string to a short provider label.
# meta-llama-* -> 'llama'; us.anthropic.* / *claude* -> 'claude';
# null model (blocked before routing) -> 'other (blocked before routing)'.
_MODEL_LABEL_EXPR = (
    "CASE"
    "  WHEN get_json_object(response, '$.model') LIKE '%llama%' THEN 'llama'"
    "  WHEN get_json_object(response, '$.model') LIKE '%anthropic%'"
    "    OR get_json_object(response, '$.model') LIKE '%claude%' THEN 'claude'"
    "  ELSE 'other (blocked before routing)'"
    " END"
)


def tokens_cost_by_provider() -> str:
    """Total tokens by provider derived from the inference table.

    Sums total_tokens (from response $.usage.total_tokens) grouped by the
    provider label (llama / claude / other). Only includes rows where the
    model field is non-null (i.e., successful responses).
    """
    return f"""
SELECT
    {_MODEL_LABEL_EXPR} AS provider,
    count(*)                                                         AS request_count,
    sum(CAST(get_json_object(response, '$.usage.total_tokens') AS INT)) AS total_tokens
FROM {INFERENCE_TABLE}
WHERE status_code = 200
  AND get_json_object(response, '$.model') IS NOT NULL
GROUP BY provider
ORDER BY total_tokens DESC"""


def requests_and_429_over_time() -> str:
    """Request volume and non-200 count (rate-limit or gateway blocks) over time.

    Buckets by hour. non_200_count captures 429 (rate limit), 403 (gateway
    block / permission denied), 400, and any other non-success status.
    """
    return f"""
SELECT
    date_trunc('hour', request_time) AS hour_bucket,
    count(*)                          AS total_requests,
    count(CASE WHEN status_code != 200 THEN 1 END) AS non_200_count,
    count(CASE WHEN status_code = 429  THEN 1 END) AS rate_limited_count,
    count(CASE WHEN status_code = 403  THEN 1 END) AS blocked_count
FROM {INFERENCE_TABLE}
GROUP BY hour_bucket
ORDER BY hour_bucket"""


def guardrail_events_view_ddl() -> str:
    """DDL to create (or replace) the guardrail_events view.

    Classifies each inference request by the REAL reason it was not a clean
    200 pass-through. guardrail_action values (verified live 2026-08-21):

      'guardrail_mask'     -- Output PII masked by the output guardrail (status 200).
                             Detected via response $.output_guardrail[0].pii_detection = 'true'.
                             The model generated PII and the gateway replaced it with
                             angle-bracket type labels (<EMAIL_ADDRESS> etc.) before delivery.
      'none'               -- Clean pass-through (status 200, no PII masking). ~119 rows.
      'rate_limit'         -- Unity AI Gateway QPM budget exhausted (status 429). ~157 rows.
      'permission_block'   -- Gateway 403; calls=0 rate limit or caller lacks CAN_QUERY. ~36 rows.
      'guardrail_block'    -- Input or output guardrail fired and blocked the request
                             (status 400 with input_guardrail_triggered or
                             output_guardrail_triggered, flagged=true). ~9 rows.
                             Input PII blocked: input_guardrail.pii_detection=true.
                             Output PII blocked (privacy + pii both triggered): flagged=true.
      'upstream_error'     -- Upstream model error or host-resolution failure
                             (status 400/404 with no guardrail payload). ~15 rows.

    Detection:
      guardrail_mask: status 200 AND output_guardrail[0].pii_detection = 'true' in the
        response JSON. Reliable -- this field is only present when the output guardrail ran
        PII detection and masked at least one value.
      guardrail_block: non-200 response whose message JSON contains input_guardrail or
        output_guardrail with flagged=true.

    Note: output_guardrail_triggered blocks (flagged=true) are classified 'guardrail_block'
    not 'guardrail_mask'. These occur when both the safety privacy category AND pii_detection
    fire simultaneously; the gateway blocks entirely rather than masking.

    Run this once (or after schema changes) to materialize the view in UC.
    """
    return f"""CREATE OR REPLACE VIEW {GUARDRAIL_VIEW} AS
SELECT
    request_time,
    status_code,
    served_entity_id,
    requester,
    {_MODEL_LABEL_EXPR}                                          AS provider,
    CASE
      WHEN status_code = 200
           AND get_json_object(response, '$.output_guardrail[0].pii_detection') = 'true'
           THEN 'guardrail_mask'
      WHEN status_code = 200 THEN 'none'
      WHEN status_code = 429 THEN 'rate_limit'
      WHEN status_code = 403 THEN 'permission_block'
      WHEN (get_json_object(response, '$.message') LIKE '%input_guardrail%'
            OR get_json_object(response, '$.message') LIKE '%output_guardrail%')
           AND get_json_object(response, '$.message') LIKE '%"flagged":true%'
           THEN 'guardrail_block'
      ELSE 'upstream_error'
    END                                                          AS guardrail_action,
    execution_duration_ms
FROM {INFERENCE_TABLE}"""


def guardrail_trigger_counts() -> str:
    """Count of events by guardrail_action from the guardrail_events view.

    Returns one row per distinct guardrail_action value. Callers computing
    POLICY metrics must read both guardrail categories:
      'guardrail_block' -- input/output guardrail blocked the request (HTTP 400).
      'guardrail_mask'  -- output guardrail masked model-generated PII (HTTP 200).
    Other non-200 actions: rate_limit (429), permission_block (403),
    upstream_error (400/404 non-guardrail). 'none' = 200 clean pass-through.

    Requires the view to exist (run guardrail_events_view_ddl() first).
    """
    return f"""
SELECT
    guardrail_action,
    count(*) AS event_count
FROM {GUARDRAIL_VIEW}
GROUP BY guardrail_action
ORDER BY event_count DESC"""


def provider_mix() -> str:
    """Percentage share of llama vs claude vs other from the inference table.

    Counts all requests (including non-200) to show true traffic split;
    null model rows (gateway blocks before model selection) are grouped as 'other'.
    """
    return f"""
SELECT
    {_MODEL_LABEL_EXPR} AS provider,
    count(*)            AS request_count,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct_of_total
FROM {INFERENCE_TABLE}
GROUP BY provider
ORDER BY request_count DESC"""


def latency_by_provider() -> str:
    """p50 and p95 execution_duration_ms by provider, successful requests only.

    execution_duration_ms is inference-only time (excludes network overhead).
    Filters to status_code = 200 so blocked/rate-limited requests (which have
    near-zero duration) do not skew the percentile distribution.
    """
    return f"""
SELECT
    {_MODEL_LABEL_EXPR}                                             AS provider,
    count(*)                                                         AS request_count,
    percentile(execution_duration_ms, 0.5)                           AS p50_ms,
    percentile(execution_duration_ms, 0.95)                          AS p95_ms
FROM {INFERENCE_TABLE}
WHERE status_code = 200
  AND get_json_object(response, '$.model') IS NOT NULL
GROUP BY provider
ORDER BY p50_ms"""
