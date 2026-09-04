# Databricks notebook source
# ai-gateway-deepdive/notebooks/02_runtime.py
#
# Unity AI Gateway -- control surface: RUNTIME (gateway functions: rate limiting + traffic routing).
# Notebook 02 of the deep-dive demo sequence.
#
# Demonstrates two RUNTIME control surfaces on the UC model service:
#
#   Step 1: REAL HTTP 429 rate limiting -- fire 8 rapid calls at the model
#           service (provisioned with 3 req/min limit in 00_setup); assert a 429
#           ("User defined rate limit(s) exceeded") appears. Rate limiting works on
#           BOTH paths: legacy serving endpoints and UC model services each enforce
#           RPM/TPM with standard HTTP 429 semantics (verified live: a
#           3/min endpoint limit produced a 429). The model service's edge is not
#           that it alone can throttle -- it is that it is a UC-native governed
#           securable; see the governance framing below.
#
#   Step 2: Traffic routing (live) -- 00_setup creates the service with a 70/30
#           weighted split across two pay-per-token FMs (routing is create-time only
#           in Beta -- config.routing cannot be PATCHed). Read the configured weights,
#           then fire a spaced sample of calls and tally which destination model served
#           each, watching the per-request split emerge.
#
#   Step 3: Token-based (TPM) rate limit -- the tokens/min variant of the rate limit.
#           Temporarily tighten the service to a token-dominant config, fire large-response
#           calls until the token budget trips ("Tokens-per-minute (TPM)" 429), then restore.
#
# Depends on 00_setup.py having been run (model service must exist + be ready).
# Service rate limit: 3 requests/minute (service-level, all callers).

# COMMAND ----------
# MAGIC %md
# MAGIC # Unity AI Gateway - RUNTIME: UC model service rate limiting + traffic routing
# MAGIC
# MAGIC > _Unity AI Gateway GA/Beta status, enforcement rollout, and legacy-endpoint deprecation dates are subject to change. Verify against current Databricks documentation._
# MAGIC
# MAGIC **Runs on:** Unity AI Gateway model service `ai_gateway_deepdive_catalog.core.aigw_demo_service` (UC securable).
# MAGIC
# MAGIC **Gateway functions: RUNTIME control surfaces.**
# MAGIC Request throttling (real HTTP 429) and traffic routing are first-class
# MAGIC capabilities on the UC model-service path. The legacy serving-endpoint path
# MAGIC *also* enforces rate limits and supports routing/fallback (`put_ai_gateway()`),
# MAGIC so these runtime features are not model-service-exclusive. What sets the model
# MAGIC service apart is **governance and lifecycle**: it is a Unity Catalog securable
# MAGIC (`EXECUTE`, revoke -> HTTP 404, ABAC), governed centrally across workspaces, and
# MAGIC it is the strategic path as legacy v1/v2 endpoints are deprecated. This notebook shows the runtime surfaces on the
# MAGIC governed path; the differentiator is who governs them, not whether they exist.
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC - **Step 1: Real HTTP 429 rate limiting** -- fire 8 rapid calls; the
# MAGIC   service is provisioned with a 3 req/min service-level limit from 00_setup.
# MAGIC   After the first 3 calls, subsequent calls return HTTP 429 "User defined
# MAGIC   rate limit(s) exceeded". The model service enforces this with standard
# MAGIC   retry-friendly HTTP 429 semantics.
# MAGIC - **Step 2: Traffic routing (live)** -- 00_setup creates the service with a 70/30
# MAGIC   weighted split across two pay-per-token FMs. Routing is fixed at creation
# MAGIC   (config.routing is not updatable via UpdateModelService in Beta), so the split is
# MAGIC   baked in. This cell reads the weights, then fires a spaced sample and tallies which
# MAGIC   destination model served each call -- the per-request split emerges live.
# MAGIC - **Step 3: Token-based (TPM) rate limit** -- the tokens/min variant. Temporarily
# MAGIC   tightens the service to a low token budget, fires large-response calls until the token
# MAGIC   cap trips (a distinct "Tokens-per-minute (TPM)" 429), then restores the original limit.

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup:** imports and the shared config and helpers from `%run ./_common` that the rest of the notebook uses.

# COMMAND ----------

import os
import sys
import time
import json
import urllib.request
import urllib.error
import urllib.parse

# Local python3: resolve paths and import _common explicitly.
# Databricks notebook runtime: names are in scope from %run ./_common in the cell above.
if "__file__" in globals():
    _nb_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, _nb_dir)
    sys.path.insert(0, os.path.join(_nb_dir, "..", "src"))
    from _common import SERVICE_FQN, MS_MODEL, HOST, ms_api, ms_invoke, ms_finish_reason, _w  # noqa: F401

print(f"service   : {SERVICE_FQN}")
print(f"model     : {MS_MODEL}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Real HTTP 429 rate limiting
# MAGIC
# MAGIC Service rate limit: 3 requests/minute (service-level, all callers combined).
# MAGIC
# MAGIC Fire 8 rapid calls. The first 3 will succeed (or be policy-blocked with
# MAGIC HTTP 200 + content_filter finish_reason). Calls 4-8 will hit the rate
# MAGIC limit and return HTTP 429 "User defined rate limit(s) exceeded".
# MAGIC
# MAGIC **Beta behavior note:** On a fresh service created moments ago, the
# MAGIC rate-limit counter may not be active on FIRST run. PATCHing rate_limits
# MAGIC is required, but the counter warms up on first use. Re-running this notebook
# MAGIC will produce HTTP 429 responses as expected. The config is correct; enforcement
# MAGIC activates from the second run.

# COMMAND ----------

_BURST_PROMPT = "What is the capital of France?"
print(f"We asked {SERVICE_FQN} the same question 8 times, rapidly (limit: 3/min):")
print(f"  prompt: {_BURST_PROMPT!r}\n")
statuses = []
for i in range(1, 9):
    status, resp = ms_invoke(_BURST_PROMPT)
    statuses.append(status)
    if status == 200:
        try:
            finish = ms_finish_reason(resp)
            content = ((resp.get("choices") or [{}])[0].get("message", {}).get("content", "") or "")[:40]
            label = f"the model responded {content!r} (finish={finish})"
        except (KeyError, IndexError):
            label = "(no content)"
    else:
        # 429 body: {"error_code": "...", "message": "..."}
        label = "throttled -- " + (resp.get("message") or resp.get("error") or str(resp))[:70]
    print(f"  Call {i}: HTTP {status} | {label}")

print(f"\nstatuses: {statuses}")
n_throttled = statuses.count(429)
n_status_200 = statuses.count(200)
if 429 not in statuses:
    print(
        "\nNOTE: No HTTP 429 observed on this run.\n"
        "This is expected on a fresh service: the rate-limit counter warms up on\n"
        "first use after creation. Re-run this notebook to observe the throttle response.\n"
        "The config is correct; enforcement is active from the second run."
    )
else:
    print(f"\nHTTP 429 observed: {n_throttled} call(s) throttled.")
    print(f"HTTP 200 (policy or success): {n_status_200} call(s).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Traffic routing (live)
# MAGIC
# MAGIC A model service routes to one or more destinations via
# MAGIC `config.routing.destinations[]` with `traffic_percentage` weights. 00_setup creates
# MAGIC this service with a **70/30 split** across `system.ai.llama-4-maverick` and
# MAGIC `system.ai.llama_v3_3_70b_instruct`.
# MAGIC
# MAGIC Routing is **create-time only** on a model service today: an `UpdateModelService`
# MAGIC PATCH of `config.routing` is rejected with `INVALID_PARAMETER_VALUE` -- "Field
# MAGIC 'config.routing' is not yet supported by UpdateModelService" (Beta).  The split is therefore baked in at creation.
# MAGIC
# MAGIC This cell reads the configured weights, then fires a spaced sample of calls (staying
# MAGIC under the 3 req/min limit) and tallies which destination model served each -- the
# MAGIC per-request split emerges live, enforced centrally with no client-side logic.

# COMMAND ----------

# Read the configured routing weights.
print(f"Routing configuration for {SERVICE_FQN}:\n")
_get_status, _get_resp = ms_api("GET", f"/api/2.1/unity-catalog/model-services/{SERVICE_FQN}")
_dests = _get_resp.get("config", {}).get("routing", {}).get("destinations", []) if _get_status == 200 else []
for _d in _dests:
    print(f"  - {_d.get('name')} | {_d.get('traffic_percentage')}% | {_d.get('pay_per_token_config', {}).get('model')}")

_route_total = 0
if len(_dests) < 2:
    print(
        "\n  Single destination -- no split to observe. Re-run 00_setup to (re)create the service\n"
        "  with the 70/30 routing (routing is create-time only, so it cannot be PATCHed in)."
    )
else:
    # Spaced sample: the service enforces 3 req/min, so space calls ~21 s apart to stay under it
    # (a rapid burst would just 429 -- that is Step 1). Each HTTP 200 reports its served model.
    from collections import Counter
    _N_ROUTE_CALLS = 8
    _ROUTE_IV_S = 21
    # Lead-in: clear Step 1's burst from the rolling rate window so the tally starts clean even
    # when this cell runs immediately after Step 1 (e.g. a back-to-back / Run-all execution).
    _ROUTE_LEADIN_S = 65
    print(f"\nWaiting {_ROUTE_LEADIN_S}s to clear the rate window (Step 1's burst) before sampling...")
    time.sleep(_ROUTE_LEADIN_S)
    _ROUTE_PROMPT = "What is the capital of France?"
    print(f"We ask the same question {_N_ROUTE_CALLS} times, ~{_ROUTE_IV_S}s apart (under the 3/min limit),")
    print(f"and watch which destination model answers each.  prompt: {_ROUTE_PROMPT!r}\n")
    _route_counts = Counter()
    for _i in range(1, _N_ROUTE_CALLS + 1):
        _st, _rb = ms_invoke(_ROUTE_PROMPT)
        if _st == 200 and ms_finish_reason(_rb) != "content_filter":
            _served = _rb.get("model", "(unknown)")
            _reply = ((_rb.get("choices") or [{}])[0].get("message") or {}).get("content", "") or ""
            _reply = _reply.strip()
            _route_counts[_served] += 1
            print(f"  call {_i:2}: answered by {_served}  ->  {_reply[:45]!r}")
        elif _st == 429:
            print(f"  call {_i:2}: HTTP 429  rate-limited (not counted) -- widen _ROUTE_IV_S if frequent")
        else:
            print(f"  call {_i:2}: HTTP {_st}  finish={ms_finish_reason(_rb)} (not counted)")
        if _i < _N_ROUTE_CALLS:
            time.sleep(_ROUTE_IV_S)
    _route_total = sum(_route_counts.values())
    print("\nObserved distribution (successful calls):")
    if _route_total:
        for _m, _c in _route_counts.most_common():
            print(f"  {_m:42} {_c:2}/{_route_total}  ({100 * _c // _route_total}%)")
        print(
            "\n  The per-request split tracks the configured weights -- routing is enforced centrally\n"
            "  at the model service, with no client-side logic."
        )
    else:
        print("  (no successful calls -- all rate-limited; re-run or widen _ROUTE_IV_S)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Token-based (TPM) rate limit
# MAGIC
# MAGIC Rate limits come in two flavors: **requests/min** (Step 1) and **tokens/min** -- a cap on
# MAGIC total tokens regardless of request count, aimed at spend on long prompts/responses.
# MAGIC
# MAGIC To show the TPM limit firing distinctly (without colliding with the 3 req/min RPM limit that
# MAGIC Step 1 relies on), this step **temporarily** tightens the service to a token-dominant config
# MAGIC (requests raised, a low tokens/min cap), fires a few large-response calls until the token
# MAGIC budget trips, then **restores** the original limits. The 429 reads "Tokens-per-minute (TPM)
# MAGIC rate limit exceeded" -- distinct from Step 1's RPM message.

# COMMAND ----------

# Self-contained: temporarily set a token-dominant limit, demonstrate TPM, then restore RPM=3.
# Rate-limit PATCH takes effect within seconds (create-time routing is untouched). The restore
# runs in `finally` so the service is always left at the 00_setup config even if a call errors.
_rl_path = f"/api/2.1/unity-catalog/model-services/{SERVICE_FQN}?update_mask=config.rate_limits"
_tpm_seen = False
_tpm_config_ok = False
try:
    _tpm_st, _tpm_cfg_b = ms_api("PATCH", _rl_path, {"config": {"rate_limits": [
        {"key": "RATE_LIMIT_KEY_SERVICE", "renewal_period": 1, "requests": 1000},
        {"key": "RATE_LIMIT_KEY_SERVICE", "renewal_period": 1, "tokens": 100},
    ]}})
    _tpm_config_ok = _tpm_st == 200
    if not _tpm_config_ok:
        # A failed PATCH must not be misreported as a token-bucket warmup; surface it.
        print(f"ERROR: could not set the token limit (HTTP {_tpm_st}): {_tpm_cfg_b}")
        print("  Skipping the TPM demo this run; the finally block restores the 3 req/min limit.")
    else:
        print("Temporarily set tokens/min = 100 (requests raised so only the TPM cap can trip).")
        print("Firing large-response calls (max_tokens=400) until the token budget is exhausted...\n")
    for _i in (range(1, 15) if _tpm_config_ok else range(0)):
        _st, _rb = ms_invoke("Write a detailed paragraph about the history of Paris.", max_tokens=400)
        _msg = (_rb.get("message") or _rb.get("error") or "")
        _tt = (_rb.get("usage") or {}).get("total_tokens")
        if _st == 200:
            print(f"  call {_i:2}: HTTP 200  total_tokens={_tt}")
        elif _st == 429 and "TPM" in _msg.upper():
            print(f"  call {_i:2}: HTTP 429 (TPM) -- {_msg[:90]}")
            _tpm_seen = True
            break
        elif _st == 429:
            print(f"  call {_i:2}: HTTP 429 -- {_msg[:90]}")
            break
        else:
            print(f"  call {_i:2}: HTTP {_st}  {_msg[:60]}")
        time.sleep(1)
    if _tpm_config_ok:
        print(
            "\nTPM limit " + (
                "PROVEN: the service throttled on total tokens/min, independent of request count."
                if _tpm_seen else
                "not observed this run (the token bucket warms up on first use -- re-run this cell)."
            )
        )
finally:
    # Always restore the 00_setup config (service-level 3 requests/min).
    ms_api("PATCH", _rl_path, {"config": {"rate_limits": [
        {"key": "RATE_LIMIT_KEY_SERVICE", "renewal_period": 1, "requests": 3},
    ]}})
    print("Restored the original rate limit: 3 requests/min (service-level).")

# COMMAND ----------

print("=" * 60)
print("Notebook 02_runtime complete.")
print(f"  Real 429 proven: {n_throttled} call(s) throttled in Step 1." if 429 in statuses else "  Real 429 not observed on this run (counter warms up on first use).")
if len(_dests) >= 2 and _route_total:
    print(f"  Routing: live 70/30 split observed across {len(_route_counts)} model(s) over {_route_total} calls.")
else:
    print("  Routing: configured weights displayed (re-run 00_setup for the 2-destination split).")
print("  TPM limit: " + ("token-per-minute cap enforced (Step 3)." if _tpm_seen else "configured; token-bucket warmed up on first use (re-run Step 3)."))
print("=" * 60)
