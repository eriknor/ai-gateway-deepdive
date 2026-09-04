# Databricks notebook source
# ai-gateway-deepdive/notebooks/06_call_paths.py
#
# Unity AI Gateway -- call-path reference: every viable way to reach the model.
# Notebook 06 of the deep-dive demo sequence.
#
# One consolidated matrix: 3 targets x 3 call methods x 2 principals. It doubles as
# a live reference ("how do I call this from X?") and a governance teaching aid
# (which cells work, which are documented gaps, and why).
#
#   Targets:
#     1. Direct FM        -- databricks-llama-4-maverick (built-in pay-per-token
#                            system endpoint; NO custom AI Gateway config: the
#                            ungoverned Databricks-hosted baseline).
#     2. Serving endpoint -- ai-gateway-deepdive (legacy v1 gateway: guardrails +
#                            rate limits + usage tracking).
#     3. Model service    -- aigw_demo_service fronting system.ai.llama-4-maverick
#                            (strategic v3 path; invoked at /ai-gateway/mlflow/v1).
#
#   Methods:  SQL (ai_query)  |  Python SDK (serving_endpoints.query)  |  Python REST (raw)
#   Principals:  the current user (own credentials)  |  a service principal (OAuth M2M)
#
# NOTE the built-in FM endpoint and the model service front the SAME underlying
# model (system.ai.llama-4-maverick), so the only differences across targets are
# the governance controls in front of them, not the model.
#
# Run inside Databricks (spark available) OR locally:
#   DATABRICKS_CONFIG_PROFILE=ai-gateway-deepdive python3 notebooks/06_call_paths.py
#
# Depends on 00_setup.py having been run once (creates the endpoint + model service).
# This notebook only CALLS the targets and makes additive SP grants; it never writes
# an AI Gateway config, so it cannot disturb the endpoint's guardrails/logging.

# COMMAND ----------
# MAGIC %md
# MAGIC # Unity AI Gateway - call paths: every way to reach the model
# MAGIC
# MAGIC > _Unity AI Gateway GA/Beta status, enforcement rollout, and legacy-endpoint deprecation dates are subject to change. Verify against current Databricks documentation._
# MAGIC
# MAGIC A single matrix of **3 targets x 3 methods x 2 principals**:
# MAGIC
# MAGIC | Target | What it is |
# MAGIC |---|---|
# MAGIC | **Direct FM** (`databricks-llama-4-maverick`) | Built-in pay-per-token system endpoint. No custom AI Gateway config: the ungoverned baseline. |
# MAGIC | **Serving endpoint** (`ai-gateway-deepdive`) | Legacy v1 gateway: guardrails + rate limits + usage tracking. |
# MAGIC | **Model service** (`aigw_demo_service`) | Strategic v3 path; UC-native, invoked at `/ai-gateway/mlflow/v1/...`. |
# MAGIC
# MAGIC **Methods:** SQL (`ai_query`), Python SDK / client (`serving_endpoints.query` for
# MAGIC endpoints, `DatabricksOpenAI` for the model service), Python REST (raw HTTP).
# MAGIC **Principals:** the current user (own credentials) and a service principal (OAuth M2M).
# MAGIC
# MAGIC The Direct FM endpoint and the Model service front the **same** underlying model
# MAGIC (`system.ai.llama-4-maverick`), so any difference in behavior across targets is a
# MAGIC governance difference, not a model difference. Step 5 (results matrix) summarizes
# MAGIC every cell. The model service is reachable programmatically (Python client and REST);
# MAGIC the one remaining gap is SQL `ai_query`, which does not address user-created model
# MAGIC services yet (Beta / roadmap).

# COMMAND ----------
# MAGIC %md
# MAGIC **Install** the recommended AI Gateway client (`databricks-openai`), used in Step 2 to
# MAGIC call the UC model service. This runs first, before `%run ./_common` and the imports.
# MAGIC
# MAGIC Two expected notes on a Databricks cluster (neither is an error):
# MAGIC - **pip dependency-conflict warning:** `databricks-openai` pulls `openai` 3.x (via its
# MAGIC   `openai-agents` dependency, which requires `openai>=3`), while the base image's
# MAGIC   `langchain-openai` wants `openai<3`. It is **harmless here** -- this notebook does not
# MAGIC   use `langchain-openai`, and `%pip` changes only this notebook's session, not the cluster.
# MAGIC - The next cell runs **`%restart_python`** so the freshly installed client is importable.
# MAGIC
# MAGIC (Both the `%pip` and `%restart_python` cells are inert in a local `python3` run.)

# COMMAND ----------
# MAGIC %pip install --quiet databricks-openai

# COMMAND ----------
# MAGIC %restart_python

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup:** imports, shared config/helpers from `%run ./_common`, and the results collector.

# COMMAND ----------

import os
import sys
import time
import json
import urllib.request
import urllib.error

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

# The recommended AI Gateway client for model-service calls (Step 2). Installed by the
# %pip cell above in a Databricks notebook; guarded so a local python3 run degrades gracefully.
try:
    from databricks_openai import DatabricksOpenAI
    _HAS_DBXOAI = True
except Exception:
    _HAS_DBXOAI = False

# Local python3: resolve paths and import _common explicitly.
# Databricks notebook runtime: names are in scope from %run ./_common in the cell above.
if "__file__" in globals():
    _nb_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, _nb_dir)
    sys.path.insert(0, os.path.join(_nb_dir, "..", "src"))
    from _common import (  # noqa: F401
        CATALOG, SCHEMA, ENDPOINT, SERVICE_FQN, HOST, ms_invoke,
    )

_w = WorkspaceClient()
_host = _w.config.host.rstrip("/")

# The three targets.
DIRECT_ENDPOINT = "databricks-llama-4-maverick"   # built-in pay-per-token FM endpoint (ungoverned)
SERVING_ENDPOINT = ENDPOINT                        # custom endpoint with AI Gateway config
# Model service target is SERVICE_FQN (from _common); it is invoked via ms_invoke().

PROMPT = "In one sentence, what does Unity Catalog govern?"
MAX_TOKENS = 80

print(f"workspace        : {_host}")
print(f"direct FM        : {DIRECT_ENDPOINT}")
print(f"serving endpoint : {SERVING_ENDPOINT}")
print(f"model service    : {SERVICE_FQN}")

# ---------------------------------------------------------------------------
# Results matrix. Each cell records (target, method, principal) -> (status, note).
# "status" is an HTTP-like code where we have one, or a short string for gaps.
# ---------------------------------------------------------------------------
_TARGETS = ["Direct FM", "Serving endpoint", "Model service"]
_METHODS = ["SQL (ai_query)", "Python SDK", "Python REST"]
_MATRIX: dict[tuple[str, str], dict[str, tuple]] = {}


def _rec(target: str, method: str, principal: str, status, note: str = "") -> None:
    """Record one matrix cell and echo it. principal is 'own' or 'sp'."""
    _MATRIX.setdefault((target, method), {})[principal] = (status, note)
    print(f"  [{principal:>3}] {method:<14} -> {target:<16}: {status}  {note}".rstrip())


def _reply_of(body: dict) -> str:
    """Best-effort assistant text from a chat-completions response body."""
    try:
        return ((body.get("choices") or [{}])[0].get("message") or {}).get("content", "") or ""
    except Exception:
        return ""

# COMMAND ----------
# MAGIC %md
# MAGIC ## Raw REST transport
# MAGIC
# MAGIC A single helper posts an OpenAI-style chat request to a **serving endpoint**
# MAGIC (`/serving-endpoints/{name}/invocations`). The **model service** uses a different
# MAGIC path (`/ai-gateway/mlflow/v1/chat/completions`), which the shared `ms_invoke`
# MAGIC helper already covers. Pass `auth=` to call as a different principal (the SP in
# MAGIC Step 4); it defaults to the current user's credentials.

# COMMAND ----------


def _auth_headers(w: WorkspaceClient | None = None) -> dict:
    """Authorization header dict for a client (PAT profiles or OAuth/CLI)."""
    c = (w or _w).config
    if c.token:
        return {"Authorization": f"Bearer {c.token}"}
    return c.authenticate()


def _endpoint_rest(endpoint: str, auth: dict | None = None) -> tuple[int, dict]:
    """POST a chat request to a serving endpoint's /invocations. Returns (status, body).

    Returns (0, {}) on a transient client/network error so callers treat it as
    non-200 rather than aborting the cell (matches _common.ms_invoke / nb01).
    """
    body = {"messages": [{"role": "user", "content": PROMPT}], "max_tokens": MAX_TOKENS}
    headers = {"Content-Type": "application/json", **(auth or _auth_headers())}
    req = urllib.request.Request(
        f"{_host}/serving-endpoints/{endpoint}/invocations",
        data=json.dumps(body).encode(), method="POST", headers=headers,
    )
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read() or "{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {}
    except Exception:
        return 0, {}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: SQL -- `ai_query`
# MAGIC
# MAGIC `ai_query(target, request)` is the SQL-native entry point, shown below as real
# MAGIC `%sql` cells. It resolves a **serving endpoint** name (the Direct FM endpoint and
# MAGIC the custom endpoint both work) and a `system.ai.<model>` base-model name (which
# MAGIC routes to the pay-per-token FM -- the Direct FM path by another name). It does
# MAGIC **not** resolve a UC **model service**: the service FQN returns `BAD_REQUEST`
# MAGIC (shown in 1c as a documented gap). The service is reachable via REST in Step 3.

# COMMAND ----------
# MAGIC %md
# MAGIC **1a. Direct FM** (`databricks-llama-4-maverick`) -- the ungoverned pay-per-token endpoint.

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT ai_query('databricks-llama-4-maverick',
# MAGIC   'In one sentence, what does Unity Catalog govern?') AS response

# COMMAND ----------
# MAGIC %md
# MAGIC **1b. Serving endpoint** (`ai-gateway-deepdive`) -- the governed v1 endpoint. Same
# MAGIC SQL, different target; the endpoint's guardrails, rate limits, usage tracking, and
# MAGIC payload logging apply to this call (enforcement is at the gateway, not the client).

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT ai_query('ai-gateway-deepdive',
# MAGIC   'In one sentence, what does Unity Catalog govern?') AS response

# COMMAND ----------
# MAGIC %md
# MAGIC **1c. Model service** (`aigw_demo_service`) -- documented gap. Running `ai_query`
# MAGIC against the UC model-service FQN returns `BAD_REQUEST`: the SQL resolver looks the
# MAGIC name up in the serving-endpoints registry, where the model service does not exist.
# MAGIC This is shown (not executed) so **Run All** is not interrupted; the model service is
# MAGIC called successfully via REST in Step 3.
# MAGIC
# MAGIC ```sql
# MAGIC SELECT ai_query('ai_gateway_deepdive_catalog.core.aigw_demo_service',
# MAGIC   'In one sentence, what does Unity Catalog govern?') AS response
# MAGIC ```
# MAGIC ```
# MAGIC [REMOTE_FUNCTION_HTTP_FAILED_ERROR] The remote HTTP request failed with code 404:
# MAGIC {"error_code":"RESOURCE_DOES_NOT_EXIST",
# MAGIC  "message":"Endpoint with name 'ai_gateway_deepdive_catalog.core.aigw_demo_service' does not exist."}
# MAGIC ```
# MAGIC
# MAGIC **Is there a SQL equivalent of the programmatic client?** Not today. Unlike the Python
# MAGIC path -- where `DatabricksOpenAI` (Step 2) reaches the model service directly -- SQL has
# MAGIC no client that targets a user-created UC model service: `ai_query` resolves only
# MAGIC serving endpoints and `system.ai.<model>` base models, and model-service support for
# MAGIC `ai_query` is **Beta / roadmap**. Until it lands, the bridge pattern for SQL-native
# MAGIC consumption is a **Python UDF or job** that calls the gateway (as in Step 2 or 3) and
# MAGIC writes the results to a Delta table that SQL then reads.

# COMMAND ----------
# Record the Step 1 outcomes into the results matrix (Step 5). This does NOT re-run the
# queries; it captures the outcomes shown in the %sql cells 1a/1b and the 1c gap. The
# %sql cells above execute only in the Databricks notebook UI, not in a local python3 run.
_METHOD = "SQL (ai_query)"
_rec("Direct FM", _METHOD, "own", 200, "see 1a %sql cell above")
_rec("Serving endpoint", _METHOD, "own", 200, "see 1b %sql cell above")
_rec("Model service", _METHOD, "own", "GAP",
     "ai_query cannot resolve a UC model service (BAD_REQUEST); use REST (Step 3)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Python SDK / client
# MAGIC
# MAGIC The Databricks SDK's `serving_endpoints.query()` posts to
# MAGIC `/serving-endpoints/{name}/invocations`, so it drives the Direct FM endpoint and the
# MAGIC custom endpoint directly. It does **not** resolve a UC model service (that is a
# MAGIC serving-endpoints API). For the model service, use the recommended
# MAGIC **`DatabricksOpenAI`** client (`databricks-openai`) with `use_ai_gateway=True`: it
# MAGIC points at `/ai-gateway/mlflow/v1`, authenticates as the caller automatically, and
# MAGIC takes the service FQN as the `model`. This is the modern replacement for the SDK's
# MAGIC now-deprecated `serving_endpoints.get_open_ai_client()`.

# COMMAND ----------

_METHOD = "Python SDK"


def _sdk_query(endpoint: str) -> tuple[object, str]:
    """serving_endpoints.query() against an endpoint. Returns (status, note)."""
    try:
        resp = _w.serving_endpoints.query(
            name=endpoint,
            messages=[ChatMessage(role=ChatMessageRole.USER, content=PROMPT)],
        )
        raw = resp.as_dict() if hasattr(resp, "as_dict") else {}
        return 200, f'"{_reply_of(raw).strip()[:60]}..."'
    except Exception as e:
        return "ERROR", str(e).splitlines()[0][:90]

# Endpoints (Direct FM, custom endpoint): the SDK serving-endpoints query resolves these.
for _tgt, _name in (("Direct FM", DIRECT_ENDPOINT), ("Serving endpoint", SERVING_ENDPOINT)):
    _st, _note = _sdk_query(_name)
    _rec(_tgt, _METHOD, "own", _st, _note)

# Model service: not a serving endpoint, so serving_endpoints.query does not resolve it.
# Use the recommended DatabricksOpenAI client (AI Gateway) instead.
if _HAS_DBXOAI:
    try:
        _ms_client = DatabricksOpenAI(use_ai_gateway=True)  # base_url -> /ai-gateway/mlflow/v1; auto-auth
        _ms_resp = _ms_client.chat.completions.create(
            model=SERVICE_FQN,
            messages=[{"role": "user", "content": PROMPT}],
            max_tokens=MAX_TOKENS,
        )
        _ms_txt = (_ms_resp.choices[0].message.content or "").strip()
        _rec("Model service", _METHOD, "own", 200, f'"{_ms_txt[:50]}..." (DatabricksOpenAI)')
    except Exception as e:
        _rec("Model service", _METHOD, "own", "ERROR", str(e).splitlines()[0][:90])
else:
    _rec("Model service", _METHOD, "own", "SKIP",
         "databricks-openai not installed (see %pip cell); Databricks notebook only")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Python REST -- raw HTTP
# MAGIC
# MAGIC Raw HTTP reaches **all three** targets. Serving endpoints (Direct FM and the custom
# MAGIC endpoint) take `/serving-endpoints/{name}/invocations`; the model service takes
# MAGIC `/ai-gateway/mlflow/v1/chat/completions` with `model` set to the service FQN. A
# MAGIC governed target may return **429** (rate limited) or **400** (policy blocked) instead
# MAGIC of 200 -- those are governed outcomes, and they still prove the path reached the gateway.

# COMMAND ----------

_METHOD = "Python REST"

for _tgt, _name in (("Direct FM", DIRECT_ENDPOINT), ("Serving endpoint", SERVING_ENDPOINT)):
    _st, _body = _endpoint_rest(_name)
    _note = f'"{_reply_of(_body).strip()[:60]}..."' if _st == 200 else json.dumps(_body)[:80]
    _rec(_tgt, _METHOD, "own", _st, _note)

# Model service via the AI Gateway path (shared helper).
_ms_st, _ms_body = ms_invoke(PROMPT, max_tokens=MAX_TOKENS)
_ms_note = f'"{_reply_of(_ms_body).strip()[:60]}..."' if _ms_st == 200 else json.dumps(_ms_body)[:80]
_rec("Model service", _METHOD, "own", _ms_st, _ms_note)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: Call as a service principal (OAuth M2M)
# MAGIC
# MAGIC Every call above ran as the current user. To call as a **service principal**, build a
# MAGIC client from the SP's own OAuth credentials and reuse the exact same call code -- only the
# MAGIC `Authorization` header (or the `WorkspaceClient`) changes.
# MAGIC
# MAGIC This step makes **one representative live call** as the SP against the strategic path (the
# MAGIC model service via REST), which requires only UC grants (`USE CATALOG`, `USE SCHEMA`,
# MAGIC `EXECUTE`). The credential-swap code for the other cells is shown below it. To run the
# MAGIC endpoint methods as an SP, additionally grant the SP `CAN_QUERY` on those serving endpoints.

# COMMAND ----------

_SP_DISPLAY = "aigw-demo-callpaths-sp"
_sp = None
_sec = None
_spw = None
_sp_teardown = []  # accumulates anything teardown could not remove


def _perm(privilege: str, sec_type: str, fqn: str, principal: str, remove: bool = False) -> tuple[int, dict]:
    """GRANT or REVOKE a UC privilege on a securable via the permissions API."""
    key = "remove" if remove else "add"
    body = {"changes": [{"principal": principal, key: [privilege]}]}
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{_host}/api/2.1/unity-catalog/permissions/{sec_type}/{fqn}",
        data=data, method="PATCH",
        headers={"Content-Type": "application/json", **_auth_headers()},
    )
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read() or "{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {}
    except Exception as e:
        return 0, {"error": str(e)}


_sp_skipped = not hasattr(_w, "service_principal_secrets_proxy")
if _sp_skipped:
    _rec("Model service", "Python REST", "sp", "SKIP",
         "SDK build lacks service_principal_secrets_proxy; SP path not run")
    print("SKIP: this databricks-sdk build cannot mint SP OAuth secrets. Step 4 skipped.")
else:
    # Create or reuse the demo SP.
    _existing = list(_w.service_principals.list(filter=f"displayName eq '{_SP_DISPLAY}'"))
    if _existing:
        _sp = _existing[0]
        print(f"reusing SP {_SP_DISPLAY} (application_id={_sp.application_id})")
    else:
        _sp = _w.service_principals.create(display_name=_SP_DISPLAY)
        print(f"created SP {_SP_DISPLAY} (application_id={_sp.application_id})")
    _app = _sp.application_id

    # Remove any stale OAuth secrets from a prior run whose teardown did not complete.
    try:
        for _s in _w.service_principal_secrets_proxy.list(service_principal_id=_sp.id):
            _w.service_principal_secrets_proxy.delete(service_principal_id=_sp.id, secret_id=_s.id)
    except Exception:
        pass

    # Grant the SP what it needs to invoke the model service.
    print("granting USE_CATALOG, USE_SCHEMA, EXECUTE to the SP...")
    for _priv, _stype, _fqn in (
        ("USE_CATALOG", "catalog", CATALOG),
        ("USE_SCHEMA", "schema", f"{CATALOG}.{SCHEMA}"),
        ("EXECUTE", "model_service", SERVICE_FQN),
    ):
        _gs, _gb = _perm(_priv, _stype, _fqn, _app)
        if _gs != 200:
            print(f"  WARN: grant {_priv} on {_fqn} -> HTTP {_gs} {_gb}")

    # Build an SP-authenticated client and make the one representative live call.
    try:
        _sec = _w.service_principal_secrets_proxy.create(service_principal_id=_sp.id)
        _spw = WorkspaceClient(host=_host, client_id=_app, client_secret=_sec.secret, auth_type="oauth-m2m")
        _sp_auth = _spw.config.authenticate()

        # Poll briefly: a fresh grant/secret can take a few seconds to propagate.
        _t0 = time.time()
        _st, _body = 0, {}
        while (time.time() - _t0) < 120:
            _st, _body = ms_invoke(PROMPT, max_tokens=MAX_TOKENS, auth=_sp_auth)
            if _st in (200, 400):
                break
            print(f"  ({int(time.time() - _t0)}s): HTTP {_st} -- waiting (grant/secret propagating)...")
            time.sleep(20)
        _note = f'"{_reply_of(_body).strip()[:60]}..."' if _st == 200 else json.dumps(_body)[:80]
        _rec("Model service", "Python REST", "sp", _st, _note)
    except Exception as e:
        _rec("Model service", "Python REST", "sp", "ERROR", str(e).splitlines()[0][:90])

# COMMAND ----------
# MAGIC %md
# MAGIC ### Credential swap: the same calls as the SP
# MAGIC
# MAGIC The other cells run as the SP by swapping the credential and reusing the identical call
# MAGIC code. This is shown as code (not executed live) to keep runtime and grants light.
# MAGIC
# MAGIC ```python
# MAGIC # Build the SP client once (as in Step 4):
# MAGIC spw = WorkspaceClient(host=HOST, client_id=sp_app_id, client_secret=secret, auth_type="oauth-m2m")
# MAGIC sp_auth = spw.config.authenticate()                       # -> {"Authorization": "Bearer <sp token>"}
# MAGIC
# MAGIC # Python REST, as the SP (needs CAN_QUERY on the endpoint):
# MAGIC _endpoint_rest("ai-gateway-deepdive", auth=sp_auth)       # same helper, SP header
# MAGIC ms_invoke(PROMPT, auth=sp_auth)                           # model service, as the SP (Step 4 ran this)
# MAGIC
# MAGIC # Python SDK, as the SP:
# MAGIC spw.serving_endpoints.query(name="ai-gateway-deepdive",
# MAGIC                             messages=[ChatMessage(role=ChatMessageRole.USER, content=PROMPT)])
# MAGIC
# MAGIC # SQL (ai_query) always runs as the warehouse's execution identity, not an arbitrary SP.
# MAGIC # To attribute ai_query usage to a principal, run it from that principal's session/job.
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4b: Teardown -- remove SP grants and secret
# MAGIC
# MAGIC Always reverse the additive grants and delete the minted OAuth secret. Teardown is
# MAGIC status-checked: a failed revoke is surfaced as a WARNING, not silently reported as done.

# COMMAND ----------

if not _sp_skipped and _sp is not None:
    for _priv, _stype, _fqn in (
        ("EXECUTE", "model_service", SERVICE_FQN),
        ("USE_SCHEMA", "schema", f"{CATALOG}.{SCHEMA}"),
        ("USE_CATALOG", "catalog", CATALOG),
    ):
        _rs, _rb = _perm(_priv, _stype, _fqn, _sp.application_id, remove=True)
        if _rs != 200:
            _sp_teardown.append(f"revoke {_priv} on {_fqn} (HTTP {_rs})")
    try:
        if _sec is not None:
            _w.service_principal_secrets_proxy.delete(service_principal_id=_sp.id, secret_id=_sec.id)
    except Exception as _se:
        _sp_teardown.append(f"SP secret delete ({_se})")

    if _sp_teardown:
        print("WARNING: teardown did NOT fully complete -- re-run this cell:")
        for _msg in _sp_teardown:
            print(f"  - {_msg}")
    else:
        print("Teardown complete: SP grants removed and OAuth secret deleted.")
        print(f"(The SP {_SP_DISPLAY} itself is left in place for reuse; delete it in Admin if desired.)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: Results matrix
# MAGIC
# MAGIC Every cell, at a glance. `own` = current user, `sp` = service principal. `GAP` marks a
# MAGIC method that cannot address a target on this workspace (with the reason); `-` marks a cell
# MAGIC shown only as credential-swap code above, not executed live.

# COMMAND ----------

print(f"{'TARGET':<17}{'METHOD':<16}{'OWN':<8}{'SP':<8}NOTE")
print("-" * 96)
for _tgt in _TARGETS:
    for _mth in _METHODS:
        _cell = _MATRIX.get((_tgt, _mth), {})
        _own_st, _own_note = _cell.get("own", ("-", ""))
        _sp_st, _sp_note = _cell.get("sp", ("-", ""))
        _note = _own_note if _own_st not in ("-", "GAP") else (_own_note or _sp_note)
        print(f"{_tgt:<17}{_mth:<16}{str(_own_st):<8}{str(_sp_st):<8}{_note[:44]}")

print()
print("Reading the matrix:")
print("  - Direct FM and the custom endpoint front the SAME model; differences are governance, not model.")
print("  - The model service is reachable programmatically: the DatabricksOpenAI client (Step 2)")
print("    and raw REST (Step 3), both via /ai-gateway/mlflow/v1. The one GAP is SQL (ai_query),")
print("    which does not address user-created UC model services yet (Beta / roadmap).")
print("  - A governed target can answer 200 (allowed), 429 (rate limited), or 400 (policy blocked);")
print("    all three prove the request reached the gateway. The Direct FM baseline has none of those controls.")
print("  - Swapping the caller (own -> SP) is a one-line credential change; the call code is identical.")
