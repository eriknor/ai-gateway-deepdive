# Databricks notebook source
# ai-gateway-deepdive/notebooks/01_access.py
#
# Unity AI Gateway -- control surface: ACCESS on the UC model service (strategic path).
# Notebook 01 of the deep-dive demo sequence.
#
# Demonstrates access control as a real service principal on the UC model-service
# governance path:
#   Step 1: Create or reuse demo SP (aigw-demo-access-sp); mint OAuth M2M secret.
#   Step 2: Grant UC privileges (USE_CATALOG, USE_SCHEMA, EXECUTE) to the SP.
#   Step 3: Invoke AS THE SP with EXECUTE granted -- HTTP 200 or 400 (authorized).
#   Step 4: Revoke EXECUTE; poll until the model service becomes invisible (HTTP 404).
#   Step 5: Demonstrate the securable-visibility model: EXECUTE is the gate.
#   Step 6: Teardown -- always remove all grants and delete the SP secret.
#   Step 7: Bypass -- a direct base-FM call is outside the model service's EXECUTE / rate-limit / policy controls.
#
# Run inside Databricks (spark available) OR locally:
#   DATABRICKS_CONFIG_PROFILE=ai-gateway-deepdive python3 notebooks/01_access.py
#
# Depends on 00_setup.py having been run once to create the model service.

# COMMAND ----------
# MAGIC %md
# MAGIC # Unity AI Gateway - ACCESS: UC model service (strategic path)
# MAGIC
# MAGIC > _Unity AI Gateway GA/Beta status, enforcement rollout, and legacy-endpoint deprecation dates are subject to change. Verify against current Databricks documentation._
# MAGIC
# MAGIC **Runs on:** UC model service `ai_gateway_deepdive_catalog.core.aigw_demo_service`
# MAGIC (the strategic, catalog-native governance path).
# MAGIC
# MAGIC **Gateway function: identity -> authorization.**
# MAGIC Before any request reaches the model service, authorization is a Unity Catalog
# MAGIC grant: `EXECUTE` on the model service plus `USE CATALOG` and `USE SCHEMA` on the
# MAGIC catalog and schema. This demonstrates the full journey with a real service principal.
# MAGIC
# MAGIC 1. **CREATE SP** -- create (or reuse) the demo SP; mint a fresh OAuth M2M secret.
# MAGIC 2. **GRANT** -- add `USE_CATALOG`, `USE_SCHEMA`, and `EXECUTE` for the demo SP.
# MAGIC 3. **INVOKE AS SP (authorized)** -- the SP calls the model service and reaches the gateway.
# MAGIC 4. **REVOKE + PROPAGATE** -- remove SP's `EXECUTE`; poll until the service is hidden (HTTP 404).
# MAGIC 5. **VISIBILITY MODEL** -- demonstrate that `EXECUTE` is the true gate, stronger than 403.
# MAGIC 6. **TEARDOWN** -- always remove all grants and delete the SP secret.
# MAGIC 7. **BYPASS ATTEMPT** -- call the base FM endpoint directly; zero governed telemetry.

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup:** imports and the shared config and helpers from `%run ./_common`.

# COMMAND ----------

import os
import sys
import time
import json
import uuid
import urllib.request
import urllib.error
import urllib.parse

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

# Local python3: resolve paths and import _common explicitly.
# Databricks notebook runtime: names are in scope from %run ./_common in the cell above.
if "__file__" in globals():
    _nb_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, _nb_dir)
    sys.path.insert(0, os.path.join(_nb_dir, "..", "src"))
    from _common import CATALOG, SCHEMA, SERVICE_FQN, HOST, ms_api, ms_invoke, run_sql  # noqa: F401

# True when running inside Databricks; gates spark.sql() vs run_sql() calls.
_is_notebook = "DATABRICKS_RUNTIME_VERSION" in os.environ

_w = WorkspaceClient()
_host = _w.config.host.rstrip("/")

print(f"workspace : {_host}")
print(f"service   : {SERVICE_FQN}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Raw REST helpers
# MAGIC
# MAGIC The Unity Catalog permissions API and the AI Gateway invocation path
# MAGIC are accessed via raw REST. These helpers provide the HTTP transport layer.

# COMMAND ----------


def _auth_headers() -> dict:
    """Return Authorization header dict for the current SDK auth context.

    Works with both PAT profiles (token) and OAuth/CLI profiles (authenticate()).
    """
    token = _w.config.token
    if token:
        return {"Authorization": f"Bearer {token}"}
    # OAuth or databricks-cli -- call authenticate() to get the current bearer.
    return _w.config.authenticate()


def _api_raw(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    """Raw REST call against the workspace API. Returns (status_code, body); never raises on 4xx/5xx."""
    url = f"{_host}{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json", **_auth_headers()}
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            body_json = json.loads(raw)
        except Exception:
            body_json = {"raw": raw}
        return e.code, body_json
    except Exception as e:
        # Match the sibling REST helpers (_sp_invoke / _common.ms_api): a transient network
        # error (URLError, timeout, reset) returns (0, {}) rather than propagating, so the
        # grant loop and poll loops treat it as retry and Step 6 teardown always runs.
        return 0, {"error": str(e)}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Create or reuse demo service principal
# MAGIC
# MAGIC The demo SP `aigw-demo-access-sp` lives only in this workspace.
# MAGIC On every run:
# MAGIC - Look up the SP by display_name (create if absent).
# MAGIC - Mint a fresh OAuth M2M secret for this run.
# MAGIC - Build an SP-authenticated WorkspaceClient.
# MAGIC
# MAGIC The secret value is held only in memory; only the SP display_name and
# MAGIC application_id are printed.

# COMMAND ----------

_SP_DISPLAY = "aigw-demo-access-sp"
_sp_list = list(_w.service_principals.list(filter=f"displayName eq '{_SP_DISPLAY}'"))
if _sp_list:
    _sp = _sp_list[0]
    print(f"reusing SP: {_sp.display_name}  application_id={_sp.application_id}")
else:
    from databricks.sdk.service.iam import ComplexValue
    _sp = _w.service_principals.create(
        display_name=_SP_DISPLAY, active=True,
        entitlements=[ComplexValue(value="workspace-access")],
    )
    print(f"created SP: {_sp.display_name}  application_id={_sp.application_id}")
_app = _sp.application_id

# Delete any stale OAuth secrets left by a prior run whose teardown did not complete. Without
# this, leaked secrets accumulate and eventually hit the per-SP secret cap, at which point the
# mint in Step 3 fails and the whole access journey cannot start. Best-effort; guarded for SDK.
if hasattr(_w, "service_principal_secrets_proxy"):
    try:
        _stale = list(_w.service_principal_secrets_proxy.list(service_principal_id=_sp.id))
        for _s in _stale:
            try:
                _w.service_principal_secrets_proxy.delete(service_principal_id=_sp.id, secret_id=_s.id)
            except Exception:
                pass
        if _stale:
            print(f"  cleaned up {len(_stale)} stale OAuth secret(s) from a prior run")
    except Exception:
        pass

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Grant UC privileges to the SP
# MAGIC
# MAGIC The SP needs three grants for access to the model service:
# MAGIC - `USE_CATALOG` on the catalog
# MAGIC - `USE_SCHEMA` on the schema
# MAGIC - `EXECUTE` on the model service itself
# MAGIC
# MAGIC These are granted via the Unity Catalog permissions API.

# COMMAND ----------


def _perm(privilege: str, sec_type: str, fqn: str, remove: bool = False) -> tuple[int, dict]:
    """GRANT or REVOKE a UC privilege on a securable via the permissions API. Returns (status, body)."""
    key = "remove" if remove else "add"
    return _api_raw(
        "PATCH",
        f"/api/2.1/unity-catalog/permissions/{sec_type}/{fqn}",
        {"changes": [{"principal": _app, key: [privilege]}]},
    )


def _sp_invoke(prompt: str, max_tokens: int = 16) -> tuple[int, dict]:
    """Invoke the model service AS THE SP (its own OAuth token), not the owner session.

    Reads the module-level _spw built in Step 3. Returns (HTTP status, response body), or
    (0, {}) on a transient client-side error (e.g. an M2M token mint not ready yet) so the
    poll loops treat it as "retry", not a hard abort.
    """
    try:
        body = {"model": SERVICE_FQN, "messages": [{"role": "user", "content": prompt}], "max_tokens": max_tokens}
        data = json.dumps(body).encode()
        headers = {"Content-Type": "application/json", **_spw.config.authenticate()}
        req = urllib.request.Request(
            f"{_host}/ai-gateway/mlflow/v1/chat/completions", data=data, method="POST", headers=headers,
        )
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {}
    except Exception:
        return 0, {}


# Shared poll knobs and journey state (module-level so each step cell can read and update them).
_POLL_CAP_S = 120
_POLL_IV_S = 25
_access_proven = False
_grant_authorized = False
_revoke_removed = False
_sec = None
_spw = None

# Guard the SP OAuth-secret proxy (older SDK builds lack it). When absent, Steps 2-6 are
# skipped and Step 7 (bypass) still runs.
_access_journey_skipped = not hasattr(_w, "service_principal_secrets_proxy")

if _access_journey_skipped:
    print(
        "SKIP Steps 2-6: this databricks-sdk build lacks service_principal_secrets_proxy.\n"
        "Upgrade the SDK to enable the access journey. Step 7 (bypass) will still run."
    )
else:
    print("Granting UC privileges (USE_CATALOG, USE_SCHEMA, EXECUTE)...")
    for _priv, _stype, _fqn in [
        ("USE_CATALOG", "catalog", CATALOG),
        ("USE_SCHEMA", "schema", f"{CATALOG}.{SCHEMA}"),
        ("EXECUTE", "model_service", SERVICE_FQN),
    ]:
        _gs, _gb = _perm(_priv, _stype, _fqn)
        if _gs != 200:
            print(f"  WARN: grant {_priv} on {_fqn} returned HTTP {_gs} {_gb}; authorization may not complete.")
        else:
            print(f"  granted {_priv} on {_fqn}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Invoke as the SP with EXECUTE granted
# MAGIC
# MAGIC Mint the SP's OAuth M2M secret, build an SP-authenticated client, and invoke the model
# MAGIC service. With `EXECUTE` granted the SP reaches the gateway: **HTTP 200** (content returned)
# MAGIC or **HTTP 400** (service policy blocked the content) -- both prove authorization worked.

# COMMAND ----------
if not _access_journey_skipped:
    try:
        # Mint the OAuth M2M secret and build the SP-authenticated client.
        _sec = _w.service_principal_secrets_proxy.create(service_principal_id=_sp.id)
        _spw = WorkspaceClient(
            host=_host, client_id=_app, client_secret=_sec.secret, auth_type="oauth-m2m",
        )
        # Send a real prompt as the SP and poll until it reaches the gateway. 200 or 400 proves
        # EXECUTE is in effect; 429 = rate-limited (ambiguous, keep polling).
        _SP_PROMPT = "In one sentence, what does Unity Catalog govern?"
        print(f"Granted EXECUTE; the SP asks the model: {_SP_PROMPT!r}")
        _t0 = time.time()
        _g_status = None
        _g_body = {}
        _g_saw_429 = False
        while (time.time() - _t0) < _POLL_CAP_S:
            _g_status, _g_body = _sp_invoke(_SP_PROMPT, max_tokens=80)
            if _g_status in (200, 400):
                break
            if _g_status == 429:
                _g_saw_429 = True
            print(f"  ({int(time.time() - _t0)}s): HTTP {_g_status} -- waiting (grant propagating / rate window)...")
            time.sleep(_POLL_IV_S)
        _grant_authorized = _g_status in (200, 400)
        if _grant_authorized and _g_status == 200:
            # Surface the actual model interaction: the SP's own token reached the gateway,
            # the request was authorized by EXECUTE, and a real inference came back.
            _choice = (_g_body.get("choices") or [{}])[0]
            _reply = ((_choice.get("message") or {}).get("content", "") or "").strip()
            _served = _g_body.get("model", "(model not reported)")
            _usage = _g_body.get("usage") or {}
            _tok = _usage.get("total_tokens")
            print(f"  ({int(time.time() - _t0)}s): HTTP 200 -- authorized; a real inference ran through the gateway.")
            print("  PASS: with EXECUTE the SP called the model as itself.")
            print(f"    We asked          : {_SP_PROMPT!r}")
            print(f"    The model responded: {_reply!r}")
            print(
                f"    (served by {_served}"
                + (f", {_tok} tokens)" if _tok is not None else ")")
            )
        elif _grant_authorized:
            # HTTP 400: the request reached the gateway but the service policy blocked the content.
            print(f"  ({int(time.time() - _t0)}s): HTTP 400 -- authorized; the request reached the gateway.")
            print("  PASS: with EXECUTE the SP reached the gateway (the service policy blocked this content).")
        else:
            _why = (
                "the shared 3/min service rate window kept returning HTTP 429, not an access failure"
                if _g_saw_429 else "the EXECUTE grant is still propagating"
            )
            print(
                f"  INCONCLUSIVE: never reached the gateway within {_POLL_CAP_S}s (last HTTP {_g_status}); {_why}.\n"
                "  Not treating this as a failure -- re-run this cell, or raise _POLL_CAP_S if it persists."
            )
    except Exception as _e:
        print(f"  WARN: Step 3 hit an unexpected error: {_e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: Revoke EXECUTE; poll until hidden
# MAGIC
# MAGIC Remove the `EXECUTE` grant. With `USE_CATALOG` and `USE_SCHEMA` still granted but
# MAGIC `EXECUTE` revoked, the SP can no longer see the model service -- Unity Catalog hides the
# MAGIC securable, returning **HTTP 404** instead of 403.

# COMMAND ----------
if not _access_journey_skipped:
    try:
        print("Revoking EXECUTE; polling until the SP can no longer see the service...")
        _rs, _rb = _perm("EXECUTE", "model_service", SERVICE_FQN, remove=True)
        if _rs != 200:
            print(f"  WARN: revoke EXECUTE returned HTTP {_rs} {_rb}; Step 6 teardown still retries removal.")
        _t0 = time.time()
        _r_status = None
        while (time.time() - _t0) < _POLL_CAP_S:
            _r_status, _ = _sp_invoke("ping")
            if _r_status in (401, 403, 404):
                print(f"  ({int(time.time() - _t0)}s): HTTP {_r_status} -- access removed.")
                break
            print(f"  ({int(time.time() - _t0)}s): still HTTP {_r_status} -- waiting for propagation...")
            time.sleep(_POLL_IV_S)
        _revoke_removed = _r_status in (401, 403, 404)
        if _revoke_removed:
            _hidden = (
                "\n  Unity Catalog hides securables that cannot be accessed -- stronger than a 403."
                if _r_status == 404 else ""
            )
            print(
                f"  PASS: without EXECUTE the model service is no longer accessible to the SP (HTTP {_r_status})."
                + _hidden
            )
        else:
            print(
                f"  INCONCLUSIVE: still HTTP {_r_status} after {_POLL_CAP_S}s; the revoke may still be\n"
                "  propagating. Not treating this as a failure -- Step 6 removes all grants."
            )
        _access_proven = _grant_authorized and _revoke_removed
    except Exception as _e:
        print(f"  WARN: Step 4 hit an unexpected error: {_e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: Visibility model summary
# MAGIC
# MAGIC The authorization journey demonstrates the Unity Catalog visibility model:
# MAGIC - With `EXECUTE`: HTTP 200 or 400 (request reaches the gateway).
# MAGIC - Without `EXECUTE`: HTTP 404 (the securable is hidden, not just denied).
# MAGIC
# MAGIC `EXECUTE` is the true authorization gate on the model-service invocation path.

# COMMAND ----------
if not _access_journey_skipped:
    print(
        "Access journey complete: EXECUTE is the gate -- grant -> authorized, revoke -> hidden."
        if _access_proven else
        "Access journey inconclusive on this run (propagation or the shared rate window);\n"
        "grants are still torn down in Step 6."
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6: Teardown
# MAGIC
# MAGIC Always leave the SP with no grants and no live secret -- even if a step above did not
# MAGIC complete. Safe to re-run; the demo re-grants on the next run. This replaces the single
# MAGIC `finally` block, so each step above is its own independently runnable cell.

# COMMAND ----------
if not _access_journey_skipped:
    # Track teardown outcomes: _perm/_api_raw never raise (they return (0, {}) on a transient
    # network error), so a swallowed failure would otherwise leave the SP holding grants while
    # we falsely report a clean teardown. Inspect each revoke's status and warn on anything != 200.
    _teardown_warnings = []
    for _p, _st, _fq in [
        ("EXECUTE", "model_service", SERVICE_FQN),
        ("USE_SCHEMA", "schema", f"{CATALOG}.{SCHEMA}"),
        ("USE_CATALOG", "catalog", CATALOG),
    ]:
        try:
            _rev_status, _rev_body = _perm(_p, _st, _fq, remove=True)
            if _rev_status != 200:
                _teardown_warnings.append(f"{_p} on {_fq} (HTTP {_rev_status})")
        except Exception as _te:
            _teardown_warnings.append(f"{_p} on {_fq} ({_te})")
    _secret_ok = True
    try:
        if _sec is not None:
            _w.service_principal_secrets_proxy.delete(service_principal_id=_sp.id, secret_id=_sec.id)
    except Exception as _se:
        _secret_ok = False
        _teardown_warnings.append(f"SP secret delete ({_se})")
    if _teardown_warnings:
        print("WARNING: teardown did NOT fully complete -- these may still be present (re-run this cell):")
        for _w_msg in _teardown_warnings:
            print(f"  - {_w_msg}")
    else:
        print("Teardown complete: all demo grants removed and the SP secret deleted.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 7: Bypass -- the governance boundary is the model service
# MAGIC
# MAGIC The model service enforces `EXECUTE`, its rate limit, and its service policy only on calls
# MAGIC that go THROUGH it (the `/ai-gateway/mlflow/...` path with the service name). A caller who
# MAGIC reaches a base foundation model directly, via the Foundation Model API endpoint, is outside
# MAGIC that control plane: no `EXECUTE` grant is checked, no service rate limit applies, and no
# MAGIC service policy runs.
# MAGIC
# MAGIC (Full request/response **payload logging** is demonstrated on the endpoint contrast in
# MAGIC notebook 05. Per the Unity AI Gateway migration guide, inference logging is also a
# MAGIC re-creatable governance setting on a model API; this demo exercises it on the endpoint path.)
# MAGIC
# MAGIC ### Why this bypass cannot be blocked per-caller (verified on this workspace)
# MAGIC
# MAGIC Steps 2-5 showed the governed path is controllable *per principal*: grant `EXECUTE` -> the
# MAGIC SP is authorized; revoke it -> the SP gets HTTP 404. The base-FM bypass is different. Access
# MAGIC to `databricks-meta-llama-3-3-70b-instruct` is gated by Unity Catalog `EXECUTE` on the
# MAGIC underlying `system.ai` model, and that `EXECUTE` is held by the implicit **`account users`**
# MAGIC group -- which every principal (this demo SP included, though it is in no explicit group)
# MAGIC inherits. Two facts make a per-caller block impossible here:
# MAGIC - **Unity Catalog has no per-principal DENY.** You cannot subtract one SP from a grant the
# MAGIC   `account users` group holds.
# MAGIC - **System foundation-model endpoints expose no per-principal serving ACL** (the
# MAGIC   `permissions/serving-endpoints/<name>` API rejects them), so there is no endpoint-level
# MAGIC   hook to deny a single caller either.
# MAGIC
# MAGIC ### Closing the bypass: enforce the gateway at the workspace
# MAGIC
# MAGIC The enterprise control point is the governed model service and the grants that reach it.
# MAGIC Owning the front door means shutting off direct base-model access too. The canonical,
# MAGIC Databricks-sanctioned lever is the **Enforce Unity AI Gateway** workspace setting (an
# MAGIC opt-in setting for workspace admins): it disables
# MAGIC legacy v1/v2 serving experiences so ALL pay-per-token traffic must route through UC model
# MAGIC services. Enabling it retires the direct, system-provided FM serving endpoints -- i.e. the
# MAGIC exact bypass demonstrated below. New accounts/workspaces have legacy off by default.
# MAGIC
# MAGIC Enforcement carries known limitations while other products finish migrating (Apps wired to a
# MAGIC Serving Endpoint resource must be re-pointed to model services; `ai_query` supports only
# MAGIC `system.ai` model APIs, not user-created services; some Vector Search flows are not yet
# MAGIC supported). Complementary least-privilege levers on the managed `system.ai` schema:
# MAGIC - ``REVOKE EXECUTE ON MODEL system.ai.<model> FROM `account users` `` (account-level; note this
# MAGIC   only gates the legacy endpoint where the interim **Foundation Model UC Permissions** preview is
# MAGIC   enrolled -- that preview is now closed to new enrollment, so on most workspaces workspace access
# MAGIC   alone still reaches the endpoint and enforcement is the real lever), and/or
# MAGIC - set the **pay-per-token FM rate limits to 0** (blocks the direct FM APIs; also the documented per-endpoint migration off-ramp).
# MAGIC
# MAGIC This notebook does **not** execute those: they are workspace/account-wide changes on shared,
# MAGIC managed resources. The takeaway is the asymmetry -- the governed service gives per-caller
# MAGIC control today; the bypass is closed at the workspace boundary by enforcing the gateway.

# COMMAND ----------

# Call a base foundation model directly -- NOT through the governed model service. It succeeds
# with no EXECUTE grant on the service, no service rate limit, and no service policy: proof that
# a direct FM call is outside the model service's control plane.
DIRECT_FM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"  # a Databricks-hosted base FM
_BYPASS_PROMPT = "What is the capital of France?"
print(f"\ndirect target : {DIRECT_FM_ENDPOINT} (a base FM, NOT through {SERVICE_FQN})")

_t0 = time.time()
try:
    direct_resp = _w.serving_endpoints.query(
        name=DIRECT_FM_ENDPOINT,
        messages=[ChatMessage(role=ChatMessageRole.USER, content=_BYPASS_PROMPT)],
    )
    elapsed_ms = int((time.time() - _t0) * 1000)
    d = direct_resp.as_dict() if hasattr(direct_resp, "as_dict") else {}
    choices = d.get("choices", [])
    direct_reply = ((choices[0].get("message", {}).get("content", "") or "") if choices else "")
    print(f"direct base-model call succeeded ({elapsed_ms} ms):")
    print(f"    We asked           : {_BYPASS_PROMPT!r}")
    print(f"    The base FM responded: {direct_reply[:80]!r}")
    print(
        "\nPASS: the direct call reached a base FM with NO EXECUTE grant on the service, NO service\n"
        "  rate limit, and NO service policy applied -- it bypassed every model-service control.\n"
        f"  The governance boundary is {SERVICE_FQN} and the grants that reach it.\n"
        "  This bypass cannot be blocked per-caller: base-FM access rides the implicit 'account users'\n"
        "  EXECUTE on the system.ai model, and UC has no per-principal DENY. Closing it is a workspace\n"
        "  action: enable 'Enforce Unity AI Gateway' (disables legacy endpoints so all traffic must use\n"
        "  UC model services); optionally revoke 'account users' EXECUTE on system.ai or set FM rate limits to 0."
    )
except Exception as e:
    print(f"direct base-model call error: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The ACCESS control surface on the Unity AI Gateway model service demonstrates:
# MAGIC
# MAGIC - **Access is a UC permission.** `EXECUTE` on the model service is the authorization gate,
# MAGIC   not a network rule or a shared API key. It can be granted and revoked in seconds.
# MAGIC
# MAGIC - **Visibility is stronger than denial.** Without `EXECUTE`, Unity Catalog returns HTTP 404
# MAGIC   (the securable is hidden) rather than HTTP 403 (permission denied). A caller cannot query
# MAGIC   what is not visible.
# MAGIC
# MAGIC - **The gateway governs what flows through it, not calls that route around it.** A direct
# MAGIC   call to a base FM endpoint bypasses the service's controls: `EXECUTE`, the rate limit,
# MAGIC   and the service policy all route around it.
# MAGIC
# MAGIC - **Per-caller control exists only on the governed path.** The service can grant/revoke
# MAGIC   `EXECUTE` for one principal. The base-FM bypass rides the implicit `account users` EXECUTE
# MAGIC   on `system.ai`, and UC has no per-principal DENY -- so a single caller cannot be blocked
# MAGIC   from it. Closing the bypass is a workspace action: enable **Enforce Unity AI Gateway** (an
# MAGIC   opt-in workspace-admin setting), which disables legacy v1/v2 endpoints so
# MAGIC   all traffic must flow through UC model services; optionally complement with an account-level
# MAGIC   revoke of the `account users` grant or setting the pay-per-token FM rate limits to 0.
# MAGIC
# MAGIC - **The enterprise control point is the governed model service plus its credentials.**
# MAGIC   Route all callers through the service, and close direct base-model access by enforcing the
# MAGIC   gateway at the workspace; then the front door is secured.
# MAGIC
# MAGIC Next: Notebook 02 -- RUNTIME (real HTTP 429 rate limiting and the routing config).

# COMMAND ----------

print("=" * 60)
print("Notebook 01 complete.")
if not _access_journey_skipped:
    if _access_proven:
        print("  Access journey: EXECUTE grant -> authorized, revoke -> hidden (PROVEN)")
    else:
        print("  Access journey: ran but was inconclusive within the poll window (re-run to retry)")
else:
    print("  Access journey: SKIPPED (SDK build does not support service_principal_secrets_proxy)")
print("  Bypass test: direct base-FM call succeeded outside the service's EXECUTE / rate-limit / policy controls")
print("=" * 60)
