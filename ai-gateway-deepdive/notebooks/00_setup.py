# Databricks notebook source
# MAGIC %md
# MAGIC # Unity AI Gateway - Setup (run this first)
# MAGIC
# MAGIC > _Unity AI Gateway GA/Beta status, enforcement rollout, and legacy-endpoint deprecation dates are subject to change. Verify against current Databricks documentation._
# MAGIC
# MAGIC Provisions everything the demo needs, idempotently. This is the only notebook with
# MAGIC infrastructure side effects; run it once before notebooks 01-05 (or re-run to update).
# MAGIC
# MAGIC **What it creates:**
# MAGIC - **UC namespace** `ai_gateway_deepdive_catalog.core`
# MAGIC - **Governed Model Serving endpoint** `ai-gateway-deepdive` (the AI Gateway config / legacy path), with all four pillars: usage tracking, inference table, rate limits, guardrails - plus fallback
# MAGIC - **Two served entities** (Llama + Claude, via the `databricks-model-serving` external-model provider) split 70 / 30
# MAGIC - **`guardrail_events` view** that the Streamlit App and AI/BI dashboard read
# MAGIC
# MAGIC **Prerequisite:** the secret `ai_gateway_demo/workspace_pat` must exist first
# MAGIC (`./scripts/set_secrets.sh ai-gateway-deepdive`). The Unity AI Gateway **model service**
# MAGIC (the strategic path) is provisioned in Step 6b below.
# MAGIC
# MAGIC **Run** as a Databricks notebook, or locally:
# MAGIC `DATABRICKS_CONFIG_PROFILE=ai-gateway-deepdive python3 notebooks/00_setup.py`

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
# ai-gateway-deepdive/notebooks/00_setup.py
#
# Creates the UC namespace and the Model Serving endpoint `ai-gateway-deepdive`
# (AI Gateway config -- the legacy governance path) with all four gateway pillars
# active. Run once before any other demo notebook, or re-run to update. The Unity
# AI Gateway model service (the strategic path) is provisioned in Step 6b below.
#
# Can be executed:
#   - As a Databricks notebook (spark / display available).
#   - Locally via: DATABRICKS_CONFIG_PROFILE=ai-gateway-deepdive python3 notebooks/00_setup.py
#     (UC catalog/schema creation is done via the Databricks CLI in that case).
#
# ============================================================
# Confirmed SDK signatures (Task 3 Step 1 - SDK 0.122.0)
# ============================================================
#   AiGatewayConfig(
#       fallback_config=None, guardrails=None, inference_table_config=None,
#       rate_limits=None, usage_tracking_config=None)
#
#   AiGatewayGuardrailParameters(
#       invalid_keywords=None, pii=None, safety=None, valid_topics=None)
#
#   AiGatewayRateLimit(
#       renewal_period: AiGatewayRateLimitRenewalPeriod,   # required
#       calls=None, key=None, principal=None, tokens=None)
#
#   ExternalModel(
#       provider: ExternalModelProvider, name: str, task: str,
#       ...,
#       databricks_model_serving_config=None, ...)
#
#   DatabricksModelServingConfig(
#       databricks_workspace_url: str,
#       databricks_api_token=None,
#       databricks_api_token_plaintext=None)
#
#   EndpointCoreConfigInput(name: str, ..., served_entities=None, traffic_config=None)
#       NOTE: name is a required first positional arg (use the endpoint name).
#
#   ServingEndpointsAPI.create(name, *, ai_gateway=None, config=None, ...)
#       -> Wait[ServingEndpointDetailed]
#
#   ServingEndpointsAPI.put_ai_gateway(name, *, fallback_config=None,
#       guardrails=None, inference_table_config=None,
#       rate_limits=None, usage_tracking_config=None)
#       -> PutAiGatewayResponse
#   NOTE: put_ai_gateway takes individual kwargs, NOT an AiGatewayConfig object.
#
#   ExternalModelProvider.DATABRICKS_MODEL_SERVING  (enum value for this provider)
# ============================================================

import os
import sys

# Make aigw importable when run from the notebooks/ directory.
# Guard __file__: classic Databricks notebook tasks run in an IPython kernel
# where __file__ is not defined. Fall back to os.getcwd() so the sys.path
# append still resolves correctly when the repo is the working directory.
_HERE = (
    os.path.dirname(os.path.abspath(__file__))
    if "__file__" in globals()
    else os.path.abspath(os.getcwd())
)
sys.path.append(os.path.join(_HERE, "..", "src"))
# notebooks/ is already on sys.path via the append above's sibling directory;
# add it explicitly so _common is importable when run from the repo root.
sys.path.append(_HERE)

from aigw.queries import guardrail_events_view_ddl

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import serving

# Databricks notebook / setup-job runtime: CATALOG, SCHEMA, ENDPOINT, SECRET_SCOPE, and
# INFERENCE_TABLE_PREFIX are already in scope from `%run ./_common` above -- and because %run
# shares this notebook's namespace, _common's _cfg() reads the job's base_parameters (widgets)
# directly, so no env-bridge is needed. Local python3: _common.py is a plain file on disk, so
# import it explicitly (a module import of a notebook-source file is blocked in the DBR runtime,
# which is why the notebook path uses %run instead).
if "__file__" in globals():
    from _common import CATALOG, SCHEMA, ENDPOINT, SECRET_SCOPE, INFERENCE_TABLE_PREFIX, ms_api  # noqa: F401

# ---------------------------------------------------------------------------
# Workspace client - profile set via DATABRICKS_CONFIG_PROFILE env var or
# ~/.databrickscfg default.
# ---------------------------------------------------------------------------
w = WorkspaceClient()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Unity Catalog namespace
# MAGIC
# MAGIC Create the catalog + schema (`ai_gateway_deepdive_catalog.core`). Inside Databricks
# MAGIC this uses `spark.sql`; run locally it falls back to the SQL Statements API via the CLI.

# COMMAND ----------
_is_notebook = "DATABRICKS_RUNTIME_VERSION" in os.environ

if _is_notebook:
    # spark is available. The catalog is normally FEVM-provisioned with ALL_PRIVILEGES
    # already; CREATE CATALOG IF NOT EXISTS still runs a metastore-level CREATE CATALOG auth
    # check that the run identity typically lacks. Attempt it, but if it is denied, fall back
    # to confirming the catalog already exists and continue (schema creation only needs
    # CREATE SCHEMA on the catalog, which this deployment holds).
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    except Exception as _cat_e:
        _catalogs = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
        if CATALOG not in _catalogs:
            raise
        print(f"catalog {CATALOG} already provisioned (no CREATE CATALOG on metastore); using it.")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    print(f"UC namespace ready: {CATALOG}.{SCHEMA}")
else:
    # Running locally: create catalog + schema via SQL execution API.
    import subprocess, shutil
    _cli = shutil.which("databricks")
    _profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "ai-gateway-deepdive")
    for _stmt in [
        f"CREATE CATALOG IF NOT EXISTS {CATALOG}",
        f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}",
    ]:
        result = subprocess.run(
            [_cli, "api", "post",
             "/api/2.0/sql/statements",
             "--profile", _profile,
             "--json",
             f'{{"statement": "{_stmt}", "warehouse_id": "auto", "wait_timeout": "30s"}}'],
            capture_output=True, text=True, timeout=60,
        )
        # Fallback: if warehouse_id=auto fails (no shared warehouse), just warn.
        if result.returncode != 0 and "warehouse" in result.stderr.lower():
            print(f"WARNING: Could not auto-create UC namespace via SQL API "
                  f"(no shared warehouse?). Run manually:\n  {_stmt}")
        else:
            print(f"UC: {_stmt!r} -> OK (or already exists)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Secret preflight
# MAGIC
# MAGIC The external-model entities reference `ai_gateway_demo/workspace_pat`, so the endpoint
# MAGIC create fails without it. Confirm the scope + key exist and fail loud with a fix hint
# MAGIC (`scripts/set_secrets.sh`) if either is missing.

# COMMAND ----------
try:
    _scopes = [s.name for s in w.secrets.list_scopes()]
    assert SECRET_SCOPE in _scopes, f"secret scope {SECRET_SCOPE!r} missing"
    _keys = [k.key for k in w.secrets.list_secrets(SECRET_SCOPE)]
    assert "workspace_pat" in _keys, f"{SECRET_SCOPE}/workspace_pat missing"
    print(f"secret preflight OK: {SECRET_SCOPE}/workspace_pat present")
except AssertionError as e:
    raise RuntimeError(
        f"{e}. Run scripts/set_secrets.sh ai-gateway-deepdive first to mint + store the PAT."
    ) from None

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Served entities (two models, one governed front door)
# MAGIC
# MAGIC Define two served entities - **Llama** and **Claude** - fronted through the
# MAGIC `databricks-model-serving` external-model provider. The PAT is referenced with the
# MAGIC `{{secrets/...}}` syntax, so the plaintext token never appears in source.

# COMMAND ----------
WORKSPACE_URL = w.config.host.rstrip("/")  # external-model entities point at THIS workspace's FM endpoints (no hardcode)
TOKEN_REF = f"{{{{secrets/{SECRET_SCOPE}/workspace_pat}}}}"


def _dbx_entity(entity_name: str, fm_endpoint: str) -> serving.ServedEntityInput:
    return serving.ServedEntityInput(
        name=entity_name,
        external_model=serving.ExternalModel(
            name=fm_endpoint,
            provider=serving.ExternalModelProvider.DATABRICKS_MODEL_SERVING,
            task="llm/v1/chat",
            databricks_model_serving_config=serving.DatabricksModelServingConfig(
                databricks_workspace_url=WORKSPACE_URL,
                databricks_api_token=TOKEN_REF,
            ),
        ),
    )


served = [
    _dbx_entity("llama", "databricks-meta-llama-3-3-70b-instruct"),
    _dbx_entity("claude", "databricks-claude-sonnet-4-5"),
]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: Traffic split (70 / 30)
# MAGIC
# MAGIC Route 70% of traffic to Llama and 30% to Claude so the split is visible in the
# MAGIC inference table (notebook 05 demonstrates it).

# COMMAND ----------
traffic = serving.TrafficConfig(routes=[
    serving.Route(served_model_name="llama", traffic_percentage=70),
    serving.Route(served_model_name="claude", traffic_percentage=30),
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: AI Gateway config - all four pillars
# MAGIC
# MAGIC Build the AI Gateway configuration: **usage tracking**, **inference table**,
# MAGIC **rate limits** (per-user + per-endpoint), **guardrails** (PII + safety), and
# MAGIC **fallback**. `put_ai_gateway()` takes individual kwargs (not an `AiGatewayConfig`),
# MAGIC so the same dict is reused for `create()` and the update path.

# COMMAND ----------
_gateway_kwargs = dict(
    usage_tracking_config=serving.AiGatewayUsageTrackingConfig(enabled=True),
    inference_table_config=serving.AiGatewayInferenceTableConfig(
        enabled=True,
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        table_name_prefix=INFERENCE_TABLE_PREFIX,
    ),
    rate_limits=[
        serving.AiGatewayRateLimit(
            renewal_period=serving.AiGatewayRateLimitRenewalPeriod.MINUTE,
            calls=5,
            key=serving.AiGatewayRateLimitKey.USER,
        ),
        serving.AiGatewayRateLimit(
            renewal_period=serving.AiGatewayRateLimitRenewalPeriod.MINUTE,
            calls=200,
            key=serving.AiGatewayRateLimitKey.ENDPOINT,
        ),
    ],
    guardrails=serving.AiGatewayGuardrails(
        input=serving.AiGatewayGuardrailParameters(
            safety=True,
            pii=serving.AiGatewayGuardrailPiiBehavior(
                behavior=serving.AiGatewayGuardrailPiiBehaviorBehavior.MASK,
            ),
        ),
        output=serving.AiGatewayGuardrailParameters(
            safety=True,
            pii=serving.AiGatewayGuardrailPiiBehavior(
                behavior=serving.AiGatewayGuardrailPiiBehaviorBehavior.MASK,
            ),
        ),
    ),
    fallback_config=serving.FallbackConfig(enabled=True),
)

gateway = serving.AiGatewayConfig(**_gateway_kwargs)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6: Create or update the endpoint (idempotent)
# MAGIC
# MAGIC If the endpoint exists, update its config + gateway; otherwise create it. Safe to re-run.

# COMMAND ----------
existing_names = [e.name for e in w.serving_endpoints.list()]
cfg = serving.EndpointCoreConfigInput(name=ENDPOINT, served_entities=served, traffic_config=traffic)

if ENDPOINT in existing_names:
    print(f"Endpoint {ENDPOINT!r} exists - updating config + gateway ...")
    w.serving_endpoints.update_config(
        name=ENDPOINT, served_entities=served, traffic_config=traffic
    ).result()
    w.serving_endpoints.put_ai_gateway(name=ENDPOINT, **_gateway_kwargs)
else:
    print(f"Creating endpoint {ENDPOINT!r} ...")
    w.serving_endpoints.create(name=ENDPOINT, config=cfg, ai_gateway=gateway).result()

print(f"endpoint ready: {ENDPOINT}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6b: Unity AI Gateway model service (the strategic object)
# MAGIC
# MAGIC Provision the UC model service `ai_gateway_deepdive_catalog.core.aigw_demo_service` that
# MAGIC notebooks 01-04 demo against.  It is the catalog-native governed object (the strategic
# MAGIC path): a 70/30 weighted traffic split across `system.ai.llama-4-maverick` and
# MAGIC `system.ai.llama_v3_3_70b_instruct` (the routing demo), a service-level 3 req/min rate
# MAGIC limit (real HTTP 429), and a `detect_sensitive_data` block-PII policy.  Routing is
# MAGIC create-time only in Beta, so a pre-existing single-destination service is recreated to
# MAGIC gain the split; otherwise the create-or-patch is idempotent.  The serving endpoint above
# MAGIC stays for the legacy contrast in notebook 05.

# COMMAND ----------
import json
import time
import urllib.request
import urllib.error
import urllib.parse

SERVICE_NAME = "aigw_demo_service"
SERVICE_FQN = f"{CATALOG}.{SCHEMA}.{SERVICE_NAME}"
MS_MODEL = "models/system.ai.llama-4-maverick"
MS_MODEL_2 = "models/system.ai.llama_v3_3_70b_instruct"

# Desired routing: two weighted pay-per-token destinations so notebook 02 can demo a live
# traffic split. Routing is create-time only in Beta (UpdateModelService rejects config.routing
# with HTTP 400), so the split must be baked in at creation -- a single-destination service is
# recreated below to gain it.
_DESIRED_ROUTING = {"destinations": [
    {"name": "maverick", "destination_type": "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL",
     "traffic_percentage": 70, "pay_per_token_config": {"model": MS_MODEL}},
    {"name": "llama33", "destination_type": "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL",
     "traffic_percentage": 30, "pay_per_token_config": {"model": MS_MODEL_2}},
]}


# The model-service REST transport lives in _common (ms_api / _ms_auth), loaded via `%run
# ./_common`. ms_api returns (status, body) and never raises -- it reports status 0 on a
# transient network error (URLError / timeout), which the retry helpers below treat as retryable
# (so a blip on the recreate path cannot leave the service deleted). No separate copy here.


def _ms_patch_ready(path: str, body: dict, what: str, cap_s: int = 90, iv_s: int = 5) -> dict:
    """PATCH the model service, retrying on HTTP 400/404 with bounded backoff.

    A freshly created Beta model service can reject config PATCHes (400/404) during
    warmup before it is ready to accept them. Without this, the first PATCH raises into
    the outer except and the service is left created but WITHOUT its rate limit and PII
    policy -- silently breaking the notebook-02 (429) and notebook-03 (PII block) demos.
    Retry the transient 400/404 until the PATCH lands; raise once cap_s is exceeded so a
    genuinely bad request still surfaces (into the outer non-fatal WARN).
    """
    _t0 = time.time()
    while True:
        _st, _resp = ms_api("PATCH", path, body)
        if _st == 200:
            return _resp
        # Retry transient states only: 0 (network), 400/404 (Beta warmup). Print the body so a
        # genuinely malformed 400 shows its real cause immediately, not only after the cap.
        if _st not in (0, 400, 404) or (time.time() - _t0) >= cap_s:
            raise RuntimeError(f"PATCH {what} failed: HTTP {_st} {_resp}")
        print(f"  ({int(time.time() - _t0)}s): PATCH {what} HTTP {_st} -- retrying (warmup/transient); response: {str(_resp)[:160]}")
        time.sleep(iv_s)


# Provision non-fatally: the Unity AI Gateway model service is Beta and requires
# account-console enablement. If it is not enabled (or provisioning otherwise fails),
# warn and continue so the endpoint preflight (Step 7) and the guardrail_events view
# (Step 8) -- which the App and dashboard require -- still run. Notebooks 01-04 need
# the model service; notebook 05, the App, and the dashboard use the endpoint path.
def _create_service(cap_s: int = 90, iv_s: int = 5) -> None:
    """Create the model service with the desired 70/30 weighted routing (create-time only).

    Retries on HTTP 400/404/409 with bounded backoff. This matters most on the recreate path:
    after a committed DELETE, the freshly-freed name can still be briefly reserved (409) or the
    create can 400 during Beta warmup. A single unretried POST there would raise into the outer
    non-fatal WARN and leave the service DELETED with no replacement, breaking notebooks 01-04.
    Poll the create until it returns 2xx; raise only after cap_s so a genuine failure surfaces.
    """
    _parent = urllib.parse.quote(f"schemas/{CATALOG}.{SCHEMA}")
    _path = f"/api/2.1/unity-catalog/model-services?parent={_parent}&model_service_id={SERVICE_NAME}"
    _body = {
        "comment": "Unity AI Gateway demo: 70/30 weighted routing + real 429 + detect_sensitive_data block",
        "config": {"routing": _DESIRED_ROUTING},
    }
    _t0 = time.time()
    while True:
        _st, _resp = ms_api("POST", _path, _body)
        if _st in (200, 201):
            return
        # Retry transient states: 0 (network -- critical after a committed DELETE, so a DNS blip
        # cannot leave the service deleted), 400/404 (warmup), 409 (name still freeing). Print the
        # body so a genuinely bad create surfaces its real cause during the retries, not only at cap.
        if _st not in (0, 400, 404, 409) or (time.time() - _t0) >= cap_s:
            raise RuntimeError(f"create model service failed: HTTP {_st} {_resp}")
        print(f"  ({int(time.time() - _t0)}s): create HTTP {_st} -- retrying (name freeing / warmup / transient); response: {str(_resp)[:160]}")
        time.sleep(iv_s)


try:
    _ms_status, _ms_get = ms_api("GET", f"/api/2.1/unity-catalog/model-services/{SERVICE_FQN}")
    if _ms_status == 200:
        _dests = _ms_get.get("config", {}).get("routing", {}).get("destinations", [])
        if len(_dests) < 2:
            # Routing is create-time only, so a pre-existing single-destination service must be
            # deleted and recreated to gain the traffic split. The rate-limit/policy PATCHes below
            # (via _ms_patch_ready) re-apply after the recreate, tolerating the Beta warmup gap.
            # NOTE (shared workspace): this opens a brief delete -> recreate window where the service
            # is momentarily absent; the recreate is immediate and idempotent, so re-running setup is
            # safe, but avoid running it while others are actively calling the service.
            print(f"model service {SERVICE_FQN} has {len(_dests)} destination(s) -- recreating with the 70/30 split...")
            ms_api("DELETE", f"/api/2.1/unity-catalog/model-services/{SERVICE_FQN}")
            time.sleep(2)
            _create_service()
            print(f"recreated model service {SERVICE_FQN} with 2 weighted destinations (70/30)")
        else:
            print(f"model service {SERVICE_FQN} exists with {len(_dests)} destinations -- patching to desired state...")
    else:
        _create_service()
        print(f"created model service {SERVICE_FQN} with 2 weighted destinations (70/30)")

    # Rate limit + service policy. The rate-limit counter only runs when a policy is present
    # (Beta), so both are PATCHed. key/renewal_period must be the enum string / integer forms.
    # A just-created Beta service may reject these PATCHes (400/404) during warmup, so retry
    # with bounded backoff -- otherwise the service is left without its rate limit and PII
    # policy, silently breaking the notebook-02 (429) and notebook-03 (PII block) demos.
    _ms_patch_ready(
        f"/api/2.1/unity-catalog/model-services/{SERVICE_FQN}?update_mask=config.rate_limits",
        {"config": {"rate_limits": [{"key": "RATE_LIMIT_KEY_SERVICE", "renewal_period": 1, "requests": 3}]}},
        "rate_limits",
    )
    _ms_patch_ready(
        f"/api/2.1/unity-catalog/model-services/{SERVICE_FQN}?update_mask=config.service_policies",
        {"config": {"service_policies": [{
            "name": "block-pii", "policy_type": "POLICY_TYPE_BUILTIN",
            "handler": "system.ai.detect_sensitive_data", "rank": 0,
            # categories is a comma-separated list of valid classification tags (docs: class.credit_card,
            # class.us_ssn, class.email_address, ...). An unrecognized value (e.g. "CREDIT_CARD") fails
            # closed and blocks EVERY prompt instead of only matching content -- breaking the selective
            # demo. Two categories here (credit card + US SSN) so notebook 03 can show the policy catch
            # more than one kind of PII. detect_sensitive_data actions are ask/block/transform (transform
            # = redact/mask). This service keeps the fast, deterministic PII policy always-on; the
            # LLM-as-judge guardrails (system.ai.block_unsafe_content, block_jailbreak, block_hallucination)
            # are demonstrated self-contained in notebook 03 so their per-call judge latency does not
            # affect the other notebooks.
            "options": {"action": "block", "categories": "class.credit_card,class.us_ssn", "phases": "pre_call", "dry_run": "false"},
        }]}},
        "service_policies",
    )
    _ms_cfg = ms_api("GET", f"/api/2.1/unity-catalog/model-services/{SERVICE_FQN}")[1].get("config", {})
    _ms_dests = _ms_cfg.get("routing", {}).get("destinations", [])
    print(
        f"model service ready: routing={[(d.get('name'), d.get('traffic_percentage')) for d in _ms_dests]}, "
        f"rate_limits={_ms_cfg.get('rate_limits')}, "
        f"policies={[p.get('name') for p in _ms_cfg.get('service_policies', [])]}"
    )
except Exception as _ms_e:
    print(
        f"WARN: could not provision the model service {SERVICE_FQN}: {_ms_e}\n"
        "  The Unity AI Gateway model service is Beta and needs account-console enablement.\n"
        "  Notebooks 01-04 require it; the legacy endpoint path (notebook 05, the App, and the\n"
        "  dashboard) is unaffected. Setup continues so the endpoint preflight and the\n"
        "  guardrail_events view still run."
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 7: Preflight - confirm every pillar is live
# MAGIC
# MAGIC Fail loud *here* (not on stage) if any pillar did not stick: assert usage tracking,
# MAGIC inference table, rate limits, guardrails, and fallback are all active.

# COMMAND ----------
ep = w.serving_endpoints.get(ENDPOINT)
gw = ep.ai_gateway
assert gw is not None, "AI Gateway not configured"
assert gw.usage_tracking_config and gw.usage_tracking_config.enabled, "usage tracking off"
assert gw.inference_table_config and gw.inference_table_config.enabled, "inference table off"
assert gw.rate_limits, "no rate limits configured"
assert gw.guardrails, "no guardrails configured"
assert gw.fallback_config and gw.fallback_config.enabled, "fallback off"
print("preflight OK: usage, inference-table, rate-limits, guardrails, fallback all active")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 8: Create the `guardrail_events` view
# MAGIC
# MAGIC The App, dashboard, and notebooks all read
# MAGIC `ai_gateway_deepdive_catalog.core.guardrail_events`. Materialise it from the canonical
# MAGIC DDL in `aigw.queries` (single source of truth) so a fresh deployment is fully operational.

# COMMAND ----------
_gv_ddl = guardrail_events_view_ddl()

if _is_notebook:
    spark.sql(_gv_ddl)
    print("guardrail_events view created/replaced")
    # Quick sanity check - confirm the view is queryable.
    _gv_count = spark.sql(
        f"SELECT guardrail_action, count(*) AS n "
        f"FROM {CATALOG}.{SCHEMA}.guardrail_events "
        f"GROUP BY guardrail_action"
    ).collect()
    for _row in _gv_count:
        print(f"  guardrail_action={_row['guardrail_action']}  n={_row['n']}")
else:
    # Local path: execute via the SQL Statements API (same warehouse-auto pattern
    # used for catalog/schema creation above).
    import json as _json
    _gv_result = subprocess.run(
        [_cli, "api", "post",
         "/api/2.0/sql/statements",
         "--profile", _profile,
         "--json",
         _json.dumps({"statement": _gv_ddl, "warehouse_id": "auto", "wait_timeout": "30s"})],
        capture_output=True, text=True, timeout=90,
    )
    if _gv_result.returncode != 0 and "warehouse" in (_gv_result.stderr or "").lower():
        print(
            "WARNING: Could not create guardrail_events view via SQL API "
            "(no shared warehouse?). Run the DDL manually:\n"
            f"  spark.sql(guardrail_events_view_ddl()) inside Databricks."
        )
    else:
        print("guardrail_events view created/replaced via SQL API")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 9: Reset helper (optional)
# MAGIC
# MAGIC `reset_demo_tables()` truncates the demo tables for a clean re-run. Call it manually
# MAGIC inside Databricks; it is defined here but not invoked.

# COMMAND ----------
def reset_demo_tables():
    """Truncate all payload/log tables in the demo schema. Run before a fresh demo."""
    if not _is_notebook:
        print("reset_demo_tables() requires a spark context (run inside Databricks)")
        return
    tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
    for t in tables:
        spark.sql(f"TRUNCATE TABLE {CATALOG}.{SCHEMA}.{t['tableName']}")
    print("demo tables truncated")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Note: guardrail behavior on `databricks-model-serving` entities
# MAGIC
# MAGIC The AI Gateway config is accepted and all pillars are active (Step 7 asserts this).
# MAGIC However, guardrail *behavior* (PII masking, safety filtering) on external entities
# MAGIC backed by `databricks-model-serving` can differ from native FM endpoints -- enforcement
# MAGIC depends on entity type. Notebook 04 (guardrails) verifies the actual firing and records
# MAGIC what this entity type does; treat a non-firing action as a documented finding, not a
# MAGIC setup failure. This notebook's scope is configuration acceptance and endpoint readiness.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup complete
# MAGIC
# MAGIC Provisioned and verified: the UC namespace, the governed endpoint `ai-gateway-deepdive`
# MAGIC (usage tracking, inference table, rate limits, guardrails, fallback), the 70 / 30 traffic
# MAGIC split, and the `guardrail_events` view. **Next:** run notebooks 01-05 in order, then open
# MAGIC the Streamlit App and the AI/BI dashboard.
