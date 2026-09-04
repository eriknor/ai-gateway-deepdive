# Databricks notebook source
# ai-gateway-deepdive/notebooks/_common.py
#
# Shared constants and chat() helper used by all demo notebooks.
# Works both as a Databricks notebook (via %run _common) and when
# imported directly (local SDK calls, smoke tests).

import os
import sys
import time
import json
import urllib.request
import urllib.error
import urllib.parse

# Make aigw importable when run as a notebook from the notebooks/ dir.
# Guard __file__: classic Databricks notebook tasks run in an IPython kernel
# where __file__ is not defined. Fall back to os.getcwd() so the sys.path
# append still resolves correctly when the repo is the working directory.
if "__file__" in globals():
    _HERE = os.path.dirname(os.path.abspath(__file__))
elif "dbutils" in globals():
    _nb_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _HERE = "/Workspace" + os.path.dirname(_nb_ctx.notebookPath().get())
else:
    _HERE = os.path.abspath(os.getcwd())
sys.path.append(os.path.join(_HERE, "..", "src"))

from aigw.result import parse_query_result, ChatResult
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from databricks.sdk.service.sql import StatementState
from databricks.sdk.errors import TooManyRequests, PermissionDenied

# NOTE: ai_gateway_demo catalog cannot be created (no CREATE CATALOG on metastore).
# The FEVM-provisioned catalog ai_gateway_deepdive_catalog has ALL_PRIVILEGES.
# ---------------------------------------------------------------------------
# Workspace-specific config (single source for all notebooks). For a fresh FEVM,
# edit the defaults here (catalog is the one that usually changes) OR override per
# run without editing: the setup job passes these as base_parameters, and any of
# them can be set as an environment variable of the same name.
# Resolution order: notebook widget / job base_parameter  ->  env var  ->  default.
# ---------------------------------------------------------------------------
_CFG_SOURCES: dict[str, str] = {}


def _cfg(key: str, default: str) -> str:
    try:
        _v = dbutils.widgets.get(key)  # noqa: F821  (job base_parameters populate widgets)
        if _v:
            _CFG_SOURCES[key] = "widget/base_parameter"
            return _v
    except Exception:
        pass
    _env = os.environ.get(key)
    if _env:
        _CFG_SOURCES[key] = "env var"
        return _env
    _CFG_SOURCES[key] = "default"
    return default


CATALOG = _cfg("AIGW_CATALOG", "ai_gateway_deepdive_catalog")
SCHEMA = _cfg("AIGW_SCHEMA", "core")
ENDPOINT = _cfg("AIGW_ENDPOINT", "ai-gateway-deepdive")
SECRET_SCOPE = _cfg("AIGW_SECRET_SCOPE", "ai_gateway_demo")
INFERENCE_TABLE_PREFIX = "aigw"
INFERENCE_TABLE = f"{CATALOG}.{SCHEMA}.{INFERENCE_TABLE_PREFIX}_payload"

# Interactive notebook runs (01-05 from the UI) get neither a widget nor an env var, so they
# fall back to the defaults above -- only the setup job receives base_parameters. Surface that
# here so a mismatched catalog is visible rather than silently reading the wrong tables. To point
# an interactive run at a different catalog, set AIGW_CATALOG (widget/env) or edit the default
# above (the README "Deploy to a workspace" sed step does the latter).
if "default" in _CFG_SOURCES.values():
    _cfg_summary = "  ".join(
        f"{_name}={_val} [{_CFG_SOURCES.get(_key, 'default')}]"
        for _key, _name, _val in (
            ("AIGW_CATALOG", "CATALOG", CATALOG),
            ("AIGW_SCHEMA", "SCHEMA", SCHEMA),
            ("AIGW_ENDPOINT", "ENDPOINT", ENDPOINT),
            ("AIGW_SECRET_SCOPE", "SECRET_SCOPE", SECRET_SCOPE),
        )
    )
    print(f"[aigw config] {_cfg_summary}")

# WorkspaceClient auto-picks up DATABRICKS_CONFIG_PROFILE from env,
# or falls back to the default profile in ~/.databrickscfg.
_w = WorkspaceClient()


def resolve_warehouse_id() -> str:
    """Return a usable SQL warehouse ID for the current workspace.

    Resolution order:
    1. DATABRICKS_WAREHOUSE_ID env var (lets the caller override explicitly).
    2. First RUNNING warehouse returned by the warehouses API
       (prefers serverless-enabled warehouses, then any RUNNING warehouse).

    Raises RuntimeError if no warehouse is available.
    """
    from_env = os.environ.get("DATABRICKS_WAREHOUSE_ID", "").strip()
    if from_env:
        return from_env

    warehouses = list(_w.warehouses.list())
    # Prefer serverless-enabled + RUNNING, then any RUNNING.
    running = [wh for wh in warehouses if getattr(wh.state, "value", str(wh.state)) == "RUNNING"]
    serverless_running = [
        wh for wh in running if getattr(wh, "enable_serverless_compute", False)
    ]
    candidates = serverless_running or running
    if not candidates:
        # Fall back to any warehouse (it may be stopped; the API will start it).
        candidates = warehouses
    if not candidates:
        raise RuntimeError(
            "No SQL warehouse found. Set DATABRICKS_WAREHOUSE_ID or create a warehouse."
        )
    return candidates[0].id


def run_sql(statement: str, warehouse_id: str | None = None) -> list[list]:
    """Execute a SQL statement and return rows as a list of lists.

    Uses the SDK statement execution API (no subprocess).
    Waits up to 60 s; raises RuntimeError on failure.
    """
    wh_id = warehouse_id or resolve_warehouse_id()
    resp = _w.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=wh_id,
        wait_timeout="30s",
    )
    # Poll if the warehouse is cold-starting and the statement is still running.
    stmt_id = resp.statement_id
    _in_flight = {StatementState.PENDING, StatementState.RUNNING}
    for _ in range(30):
        state = resp.status.state if resp.status else None
        if state not in _in_flight:
            break
        time.sleep(2)
        resp = _w.statement_execution.get_statement(stmt_id)
    state = resp.status.state if resp.status else None
    if state != StatementState.SUCCEEDED:
        raise RuntimeError(
            f"SQL statement failed (state={state}): "
            f"{resp.status.error if resp.status else resp}"
        )
    return resp.result.data_array or []


def chat(prompt: str, user: str | None = None, system: str | None = None) -> ChatResult:
    """Send a chat request to the governed endpoint and return a ChatResult.

    user   - reserved hook for per-user rate-limit attribution (Task 5).
             Forwarding via extra_params was found to break databricks-model-serving
             entities (400 "unknown field extra_params"), so it is NOT sent now.
             Task 5 will wire the correct per-user attribution mechanism.
    system - optional system message prepended to the conversation.
    """
    # SDK query() requires ChatMessage objects, not plain dicts.
    messages = []
    if system:
        messages.append(ChatMessage(role=ChatMessageRole.SYSTEM, content=system))
    messages.append(ChatMessage(role=ChatMessageRole.USER, content=prompt))
    # NOTE: extra_params is forwarded to the underlying external model. The
    # databricks-model-serving provider rejects unknown fields with 400.
    # Task 5 (rate limits) will determine the correct per-user attribution
    # mechanism for this entity type. The user= arg is retained as a hook.
    t0 = time.time()
    try:
        resp = _w.serving_endpoints.query(
            name=ENDPOINT,
            messages=messages,
        )
        latency = int((time.time() - t0) * 1000)
        raw = resp.as_dict() if hasattr(resp, "as_dict") else json.loads(str(resp))
        return parse_query_result(raw, latency_ms=latency, http_status=200)
    except TooManyRequests as e:
        # AI Gateway rate limit enforced (429). Fires for external-provider entities
        # (OpenAI, Anthropic, Azure). For DATABRICKS_MODEL_SERVING entities, calls>0
        # rate limits do not enforce (use calls=0 to block instead).
        latency = int((time.time() - t0) * 1000)
        return parse_query_result({"message": str(e)}, latency_ms=latency, http_status=429)
    except PermissionDenied as e:
        # Gateway returned 403. Common causes:
        #   - calls=0 rate limit on a DATABRICKS_MODEL_SERVING entity (gateway block)
        #   - caller lacks CAN_QUERY permission on the endpoint
        latency = int((time.time() - t0) * 1000)
        return parse_query_result({"message": str(e)}, latency_ms=latency, http_status=403)
    except Exception as e:  # gateway 4xx (guardrail block / other errors)
        latency = int((time.time() - t0) * 1000)
        status = getattr(getattr(e, "response", None), "status_code", 400)
        return parse_query_result({"message": str(e)}, latency_ms=latency, http_status=status)


# ---------------------------------------------------------------------------
# Unity AI Gateway model service (the strategic, UC-native path) -- shared by
# notebooks 01-04. The UC model-services API and the AI Gateway invocation path
# (/ai-gateway/mlflow/v1/...) are not exposed in the serving_endpoints SDK, so
# raw REST is used. 00_setup provisions the service; these helpers drive it.
# ---------------------------------------------------------------------------
HOST = _w.config.host.rstrip("/")
SERVICE_NAME = "aigw_demo_service"
SERVICE_FQN = f"{CATALOG}.{SCHEMA}.{SERVICE_NAME}"
MS_MODEL = "models/system.ai.llama-4-maverick"


def _ms_auth() -> dict:
    """Authorization headers for the current owner context (PAT or OAuth)."""
    tok = _w.config.token
    return {"Authorization": f"Bearer {tok}"} if tok else _w.config.authenticate()


def ms_api(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    """Raw REST against the UC model-services API. Returns (status, body); never raises.

    Returns (0, {}) on a transient client-side/network error (DNS, reset, timeout) so callers
    can treat it as "not 200" rather than having the exception abort the cell -- matching ms_invoke.
    """
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        HOST + path, data=data, method=method,
        headers={"Content-Type": "application/json", **_ms_auth()},
    )
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read() or "{}")
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"raw": raw}
    except Exception:
        # URLError / socket timeout / connection reset -- transient; report as non-200.
        return 0, {}


def ms_invoke(prompt: str, max_tokens: int = 16, auth: dict | None = None) -> tuple[int, dict]:
    """Invoke the model service via the AI Gateway path. Returns (status, body).

    Pass auth= to call as a different principal (e.g. a service principal's OAuth
    headers); it defaults to the current owner context. Returns 0 on a transient
    client error so poll loops treat it as retry, not a hard failure.
    """
    body = {"model": SERVICE_FQN, "messages": [{"role": "user", "content": prompt}], "max_tokens": max_tokens}
    headers = {"Content-Type": "application/json", **(auth or _ms_auth())}
    req = urllib.request.Request(
        f"{HOST}/ai-gateway/mlflow/v1/chat/completions",
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


def ms_finish_reason(body: dict) -> str | None:
    """Extract finish_reason from a chat-completions response body, or None."""
    try:
        return body.get("choices", [{}])[0].get("finish_reason")
    except Exception:
        return None
