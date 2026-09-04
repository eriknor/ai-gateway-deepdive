# Databricks notebook source
# ai-gateway-deepdive/notebooks/04_evidence_and_cost.py
#
# Unity AI Gateway -- control surfaces: EVIDENCE (system.billing.usage) + COST (budgets).
# Notebook 04 of the deep-dive demo sequence.
#
# Gateway function: evidence / cost.
# The gateway populates system.billing.usage with token and compute costs for every
# inference, and budgets cap spend at the account level. This notebook demonstrates
# the cost-visibility and governance layers above rate-limit enforcement.
#
# Covers:
#   Step 1: Cost visibility via system.billing.usage (MODEL_SERVING + AI_GATEWAY).
#   Step 2: Budget API introspection and availability check.
#   Step 3: Exec takeaway.
#
# Run inside Databricks (spark + display available) OR locally:
#   DATABRICKS_CONFIG_PROFILE=ai-gateway-deepdive python3 notebooks/04_evidence_and_cost.py
#
# Depends only on 00_setup.py having been run once (endpoint must exist).
#
# ============================================================
# BUDGET API INVESTIGATION FINDINGS
# ============================================================
# Databricks budgets are an ACCOUNT-LEVEL feature, not a workspace-level one.
# The WorkspaceClient SDK has no budget attributes at all (verified: dir(w)
# contains no 'budget' entry). The AccountClient SDK exposes:
#   - ac.budgets          (Budgets API)
#   - ac.budget_policy    (BudgetPolicy API)
# Both return NotFound when called against this account
# (05d08df7-ae03-43ad-bbae-14babb530ec0). The REST endpoint
# GET /api/2.0/accounts/{account_id}/budgets also returns 404.
#
# Possible reasons (any one may apply):
#   (a) Budgets are GA and account-admin-managed per docs, but are not available/enabled
#       on this FEVM account -- it is a demo account, not a customer account with billing
#       configured. Docs: https://docs.databricks.com/aws/en/admin/account-settings/budgets
#   (b) The feature requires an account admin to activate it via the account
#       console (accounts.cloud.databricks.com) before the API becomes reachable.
#   (c) The profile uses a workspace-scoped PAT; the budget API may require an
#       account-admin token regardless of AccountClient routing.
#
# The CLI also has no 'budget' subcommand (verified: databricks --help | grep budget).
#
# CONCLUSION: Budgets are NOT configurable from this workspace/profile at this time.
# The notebook does NOT fake a configured budget. Instead, it documents what
# budgets ARE, why they matter, and demonstrates the demonstrable cost surface:
# system.billing.usage.
# ============================================================

# COMMAND ----------
# MAGIC %md
# MAGIC # Unity AI Gateway - EVIDENCE + COST
# MAGIC
# MAGIC > _Unity AI Gateway GA/Beta status, enforcement rollout, and legacy-endpoint deprecation dates are subject to change. Verify against current Databricks documentation._
# MAGIC
# MAGIC **Runs on:** account/workspace **system tables** (`system.billing.usage`,
# MAGIC `system.serving.endpoint_usage`) and the account **Budgets API** -- cross-cutting cost
# MAGIC governance that spans both the model service and the serving endpoint, not tied to one path.
# MAGIC
# MAGIC **Gateway function: evidence / cost.**
# MAGIC
# MAGIC Unity AI Gateway exposes two layers of cost governance above application code:
# MAGIC
# MAGIC | Layer | Mechanism | Source of truth | Configured by |
# MAGIC |---|---|---|---|
# MAGIC | **Evidence** | Token/compute cost per request | `system.billing.usage` | Workspace users (read-only query) |
# MAGIC | **Cost** | Spend ceiling per time window | Budgets API | Account admin (account console or API) |
# MAGIC
# MAGIC This notebook focuses on both surfaces:
# MAGIC
# MAGIC - **EVIDENCE**: query `system.billing.usage` for token and compute costs
# MAGIC   from the last ~14 days, broken down by product line (MODEL_SERVING for inference,
# MAGIC   AI_GATEWAY for gateway overhead).
# MAGIC - **COST**: demonstrate budget API availability and explain how budgets
# MAGIC   wire to billing data.
# MAGIC
# MAGIC > **Note on payload-level logging:** Detailed request/response logging is
# MAGIC > demonstrated on the model-serving endpoint contrast in notebook 05
# MAGIC > (`system.serving.endpoint_usage` and inference tables). Payload-level
# MAGIC > logging for UC model services is a roadmap item and is not yet available
# MAGIC > in the self-serve API.

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup:** imports and the shared config and helpers from `%run ./_common` that the rest of the notebook uses.

# COMMAND ----------

import os
import sys
import json

# Local python3: resolve paths and import _common explicitly.
# Databricks notebook runtime: names are in scope from %run ./_common in the cell above.
if "__file__" in globals():
    _nb_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, _nb_dir)
    sys.path.insert(0, os.path.join(_nb_dir, "..", "src"))
    from _common import run_sql, CATALOG, SCHEMA, _w  # noqa: F401

print("Notebook 04: Unity AI Gateway - EVIDENCE + COST")
print(f"Catalog={CATALOG}, Schema={SCHEMA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Cost visibility - `system.billing.usage`
# MAGIC
# MAGIC `system.billing.usage` is the authoritative billing record for all
# MAGIC Databricks consumption. For AI Gateway governance it is the ground truth
# MAGIC for model-serving spend, including:
# MAGIC
# MAGIC - `billing_origin_product = 'MODEL_SERVING'` -- inference via serving
# MAGIC   endpoints (pay-per-token Foundation Model API calls, `ai_query()`,
# MAGIC   provisioned-throughput endpoints).
# MAGIC - `billing_origin_product = 'AI_GATEWAY'` -- Gateway overhead DBUs
# MAGIC   (routing, payload logging, guardrail evaluation).
# MAGIC
# MAGIC Budgets read from this same table internally -- so what appears here is
# MAGIC exactly what a budget threshold is measured against.

# COMMAND ----------

# Confirm the exact product filter values present on this workspace.
# MODEL_SERVING covers token-billed inference; AI_GATEWAY covers gateway overhead DBUs.
# These are the two product lines a budget for AI Gateway governance would target.
print("Step 1a: distinct billing_origin_product values in this workspace")
rows = run_sql(
    "SELECT DISTINCT billing_origin_product "
    "FROM system.billing.usage "
    "ORDER BY billing_origin_product"
)
all_products = [r[0] for r in rows]
print("  All products:", all_products)
print(
    "\n  MODEL_SERVING present:", "MODEL_SERVING" in all_products,
    "| AI_GATEWAY present:", "AI_GATEWAY" in all_products,
)

# COMMAND ----------
# MAGIC %md
# MAGIC **Daily DBU breakdown:** query `system.billing.usage` for per-day MODEL_SERVING (token + compute) and AI_GATEWAY SKUs.

# COMMAND ----------

# Daily DBU breakdown: MODEL_SERVING (token + compute) and AI_GATEWAY - last 14 days
print("\nStep 1b: MODEL_SERVING + AI_GATEWAY DBUs by day (last 14 days)")
cost_rows = run_sql("""
    SELECT
        usage_date,
        billing_origin_product,
        usage_type,
        ROUND(SUM(usage_quantity), 4) AS total_dbus
    FROM system.billing.usage
    WHERE billing_origin_product IN ('MODEL_SERVING', 'AI_GATEWAY')
      AND usage_date >= current_date() - INTERVAL 14 DAYS
    GROUP BY usage_date, billing_origin_product, usage_type
    ORDER BY usage_date DESC, billing_origin_product, usage_type
    LIMIT 42
""")

if cost_rows:
    print(f"  {'Date':<12} {'Product':<16} {'Usage Type':<16} {'Total DBUs':>16}")
    print("  " + "-" * 64)
    for row in cost_rows:
        print(f"  {row[0]:<12} {row[1]:<16} {row[2]:<16} {float(row[3]):>16,.4f}")
else:
    print("  (no rows returned -- billing lag or no serving traffic yet)")

# COMMAND ----------
# MAGIC %md
# MAGIC **Aggregate spend:** total MODEL_SERVING DBUs per day, token and compute combined.

# COMMAND ----------

# Aggregate: total MODEL_SERVING DBUs per day (token + compute combined)
print("\nStep 1c: Total MODEL_SERVING DBUs per day (all usage types combined)")
total_rows = run_sql("""
    SELECT
        usage_date,
        ROUND(SUM(usage_quantity), 2) AS total_dbus
    FROM system.billing.usage
    WHERE billing_origin_product = 'MODEL_SERVING'
      AND usage_date >= current_date() - INTERVAL 14 DAYS
    GROUP BY usage_date
    ORDER BY usage_date DESC
    LIMIT 14
""")

if total_rows:
    print(f"  {'Date':<12}  {'Total DBUs':>16}")
    print("  " + "-" * 30)
    for row in total_rows:
        print(f"  {row[0]:<12}  {float(row[1]):>16,.2f}")
else:
    print("  (no rows -- billing lag or no MODEL_SERVING traffic on this workspace)")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Reading the numbers
# MAGIC
# MAGIC The `MODEL_SERVING` product line breaks into two `usage_type` values:
# MAGIC
# MAGIC - **TOKEN** -- pay-per-token Foundation Model API consumption (the
# MAGIC   Gateway-fronted, token-billed path; includes `ai_query()` calls).
# MAGIC   This is what a budget threshold is primarily designed to cap.
# MAGIC - **COMPUTE_TIME** -- provisioned-throughput or custom model endpoint
# MAGIC   compute (billed by runtime, not tokens).
# MAGIC
# MAGIC The **AI_GATEWAY** product line captures the Gateway's own overhead DBUs
# MAGIC (payload logging writes, guardrail evaluation, routing) -- typically two
# MAGIC to three orders of magnitude smaller than MODEL_SERVING.
# MAGIC
# MAGIC A budget configured on an account applies against the union of whichever
# MAGIC products and workloads the budget targets. For most AI governance budgets,
# MAGIC the signal to watch is `MODEL_SERVING / TOKEN`.

# COMMAND ----------
# MAGIC %md
# MAGIC ### Per-user attribution: who consumed the tokens
# MAGIC
# MAGIC `system.serving.endpoint_usage` records **every** gateway/serving request with its
# MAGIC `requester` (the calling user or service principal) and per-request `input_token_count` /
# MAGIC `output_token_count`. That makes spend attributable per caller -- the chargeback / showback
# MAGIC signal finance and platform teams use, captured centrally with no application-side logging.

# COMMAND ----------

print("\nStep 1d: token usage by caller (system.serving.endpoint_usage, last 14 days)")
attrib_rows = run_sql("""
    SELECT
        requester,
        count(*) AS requests,
        SUM(input_token_count)  AS input_tokens,
        SUM(output_token_count) AS output_tokens
    FROM system.serving.endpoint_usage
    WHERE request_time > now() - INTERVAL 14 DAYS
      AND requester IS NOT NULL
    GROUP BY requester
    ORDER BY (COALESCE(SUM(input_token_count), 0) + COALESCE(SUM(output_token_count), 0)) DESC
    LIMIT 10
""")
if attrib_rows:
    print(f"  {'Requester':<44} {'Requests':>10} {'In tokens':>14} {'Out tokens':>14}")
    print("  " + "-" * 86)
    for _r in attrib_rows:
        _who = (_r[0] or "(unknown)")[:44]
        print(f"  {_who:<44} {int(_r[1] or 0):>10,} {int(_r[2] or 0):>14,} {int(_r[3] or 0):>14,}")
    print("\n  Attribution is per principal (users and service principals) -- the basis for")
    print("  chargeback / showback, and for spotting a single heavy caller.")
else:
    print("  (no rows -- no serving traffic in the last 14 days on this workspace)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Budget API introspection
# MAGIC
# MAGIC Verify what is (and is not) reachable from the current profile before
# MAGIC claiming any budget configuration.

# COMMAND ----------

from databricks.sdk import AccountClient

# Reuse the WorkspaceClient from _common (_w) instead of constructing a second one.
w = _w

# Workspace client: check for any budget-related attributes
workspace_budget_attrs = [a for a in dir(w) if "budget" in a.lower()]
print("WorkspaceClient budget attributes:", workspace_budget_attrs or "(none)")

# AccountClient routes to accounts.cloud.databricks.com (account-level features).
# Budgets are not on the workspace API -- WorkspaceClient has no budget attributes.
# Construct defensively: on a workspace-scoped profile with no account credentials,
# AccountClient() raises at construction. Report it rather than crashing the notebook.
ac = None
try:
    ac = AccountClient()
    account_budget_attrs = [a for a in dir(ac) if "budget" in a.lower()]
    print("AccountClient budget attributes:", account_budget_attrs)
except Exception as e:
    print(f"AccountClient unavailable ({type(e).__name__}: {e}); account budgets cannot be probed from this profile.")

# COMMAND ----------
# MAGIC %md
# MAGIC **Budgets API probe:** attempt to list account budgets (GA, account-admin-managed); this FEVM account returns 404, handled below.

# COMMAND ----------

# If AccountClient construction failed above, `ac` is None -- report that as the real cause
# (a client/credential issue) rather than mislabeling it as a budget-API failure.
if ac is None:
    print("\nAccountClient is unavailable (see the construction error above); skipping budget probes.")
    print("This is a profile/credential issue, not a budget-API result.")

# Try to list budgets via AccountClient
print("\nAttempting AccountClient.budgets.list() ...")
try:
    if ac is None:
        raise RuntimeError("AccountClient unavailable on this profile")
    budgets = list(ac.budgets.list())
    print(f"  Found {len(budgets)} budget(s):")
    for b in budgets:
        print("   -", b)
except Exception as e:
    print(f"  Result: {type(e).__name__} -- {str(e)}")
    print("  -> Budget API not reachable on this account/profile.")

print("\nAttempting AccountClient.budget_policy.list() ...")
try:
    if ac is None:
        raise RuntimeError("AccountClient unavailable on this profile")
    policies = list(ac.budget_policy.list())
    print(f"  Found {len(policies)} budget polic(ies):")
    for p in policies:
        print("   -", p)
except Exception as e:
    print(f"  Result: {type(e).__name__} -- {str(e)}")
    print("  -> BudgetPolicy API not reachable on this account/profile.")

# COMMAND ----------
# MAGIC %md
# MAGIC **Findings:** Both `budgets` and `budget_policy` return `NotFound` on
# MAGIC this FEVM demo account. Budgets require an account admin to activate the
# MAGIC feature. The CLI and workspace-level SDK have no budget surface at all.
# MAGIC
# MAGIC The feature exists on production accounts -- the SDK classes are present,
# MAGIC the REST API is documented, and the account console exposes a Budgets UI.
# MAGIC This demo proceeds to document the billing source of truth and reference
# MAGIC the production configuration pattern instead.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: How budgets wire to system.billing.usage
# MAGIC
# MAGIC When budgets ARE available on a customer account, the configuration
# MAGIC pattern is:
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import AccountClient
# MAGIC from databricks.sdk.service.billing import Budget, AlertConfiguration
# MAGIC
# MAGIC ac = AccountClient()   # must be account admin
# MAGIC
# MAGIC # Create a budget: 500 DBU cap on MODEL_SERVING, alert at 80 percent
# MAGIC budget = ac.budgets.create(
# MAGIC     budget=Budget(
# MAGIC         name="ai-gateway-demo-budget",
# MAGIC         filter={
# MAGIC             "tags": {"billing_origin_product": ["MODEL_SERVING"]},
# MAGIC         },
# MAGIC         period="MONTHLY",
# MAGIC         start_date="2026-08-01",
# MAGIC         target_amount="500",  # DBUs
# MAGIC         alerts=[
# MAGIC             AlertConfiguration(
# MAGIC                 time_period="MONTHLY",
# MAGIC                 trigger_type="CUMULATIVE_SPENDING_EXCEEDED",
# MAGIC                 quantity_type="TARGET_PERCENTAGE",
# MAGIC                 quantity_threshold="80",       # alert at 80 percent of ceiling
# MAGIC                 email_notifications=["platform-cost@example.com"],
# MAGIC                 action_configurations=[],      # no hard block, alert only
# MAGIC             )
# MAGIC         ],
# MAGIC     )
# MAGIC )
# MAGIC print(budget.budget_id, budget.name)
# MAGIC ```
# MAGIC
# MAGIC **Alert vs. block tradeoff:**
# MAGIC
# MAGIC | Mode | Behavior at threshold | Risk |
# MAGIC |---|---|---|
# MAGIC | Alert only | Email sent; inference continues | Spend may overshoot |
# MAGIC | Alert + block | Inference hard-blocked until window resets | Apps go down if threshold is too tight |
# MAGIC
# MAGIC For production, prefer **alert-only** until at least two weeks of baseline
# MAGIC consumption data from `system.billing.usage` is available. Set the threshold
# MAGIC at 110-120 percent of the average daily spend to catch anomalies without
# MAGIC triggering false positives.
# MAGIC
# MAGIC > **Note:** The code block above is NOT executed in this notebook because
# MAGIC > the budget API returns 404 on this FEVM demo account. It is shown as
# MAGIC > reference for production account configuration.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exec takeaway
# MAGIC
# MAGIC Unity AI Gateway provides two layers of cost governance that sit above
# MAGIC application code and work independently of rate-limit enforcement:
# MAGIC
# MAGIC ```
# MAGIC system.billing.usage (source of truth - every token + compute call logged)
# MAGIC        |
# MAGIC        v
# MAGIC   BUDGETS  <-- spend ceiling + alert/block (COST surface)
# MAGIC        |
# MAGIC        v
# MAGIC   RATE LIMITS  <-- request throttle, calls/min (RUNTIME surface)
# MAGIC        |
# MAGIC        v
# MAGIC   Endpoint / model
# MAGIC ```
# MAGIC
# MAGIC - **Budgets** sit highest: they cap total DBU spend per time window and
# MAGIC   fire alerts (or optionally hard-block) when the threshold is crossed.
# MAGIC   Configured at the account level by an account admin. Near-real-time,
# MAGIC   approximate. Track MODEL_SERVING token usage; external-model calls are
# MAGIC   not budget-tracked here.
# MAGIC
# MAGIC - **Rate limits** sit at the request level: calls/minute per user or
# MAGIC   per endpoint. They throttle throughput without reference to spend.
# MAGIC
# MAGIC - **`system.billing.usage`** is the shared source of truth for both.
# MAGIC   What appears in this table is exactly what budget thresholds are
# MAGIC   measured against, and it is the audit record finance and compliance
# MAGIC   teams use for chargebacks.
# MAGIC
# MAGIC Neither control requires application code changes. Both are configured
# MAGIC centrally and inherited by every caller behind the endpoint.

# COMMAND ----------

print("Notebook 04 complete.")
print(
    "system.billing.usage: WORKING. MODEL_SERVING + AI_GATEWAY rows queried."
)
print(
    "Budget API: NOT available on this FEVM demo account "
    "(account-scoped, requires account admin + feature activation)."
)
