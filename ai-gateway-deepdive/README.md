# Unity AI Gateway -- Deep Dive Demo

**Unity AI Gateway -- the governed front door, not a universal proxy.**

> **Status note.** Unity AI Gateway is evolving rapidly. Feature availability (GA/Beta), the "Enforce Unity AI Gateway" enforcement rollout, and legacy-endpoint (v1/v2) deprecation timelines in this material are **subject to change** -- verify against the current Databricks documentation before relying on any status claim.

Unity AI Gateway sits between the calling applications and the underlying language models.  It enforces identity, applies rate limits, runs content policy checks, logs payloads (demonstrated here on the serving-endpoint path; inference logging is also a re-creatable setting on a UC model API), and tracks cost, all in one control plane that the platform team configures and app teams consume transparently. It does **not** govern direct SDK or PAT calls that go straight to a base model endpoint: those calls bypass the gateway entirely, leaving no governed telemetry behind (the workspace-level **Enforce Unity AI Gateway** setting closes that gap by disabling legacy direct-serving paths).

**Who runs this, and where.** The demo runs the same way whether a **Databricks FDE** stands it up on a disposable **FEVM workspace** or a **client** deploys it to their **own existing workspace** -- the only difference is where the workspace, catalog, and warehouse come from. The bundle is parameterized, so switching between the two is a one-file edit (see **Prerequisites** and **Deploy to a workspace**).

---

## 5-Act Exec Talk Track

The demo leads with the **Unity AI Gateway model service** (the strategic, UC-native path) across four governance surfaces, then contrasts the **legacy serving-endpoint** path.  Each act maps to one notebook.

### Act 1 -- Access (ACCESS surface)

**Notebook:** `01_access.py` (model service)

A dedicated service principal (`aigw-demo-access-sp`) drives a **real** UC access journey: grant `USE CATALOG` + `USE SCHEMA` + `EXECUTE`, invoke as the SP with its own OAuth token (HTTP 200 -- a real model answer comes back through the gateway), then revoke `EXECUTE` and invoke again -- Unity Catalog **hides** the securable (HTTP 404), not a 403.  A final bypass step calls a base foundation model directly, outside the service: it succeeds with no `EXECUTE` check, no rate limit, and no policy -- the governed boundary is the model service.  (Closing that bypass is a workspace action: enable **Enforce Unity AI Gateway** (an opt-in workspace-admin setting) which disables legacy v1/v2 endpoints so all traffic must route through UC model services; base-FM access otherwise rides the implicit `account users` EXECUTE on `system.ai`, and UC has no per-principal deny.)

**Exec takeaway:** access to a model service is a UC privilege (`EXECUTE`), not a shared key or a network rule.  Without it the service is not even visible (404), which is stronger than permission-denied (403).  Direct base-model calls bypass every gateway surface.

### Act 2 -- Runtime (RUNTIME surface)

**Notebook:** `02_runtime.py` (model service)

Fire rapid calls at the service (provisioned with a 3 req/min service-level limit): once the budget is spent, calls return a **real HTTP 429** ("User defined rate limit(s) exceeded").  Then a **live routing tally**: the service is created with a 70/30 weighted split across two FMs, and repeated calls land on each model in proportion (routing is create-time only -- `config.routing` is not PATCHable in Beta).  A token-per-minute (TPM) limit variant is also demonstrated (distinct "Tokens-per-minute (TPM)" 429).

**Exec takeaway:** the UC model service enforces per-service rate limits (requests/min and tokens/min) with standard, retry-friendly **HTTP 429** semantics, plus weighted traffic routing.  Note these runtime features are **not** model-service-exclusive: legacy serving endpoints also enforce rate limits (verified live -- a 3/min endpoint limit returned HTTP 429) and support routing/fallback.  The real differentiator is **governance and lifecycle** -- the model service is a UC-native securable (`EXECUTE`, revoke->404, ABAC), governed centrally across workspaces, and it is the strategic path as legacy v1/v2 endpoints are deprecated.

### Act 3 -- Policy (POLICY surface)

**Notebook:** `03_policy.py` (model service)

A `system.ai.detect_sensitive_data` service policy (action `block`, categories `class.credit_card,class.us_ssn`) denies PII prompts (HTTP 200 with finish_reason `content_filter`; not HTTP 4xx).  The model service also carries **LLM-as-judge** policies -- `block_unsafe_content` (safety / content moderation) and `block_jailbreak` (prompt-injection defense) -- demonstrated live in the same notebook.  Redaction is the `transform` action (verified accepted): it masks matched values in place (e.g. `[CREDIT_CARD]`) and forwards the request; this demo exercises both `block` (deny) and `transform` (redact/mask, then forwarded) live.

**Exec takeaway:** policy is a named, ranked UC service policy attached to the service and enforced before the model is called.  The model service offers four built-in handlers -- `detect_sensitive_data` (PII; ask/block/transform), `block_unsafe_content` (safety), `block_jailbreak`, and `block_hallucination`.

### Act 4 -- Evidence and Cost (EVIDENCE + COST surfaces)

**Notebook:** `04_evidence_and_cost.py` (cost + evidence, cross-cutting system tables)

Query `system.billing.usage` for the model service's `MODEL_SERVING` (token + compute) and `AI_GATEWAY` spend, show **per-user attribution** (`system.serving.endpoint_usage` by `requester`, with token totals), and probe account budgets (GA, account-admin-managed; 404 on this FEVM account).  Payload-level inference-table logging is a serving-endpoint capability today (a model-service roadmap gap), so it is demonstrated on the contrast in Act 5.

**Exec takeaway:** model-service spend lands in the billing usage tables, is attributable per caller for chargeback, and is capped by GA budgets; full request/response payload logging currently rides the endpoint path.

### Act 5 -- Legacy contrast: serving endpoint + AI Gateway config

**Notebook:** `05_serving_endpoint_contrast.py` (serving endpoint)

The GA, workspace-scoped path Databricks is migrating away from.  It shows the two things the model service does not do UC-natively today: **payload / inference-table logging** (a governed call is captured to the inference table) and the honest **fallback** finding (automatic failover does not engage on `databricks-model-serving` entities).  It also contrasts the denial shapes: endpoint `CAN_QUERY` (403, visible but not callable) vs model-service `EXECUTE` (404, hidden).

**Exec takeaway:** the serving-endpoint AI Gateway config is GA and fully supported, but workspace-bound and the migrate-from path.  Start new work on UC model services.

## Serving Endpoint (AI Gateway config) vs. Unity AI Gateway Model Service

Unity AI Gateway exposes two **distinct constructs** at different lifecycle stages. Both appear in this demo. They are not interchangeable configs of one object -- they differ in resource model, scope, authorization, reuse, and product direction.

**Decision rule:** existing serving endpoint, custom entity, or provisioned throughput -> use the endpoint overlay. New work, centralized governance, or cross-workspace reuse -> use UC model services.

| Dimension | Model Serving endpoint (AI Gateway config, legacy path) | Unity AI Gateway model service (strategic) |
|-----------|--------------------------------------|-------------------------------|
| What it is | Governance overlaid on an EXISTING serving endpoint (workspace-scoped) | A Unity Catalog securable governing a model API (catalog.schema.name) |
| Setup | `PUT /ai-gateway` on an existing serving endpoint | `databricks ai-gateway create-model-service` |
| Notebooks | 05 (endpoint contrast) | 01-04 (access, runtime, policy, evidence+cost) |
| Rate limits | Real RPM/TPM throttle with HTTP 429 (verified live: a 3/min endpoint limit returned 429); calls=0 also hard-blocks -- the documented per-endpoint migration off-ramp | Real RPM + TPM throttle with HTTP 429 ("User defined rate limit(s) exceeded") |
| Guardrails (input PII) | BLOCKED (HTTP 400, `input_guardrail_triggered`) | BLOCKED via `detect_sensitive_data` (categories `class.credit_card,class.us_ssn`); redaction is the `transform` action (masks in place, verified accepted); a block returns HTTP 200 with `finish_reason=content_filter` (not HTTP 400) |
| Guardrails (safety / jailbreak) | `safety=True` content moderation (HTTP 400) | `block_unsafe_content` + `block_jailbreak` LLM-as-judge service policies (verified enforcing) |
| Guardrails (output PII) | MASKED to type labels (`<EMAIL_ADDRESS>` etc., HTTP 200) | Not on the model-service path today -- output-side PII masking is endpoint-only (the model-service `transform` action is pre-call / input) |
| Guardrail config surface | `ai_gateway.guardrails.pii.behavior=MASK` via `put_ai_gateway` | `detect_sensitive_data` service policy, action: block/transform/ask |
| Fallback | Does not engage on `databricks-model-serving` entities | N/A (routing configured differently) |
| Traffic split | Works (70/30 demonstrated) | 70/30 weighted split across two FMs (routing set at service creation) |
| Authorization | `CAN_QUERY` on the serving endpoint | UC grants: `USE CATALOG` / `USE SCHEMA` / `EXECUTE` on the model service |
| Cross-workspace reuse | No -- workspace-scoped | Yes -- UC object, reusable across workspaces |
| Product direction | Legacy/compatibility path; Databricks docs label this the legacy path | **Strategic direction** -- "Start everything new on Unity AI Gateway" |
| Invocation path | `/serving-endpoints/{name}/invocations` | `/ai-gateway/mlflow/v1/chat/completions` |
| Demo models | `databricks-meta-llama-3-3-70b-instruct` (primary) + `databricks-claude-sonnet-4-5` (secondary) | `system.ai.llama-4-maverick` + `system.ai.llama_v3_3_70b_instruct` (70/30) |
| UC model service name | -- | `ai_gateway_deepdive_catalog.core.aigw_demo_service` |

**Lead with UC model services (notebooks 01-04) for the strategic direction: real 429 throttling, named service policies, UC-native identity (`EXECUTE`, 404-hides), and cross-workspace reuse. Show the serving-endpoint AI Gateway config (notebook 05) as the legacy/compatibility contrast: payload/inference-table logging plus traffic and fallback on an existing endpoint. Creating a model service does NOT convert or inherit an endpoint's config -- it is a separate resource; the caller's URL decides which construct governs.**

---

## Databricks-hosted vs. External Models

The two constructs above concern *how* governance is applied (workspace overlay vs UC securable).  This section concerns *what* is governed: the model behind the gateway can be Databricks-hosted or a third-party provider.  The **same governance surfaces** (access, rate limits, guardrails, logging, cost) apply either way, since the gateway abstracts the provider so app teams call one governed front door regardless.

| Dimension | Databricks-hosted | External providers |
|-----------|-------------------|--------------------|
| What it is | Foundation Model APIs (`system.ai.*`), provisioned throughput, and self-hosted served models | OpenAI, Anthropic, Azure OpenAI, Bedrock, Google -- registered once as UC model provider services |
| Provider credentials | None (runs on Databricks compute) | BYOK: the provider key is held centrally; callers use Databricks credentials and never touch the provider key |
| Cost | Flows through Databricks billing (`system.billing.usage`) | Provider spend is estimated, not budget-covered in this setup |
| Swap / add models | Change the served entity or service config -- no app change | Register a new provider service -- no app change |
| In this demo | **Yes** -- all pillars run on Databricks-hosted FMs (Llama, Claude, `system.ai.llama-4-maverick`) | Not built here (no external keys); registered the same way when keys exist |

**This demo uses Databricks-hosted FMs only** (no external provider keys). The external-provider path is registered identically -- as a UC model provider service with BYOK held centrally -- so the same access grants, guardrails, rate limits, and inference logging cover both without app-side changes. The one difference to call out: external-provider spend is estimated and not covered by Databricks budgets today.

---

## What's GA vs Beta

Status reflects the v1/v2 (legacy) vs v3 (Unity AI Gateway) split per the Databricks migration guidance: legacy endpoints are GA but deprecating; the UC model service (v3, "Unity Catalog Services") is GA, with individual capabilities still rolling out.

| Feature | Status | Caveat |
|---------|--------|--------|
| Model Serving endpoint + AI Gateway config (v1/v2, legacy) | GA, **being deprecated** | Migrate to Unity AI Gateway (v3) |
| Unity AI Gateway model service (v3, UC Catalog Services) | **GA** | Some capabilities still rolling out (routing is create-time-only; ABAC GRANT binding is Beta). Availability varies by workspace; some workspaces still require enablement |
| Enforce Unity AI Gateway | Early preview (opt-in) | Workspace-admin setting; disables legacy endpoints so all traffic must use UC model services. Known limitations while products migrate: Apps on a Serving Endpoint resource, `ai_query` against user-created services, some Vector Search flows |
| Guardrails -- input PII block | Beta | Input PII blocked (HTTP 400, `input_guardrail_triggered`); verified on `databricks-model-serving` entities with `pii.behavior=MASK` |
| Guardrails -- output PII mask | Beta | Model-generated PII masked to type labels (`<EMAIL_ADDRESS>` etc., HTTP 200); verified live |
| Guardrails -- model-service `detect_sensitive_data` | Beta | Actions `ask` / `block` / `transform` (transform redacts/masks in place to type tokens) per [docs](https://docs.databricks.com/aws/en/data-governance/unity-catalog/service-policies/detect-sensitive-data); verified accepted on this workspace. Demo uses `block`; enforcement is selective once propagated |
| Guardrails -- model-service safety / jailbreak (LLM-as-judge) | Beta | `system.ai.block_unsafe_content`, `block_jailbreak`, `block_hallucination` (each needs a `model_service` judge + `phases`); verified `block_unsafe_content` + `block_jailbreak` enforce (nb03) |
| Rate-limit throttle with HTTP 429 | **Both paths** | Endpoint and model service both enforce RPM/TPM with HTTP 429 (verified live: a 3/min endpoint limit returned 429). The calls=0 hard-block is the documented per-endpoint migration off-ramp, not a capability gap |
| Automatic fallback | Not on `databricks-model-serving` entities | An entity-type-specific gap; works with external-provider entities (OpenAI/Azure/Anthropic) |
| Inference tables | GA (best-effort) | Payload cap applies; some error responses may not be logged; external storage required for full retention |
| Budgets | GA (account-admin) | Not available on this FEVM account (Budgets API 404); also does not cover PT or external-model usage per current Databricks documentation; `system.billing.usage` is the cost source here. [Docs](https://docs.databricks.com/aws/en/admin/account-settings/budgets) |

---

## Team Ownership

Unity AI Gateway is a **platform-team concern**, not an app-team concern.

- **Platform team** creates and configures the serving endpoint (or UC model service), sets rate limits, enables guardrails, manages budget alerts, and grants `CAN_QUERY` to the right groups.
- **BU teams** receive a `GRANT CAN_QUERY ON SERVING ENDPOINT` (for the serving-endpoint path) or a `GRANT EXECUTE ON MODEL SERVICE` (for the UC model-service path) and call the endpoint as a standard REST service.
- **App teams** consume the governed endpoint with no direct provider credentials. They never hold base-model PATs.

Adjacent capabilities (mention-only, not demonstrated here): cross-workspace model services (a model service in catalog A callable from workspace B), governed MCP services (Unity AI Gateway can sit in front of MCP tool servers), and MLflow traces (automatic tracing of agent calls through the gateway).

---

## Prerequisites

Both paths use the same bundle; they differ only in where the workspace, catalog, and warehouse come from.

**Both paths need:**
- **A workspace PAT** stored as a secret -- no external provider keys, since the internal Llama and Claude FMs are reached via `databricks-model-serving` gateway entities. `set_secrets.sh` mints it into scope `ai_gateway_demo`, key `workspace_pat`.
- **A UC catalog + schema** the runner can create tables and services in (needs `CREATE SERVICE` for the model-service path, notebooks 01-04), plus a **SQL warehouse** for the App metrics + dashboard.
- **Account-level enablement** for the Unity AI Gateway model-service path (notebooks 01-04, provisioned by 00_setup). The UC model service (v3) is GA, but availability is still rolling out; on workspaces where it is not yet on, an account admin turns it on from the account console Previews page.

**FEVM path (Databricks FDE):** a workspace provisioned via **FEVM** (Field Engineering Vending Machine -- Databricks' internal demo-workspace provisioner) with the `aws_stable_serverless` template (TTL 30 days). This demo's reference workspace and the defaults baked into `databricks.yml`: host `https://fevm-ai-gateway-deepdive.cloud.databricks.com`, CLI profile `ai-gateway-deepdive`, catalog `ai_gateway_deepdive_catalog` (ALL_PRIVILEGES), schema `core`, warehouse `060617dab4eb0c0c`.

**Existing workspace path (client):** any Databricks workspace with Model Serving + Foundation Model APIs. The runner supplies their own host, a catalog/schema they own, and a SQL warehouse id, set once in `databricks.yml` (see **Deploy to a workspace**) -- no Python edits required.

---

## Setup Steps

These steps use the **FEVM defaults** (profile `ai-gateway-deepdive`, and the catalog / warehouse baked into `databricks.yml`). **On a client's existing workspace**, first set the host, catalog, and warehouse in `databricks.yml` (see **Deploy to a workspace** below) and substitute a local CLI profile name for `ai-gateway-deepdive` throughout.

1. **Authenticate the CLI:**
   ```
   databricks auth login --host https://fevm-ai-gateway-deepdive.cloud.databricks.com --profile ai-gateway-deepdive
   ```

2. **Store the workspace PAT in the secret scope:**
   ```
   cd ai-gateway-deepdive
   ./scripts/set_secrets.sh
   ```
   The script mints a 30-day PAT via `databricks tokens create` and writes it to `ai_gateway_demo/workspace_pat`.

3. **Deploy bundle resources (app + dashboard + setup job):**
   ```
   databricks bundle deploy -t dev -p ai-gateway-deepdive
   ```

4. **Run the setup job** (`ai-gateway-deepdive-setup`, which runs `00_setup.py`):
   ```
   databricks bundle run aigw_setup -t dev -p ai-gateway-deepdive
   ```
   This creates the UC schema, the governed serving endpoint `ai-gateway-deepdive` with all four gateway pillars active (usage tracking, rate limits, inference table, guardrails), and the `guardrail_events` view.

5. **Smoke test** (validates the serving-endpoint path is live before you present):
   ```
   DATABRICKS_CONFIG_PROFILE=ai-gateway-deepdive python3 scripts/smoke_test.py
   ```
   **PASS:** prints `SMOKE OK` with a ping response, `input PII BLOCKED as 'block' (HTTP 400)`, and `output PII BLOCKED ... guardrail acted` (or MASKED if the mask path fires).
   **FAIL:** if it errors, the endpoint or secret is not ready -- see **Troubleshooting** below (usually: re-run the setup job, or the PAT secret is missing).

---

## Deploy to a workspace (fresh FEVM or existing / client)

Whether standing this up as a Databricks FDE on a **fresh FEVM workspace** (for example, after
the old one's TTL expired) or deploying to an **existing workspace** as a client, the process is the same
turnkey flow: the workspace-specific values live in one place (`databricks.yml`) and flow to
the resources, the setup job, and the notebooks. `00_setup` derives the workspace URL from
the client (no hardcoded host).

1. **Point the CLI at the target workspace** and authenticate (use a local profile name on a client workspace):
   ```
   databricks auth login --host <NEW_HOST> --profile ai-gateway-deepdive
   ```
2. **Edit `databricks.yml`**: set the target `host` to `<NEW_HOST>`, and update any
   variable whose value changed on the new workspace (usually `catalog` and
   `warehouse_id`; `schema` / `endpoint_name` / `secret_scope` rarely change).
3. **If the catalog name changed**, sync the catalog in the files that hardcode it: the
   notebook default (manual UI runs), the App and view query modules, and the dashboard (the `sed -i ''` form below is
   macOS/BSD; on Linux use `sed -i` with no argument):
   ```
   sed -i '' 's/ai_gateway_deepdive_catalog/<NEW_CATALOG>/'  notebooks/_common.py   # macOS; Linux: sed -i
   sed -i '' 's/ai_gateway_deepdive_catalog/<NEW_CATALOG>/'  app/aigw/queries.py       # App metrics queries
   sed -i '' 's/ai_gateway_deepdive_catalog/<NEW_CATALOG>/'  src/aigw/queries.py       # guardrail_events view DDL
   sed -i '' 's/ai_gateway_deepdive_catalog/<NEW_CATALOG>/g' dashboard/ai-gateway-deepdive.lvdash.json
   ```
   The setup job passes the catalog to `00_setup` via `base_parameters`, so the job needs no
   edit. The `_common.py` default matters for manual UI runs; `app/aigw/queries.py` for the App
   metrics; `src/aigw/queries.py` for the `guardrail_events` view DDL; the dashboard JSON for its widgets.
4. **Fresh *account* only:** an account admin must enable the Unity AI Gateway Beta
   (service policies) from the account console Previews page (required for the model service in notebooks 01-04).
5. **Store the workspace PAT:** `cd ai-gateway-deepdive && ./scripts/set_secrets.sh ai-gateway-deepdive`
6. **Deploy + run:**
   ```
   databricks bundle deploy -t dev -p ai-gateway-deepdive
   databricks bundle run aigw_setup -t dev -p ai-gateway-deepdive   # creates the endpoint + guardrail_events view
   ```
   Then run notebooks 01-05 in order (00_setup provisions the model service + the endpoint).
7. **Refresh demo URLs:** the deck speaker notes and the links in "Setup Steps" /
   "Demo Run Order" reference the old host, App, and dashboard id; update them to the
   new workspace.

---

## Before running the notebooks (go / no-go)

Confirm all of these before Act 1, or a notebook will error midway:
- [ ] `databricks bundle run aigw_setup -t dev -p ai-gateway-deepdive` finished **SUCCESS** (creates the endpoint, the UC model service, and the `guardrail_events` view).
- [ ] The model service exists: `databricks --profile ai-gateway-deepdive ai-gateway list-model-services` lists `ai_gateway_deepdive_catalog.core.aigw_demo_service`.
- [ ] The smoke test printed `SMOKE OK` (Setup Steps, step 5).
- [ ] (Account) an admin has enabled the Unity AI Gateway Beta on the account-console Previews page -- otherwise 00_setup's model-service step only warns and notebooks 01-04 have no service to call.

## Demo Run Order

Run notebooks in number order from the Databricks workspace UI (or locally with `DATABRICKS_CONFIG_PROFILE=ai-gateway-deepdive`):

1. `notebooks/01_access.py` -- Access: `EXECUTE` grant/revoke (404-hides) + bypass (model service)
2. `notebooks/02_runtime.py` -- Runtime: real HTTP 429 + routing (model service)
3. `notebooks/03_policy.py` -- Policy: `detect_sensitive_data` PII block + redact (transform) + safety/jailbreak LLM-judge guardrails (model service)
4. `notebooks/04_evidence_and_cost.py` -- Evidence + cost: usage, billing, budgets, per-user attribution (cross-cutting)
5. `notebooks/05_serving_endpoint_contrast.py` -- Legacy contrast: endpoint payload logging + fallback

After the notebooks:

- **Streamlit App**: open `https://ai-gateway-deepdive-7474647630497631.aws.databricksapps.com`. The chat panel uses the endpoint overlay (serving endpoint `ai-gateway-deepdive`); the "flood" button targets the UC model service to demonstrate a real HTTP 429.
- **Lakeview Dashboard** (`Unity AI Gateway - Deep Dive`, id `01f19ce05f241e8e9d7ffc89942eb25a`): token usage by model, guardrail event counts, provider mix, latency percentiles. All widgets draw from the **endpoint** inference table; model-service policy blocks (HTTP 200 content_filter) are shown live in the App's model-service tab, not here.

---

## Troubleshooting

- **Smoke test or a notebook fails with HTTP 404 on the model service** -- 00_setup's model-service step did not complete (most often the account Beta is not enabled). Re-run `databricks bundle run aigw_setup -t dev -p ai-gateway-deepdive`, then confirm with `ai-gateway list-model-services`.
- **Notebook 02 shows no HTTP 429 on the first run** -- the rate-limit counter warms up on first use; re-run the Step 1 cell to see the throttle.
- **Notebook 03 prints "not blocked this run" for a guardrail** -- the LLM-as-judge guardrails are non-deterministic and warm up; the cell retries, and re-running Step 4 clears it. If it prints `ERROR: could not apply the guardrail config`, the account Beta is likely off.
- **`PERMISSION_DENIED ... CREATE CATALOG`** -- expected when the catalog is pre-provisioned; 00_setup tolerates it and continues.
- **App / dashboard link 404s, or notebook links in the deck point at the wrong host** -- those URLs are workspace-specific; update them to your host after deploying (see **Deploy to a workspace**, step 7).

---

## Teardown

```
# Remove bundle resources (app + dashboard + setup job):
databricks bundle destroy -t dev -p ai-gateway-deepdive

# Drop the UC model service (provisioned by 00_setup):
databricks --profile ai-gateway-deepdive ai-gateway delete-model-service ai_gateway_deepdive_catalog.core.aigw_demo_service
```

The FEVM workspace self-expires after its 30-day TTL and any remaining resources are reclaimed automatically.

**Demo SP cleanup** (to remove the service principal created by notebook 01):
```python
# Run locally or in a notebook with the workspace profile active:
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
sp_list = list(w.service_principals.list(filter="displayName eq 'aigw-demo-access-sp'"))
if sp_list:
    sp = sp_list[0]
    # Delete any lingering secrets first
    for s in list(w.service_principal_secrets_proxy.list(service_principal_id=sp.id)):
        w.service_principal_secrets_proxy.delete(service_principal_id=sp.id, secret_id=s.id)
    w.service_principals.delete(id=sp.id)
    print("demo SP removed")
```
The notebook leaves the SP in place between runs so step 1 can reuse it. OAuth secrets are deleted automatically in step 7 of every run, so there are no dangling credentials after a successful run.

**Optional demo reset** (re-run from a clean state without teardown):
```python
# From a notebook or Python session with the workspace profile active:
import sys; sys.path.insert(0, 'notebooks'); sys.path.insert(0, 'src')
from _common import run_sql
run_sql("TRUNCATE TABLE ai_gateway_deepdive_catalog.core.aigw_payload")
```
Then re-run the setup job to restore the endpoint configuration and the `guardrail_events` view.

---

## Architecture Summary

```
App / SDK / Notebook
       |
       v  (governed path)
Unity AI Gateway
  - Serving endpoint: ai-gateway-deepdive   [Model Serving endpoint + AI Gateway config -- legacy path]
       |-- Usage tracking
       |-- Rate limits (RPM/TPM throttle, HTTP 429; calls=0 also hard-blocks)
       |-- Inference table (aigw_payload)
       |-- Guardrails (input PII blocked; output PII masked to type labels; safety blocked)
       |-- Traffic split (70/30 llama/claude)
       |-- AI Gateway entities: databricks-model-serving (llama + claude)
       |
  - UC model service: ai_gateway_deepdive_catalog.core.aigw_demo_service  [Unity AI Gateway model service -- strategic]
       |-- Rate limits (RPM + TPM throttle, HTTP 429)
       |-- Service policies (detect_sensitive_data PII block/transform; block_unsafe_content safety; block_jailbreak)
       |-- UC grants: USE CATALOG / USE SCHEMA / EXECUTE
       |-- Invoke via: POST /ai-gateway/mlflow/v1/chat/completions
       |
       v
  Databricks-hosted FMs (internal, no external provider keys)
       - databricks-meta-llama-3-3-70b-instruct  (endpoint overlay primary, served entity name: llama)
       - databricks-claude-sonnet-4-5            (endpoint overlay secondary, served entity name: claude)
       - system.ai.llama-4-maverick + system.ai.llama_v3_3_70b_instruct  (UC model service, 70/30 split)

       |
       v  (bypass -- UNGOVERNED)
Direct base-model endpoint / PAT call
  (no inference table row, no gateway policy applied)
```

---

## Spec Validation Evidence

This section records verified behavior. Notebook references reflect the current 01-05 layout.

| Criterion | Status | Evidence |
|-----------|--------|---------|
| Smoke test (both models answer) | PASS | `SMOKE OK`; served_model=`meta-llama-3.3-70b-instruct-121024` (Llama) + `us.anthropic.claude-sonnet-4-5-20250929-v1:0` (Claude) confirmed across runs |
| ACCESS + bypass gap | PASS | Notebook 01: real SP journey (grant-200, revoke-404 hidden), direct base-FM bypass; SP secrets deleted post-run (stale secrets also cleaned at start) |
| calls=0 hard block (403) on endpoint overlay | PASS | Endpoint overlay behavior (see comparison table); the endpoint path is contrasted in notebook 05 |
| Payload rows visible (both models) | PASS | 151+ rows in aigw_payload at review; 333 rows in guardrail_events view confirmed live |
| Input PII BLOCKED | PASS | Input PII prompt: guardrail_action=block, http_status=400, input_guardrail_triggered (verified live) |
| Output PII MASKED | PASS | Model-generated PII: guardrail_action=mask, http_status=200, content contains `<EMAIL_ADDRESS>` type labels (verified live) |
| Fallback finding documented | PASS | Notebook 05: fallback does not engage on databricks-model-serving entities (HTTP 400 on all 6 calls). Traffic split 70/30 works |
| Real HTTP 429 on UC model service | PASS | Notebook 02: rapid calls return HTTP 429 "User defined rate limit(s) exceeded" (RPM key=RATE_LIMIT_KEY_SERVICE, 3/min); TPM (tokens/min) variant also verified |
| Service-policy block on UC model service | PASS | Notebook 03: detect_sensitive_data blocks credit-card + SSN (selective); LLM-as-judge block_unsafe_content (safety) + block_jailbreak verified enforcing |
| Budgets note | PASS | Notebook 04: budgets are GA (account-admin) but not available on this FEVM account (API 404); billing.usage + per-user attribution (endpoint_usage) return rows |
| Dashboard non-empty | PASS | All 5 datasets non-empty: tokens llama4415/claude900; guardrail block216/none117; latency llama p50 304ms; lifecycle ACTIVE confirmed |

Serving endpoint `ai-gateway-deepdive`: READY, NOT_UPDATING, 2 entities.
UC model service `ai_gateway_deepdive_catalog.core.aigw_demo_service`: present (confirmed via `list-model-services`).
App `ai-gateway-deepdive`: RUNNING at `https://ai-gateway-deepdive-7474647630497631.aws.databricksapps.com`.
Dashboard `01f19ce05f241e8e9d7ffc89942eb25a`: lifecycle ACTIVE.
`guardrail_events` view: 333 rows.
Bundle validate: PASS (all 3 resources: app.yml, dashboard.yml, job_setup.yml).

---

## Platform findings (verified live)

Findings confirmed against this workspace during demo development. Recorded for reference when demoing or updating this content.

### detect_sensitive_data: Block or Redact (docs) vs. observed Beta behavior

Per current Databricks docs, the built-in `system.ai.detect_sensitive_data` policy supports two actions: **Block** (deny; the caller gets an HTTP 200 whose assistant turn names the blocking policy) and **Redact** (replace each matched value in place with a placeholder token such as `[US_SSN]` / `[EMAIL_ADDRESS]` and forward the rewritten content). Redaction is documented for model services and model provider services. Source: https://docs.databricks.com/aws/en/data-governance/unity-catalog/service-policies/detect-sensitive-data

Verified: `action` accepts `ask`, `block`, `transform` -- **redaction/masking is the `transform` action** (there is no `redact` value; an earlier note that the API rejects `action: redact` used the wrong string). With `action: block`, enforcement is selective once the policy propagates (benign passes, PII blocked), though it can broad-deny transiently right after attaching. The model service also exposes LLM-as-judge policies `block_unsafe_content` (safety), `block_jailbreak`, and `block_hallucination`; `block_unsafe_content` and `block_jailbreak` were verified blocking unsafe and jailbreak prompts.

**How the valid names were confirmed (method note):** the built-in handler names and the `action` values were read from the API's own errors, not guessed -- PATCHing a deliberately-wrong value returns an enumerated list (`Unknown built-in policy handler '...'`, `option 'action' must be one of [ask, block, transform]`, `allowed: [dry_run, max_turns, model_service, phases]`). Treat that enumerated error as the authoritative source; do not conclude a capability is absent because a guessed name was rejected.

### Output PII masking: real but environment-dependent

Output PII masking (`pii.behavior=MASK` on the output guardrail, Model Serving endpoint AI Gateway config) is real and is recorded in the inference table as `guardrail_action='guardrail_mask'` in the `guardrail_events` view. However, the behavior at the live endpoint is not always pure masking -- when the safety/privacy category fires alongside `pii_detection`, the gateway blocks entirely (HTTP 400, `output_guardrail_triggered`) rather than masking.

Additionally, the SDK query path (`serving_endpoints.query().as_dict()`) drops `output_guardrail` from the typed response. The `chat()` helper therefore cannot surface `guardrail_action='mask'`; it yields only `'none'` or `'block'` regardless of the actual masking that occurred. Masking is evidenced via the inference table and Lakeview dashboard, not via live chat assertions.

### Model-service invoke path

The UC model service is invoked via `POST /ai-gateway/mlflow/v1/chat/completions` with the model FQN in the request body. The rate-limit key reported in 429 responses is `RATE_LIMIT_KEY_SERVICE`. The endpoint overlay (`/serving-endpoints/{name}/invocations`) enforces its own RPM/TPM limits with HTTP 429 as well (keys `user` / `endpoint`; verified live) -- rate limiting is not unique to the model-service path; setting a limit to calls=0 additionally hard-blocks and is the documented migration off-ramp.
