#!/usr/bin/env python3
"""
Lock-down toolkit for Databricks serving endpoints, system.ai, and UC model services.

Three independent, opt-in actions (combine any, or use one), each dry-run by default
with a snapshot for --restore:

  --rate-limit             Set an AI Gateway rate limit on every serving endpoint
                           (default calls=0, i.e. a hard block). Use --calls/--key/--period.
  --enable-usage-tracking  Turn on AI Gateway usage tracking on every serving endpoint
                           (idempotent; matches the Unity AI Gateway migration guide's
                           "enable usage tracking on all legacy endpoints" step).
  --revoke-access          Revoke broad access:
                             - EXECUTE on schema system.ai            (ACCOUNT-WIDE -- see warning)
                             - CAN_QUERY on every workspace serving endpoint with an editable ACL
                             - EXECUTE on each model service passed via --services (explicit only)

Why this is careful:
  - put_ai_gateway has REPLACE semantics: the body sent becomes the entire AI Gateway
    config, so --rate-limit / --enable-usage-tracking re-send every other field
    (inference_table, guardrails, fallback, and the one they are not changing) to avoid
    silently wiping guardrails or logging.
  - DRY-RUN is the default: it prints exactly what it would change and touches nothing.
  - Before applying, it snapshots the prior state (rate limits, usage tracking, and the
    exact grants removed) so every change is reversible with --restore.

BLAST RADIUS:
  - --rate-limit --calls 0 blocks EVERY serving endpoint in the workspace.
  - --revoke-access removes EXECUTE on system.ai from `account users`, which is an
    ACCOUNT-LEVEL grant (affects ALL workspaces on the account), and strips CAN_QUERY
    from the workspace `users` group on every endpoint. Run dry first, read the plan,
    then apply. Unity Catalog has no per-principal DENY: this closes broad/default access,
    it cannot subtract one caller from a group grant.

Usage:
  # dry run of a full lock-down (rate-limit + usage tracking + revoke), touches nothing
  python3 rate_limit_all_endpoints.py --profile ai-gateway-deepdive \
      --rate-limit --enable-usage-tracking --revoke-access \
      --services ai_gateway_deepdive_catalog.core.aigw_demo_service

  # apply just the rate-limit block
  python3 rate_limit_all_endpoints.py --profile ai-gateway-deepdive --rate-limit --apply

  # usage tracking everywhere, nothing else (non-disruptive)
  python3 rate_limit_all_endpoints.py --profile ai-gateway-deepdive --enable-usage-tracking --apply

  # revoke broad access (account-wide on system.ai!) -- read the dry run first
  python3 rate_limit_all_endpoints.py --profile ai-gateway-deepdive --revoke-access --apply

  # roll everything back from the snapshot written during --apply
  python3 rate_limit_all_endpoints.py --profile ai-gateway-deepdive --restore

Run from a Databricks notebook (uses the notebook's own credentials -- no profile):
  import sys; sys.path.append("/Workspace/Users/<you>/.bundle/ai-gateway-deepdive/dev/files/scripts")
  import rate_limit_all_endpoints as lockdown
  lockdown.run(report=True, services=["<cat>.<sch>.<service>"])            # read-only audit
  lockdown.run(rate_limit=True, enable_usage_tracking=True)                # dry-run (nothing changes)
  lockdown.run(rate_limit=True, apply=True,                                # apply for real;
               snapshot="/Volumes/<cat>/<sch>/<vol>/lockdown_snapshot.json")  # put snapshot on a Volume so --restore survives
  lockdown.run(do_restore=True, snapshot="/Volumes/<cat>/<sch>/<vol>/lockdown_snapshot.json")
Note: in a notebook the default snapshot path is driver-local and ephemeral; pass a Volume/Workspace
path so a later restore can find it.
"""

import argparse
import json
import time
import urllib.error
import urllib.request

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import serving

_KEYS = {
    "endpoint": serving.AiGatewayRateLimitKey.ENDPOINT,
    "user": serving.AiGatewayRateLimitKey.USER,
    "user_group": serving.AiGatewayRateLimitKey.USER_GROUP,
    "service_principal": serving.AiGatewayRateLimitKey.SERVICE_PRINCIPAL,
}
_PERIODS = {"minute": serving.AiGatewayRateLimitRenewalPeriod.MINUTE}  # only per-minute is supported

# Default "broad access" principals. UC securables (system.ai, model services) grant to
# `account users`; workspace serving-endpoint ACLs grant to the `users` group. Revoking
# both closes the default-open door on either surface.
_BROAD_PRINCIPALS = ["account users", "users"]
_LEVEL_RANK = {"CAN_MANAGE": 3, "CAN_QUERY": 2, "CAN_VIEW": 1}


# --------------------------------------------------------------------------- REST helpers
def _host(w):
    return w.config.host.rstrip("/")


def _auth(w):
    tok = w.config.token
    return {"Authorization": f"Bearer {tok}"} if tok else w.config.authenticate()


def _rest(w, method, path, body=None):
    """Raw workspace REST call. Returns (status, body); never raises on HTTP error."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(_host(w) + path, data=data, method=method,
                                 headers={"Content-Type": "application/json", **_auth(w)})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read() or "{}")
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"raw": raw}
    except Exception as e:
        return 0, {"error": str(e)}


def _uc_privileges(w, sec_type, fqn, principal):
    """Return the list of UC privileges `principal` currently holds on the securable."""
    _s, b = _rest(w, "GET", f"/api/2.1/unity-catalog/permissions/{sec_type}/{fqn}")
    for pa in (b.get("privilege_assignments") or []):
        if pa.get("principal") == principal:
            return pa.get("privileges") or []
    return []


def _uc_change(w, sec_type, fqn, principal, add=None, remove=None):
    changes = {"principal": principal}
    if add:
        changes["add"] = add
    if remove:
        changes["remove"] = remove
    return _rest(w, "PATCH", f"/api/2.1/unity-catalog/permissions/{sec_type}/{fqn}",
                 {"changes": [changes]})


# --------------------------------------------------------------------------- put_ai_gateway
def _preserved_kwargs(ag):
    """AI Gateway config this script never changes, so a put does not wipe it."""
    if ag is None:
        return dict(inference_table_config=None, guardrails=None, fallback_config=None)
    return dict(inference_table_config=ag.inference_table_config,
                guardrails=ag.guardrails, fallback_config=ag.fallback_config)


def gateway_pass(w, do_rate_limit, calls, key, period, do_usage, dry_run, snap):
    """Apply --rate-limit and/or --enable-usage-tracking across all serving endpoints."""
    new_limit = [serving.AiGatewayRateLimit(calls=calls, renewal_period=period, key=key)]
    ok, skipped, failed = [], [], []
    for ep in w.serving_endpoints.list():
        name = ep.name
        try:
            ag = w.serving_endpoints.get(name).ai_gateway
            snap.setdefault("endpoints", {}).setdefault(name, {})
            if do_rate_limit:
                snap["endpoints"][name]["rate_limits"] = [rl.as_dict() for rl in (ag.rate_limits or [])] if ag else []
            if do_usage:
                snap["endpoints"][name]["usage_tracking"] = (ag.usage_tracking_config.as_dict()
                                                             if ag and ag.usage_tracking_config else None)
            rl_send = new_limit if do_rate_limit else (ag.rate_limits if ag else None)
            ut_send = serving.AiGatewayUsageTrackingConfig(enabled=True) if do_usage else (ag.usage_tracking_config if ag else None)
            if dry_run:
                parts = []
                if do_rate_limit:
                    parts.append(f"rate_limit -> calls={calls}/{period.value} key={key.value} (was: {ag.rate_limits if ag else None})")
                if do_usage:
                    parts.append(f"usage_tracking -> enabled=True (was: {ag.usage_tracking_config.enabled if ag and ag.usage_tracking_config else None})")
                print(f"[DRY-RUN] {name}: " + "; ".join(parts))
                ok.append(name)
                continue
            w.serving_endpoints.put_ai_gateway(name=name, rate_limits=rl_send,
                                               usage_tracking_config=ut_send, **_preserved_kwargs(ag))
            print(f"[OK]   gateway {name}")
            ok.append(name)
            time.sleep(0.2)
        except Exception as e:
            msg = str(e)
            (skipped if ("not support" in msg.lower() or "invalid" in msg.lower()) else failed).append(name)
            print(f"[{'SKIP' if name in skipped else 'FAIL'}] gateway {name}: {msg[:110]}")
    print(f"gateway summary: ok={len(ok)} skipped={len(skipped)} failed={len(failed)}")


# --------------------------------------------------------------------------- revoke access
def revoke_pass(w, principals, services, dry_run, snap):
    rev = snap.setdefault("revoke", {"system_ai": {}, "endpoints_acl": {}, "model_services": {}})

    # 1) system.ai EXECUTE (account-wide)
    for p in principals:
        privs = _uc_privileges(w, "schema", "system.ai", p)
        if "EXECUTE" in privs:
            if dry_run:
                print(f"[DRY-RUN] REVOKE EXECUTE on schema system.ai FROM '{p}' (ACCOUNT-WIDE) (current: {privs})")
            else:
                s, b = _uc_change(w, "schema", "system.ai", p, remove=["EXECUTE"])
                print(f"[{'OK' if s == 200 else 'FAIL'}] revoke EXECUTE system.ai from '{p}' -> HTTP {s}")
                if s == 200:
                    rev["system_ai"].setdefault(p, ["EXECUTE"])

    # 2) CAN_QUERY on every workspace serving endpoint with an editable ACL
    for ep in w.serving_endpoints.list():
        eid = getattr(ep, "id", None)
        if not eid:
            print(f"[SKIP] endpoint ACL {ep.name}: no editable serving ACL (system endpoint; governed via system.ai)")
            continue
        s, acl = _rest(w, "GET", f"/api/2.0/permissions/serving-endpoints/{eid}")
        if s != 200:
            print(f"[SKIP] endpoint ACL {ep.name}: cannot read permissions (HTTP {s})")
            continue
        entries = acl.get("access_control_list") or []
        removed, kept = [], []
        for e in entries:
            who = e.get("group_name") or e.get("user_name") or e.get("service_principal_name")
            ptype = "group_name" if e.get("group_name") else ("user_name" if e.get("user_name") else "service_principal_name")
            direct = [p["permission_level"] for p in (e.get("all_permissions") or []) if not p.get("inherited")]
            if not direct:
                continue  # purely inherited (e.g. admins) -- not stored, skip
            best = max(direct, key=lambda lv: _LEVEL_RANK.get(lv, 0))
            if who in principals:
                removed.append({"ptype": ptype, "who": who, "level": best})
            else:
                kept.append({ptype: who, "permission_level": best})
        if not removed:
            continue
        if dry_run:
            print(f"[DRY-RUN] {ep.name}: remove {[r['who'] + '=' + r['level'] for r in removed]} (keep {len(kept)} direct entr(y/ies))")
        else:
            s2, _ = _rest(w, "PUT", f"/api/2.0/permissions/serving-endpoints/{eid}", {"access_control_list": kept})
            print(f"[{'OK' if s2 == 200 else 'FAIL'}] revoke endpoint {ep.name} -> HTTP {s2} (removed {[r['who'] for r in removed]})")
            if s2 == 200:
                rev["endpoints_acl"][eid] = removed

    # 3) EXECUTE on explicitly-named model services only (never the metastore-wide list)
    for fqn in services:
        for p in principals:
            privs = _uc_privileges(w, "model_service", fqn, p)
            if "EXECUTE" in privs:
                if dry_run:
                    print(f"[DRY-RUN] REVOKE EXECUTE on model_service {fqn} FROM '{p}' (current: {privs})")
                else:
                    s, _ = _uc_change(w, "model_service", fqn, p, remove=["EXECUTE"])
                    print(f"[{'OK' if s == 200 else 'FAIL'}] revoke EXECUTE {fqn} from '{p}' -> HTTP {s}")
                    if s == 200:
                        rev["model_services"].setdefault(fqn, {})[p] = ["EXECUTE"]
            elif dry_run:
                print(f"[DRY-RUN] {fqn}: '{p}' has no EXECUTE to remove (current: {privs})")


# --------------------------------------------------------------------------- report
def report_pass(w, principals, services):
    """Read-only audit: list resources that are still accessible because a lock-down
    control (rate limit, usage tracking, broad-access revoke) is not yet in place."""
    print(f"=== Lock-down posture report: {_host(w)} ===")
    print(f"Controls checked: rate limit, usage tracking, broad access")
    print(f"Broad principals checked: {principals}\n")

    # system.ai (account-wide)
    sysai_exposed = [p for p in principals if "EXECUTE" in _uc_privileges(w, "schema", "system.ai", p)]
    if sysai_exposed:
        print(f"[system.ai] EXPOSED -- EXECUTE held by {sysai_exposed}")
        print("            every listed principal can invoke ALL system.ai models (account-wide).")
    else:
        print("[system.ai] controlled -- no broad EXECUTE for the checked principals.")

    # serving endpoints
    eps = list(w.serving_endpoints.list())
    unbounded, usage_off, broadly_queryable = [], [], []
    for ep in eps:
        try:
            ag = w.serving_endpoints.get(ep.name).ai_gateway
        except Exception:
            ag = None
        if not (ag and ag.rate_limits):
            unbounded.append(ep.name)
        if not (ag and ag.usage_tracking_config and ag.usage_tracking_config.enabled):
            usage_off.append(ep.name)
        eid = getattr(ep, "id", None)
        if eid:
            s, acl = _rest(w, "GET", f"/api/2.0/permissions/serving-endpoints/{eid}")
            if s == 200:
                for e in (acl.get("access_control_list") or []):
                    who = e.get("group_name") or e.get("user_name") or e.get("service_principal_name")
                    if who in principals:
                        lvls = [p["permission_level"] for p in (e.get("all_permissions") or []) if not p.get("inherited")]
                        if any(lv in ("CAN_QUERY", "CAN_MANAGE") for lv in lvls):
                            broadly_queryable.append(f"{ep.name} ({who}={','.join(lvls)})")

    def _list(title, items):
        print(f"\n  {title}: {len(items)}")
        for it in items:
            print(f"     - {it}")

    print(f"\n[serving endpoints] {len(eps)} total")
    _list("Unbounded (no rate limit)", unbounded)
    _list("Usage tracking OFF", usage_off)
    _list("Broadly queryable (CAN_QUERY/CAN_MANAGE to a broad principal)", broadly_queryable)

    # named model services
    print(f"\n[model services] checked: {len(services)} named (metastore-wide scan intentionally skipped)")
    for fqn in services:
        holders = [p for p in principals if "EXECUTE" in _uc_privileges(w, "model_service", fqn, p)]
        print(f"     - {fqn}: {'EXPOSED -- broad EXECUTE held by ' + str(holders) if holders else 'controlled (no broad EXECUTE)'}")

    print("\n--- Exposure summary ---")
    print(f"  system.ai broadly executable : {'YES' if sysai_exposed else 'no'}")
    print(f"  endpoints without rate limit : {len(unbounded)}")
    print(f"  endpoints without usage track: {len(usage_off)}")
    print(f"  endpoints broadly queryable  : {len(broadly_queryable)}")
    print("  (read-only report; nothing changed)")


# --------------------------------------------------------------------------- restore
def restore(w, snapshot_file):
    snap = json.load(open(snapshot_file))
    # gateway (rate limits + usage tracking)
    for name, rec in (snap.get("endpoints") or {}).items():
        if "rate_limits" not in rec and "usage_tracking" not in rec:
            continue
        try:
            ag = w.serving_endpoints.get(name).ai_gateway
            rl = [serving.AiGatewayRateLimit.from_dict(x) for x in rec["rate_limits"]] if rec.get("rate_limits") else (None if "rate_limits" in rec else (ag.rate_limits if ag else None))
            if "usage_tracking" in rec:
                ut = serving.AiGatewayUsageTrackingConfig.from_dict(rec["usage_tracking"]) if rec["usage_tracking"] else None
            else:
                ut = ag.usage_tracking_config if ag else None
            w.serving_endpoints.put_ai_gateway(name=name, rate_limits=rl, usage_tracking_config=ut, **_preserved_kwargs(ag))
            print(f"restored gateway {name}")
        except Exception as e:
            print(f"[FAIL restore gateway] {name}: {str(e)[:120]}")
    # revoke -> re-grant
    rev = snap.get("revoke") or {}
    for p, privs in (rev.get("system_ai") or {}).items():
        s, _ = _uc_change(w, "schema", "system.ai", p, add=privs)
        print(f"[{'OK' if s == 200 else 'FAIL'}] re-grant {privs} system.ai to '{p}' -> HTTP {s}")
    for eid, removed in (rev.get("endpoints_acl") or {}).items():
        acl = [{r["ptype"]: r["who"], "permission_level": r["level"]} for r in removed]
        s, _ = _rest(w, "PATCH", f"/api/2.0/permissions/serving-endpoints/{eid}", {"access_control_list": acl})
        print(f"[{'OK' if s == 200 else 'FAIL'}] re-grant endpoint {eid} -> HTTP {s} ({[r['who'] for r in removed]})")
    for fqn, per_principal in (rev.get("model_services") or {}).items():
        for p, privs in per_principal.items():
            s, _ = _uc_change(w, "model_service", fqn, p, add=privs)
            print(f"[{'OK' if s == 200 else 'FAIL'}] re-grant {privs} {fqn} to '{p}' -> HTTP {s}")


# --------------------------------------------------------------------------- programmatic entry
def run(*, w=None, profile=None, report=False, do_restore=False,
        rate_limit=False, enable_usage_tracking=False, revoke_access=False,
        apply=False, calls=0, key="endpoint", period="minute",
        principals=None, services=None, snapshot="endpoint_lockdown_snapshot.json"):
    """Programmatic entry point -- the same actions as the CLI, callable from a notebook.

    In a Databricks notebook, leave w and profile unset: WorkspaceClient() picks up the
    notebook's own credentials. Locally, pass profile=... (or a prebuilt w=...).
    Returns the snapshot dict for apply runs (None for report/restore).
    """
    if w is None:
        w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    principals = principals or _BROAD_PRINCIPALS
    services = services or []

    if report:
        report_pass(w, principals, services)
        return None
    if do_restore:
        restore(w, snapshot)
        return None
    if not (rate_limit or enable_usage_tracking or revoke_access):
        raise ValueError("no action requested: set report=True, rate_limit=True, "
                         "enable_usage_tracking=True, or revoke_access=True.")

    dry = not apply
    snap = {}
    if rate_limit or enable_usage_tracking:
        gateway_pass(w, rate_limit, calls, _KEYS[key], _PERIODS[period], enable_usage_tracking, dry, snap)
    if revoke_access:
        revoke_pass(w, principals, services, dry, snap)
    with open(snapshot, "w") as f:
        json.dump(snap, f, indent=2)
    print(f"\n{'DRY-RUN (nothing changed). ' if dry else ''}snapshot -> {snapshot}")
    if dry:
        print("Re-run with apply=True (or --apply) to make these changes.")
    return snap


# --------------------------------------------------------------------------- CLI
def main():
    p = argparse.ArgumentParser(description="Lock down serving endpoints, system.ai, and UC model services (or restore).")
    p.add_argument("--profile", required=True, help="Databricks CLI profile")
    p.add_argument("--apply", action="store_true", help="actually apply (default is dry-run)")
    # actions (opt-in)
    p.add_argument("--rate-limit", action="store_true", help="set a rate limit on every serving endpoint")
    p.add_argument("--enable-usage-tracking", action="store_true", help="enable usage tracking on every serving endpoint")
    p.add_argument("--revoke-access", action="store_true", help="revoke broad access to system.ai, endpoints, and named services")
    # rate-limit params
    p.add_argument("--calls", type=int, default=0, help="rate limit calls (0 = block; default 0)")
    p.add_argument("--key", choices=list(_KEYS), default="endpoint")
    p.add_argument("--period", choices=list(_PERIODS), default="minute")
    # revoke params
    p.add_argument("--principal", action="append", default=None,
                   help="principal to revoke from (repeatable). Default: 'account users' and 'users'.")
    p.add_argument("--services", default="", help="comma-separated model-service FQNs to revoke EXECUTE on (revoke only touches these, never the metastore-wide list)")
    # report / snapshot / restore
    p.add_argument("--report", action="store_true", help="read-only: list resources still accessible for lack of these controls")
    p.add_argument("--snapshot", default="endpoint_lockdown_snapshot.json")
    p.add_argument("--restore", action="store_true", help="restore rate limits, usage tracking, and grants from the snapshot")
    args = p.parse_args()

    if not (args.report or args.restore or args.rate_limit or args.enable_usage_tracking or args.revoke_access):
        p.error("no action requested. Pass at least one of --report, --rate-limit, --enable-usage-tracking, --revoke-access (or --restore).")

    services = [s.strip() for s in args.services.split(",") if s.strip()]
    run(profile=args.profile, report=args.report, do_restore=args.restore,
        rate_limit=args.rate_limit, enable_usage_tracking=args.enable_usage_tracking,
        revoke_access=args.revoke_access, apply=args.apply, calls=args.calls,
        key=args.key, period=args.period, principals=args.principal,
        services=services, snapshot=args.snapshot)


if __name__ == "__main__":
    main()
