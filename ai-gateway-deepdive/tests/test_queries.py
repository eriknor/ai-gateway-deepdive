from aigw import queries


def test_all_queries_reference_configured_objects():
    fns = [queries.tokens_cost_by_provider, queries.requests_and_429_over_time,
           queries.guardrail_trigger_counts, queries.provider_mix, queries.latency_by_provider]
    for fn in fns:
        sql = fn()
        assert "ai_gateway_deepdive_catalog" in sql or "system.serving" in sql, \
            f"{fn.__name__} references no known object"
        assert ";" not in sql.strip()[:-1], f"{fn.__name__} has an embedded semicolon"


def test_guardrail_view_ddl_is_create_view():
    ddl = queries.guardrail_events_view_ddl()
    assert ddl.strip().upper().startswith("CREATE OR REPLACE VIEW")
    assert "ai_gateway_deepdive_catalog.core" in ddl
    assert "guardrail_mask" in ddl, "guardrail_mask category must be present in the view DDL"
