from aigw.result import parse_query_result, ChatResult


def test_parses_openai_style_chat_response():
    raw = {
        "id": "chatcmpl-1",
        "model": "openai-chat",
        "choices": [{"message": {"role": "assistant", "content": "Hello there."}}],
        "usage": {"prompt_tokens": 12, "completion_tokens": 3},
    }
    r = parse_query_result(raw, latency_ms=134)
    assert isinstance(r, ChatResult)
    assert r.content == "Hello there."
    assert r.served_model == "openai-chat"
    assert r.prompt_tokens == 12
    assert r.completion_tokens == 3
    assert r.latency_ms == 134
    assert r.http_status == 200
    assert r.guardrail_action == "none"
    assert r.error is None


def test_angle_bracket_type_label_200_is_mask():
    # The serving-endpoint output guardrail replaces model-generated PII with
    # angle-bracket type labels (e.g. <EMAIL_ADDRESS>, <PHONE_NUMBER>) and returns
    # HTTP 200 with finish_reason=stop. This is output masking -- not a block.
    raw = {
        "model": "meta-llama-3.3-70b",
        "choices": [
            {
                "message": {
                    "content": "Contact: John Doe, <EMAIL_ADDRESS>, <PHONE_NUMBER>"
                },
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 20, "completion_tokens": 12},
        "output_guardrail": [{"pii_detection": True, "flagged": False}],
    }
    r = parse_query_result(raw, latency_ms=220)
    assert r.guardrail_action == "mask", (
        f"Expected 'mask' for content with <EMAIL_ADDRESS> type label, got {r.guardrail_action!r}"
    )
    assert r.http_status == 200
    assert r.error is None
    # content is returned unmodified (type labels are already in place)
    assert "<EMAIL_ADDRESS>" in r.content


def test_plain_200_reply_is_none_not_mask():
    # A normal 200 reply with no angle-bracket type labels must remain "none".
    raw = {
        "model": "openai-chat",
        "choices": [{"message": {"content": "The capital of France is Paris."}}],
        "usage": {"prompt_tokens": 8, "completion_tokens": 7},
    }
    r = parse_query_result(raw, latency_ms=90)
    assert r.guardrail_action == "none"


def test_angle_bracket_no_pii_detection_is_none():
    # A 200 response whose content contains angle-bracket tokens like <YOUR_API_KEY>
    # or <BASE_URL> must NOT trigger "mask" unless the API-side output_guardrail
    # pii_detection signal is also present.  Models frequently emit template placeholders
    # that match <[A-Z_]+> -- gating on pii_detection prevents these false-fires.
    raw_curl = {
        "model": "openai-chat",
        "choices": [
            {
                "message": {
                    "content": (
                        "curl -X GET <BASE_URL>/api/v1/users "
                        "-H 'Authorization: Bearer <YOUR_API_KEY>'"
                    )
                },
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 15, "completion_tokens": 20},
        # No output_guardrail field -- normal model response, not a PII-masked one.
    }
    r = parse_query_result(raw_curl, latency_ms=120)
    assert r.guardrail_action == "none", (
        f"False-fire: <YOUR_API_KEY> with no pii_detection should be 'none', "
        f"got {r.guardrail_action!r}"
    )

    # Same content but WITH pii_detection=False (guardrail ran but found no PII) -> still none.
    raw_no_pii = {
        "model": "openai-chat",
        "choices": [
            {
                "message": {"content": "Use header <AUTHORIZATION> to authenticate."},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 10, "completion_tokens": 8},
        "output_guardrail": [{"pii_detection": False, "flagged": False}],
    }
    r2 = parse_query_result(raw_no_pii, latency_ms=100)
    assert r2.guardrail_action == "none", (
        f"pii_detection=False should not produce 'mask', got {r2.guardrail_action!r}"
    )


def test_legacy_redaction_markers_are_not_mask():
    # "***" and "[REDACTED]" appear in normal markdown and text.
    # They must NOT trigger the mask detection -- only <UPPERCASE_LABEL> angle-bracket
    # tokens are the real PII type labels produced by the output guardrail.
    raw_redacted = {
        "model": "openai-chat",
        "choices": [{"message": {"content": "Call me at [REDACTED]."}}],
        "usage": {"prompt_tokens": 8, "completion_tokens": 5},
    }
    r1 = parse_query_result(raw_redacted, latency_ms=90)
    assert r1.guardrail_action == "none"

    raw_stars = {
        "model": "openai-chat",
        "choices": [{"message": {"content": "The answer is ***important***."}}],
        "usage": {"prompt_tokens": 8, "completion_tokens": 5},
    }
    r2 = parse_query_result(raw_stars, latency_ms=90)
    assert r2.guardrail_action == "none"


def test_maps_guardrail_block_error():
    raw = {"error_code": "BAD_REQUEST", "message": "Request blocked by guardrails: safety"}
    r = parse_query_result(raw, latency_ms=40, http_status=400)
    assert r.guardrail_action == "block"
    assert r.http_status == 400
    assert r.content == ""
    assert r.error is not None


def test_maps_input_guardrail_triggered_block():
    # input_guardrail_triggered: PII in the input prompt; gateway returns 400.
    import json
    inner = {
        "input_guardrail": [{"flagged": True, "pii_detection": True}],
        "finishReason": "input_guardrail_triggered",
    }
    raw = {"error_code": "BAD_REQUEST", "message": json.dumps(inner)}
    r = parse_query_result(raw, latency_ms=35, http_status=400)
    assert r.guardrail_action == "block"
    assert r.http_status == 400
    assert r.content == ""
