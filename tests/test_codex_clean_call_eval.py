from codex_clean_call_eval import (
    build_codex_json_schema,
    build_request_artifacts,
)


def test_build_codex_json_schema_tightens_nested_objects():
    schema = {
        "type": "object",
        "properties": {
            "outer": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "nested": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "scope": {"type": "string"},
                                "country_code": {"type": "string"},
                            },
                            "required": ["scope"],
                        },
                    },
                },
                "required": ["name"],
            }
        },
        "required": ["outer"],
    }

    normalized = build_codex_json_schema(schema)

    assert normalized["additionalProperties"] is False
    assert normalized["required"] == ["outer"]

    outer = normalized["properties"]["outer"]
    assert outer["additionalProperties"] is False
    assert outer["required"] == ["name", "nested"]

    items = outer["properties"]["nested"]["items"]
    assert items["additionalProperties"] is False
    assert items["required"] == ["scope", "country_code"]


def test_build_request_artifacts_current_variant_keeps_compact_schema_in_prompt():
    prepared, prompt, schema = build_request_artifacts(
        {"title": "Role", "description": "Do the thing."},
        prompt_max_chars=500,
        variant="current",
    )

    assert prepared == "Role\n\nDo the thing."
    assert prompt.startswith("Extract these fields as JSON:")
    assert "Job posting:\nRole\n\nDo the thing." in prompt
    assert "description" not in schema


def test_build_request_artifacts_schema_descriptions_moves_guidance_to_schema():
    prepared, prompt, schema = build_request_artifacts(
        {"title": "Role", "description": "Do the thing."},
        prompt_max_chars=500,
        variant="schema_descriptions",
    )

    assert prepared == "Role\n\nDo the thing."
    assert prompt == "Job posting:\nRole\n\nDo the thing."
    assert schema["description"].startswith("Extract these fields as JSON:")
    assert schema["properties"]["industry_primary"]["description"].startswith("One primary industry")
