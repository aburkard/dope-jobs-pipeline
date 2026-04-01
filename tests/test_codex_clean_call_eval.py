from codex_clean_call_eval import build_codex_json_schema


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
