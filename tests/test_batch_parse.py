import json

from batch_parse import (
    build_batch_request_entry,
    collect_batch,
    extract_batch_output_entry,
    extract_batch_output_job_id,
    normalize_batch_resource,
    parse_companies_file,
)
from parse import COMPACT_SCHEMA, FLAT_JSON_SCHEMA, GeminiBackend


def test_build_batch_request_entry_includes_job_metadata_and_schema():
    backend = GeminiBackend(api_key="test-key")
    entry = build_batch_request_entry(backend, "job-123", "Some job text", max_tokens=321)

    assert entry["metadata"]["job_id"] == "job-123"
    config = entry["request"]["generationConfig"]
    assert config["maxOutputTokens"] == 321
    assert config["responseMimeType"] == "application/json"


def test_gemini_build_request_includes_compact_schema_and_current_industry_labels():
    backend = GeminiBackend(api_key="test-key")

    request = backend.build_request("Some job text", max_tokens=321)
    prompt = request["contents"][0]["parts"][0]["text"]

    assert COMPACT_SCHEMA in prompt
    assert "developer_tools_infra" in prompt
    assert "fintech_payments_banking" in prompt
    assert "saas_software" not in prompt
    assert "financial_services" not in prompt


def test_gemini_schema_matches_flat_schema_field_set_and_required_keys():
    backend = GeminiBackend(api_key="test-key")

    flat_keys = set(FLAT_JSON_SCHEMA["properties"].keys())
    gemini_keys = set(backend._schema["properties"].keys())

    assert gemini_keys == flat_keys
    assert set(backend._schema["required"]) == set(FLAT_JSON_SCHEMA["required"])
    assert set(FLAT_JSON_SCHEMA["required"]) == flat_keys


def test_parse_companies_file_supports_explicit_and_default_ats(tmp_path):
    path = tmp_path / "companies.txt"
    path.write_text("greenhouse:figma\nspotify\n# comment\n\nlever:aeva\n", encoding="utf-8")

    assert parse_companies_file(str(path)) == [
        ("greenhouse", "figma"),
        ("greenhouse", "spotify"),
        ("lever", "aeva"),
    ]


def test_extract_batch_output_entry_handles_wrapped_response():
    payload = {"response": {"candidates": [{"content": {"parts": [{"text": "{}"}]}}]}}
    response, error = extract_batch_output_entry(payload)
    assert error is None
    assert response == payload["response"]



def test_extract_batch_output_entry_handles_wrapped_error():
    response, error = extract_batch_output_entry({"error": {"message": "bad request"}})
    assert response is None
    assert error == "bad request"



def test_extract_batch_output_entry_handles_bare_generate_content_response():
    payload = {"candidates": [{"content": {"parts": [{"text": "{}"}]}}]}
    response, error = extract_batch_output_entry(payload)
    assert error is None
    assert response == payload


def test_extract_batch_output_job_id_reads_metadata():
    payload = {
        "metadata": {"job_id": "job-123"},
        "response": {"candidates": [{"content": {"parts": [{"text": "{}"}]}}]},
    }
    assert extract_batch_output_job_id(payload) == "job-123"


def test_normalize_batch_resource_prefers_metadata_wrapper():
    payload = {
        "name": "operations/abc",
        "metadata": {"name": "batches/xyz", "state": "BATCH_STATE_PENDING"},
    }
    assert normalize_batch_resource(payload) == payload["metadata"]



def test_gemini_backend_parse_response_payload_parses_nested_json_response():
    backend = GeminiBackend(api_key="test-key")
    metadata = {
        "tagline": "Build search infra at Acme",
        "locations": [],
        "salary": None,
        "office_type": "remote",
        "hybrid_days": None,
        "job_type": "full-time",
        "experience_level": "mid",
        "is_manager": False,
        "industry_primary": "ai_ml",
        "industry_tags": [],
        "industry_other_hint": "",
        "hard_skills": ["python"],
        "soft_skills": ["communication"],
        "cool_factor": "interesting",
        "vibe_tags": [],
        "visa_sponsorship": "unknown",
        "equity": {"offered": False, "min_pct": None, "max_pct": None},
        "benefits_categories": [],
        "salary_transparency": "not_disclosed",
    }
    payload = {
        "candidates": [
            {
                "content": {
                    "parts": [{"text": json.dumps(metadata)}],
                }
            }
        ]
    }

    parsed, error = backend.parse_response_payload(payload)

    assert error is None
    assert parsed is not None
    assert parsed.tagline == metadata["tagline"]
    assert parsed.office_type == metadata["office_type"]



def test_gemini_backend_parse_response_payload_reports_missing_text():
    backend = GeminiBackend(api_key="test-key")

    parsed, error = backend.parse_response_payload({"promptFeedback": {"blockReason": "SAFETY"}})

    assert parsed is None
    assert error is not None
    assert "missing text" in error.lower()


def test_collect_batch_matches_results_by_job_id_not_output_order(monkeypatch):
    applied = {}
    deleted = {}
    updated = {}

    class DummyParsed:
        def __init__(self, payload):
            self._payload = payload

        def model_dump(self, mode="json"):
            return self._payload

    class DummyBackend:
        def __init__(self, model):
            self.model = model

        def parse_response_payload(self, response_payload):
            return DummyParsed({"tagline": response_payload["tagline"]}), None

    class DummyGeoResolver:
        def __init__(self, conn):
            self.conn = conn

        def resolve_parsed_geo(self, parsed):
            return parsed

    class DummyClient:
        def __init__(self, model):
            self.model = model

        def download_file_text(self, file_name):
            lines = [
                {
                    "metadata": {"job_id": "job-b"},
                    "response": {"tagline": "tagline-b"},
                },
                {
                    "metadata": {"job_id": "job-a"},
                    "response": {"tagline": "tagline-a"},
                },
            ]
            return "\n".join(json.dumps(line) for line in lines)

    def fake_status_batch(conn, batch_name, model):
        return {
            "state": "BATCH_STATE_SUCCEEDED",
            "output": {"responsesFile": "files/test-batch"},
            "endTime": "2026-03-29T00:00:00Z",
        }

    def fake_get_parse_batch_job_rows(conn, batch_id):
        return [
            {
                "job_id": "job-a",
                "submitted_content_hash": "hash-a",
                "raw_json": {"id": "job-a"},
                "current_content_hash": "hash-a",
            },
            {
                "job_id": "job-b",
                "submitted_content_hash": "hash-b",
                "raw_json": {"id": "job-b"},
                "current_content_hash": "hash-b",
            },
        ]

    def fake_apply_parse_batch_chunk(conn, batch_id, success_rows, error_rows):
        applied["success_rows"] = list(success_rows)
        applied["error_rows"] = list(error_rows)
        return {
            "applied_success_ids": [row[0] for row in success_rows],
            "applied_failure_count": 0,
            "stale_count": 0,
        }

    def fake_delete_parse_batch_jobs(conn, batch_id, job_ids=None):
        deleted["job_ids"] = job_ids

    def fake_update_parse_batch(conn, batch_id, **kwargs):
        updated["kwargs"] = kwargs

    monkeypatch.setattr("batch_parse.GeminiBackend", DummyBackend)
    monkeypatch.setattr("batch_parse.GeoResolver", DummyGeoResolver)
    monkeypatch.setattr("batch_parse.GeminiBatchClient", DummyClient)
    monkeypatch.setattr("batch_parse.status_batch", fake_status_batch)
    monkeypatch.setattr("batch_parse.get_parse_batch_job_rows", fake_get_parse_batch_job_rows)
    monkeypatch.setattr("batch_parse.apply_parse_batch_chunk", fake_apply_parse_batch_chunk)
    monkeypatch.setattr("batch_parse.delete_parse_batch_jobs", fake_delete_parse_batch_jobs)
    monkeypatch.setattr("batch_parse.update_parse_batch", fake_update_parse_batch)
    monkeypatch.setattr("batch_parse.merge_api_data", lambda raw, parsed: parsed)

    parsed_ids = collect_batch(conn=None, batch_name="batches/test", model="gemini-test")

    assert parsed_ids == ["job-b", "job-a"]
    assert applied["success_rows"] == [
        ("job-b", "hash-b", {"tagline": "tagline-b"}),
        ("job-a", "hash-a", {"tagline": "tagline-a"}),
    ]
    assert applied["error_rows"] == []
    assert deleted.get("job_ids") is None
    assert updated["kwargs"]["failed_count"] == 0
    assert updated["kwargs"]["stale_count"] == 0
    assert updated["kwargs"]["last_error"] is None
