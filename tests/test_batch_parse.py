import json

from batch_parse import build_batch_request_entry, extract_batch_output_entry
from parse import GeminiBackend


def test_build_batch_request_entry_includes_job_metadata_and_schema():
    backend = GeminiBackend(api_key="test-key")
    entry = build_batch_request_entry(backend, "job-123", "Some job text", max_tokens=321)

    assert entry["metadata"]["job_id"] == "job-123"
    config = entry["request"]["generationConfig"]
    assert config["maxOutputTokens"] == 321
    assert config["responseMimeType"] == "application/json"



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
        "industry": "ai_ml",
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
