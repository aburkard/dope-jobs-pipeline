from pipeline import step_parse


class DummyParsed:
    def __init__(self, payload):
        self._payload = payload

    def model_dump(self, mode="json"):
        return dict(self._payload)


class DummyGeoResolver:
    def __init__(self, conn):
        self.conn = conn

    def resolve_parsed_geo(self, parsed):
        return parsed


def _run_step_parse(monkeypatch, model: str, base_url: str):
    saved = {}

    class DummyBackend:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def extract_batch(self, job_texts):
            return [DummyParsed({"tagline": "Role", "office_type": "remote"}) for _ in job_texts]

    monkeypatch.setattr(
        "pipeline.get_jobs_needing_parse",
        lambda conn, limit=None, companies=None: [{"id": "job-1", "raw_json": {"title": "Role"}}],
    )
    monkeypatch.setattr("parse.prepare_job_text", lambda raw: "job text")
    monkeypatch.setattr("parse.merge_api_data", lambda raw, parsed: parsed)
    monkeypatch.setattr("geo_resolver.GeoResolver", DummyGeoResolver)
    monkeypatch.setattr("pipeline.record_parse_error", lambda conn, jid, error: (_ for _ in ()).throw(AssertionError(error)))

    def fake_save(conn, jid, parsed, parse_provider=None, parse_model=None, parse_params=None):
        saved["jid"] = jid
        saved["parsed"] = parsed
        saved["parse_provider"] = parse_provider
        saved["parse_model"] = parse_model
        saved["parse_params"] = parse_params

    monkeypatch.setattr("pipeline.save_parsed_result", fake_save)

    if "gemini" in model.lower():
        monkeypatch.setattr("parse.GeminiBackend", DummyBackend)
    else:
        monkeypatch.setattr("parse.OpenAIBackend", DummyBackend)

    parsed_ids = step_parse(
        conn=object(),
        base_url=base_url,
        model=model,
        api_key="test-key",
        limit=1,
        concurrency=1,
    )
    return parsed_ids, saved


def test_step_parse_records_openai_parse_provenance(monkeypatch):
    parsed_ids, saved = _run_step_parse(
        monkeypatch,
        model="gpt-5.4",
        base_url="https://api.openai.com/v1",
    )

    assert parsed_ids == ["job-1"]
    assert saved["jid"] == "job-1"
    assert saved["parse_provider"] == "openai"
    assert saved["parse_model"] == "gpt-5.4"
    assert saved["parse_params"] == {
        "method": "direct",
        "base_url": "https://api.openai.com/v1",
    }


def test_step_parse_records_gemini_parse_provenance(monkeypatch):
    parsed_ids, saved = _run_step_parse(
        monkeypatch,
        model="gemini-3.1-flash-lite-preview",
        base_url="https://api.openai.com/v1",
    )

    assert parsed_ids == ["job-1"]
    assert saved["jid"] == "job-1"
    assert saved["parse_provider"] == "google"
    assert saved["parse_model"] == "gemini-3.1-flash-lite-preview"
    assert saved["parse_params"] == {"method": "direct"}
