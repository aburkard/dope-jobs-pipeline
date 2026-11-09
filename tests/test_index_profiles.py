import hashlib
import os
from types import SimpleNamespace

import numpy as np
import pytest

import embeddings
import index_profiles
import pipeline

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "voy4lite_cleanP_parity.npz")

# What dopejobs-front filters/facets/sorts on (web-app session's checked list) plus distinct + geo.
APP_FILTERS = {
    "office_type", "experience_level", "industry_tags", "cool_factor", "vibe_tags", "years_experience_buckets",
    "education_level", "job_type", "visa_sponsorship", "benefits_categories", "company_stage",
    "equity_offered", "is_manager", "salary_annual_min_usd", "salary_annual_max_usd", "posted_at_ts",
    "work_country_codes", "work_admin1_keys", "work_geoname_ids", "applicant_country_codes",
    "applicant_admin1_keys", "id", "job_group",
}
APP_DISPLAY = {
    "id", "public_job_id", "title", "company", "company_slug", "company_domain", "company_logo", "apply_url",
    "office_type", "job_type", "location", "locations_all", "salary_min", "salary_max", "salary_currency",
    "salary_period", "cool_factor", "tagline", "visa_sponsorship", "equity_offered", "job_group",
    "description", "location_count", "_geo", "posted_at_ts", "hard_skills", "soft_skills", "industry_tags",
    "experience_level", "meili_id",
}


def test_default_profile_is_pplx_and_unknown_profiles_fail(monkeypatch):
    monkeypatch.delenv(index_profiles.PROFILE_ENV, raising=False)
    assert index_profiles.active_profile() == "pplx512"
    monkeypatch.setenv(index_profiles.PROFILE_ENV, "bogus")
    with pytest.raises(ValueError):
        index_profiles.active_profile()


def test_projection_file_is_pinned():
    with open(embeddings.V2_PROJECTION_PATH, "rb") as f:
        assert hashlib.sha256(f.read()).hexdigest() == embeddings.V2_PROJECTION_SHA256
    assert embeddings.v2_projection().shape == (1024, 256)


def test_projection_parity_with_gate_vectors():
    # The 256d vectors the full-scale gate was judged on, from the same raw voyage-4-lite vectors.
    fx = np.load(FIXTURE)
    out = np.asarray(embeddings.project_v2(fx["raw_1024"].tolist()))
    cos = np.sum(out * fx["gate_256"], axis=1)
    assert out.shape == (5, 256) and cos.min() >= 0.9999
    assert np.allclose(np.linalg.norm(out, axis=1), 1.0, atol=1e-5)


def test_v2_attach_vectors_requests_lite_documents_and_projects(monkeypatch):
    monkeypatch.setenv(index_profiles.PROFILE_ENV, "v2")
    monkeypatch.setenv("OPENROUTER_API_KEY", "k")
    fx = np.load(FIXTURE)
    bodies = []

    class Response:
        status_code = 200
        def raise_for_status(self): pass
        def json(self):
            n = len(bodies[-1]["input"])
            return {"data": [{"index": i, "embedding": fx["raw_1024"][i % 5].tolist()} for i in range(n)]}

    class Session:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def post(self, url, headers, json, timeout):
            bodies.append(json)
            return Response()

    monkeypatch.setattr(embeddings.requests, "Session", Session)
    docs = [{"title": f"t{i}"} for i in range(5)]
    assert embeddings.attach_vectors(docs) is True
    body = bodies[0]
    assert body["model"] == "mongodb/voyage-4-lite" and body["input_type"] == "document"
    assert "dimensions" not in body  # native 1024d, as the gate embedded them
    got = np.array([d["_vectors"]["default"]["embeddings"] for d in docs])
    assert got.shape == (5, 256)
    assert np.sum(got * fx["gate_256"], axis=1).min() >= 0.9999
    assert all(d["_vectors"]["default"]["regenerate"] is False for d in docs)


def test_default_profile_still_requests_pplx_512(monkeypatch):
    monkeypatch.delenv(index_profiles.PROFILE_ENV, raising=False)
    monkeypatch.setenv("OPENROUTER_API_KEY", "k")
    bodies = []

    class Response:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"data": [{"index": 0, "embedding": [1.0] + [0.0] * 600}]}

    class Session:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def post(self, url, headers, json, timeout):
            bodies.append(json); return Response()

    monkeypatch.setattr(embeddings.requests, "Session", Session)
    docs = [{"title": "t"}]
    assert embeddings.attach_vectors(docs) is True
    assert bodies[0]["model"] == embeddings.MODEL and bodies[0]["dimensions"] == 512
    assert len(docs[0]["_vectors"]["default"]["embeddings"]) == 512


def test_slim_doc_drops_only_unused_fields():
    doc = {f: 1 for f in APP_DISPLAY | APP_FILTERS | set(index_profiles.V2_DROP_FIELDS)}
    slim = index_profiles.slim_doc(doc)
    assert set(slim) == (APP_DISPLAY | APP_FILTERS)
    assert not (set(index_profiles.V2_DROP_FIELDS) & (APP_DISPLAY | APP_FILTERS))


def test_v2_settings_cover_the_app_and_have_no_shift():
    s = index_profiles.v2_settings()
    rules = s["filterableAttributes"]
    names, facet_search, comparison = set(), set(), set()
    for rule in rules:
        if isinstance(rule, str):
            names.add(rule); continue
        for attr in rule["attributePatterns"]:
            names.add(attr)
            if rule["features"]["facetSearch"]:
                facet_search.add(attr)
            if rule["features"]["filter"]["comparison"]:
                comparison.add(attr)
    assert APP_FILTERS | {"_geo"} == names
    assert facet_search == {"vibe_tags", "benefits_categories"}
    assert comparison == {"salary_annual_min_usd", "salary_annual_max_usd", "posted_at_ts"}
    assert s["sortableAttributes"] == ["posted_at_ts"]
    assert s["proximityPrecision"] == "byAttribute"
    emb = s["embedders"]["default"]
    # Single REST embedder (a composite fails Meili's search-vs-indexing similarity check for an
    # asymmetric pair) with the explicit 0.75/0.1 shift the gate's best student row was measured with.
    assert emb["source"] == "rest" and emb["dimensions"] == 256
    assert emb["distribution"] == {"mean": 0.75, "sigma": 0.1}
    assert emb["url"].endswith("/openai-search-tiny/v1/embeddings")
    assert emb["documentTemplate"] == index_profiles.DOCUMENT_TEMPLATE


def test_document_template_matches_python_renderer():
    # The Liquid template (Meili fallback) and render_document_text (pipeline) must stay in sync:
    # spot-check the structure by rendering a doc whose fields map 1:1 onto template lines.
    lines = index_profiles.DOCUMENT_TEMPLATE.split("\n")
    rendered = embeddings.render_document_text({"title": "T", "company": "C", "office_type": "remote",
                                                "tagline": "x", "description": "y"}).split("\n")
    assert len(lines) == len(rendered) == 5
    for line, out, prefix in zip(lines, rendered, ["Job title:", " Company:", " Work setup:", " Summary:", " Description:"]):
        assert prefix in line and out.startswith(prefix)


def _fake_meili(events, embedder_dims=None):
    class Index:
        def get_primary_key(self): return "meili_id"
        def get_embedders(self):
            if embedder_dims is None:
                raise AttributeError
            return {"default": {"source": "composite", "searchEmbedder": {"dimensions": embedder_dims}}}
        def update_settings(self, settings):
            events.append(("settings", settings))
            return SimpleNamespace(task_uid=9)
        def update_filterable_attributes(self, a): events.append(("legacy-filterable", a))
        def update_searchable_attributes(self, a): pass
        def update_sortable_attributes(self, a): pass
        def delete_documents(self, ids): return SimpleNamespace(task_uid=1)
        def add_documents(self, docs, primary_key):
            events.append(("upsert", docs)); return SimpleNamespace(task_uid=2)
        def get_stats(self): return SimpleNamespace(number_of_documents=0)

    class Client:
        def __init__(self, *a, **k): pass
        def get_index(self, uid):
            events.append(("get_index", uid)); return Index()
        def index(self, uid):
            events.append(("index", uid)); return Index()
        def wait_for_task(self, uid, **k): return {"status": "succeeded"}
    return Client


def _patch_load(monkeypatch, client, docs):
    import sys

    class Connection:
        sql = ""
        def cursor(self): return self
        def __enter__(self): return self
        def __exit__(self, *exc): pass
        def execute(self, sql, *args): Connection.sql = sql
        def fetchall(self):
            # Only the full-reload id query returns rows; lookups (companies, groups, ...) are empty.
            return [("live",)] if "SELECT id FROM pipeline_jobs WHERE raw_json IS NOT NULL" in Connection.sql else []
        def commit(self): pass

    monkeypatch.setitem(sys.modules, "meilisearch", SimpleNamespace(Client=client))
    monkeypatch.setattr(pipeline, "attach_vectors", lambda d: True)
    monkeypatch.setattr(pipeline, "get_latest_fx_rates", lambda c: ({}, None))
    monkeypatch.setattr(pipeline, "get_removed_job_ids", lambda c, **kw: [])
    monkeypatch.setattr(pipeline, "get_active_jobs_for_meili", lambda c, job_ids: [{"id": j} for j in job_ids])
    monkeypatch.setattr(pipeline, "_build_docs_for_chunk", lambda rows, *a: [dict(docs) for _ in rows])
    monkeypatch.setattr(pipeline, "mark_jobs_meili_loaded", lambda c, ids: None)
    monkeypatch.setattr(pipeline, "mark_jobs_meili_deleted", lambda c, ids: None)
    monkeypatch.setattr(pipeline, "revalidate_changed_jobs", lambda c, ids: None)
    return Connection()


def test_v2_full_load_targets_index_uid_applies_v2_settings_and_slims(monkeypatch):
    monkeypatch.setenv(index_profiles.PROFILE_ENV, "v2")
    events = []
    conn = _patch_load(monkeypatch, _fake_meili(events, embedder_dims=256),
                       {"id": "live", "title": "t", "_geojson": {"x": 1}, "salary_fx_as_of": "d", "ats_type": "ashby"})
    pipeline.step_load(conn, parsed_job_ids=["live"], full_reload=True, index_uid="jobs_v2")
    assert ("index", "jobs_v2") in events and ("index", "jobs") not in events
    settings = [e[1] for e in events if e[0] == "settings"]
    assert settings and settings[0]["embedders"]["default"]["distribution"] == {"mean": 0.75, "sigma": 0.1}
    assert not [e for e in events if e[0] == "legacy-filterable"]
    upserted = [e[1] for e in events if e[0] == "upsert"][0][0]
    assert "_geojson" not in upserted and "salary_fx_as_of" not in upserted and "ats_type" not in upserted
    assert upserted["title"] == "t"


def test_refuses_to_write_v2_vectors_into_a_512d_index(monkeypatch):
    monkeypatch.setenv(index_profiles.PROFILE_ENV, "v2")
    events = []
    conn = _patch_load(monkeypatch, _fake_meili(events, embedder_dims=512), {"id": "live"})
    with pytest.raises(RuntimeError, match="refusing to mix vector spaces"):
        pipeline.step_load(conn, parsed_job_ids=["live"], index_uid="jobs")
    assert not [e for e in events if e[0] == "upsert"]


def test_default_profile_keeps_full_docs_and_legacy_settings(monkeypatch):
    monkeypatch.delenv(index_profiles.PROFILE_ENV, raising=False)
    events = []
    conn = _patch_load(monkeypatch, _fake_meili(events, embedder_dims=512), {"id": "live", "_geojson": {"x": 1}})
    pipeline.step_load(conn, parsed_job_ids=["live"], full_reload=True)
    assert ("index", "jobs") in events
    assert [e for e in events if e[0] == "legacy-filterable"]
    upserted = [e[1] for e in events if e[0] == "upsert"][0][0]
    assert "_geojson" in upserted


def test_v2_never_falls_back_to_meili_embedding(monkeypatch):
    monkeypatch.setenv(index_profiles.PROFILE_ENV, "v2")
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    with pytest.raises(embeddings.EmbeddingRequiredError):
        embeddings.attach_vectors([{"title": "t"}])
    monkeypatch.setenv("OPENROUTER_API_KEY", "k")
    monkeypatch.setattr(embeddings.time, "sleep", lambda s: None)

    class Response:
        status_code = 503
        def raise_for_status(self): raise embeddings.requests.HTTPError("503")

    class Session:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def post(self, *args, **kwargs): return Response()

    monkeypatch.setattr(embeddings.requests, "Session", Session)
    docs = [{"title": "t"}]
    with pytest.raises(embeddings.EmbeddingRequiredError):
        embeddings.attach_vectors(docs)
    assert "_vectors" not in docs[0]


def test_v2_load_stops_instead_of_sending_unvectored_docs(monkeypatch):
    monkeypatch.setenv(index_profiles.PROFILE_ENV, "v2")
    events = []
    conn = _patch_load(monkeypatch, _fake_meili(events, embedder_dims=256), {"id": "live"})

    def attach(docs):
        raise embeddings.EmbeddingRequiredError("document embedding failed")

    monkeypatch.setattr(pipeline, "attach_vectors", attach)
    with pytest.raises(RuntimeError, match="Document embedding unavailable: new jobs not loaded"):
        pipeline.step_load(conn, parsed_job_ids=["live"], index_uid="jobs_v2")
    assert not [e for e in events if e[0] == "upsert"]
