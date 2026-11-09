from types import SimpleNamespace

import embeddings
import meili_reconcile


def test_render_matches_meili_template_shape():
    doc = {
        "title": "Data Engineer",
        "company": "Acme",
        "office_type": "remote",
        "tagline": " ".join(f"w{i}" for i in range(20)),
        "description": " ".join(f"d{i}" for i in range(100)),
    }
    text = embeddings.render_document_text(doc)
    lines = text.split("\n")
    assert lines[0] == "Job title: Data Engineer."
    assert lines[1] == " Company: Acme."
    assert lines[2] == " Work setup: remote."
    assert lines[3] == " Summary: " + " ".join(f"w{i}" for i in range(16)) + "...."
    assert lines[4] == " Description: " + " ".join(f"d{i}" for i in range(90)) + "..."


def test_truncatewords_splits_on_single_spaces_like_liquid_rust():
    # Newlines don't separate words and double spaces create empty words;
    # this exact behavior made pipeline vectors match Meili's (cosine 1.0).
    assert embeddings._truncatewords("a\nb c", 2) == "a\nb c"
    assert embeddings._truncatewords("a  b c", 2) == "a ..."
    assert embeddings._truncatewords("a b c", 2) == "a b..."


def test_render_uses_liquid_truthiness():
    # Liquid: only nil/false are falsy, so empty strings still render their prefix.
    text = embeddings.render_document_text({"title": "X", "company": None, "office_type": "", "tagline": None, "description": None})
    assert text.split("\n") == ["Job title: X.", "", " Work setup: .", "", ""]


def test_render_caps_bytes():
    text = embeddings.render_document_text({"title": "é" * 10000})
    assert len(text.encode("utf-8")) <= embeddings.DOCUMENT_TEMPLATE_MAX_BYTES


def test_attach_vectors_sets_user_provided_normalized(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "k")
    calls = []

    class Response:
        status_code = 200
        def raise_for_status(self): pass
        def json(self):
            texts = calls[-1]["input"]
            return {"data": [{"index": i, "embedding": [3.0, 4.0] + [0.0] * 600} for i in reversed(range(len(texts)))]}

    class Session:
        def __enter__(self): return self
        def __exit__(self, *exc): pass
        def post(self, url, headers, json, timeout):
            assert json["model"] == embeddings.MODEL and json["dimensions"] == 512
            calls.append(json)
            return Response()

    monkeypatch.setattr(embeddings.requests, "Session", Session)
    docs = [{"title": f"t{i}"} for i in range(70)]
    assert embeddings.attach_vectors(docs) is True
    assert len(calls) == 2  # 64 + 6
    vector = docs[0]["_vectors"]["default"]
    assert vector["regenerate"] is False
    assert len(vector["embeddings"]) == 512
    assert abs(vector["embeddings"][0] - 0.6) < 1e-9 and abs(vector["embeddings"][1] - 0.8) < 1e-9


def test_attach_vectors_falls_back_without_key_or_on_failure(monkeypatch):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    docs = [{"title": "t"}]
    assert embeddings.attach_vectors(docs) is False and "_vectors" not in docs[0]

    monkeypatch.setenv("OPENROUTER_API_KEY", "k")
    monkeypatch.setattr(embeddings.time, "sleep", lambda s: None)

    class Session:
        def __enter__(self): return self
        def __exit__(self, *exc): pass
        def post(self, *args, **kwargs): raise embeddings.requests.ConnectionError("down")

    monkeypatch.setattr(embeddings.requests, "Session", Session)
    assert embeddings.attach_vectors(docs) is False and "_vectors" not in docs[0]


def test_convert_vectors_skips_converted_and_honors_dry_run(monkeypatch):
    docs = [
        {"meili_id": "a", "_vectors": {"default": {"embeddings": [[0.1, 0.2]], "regenerate": True}}},
        {"meili_id": "b", "_vectors": {"default": {"embeddings": [[0.3, 0.4]], "regenerate": False}}},
        {"meili_id": "c", "_vectors": {"default": {"embeddings": [], "regenerate": True}}},
    ]
    updates = []

    class Index:
        def get_documents(self, params):
            assert params["retrieveVectors"] is True
            chunk = docs[params["offset"]:params["offset"] + params["limit"]]
            return SimpleNamespace(results=chunk, total=len(docs))
        def update_documents(self, batch, primary_key):
            updates.extend(batch)
            return SimpleNamespace(task_uid=1)

    client = SimpleNamespace(index=lambda uid: Index(), wait_for_task=lambda uid, **kw: {"status": "succeeded"})

    assert meili_reconcile.convert_vectors_to_user_provided(client, dry_run=True) == 1
    assert updates == []
    assert meili_reconcile.convert_vectors_to_user_provided(client, dry_run=False) == 1
    assert updates == [{"meili_id": "a", "_vectors": {"default": {"embeddings": [0.1, 0.2], "regenerate": False}}}]


def test_attach_vectors_raises_on_openrouter_auth_or_limit_without_retry(monkeypatch):
    # 401/402/403 never heal on retry and Meili's fallback embedder uses the same key:
    # attach_vectors must raise (no silent fallback) and must not burn retries.
    import pytest
    monkeypatch.setenv("OPENROUTER_API_KEY", "k")
    sleeps = []
    monkeypatch.setattr(embeddings.time, "sleep", lambda s: sleeps.append(s))
    for status in (401, 402, 403):
        calls = []

        class Response:
            status_code = status
            def raise_for_status(self): raise embeddings.requests.HTTPError(str(status))

        class Session:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def post(self, *args, **kwargs):
                calls.append(1)
                return Response()

        monkeypatch.setattr(embeddings.requests, "Session", Session)
        docs = [{"title": "t"}]
        with pytest.raises(embeddings.EmbeddingAuthError, match=str(status)):
            embeddings.attach_vectors(docs)
        assert calls == [1]
        assert "_vectors" not in docs[0]
    assert sleeps == []


def test_attach_vectors_still_falls_back_on_server_errors(monkeypatch):
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
    assert embeddings.attach_vectors(docs) is False
    assert "_vectors" not in docs[0]
