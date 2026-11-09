from types import SimpleNamespace

import db
import frontend_revalidate
import meili_reconcile
import pipeline
from public_ids import meili_safe_job_id


def test_plan_reconcile_classifies_every_mismatch():
    rows = [
        ("removed.flagged.absent", True, True),
        ("removed.flagged.present", True, True),
        ("removed.unflagged.present", True, False),
        ("removed.unflagged.absent", True, False),
        ("active.flagged.absent", False, True),
        ("active.flagged.present", False, True),
        ("active.unflagged.present", False, False),
        ("active.unflagged.absent", False, False),
    ]
    present = ["removed.flagged.present", "removed.unflagged.present",
               "active.flagged.present", "active.unflagged.present"]
    meili_docs = {meili_safe_job_id(job_id): job_id for job_id in present}
    meili_docs["orphan_doc"] = "orphan"

    plan = meili_reconcile.plan_reconcile(meili_docs, rows)

    assert plan == {
        "clear_removed_absent": ["removed.flagged.absent"],
        "clear_active_absent": ["active.flagged.absent"],
        "delete_removed_present": ["removed.flagged.present", "removed.unflagged.present"],
        "active_present_unflagged": ["active.unflagged.present"],
        "delete_orphans": ["orphan_doc"],
    }


def test_reconcile_deletes_then_clears_flags(monkeypatch):
    removed_present = "greenhouse__acme__1"
    meili_docs = {meili_safe_job_id(removed_present): removed_present, "orphan_doc": "orphan"}
    rows = [(removed_present, True, True), ("greenhouse__acme__2", True, True)]
    monkeypatch.setattr(meili_reconcile, "fetch_meili_job_ids", lambda index: meili_docs)
    monkeypatch.setattr(meili_reconcile, "_iter_db_rows", lambda conn: iter(rows))
    cleared = []
    monkeypatch.setattr(meili_reconcile, "mark_jobs_meili_deleted", lambda conn, ids: cleared.extend(ids))
    deleted = []

    class Index:
        def delete_documents(self, ids):
            deleted.extend(ids)
            return SimpleNamespace(task_uid=7)

    class Client:
        def index(self, uid): return Index()
        def wait_for_task(self, uid, **kwargs): return {"status": "succeeded"}

    conn = SimpleNamespace(commit=lambda: None)
    meili_reconcile.reconcile_meili(conn, Client(), dry_run=False)

    assert deleted == [meili_safe_job_id(removed_present), "orphan_doc"]
    assert sorted(cleared) == sorted(["greenhouse__acme__2", removed_present])


def test_reconcile_dry_run_changes_nothing(monkeypatch):
    monkeypatch.setattr(meili_reconcile, "fetch_meili_job_ids", lambda index: {"orphan_doc": "orphan"})
    monkeypatch.setattr(meili_reconcile, "_iter_db_rows", lambda conn: iter([("a", True, True)]))
    monkeypatch.setattr(meili_reconcile, "mark_jobs_meili_deleted",
                        lambda conn, ids: (_ for _ in ()).throw(AssertionError("wrote flags")))

    class Index:
        def delete_documents(self, ids): raise AssertionError("deleted docs")

    client = SimpleNamespace(index=lambda uid: Index())
    plan = meili_reconcile.reconcile_meili(SimpleNamespace(commit=lambda: None), client, dry_run=True)
    assert plan["clear_removed_absent"] == ["a"]
    assert plan["delete_orphans"] == ["orphan_doc"]


def test_pending_load_backlog_gate_only_when_requested():
    executed = []

    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *exc): pass
        def execute(self, query, params): executed.append((query, params))
        def fetchall(self): return []

    conn = SimpleNamespace(cursor=lambda: Cursor())
    db.get_job_ids_pending_meili_load(conn)
    db.get_job_ids_pending_meili_load(conn, backlog_max_age_days=2, limit=5)

    assert "first_seen_at" not in executed[0][0]
    assert "meili_loaded_at IS NOT NULL OR first_seen_at >=" in executed[1][0]
    assert executed[1][1][-2:] == [2, 5]


def test_load_deletes_before_upserting_and_revalidates(monkeypatch):
    events = []

    class Connection:
        def cursor(self): return self
        def __enter__(self): return self
        def __exit__(self, *exc): pass
        def execute(self, *args): pass
        def fetchall(self): return []
        def commit(self): pass

    class Index:
        def get_primary_key(self): return "meili_id"
        def delete_documents(self, ids):
            events.append("delete")
            return SimpleNamespace(task_uid=1)
        def add_documents(self, docs, primary_key):
            events.append("upsert")
            return SimpleNamespace(task_uid=2)
        def get_stats(self): return SimpleNamespace(number_of_documents=0)

    class Client:
        def __init__(self, *args, **kwargs): pass
        def get_index(self, uid): return Index()
        def index(self, uid): return Index()
        def wait_for_task(self, uid, **kwargs): return {"status": "succeeded"}

    import sys
    monkeypatch.setitem(sys.modules, "meilisearch", SimpleNamespace(Client=Client))
    monkeypatch.setattr(pipeline, "get_latest_fx_rates", lambda c: ({}, None))
    monkeypatch.setattr(pipeline, "get_removed_job_ids", lambda c, **kw: ["gone"])
    monkeypatch.setattr(pipeline, "get_active_jobs_for_meili", lambda c, job_ids: [{"id": "live"}])
    monkeypatch.setattr(pipeline, "_build_docs_for_chunk", lambda rows, *a: [{"id": "live"}])
    monkeypatch.setattr(pipeline, "mark_jobs_meili_loaded", lambda c, ids: None)
    monkeypatch.setattr(pipeline, "mark_jobs_meili_deleted", lambda c, ids: None)
    revalidated = []
    monkeypatch.setattr(pipeline, "revalidate_changed_jobs", lambda c, ids: revalidated.extend(ids))

    pipeline.step_load(Connection(), parsed_job_ids=["live"], removed_job_ids=["gone"])

    assert events == ["delete", "upsert"]
    assert revalidated == ["gone", "live"]



def test_load_stops_upserts_and_fails_red_on_openrouter_auth_error(monkeypatch):
    import pytest
    from embeddings import EmbeddingAuthError
    events = []

    class Connection:
        def cursor(self): return self
        def __enter__(self): return self
        def __exit__(self, *exc): pass
        def execute(self, *args): pass
        def fetchall(self): return []
        def commit(self): pass

    class Index:
        def get_primary_key(self): return "meili_id"
        def delete_documents(self, ids):
            events.append("delete")
            return SimpleNamespace(task_uid=1)
        def add_documents(self, docs, primary_key):
            events.append("upsert")
            return SimpleNamespace(task_uid=2)
        def get_stats(self): return SimpleNamespace(number_of_documents=0)

    class Client:
        def __init__(self, *args, **kwargs): pass
        def get_index(self, uid): return Index()
        def index(self, uid): return Index()
        def wait_for_task(self, uid, **kwargs): return {"status": "succeeded"}

    def attach_vectors(docs):
        events.append("embed")
        raise EmbeddingAuthError("OpenRouter key limit/auth: HTTP 402 from embeddings endpoint")

    import sys
    monkeypatch.setitem(sys.modules, "meilisearch", SimpleNamespace(Client=Client))
    monkeypatch.setattr(pipeline, "attach_vectors", attach_vectors)
    monkeypatch.setattr(pipeline, "get_latest_fx_rates", lambda c: ({}, None))
    monkeypatch.setattr(pipeline, "get_removed_job_ids", lambda c, **kw: ["gone"])
    monkeypatch.setattr(pipeline, "get_active_jobs_for_meili", lambda c, job_ids: [{"id": j} for j in job_ids])
    monkeypatch.setattr(pipeline, "_build_docs_for_chunk", lambda rows, *a: [{"id": r["id"]} for r in rows])
    loaded = []
    monkeypatch.setattr(pipeline, "mark_jobs_meili_loaded", lambda c, ids: loaded.extend(ids))
    monkeypatch.setattr(pipeline, "mark_jobs_meili_deleted", lambda c, ids: None)
    revalidated = []
    monkeypatch.setattr(pipeline, "revalidate_changed_jobs", lambda c, ids: revalidated.extend(ids))

    with pytest.raises(RuntimeError, match="OpenRouter key limit/auth: new jobs not loaded"):
        pipeline.step_load(Connection(), parsed_job_ids=["a", "b"], removed_job_ids=["gone"], meili_batch_size=1)

    # Deletions still ran first and were revalidated; no upsert was attempted after the first auth error.
    assert events == ["delete", "embed"]
    assert loaded == []
    assert revalidated == ["gone"]

def test_revalidation_posts_tag_batches(monkeypatch):
    requests = []

    class Response:
        def __enter__(self): return self
        def __exit__(self, *exc): pass
        def read(self): return b'{"revalidated": 2}'

    def urlopen(request, timeout):
        requests.append((request.full_url, request.headers["Authorization"], request.data))
        return Response()

    monkeypatch.setattr(frontend_revalidate.urllib.request, "urlopen", urlopen)
    accepted = frontend_revalidate.post_revalidation(["job:a", "sitemaps"], url="https://x/api/revalidate", secret="s")
    assert accepted == 2
    assert requests == [("https://x/api/revalidate", "Bearer s", b'{"tags": ["job:a", "sitemaps"]}')]


def test_revalidation_skips_without_config(monkeypatch):
    monkeypatch.delenv("REVALIDATE_URL", raising=False)
    monkeypatch.delenv("REVALIDATE_SECRET", raising=False)
    assert frontend_revalidate.post_revalidation(["job:a"]) == 0
