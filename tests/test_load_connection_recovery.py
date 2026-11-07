from types import SimpleNamespace
import pytest
import pipeline
import job_groups


def test_index_creation_wait_has_no_open_lookup_transaction(monkeypatch):
    class Connection:
        in_transaction = False
        def cursor(self):
            self.in_transaction = True
            return self
        def __enter__(self): return self
        def __exit__(self, *exc): pass
        def execute(self, *args): pass
        def fetchall(self): return []
        def commit(self): self.in_transaction = False
    conn = Connection()
    class Client:
        def __init__(self, *args, **kwargs): pass
        def get_index(self, *args): raise RuntimeError("missing index")
        def create_index(self, *args): return SimpleNamespace(task_uid=1)
        def wait_for_task(self, *args, **kwargs):
            assert not conn.in_transaction
            return {"status": "failed"}
    import sys
    monkeypatch.setitem(sys.modules, "meilisearch", SimpleNamespace(Client=Client))
    monkeypatch.setattr(pipeline, "get_latest_fx_rates", lambda c: ({}, None))
    monkeypatch.setattr(pipeline, "get_removed_job_ids", lambda c, **kw: [])
    with pytest.raises(RuntimeError, match="creating Meili"):
        pipeline.step_load(conn, parsed_job_ids=["job"])


@pytest.mark.parametrize("timeout_batch", [None, 1, 2])
def test_delete_batches_wait_and_acknowledge_before_submitting_more(monkeypatch, timeout_batch):
    class Connection:
        in_transaction = False
        def cursor(self):
            self.in_transaction = True
            return self
        def __enter__(self): return self
        def __exit__(self, *exc): pass
        def execute(self, *args): pass
        def fetchall(self): return []
        def commit(self): self.in_transaction = False
    conn = Connection()
    events = []
    class Index:
        def get_primary_key(self): return "meili_id"
        def delete_documents(self, **kwargs):
            task_uid = 1 + sum(event[0] == "submit" for event in events)
            events.append(("submit", task_uid))
            return SimpleNamespace(task_uid=task_uid)
        def get_stats(self): return SimpleNamespace(number_of_documents=0)
    class Client:
        def __init__(self, *args, **kwargs): pass
        def get_index(self, *args): return Index()
        def index(self, *args): return Index()
        def wait_for_task(self, task_uid, **kwargs):
            assert not conn.in_transaction
            events.append(("wait", task_uid))
            if task_uid == timeout_batch:
                raise TimeoutError("index queue stalled")
            return {"status": "succeeded"}
    import sys
    monkeypatch.setitem(sys.modules, "meilisearch", SimpleNamespace(Client=Client))
    monkeypatch.setattr(pipeline, "get_latest_fx_rates", lambda c: ({}, None))
    removed_ids = [f"gone-{i}" for i in range(4001)]
    monkeypatch.setattr(pipeline, "get_removed_job_ids", lambda c, **kw: removed_ids)
    deleted = []
    def acknowledge(conn, ids):
        deleted.extend(ids)
        events.append(("ack", (len(deleted) + 1999) // 2000))
    monkeypatch.setattr(pipeline, "mark_jobs_meili_deleted", acknowledge)
    pipeline.step_load(conn, parsed_job_ids=[], removed_job_ids=removed_ids)
    expected = []
    for task_uid in range(1, 4):
        expected.extend([("submit", task_uid), ("wait", task_uid)])
        if task_uid == timeout_batch:
            break
        expected.append(("ack", task_uid))
    assert events == expected
    successful_count = len(removed_ids) if timeout_batch is None else (timeout_batch - 1) * 2000
    assert deleted == removed_ids[:successful_count]


@pytest.mark.parametrize("commit_applied", [False, True])
def test_group_save_recovers_without_recomputing_groups(monkeypatch, commit_applied):
    class Connection:
        closed = False
        def close(self): self.closed = True
    initial, replacement = Connection(), Connection()
    computed = []
    groups, stats = {"job": "group"}, {"groups": 1}
    def compute(conn, boards):
        computed.append(conn)
        return groups, stats
    attempts = []
    def save(conn, incoming, boards, changed_ids_out=None):
        assert incoming is groups
        attempts.append(conn)
        if conn is initial:
            if commit_applied:
                changed_ids_out.append("job")
            raise job_groups.OperationalError("closed while computing")
        return [] if commit_applied else ["job"]
    monkeypatch.setattr(job_groups, "compute_job_groups", compute)
    monkeypatch.setattr(job_groups, "save_job_groups", save)
    monkeypatch.setattr(job_groups, "get_connection", lambda: replacement)
    assert job_groups.recompute_job_groups_for_boards(initial, [("ats", "board")]) == (["job"], stats)
    assert computed == [initial]
    assert attempts == [initial, replacement]
    assert initial.closed and replacement.closed
