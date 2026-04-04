from job_groups import _cluster_candidate_jobs, compute_job_groups, save_job_groups


def test_cluster_candidate_jobs_uses_connected_components(monkeypatch):
    similarities = {
        ("a", "b"): 0.99,
        ("a", "c"): 0.96,
        ("b", "c"): 0.94,
    }

    def fake_similarity(text_a, text_b):
        return similarities[tuple(sorted((text_a, text_b)))]

    monkeypatch.setattr("job_groups.content_similarity", fake_similarity)

    clusters = _cluster_candidate_jobs(
        ["a", "b", "c"],
        {"a": "", "b": "", "c": ""},
        {"a": "a", "b": "b", "c": "c"},
    )

    assert clusters == [["a", "b", "c"]]


def test_cluster_candidate_jobs_groups_identical_hashes_without_text():
    clusters = _cluster_candidate_jobs(
        ["a", "b", "c"],
        {"a": "same", "b": "same", "c": "different"},
        {},
    )

    assert clusters == [["a", "b"]]


def test_compute_job_groups_scopes_by_ats_and_board():
    rows = [
        (
            "greenhouse__shared__1",
            "greenhouse",
            "shared",
            "Engineer",
            {"title": "Engineer", "description": "Same content", "location": "San Francisco, CA"},
            "same-hash",
        ),
        (
            "greenhouse__shared__2",
            "greenhouse",
            "shared",
            "Engineer",
            {"title": "Engineer", "description": "Same content", "location": "New York, NY"},
            "same-hash",
        ),
        (
            "lever__shared__3",
            "lever",
            "shared",
            "Engineer",
            {"title": "Engineer", "description": "Same content", "location": "Austin, TX"},
            "same-hash",
        ),
    ]

    class FakeCursor:
        def execute(self, query, params=None):
            self.query = query
            self.params = params

        def fetchall(self):
            return rows

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def cursor(self):
            return FakeCursor()

    groups, stats = compute_job_groups(FakeConn(), boards=[("greenhouse", "shared"), ("lever", "shared")])

    assert groups["greenhouse__shared__1"] == groups["greenhouse__shared__2"]
    assert "lever__shared__3" not in groups
    assert stats["groups"] == 1
    assert stats["grouped_jobs"] == 2


def test_save_job_groups_marks_rows_changed_when_group_size_changes(monkeypatch):
    executed = []

    class FakeCursor:
        def __init__(self):
            self.query = None
            self.connection = type("C", (), {"encoding": "UTF8"})()

        def execute(self, query, params=None):
            self.query = query
            executed.append((query, params))

        def fetchall(self):
            if "SELECT id, job_group" in self.query:
                return [
                    ("a", "group-1"),
                    ("b", "group-1"),
                    ("c", "group-1"),
                ]
            return []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

    def fake_execute_values(cur, sql, argslist, template=None):
        executed.append((sql, argslist))

    monkeypatch.setattr("job_groups.execute_values", fake_execute_values)
    changed_ids = save_job_groups(FakeConn(), {"a": "group-1", "b": "group-1"}, boards=[("greenhouse", "shared")])

    assert changed_ids == ["a", "b", "c"]
