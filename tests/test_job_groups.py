from job_groups import _cluster_candidate_jobs


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
