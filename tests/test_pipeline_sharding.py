import pipeline
from pipeline import filter_companies_for_shard, shard_for_company, should_mark_removed, resolve_companies


def test_shard_for_company_is_stable():
    shard_a = shard_for_company("greenhouse", "anthropic", 8)
    shard_b = shard_for_company("greenhouse", "anthropic", 8)
    assert shard_a == shard_b


def test_filter_companies_for_shard_covers_all_companies_once():
    companies = [
        ("greenhouse", "anthropic"),
        ("greenhouse", "figma"),
        ("lever", "spotify"),
        ("ashby", "ramp"),
        ("jobvite", "logitech"),
    ]
    shards = []
    for shard_index in range(4):
        shards.extend(filter_companies_for_shard(companies, shard_index, 4))

    assert sorted(shards) == sorted(companies)
    assert len(shards) == len(companies)


def test_filter_companies_for_shard_noop_without_shard_args():
    companies = [("greenhouse", "anthropic"), ("lever", "spotify")]
    assert filter_companies_for_shard(companies, None, None) == companies


def test_should_mark_removed_only_for_complete_scrapes():
    assert should_mark_removed(3, None) is True
    assert should_mark_removed(3, 10) is True
    assert should_mark_removed(10, 10) is False


def test_step_scrape_does_not_overwrite_company_job_count_for_truncated_scrapes(monkeypatch):
    class FakeScraper:
        def __init__(self, token):
            self.token = token

        def fetch_jobs(self):
            for i in range(5):
                yield {
                    "id": str(i),
                    "ats_name": "greenhouse",
                    "board_token": self.token,
                    "title": f"Job {i}",
                }

        def get_company_name(self):
            return "Toast"

        def get_company_domain(self):
            return "toasttab.com"

        def get_company_logo_url(self):
            return "https://example.com/logo.png"

    captured = {}

    monkeypatch.setitem(pipeline.ATS_SCRAPERS, "greenhouse", FakeScraper)
    monkeypatch.setattr(pipeline, "get_existing_jobs_for_board", lambda conn, ats, token: {})
    monkeypatch.setattr(pipeline, "upsert_scraped_jobs", lambda conn, jobs: {
        "new": jobs,
        "changed": [],
        "unchanged": 0,
        "needs_detail_fetch": [],
    })
    monkeypatch.setattr(pipeline, "mark_removed", lambda conn, ats, token, seen_ids: [])
    monkeypatch.setattr(pipeline, "time", type("T", (), {"sleep": staticmethod(lambda _: None)}))

    def fake_upsert_company(conn, ats, token, **kwargs):
        captured["ats"] = ats
        captured["token"] = token
        captured.update(kwargs)

    monkeypatch.setattr(pipeline, "upsert_company", fake_upsert_company)

    pipeline.step_scrape(object(), [("greenhouse", "toast")], max_per_company=5)

    assert captured["ats"] == "greenhouse"
    assert captured["token"] == "toast"
    assert captured["scraped_logo_url"] == "https://example.com/logo.png"
    assert captured["job_count"] == 5
    assert captured["job_count_exact"] is False


def test_resolve_companies_allows_unbounded_db_selection(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "get_companies_to_scrape",
        lambda conn, limit: [("greenhouse", "figma")] if limit == 10_000_000 else [],
    )
    companies = resolve_companies(object(), companies_from_db=True, db_company_limit=None)
    assert companies == [("greenhouse", "figma")]


def test_resolve_companies_uses_bounded_db_selection(monkeypatch):
    monkeypatch.setattr(pipeline, "get_companies_to_scrape", lambda conn, limit: [
        ("greenhouse", "figma"),
        ("ashby", "openai"),
    ] if limit == 2 else [])
    companies = resolve_companies(object(), companies_from_db=True, db_company_limit=2)
    assert companies == [("greenhouse", "figma"), ("ashby", "openai")]


def test_step_scrape_reuses_existing_jobvite_descriptions(monkeypatch):
    class FakeScraper:
        def __init__(self, token):
            self.token = token

        def fetch_jobs(self, existing_descriptions=None, refetch_existing_detail=False):
            assert refetch_existing_detail is False
            assert existing_descriptions == {"123": "Stored description"}
            yield {
                "id": "jobvite__ninjaone__123",
                "ats_name": "jobvite",
                "board_token": self.token,
                "title": "Account Executive",
                "description": "Stored description",
            }

        def get_company_name(self):
            return "NinjaOne"

        def get_company_domain(self):
            return "ninjaone.com"

        def get_company_logo_url(self):
            return None

    monkeypatch.setitem(pipeline.ATS_SCRAPERS, "jobvite", FakeScraper)
    monkeypatch.setattr(pipeline, "get_existing_jobs_for_board", lambda conn, ats, token: {
        "jobvite__ninjaone__123": {"description": "Stored description"},
    })
    monkeypatch.setattr(pipeline, "upsert_scraped_jobs", lambda conn, jobs: {
        "new": jobs,
        "changed": [],
        "unchanged": 0,
        "needs_detail_fetch": [],
    })
    monkeypatch.setattr(pipeline, "mark_removed", lambda conn, ats, token, seen_ids: [])
    monkeypatch.setattr(pipeline, "upsert_company", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "time", type("T", (), {"sleep": staticmethod(lambda _: None)}))

    result = pipeline.step_scrape(object(), [("jobvite", "ninjaone")], max_per_company=5)

    assert result["new_count"] == 1


def test_step_scrape_skips_greenhouse_pay_refetch_when_existing_pay_present(monkeypatch):
    class FakeScraper:
        def __init__(self, token):
            self.token = token

        def fetch_jobs(self):
            yield {
                "id": "greenhouse__figma__123",
                "ats_name": "greenhouse",
                "board_token": self.token,
                "title": "Engineer",
                "updated_at": "2026-03-26T00:00:00Z",
            }

        def fetch_job_pay(self, raw_id):
            raise AssertionError("fetch_job_pay should not be called for unchanged jobs with stored pay data")

        def get_company_name(self):
            return "Figma"

        def get_company_domain(self):
            return "figma.com"

        def get_company_logo_url(self):
            return None

    monkeypatch.setitem(pipeline.ATS_SCRAPERS, "greenhouse", FakeScraper)
    monkeypatch.setattr(pipeline, "get_existing_jobs_for_board", lambda conn, ats, token: {
        "greenhouse__figma__123": {"pay_input_ranges": [{"min_cents": 100000}]},
    })
    monkeypatch.setattr(pipeline, "upsert_scraped_jobs", lambda conn, jobs: {
        "new": [],
        "changed": [],
        "unchanged": 1,
        "needs_detail_fetch": jobs,
    })
    monkeypatch.setattr(pipeline, "mark_removed", lambda conn, ats, token, seen_ids: [])
    monkeypatch.setattr(pipeline, "upsert_company", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "time", type("T", (), {"sleep": staticmethod(lambda _: None)}))

    result = pipeline.step_scrape(object(), [("greenhouse", "figma")], max_per_company=5)

    assert result["changed_count"] == 0


def test_step_scrape_without_cap_fetches_all_jobs(monkeypatch):
    class FakeScraper:
        def __init__(self, token):
            self.token = token

        def fetch_jobs(self):
            for i in range(3):
                yield {
                    "id": f"greenhouse__figma__{i}",
                    "ats_name": "greenhouse",
                    "board_token": self.token,
                    "title": f"Job {i}",
                }

        def get_company_name(self):
            return "Figma"

        def get_company_domain(self):
            return "figma.com"

        def get_company_logo_url(self):
            return None

    monkeypatch.setitem(pipeline.ATS_SCRAPERS, "greenhouse", FakeScraper)
    monkeypatch.setattr(pipeline, "get_existing_jobs_for_board", lambda conn, ats, token: {})
    monkeypatch.setattr(pipeline, "upsert_scraped_jobs", lambda conn, jobs: {
        "new": jobs,
        "changed": [],
        "unchanged": 0,
        "needs_detail_fetch": [],
    })
    monkeypatch.setattr(pipeline, "mark_removed", lambda conn, ats, token, seen_ids: [])
    monkeypatch.setattr(pipeline, "upsert_company", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "time", type("T", (), {"sleep": staticmethod(lambda _: None)}))

    result = pipeline.step_scrape(object(), [("greenhouse", "figma")], max_per_company=None)

    assert result["new_count"] == 3
