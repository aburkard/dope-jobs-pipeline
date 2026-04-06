import sys
import types
from datetime import datetime, timezone

import pipeline
from pipeline import filter_companies_for_shard, shard_for_company, should_mark_removed, resolve_companies
from public_ids import meili_safe_job_id


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


def test_extract_posted_at_supports_iso_dates_and_epoch_millis():
    iso_value, iso_ts = pipeline._extract_posted_at({"first_published": "2026-04-01T12:30:00Z"})
    assert iso_value == "2026-04-01T12:30:00+00:00"
    assert iso_ts == int(datetime(2026, 4, 1, 12, 30, tzinfo=timezone.utc).timestamp())

    lever_value, lever_ts = pipeline._extract_posted_at({"createdAt": 1715731200000})
    assert lever_value == "2024-05-15T00:00:00+00:00"
    assert lever_ts == int(datetime(2024, 5, 15, 0, 0, tzinfo=timezone.utc).timestamp())


def test_build_years_experience_buckets_matches_overlap_semantics():
    assert pipeline._build_years_experience_buckets({"years_experience": {"min": 3, "max": 5}}) == ["3_5"]
    assert pipeline._build_years_experience_buckets({"years_experience": {"min": 3}}) == ["3_5", "6_9"]
    assert pipeline._build_years_experience_buckets({"years_experience": {"min": 10}}) == ["10_plus"]
    assert pipeline._build_years_experience_buckets({"years_experience": {}}) == ["0_2", "3_5", "6_9"]


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
    monkeypatch.setattr(pipeline, "recompute_job_groups_for_boards", lambda conn, boards: ([], {"groups": 0, "grouped_jobs": 0, "singletons": 0}))
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
        lambda conn, limit, ats_filter=None, ats_exclude_filter=None: (
            [("greenhouse", "figma")]
            if limit == 10_000_000 and ats_filter is None and ats_exclude_filter is None else []
        ),
    )
    companies = resolve_companies(object(), companies_from_db=True, db_company_limit=None)
    assert companies == [("greenhouse", "figma")]


def test_resolve_companies_uses_bounded_db_selection(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "get_companies_to_scrape",
        lambda conn, limit, ats_filter=None, ats_exclude_filter=None: (
            [
                ("greenhouse", "figma"),
                ("ashby", "openai"),
            ]
            if limit == 2 and ats_filter is None and ats_exclude_filter is None else []
        ),
    )
    companies = resolve_companies(object(), companies_from_db=True, db_company_limit=2)
    assert companies == [("greenhouse", "figma"), ("ashby", "openai")]


def test_resolve_companies_passes_ats_filter(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "get_companies_to_scrape",
        lambda conn, limit, ats_filter=None, ats_exclude_filter=None: (
            [("workable", "telegraph")]
            if limit == 5 and ats_filter == ["workable"] and ats_exclude_filter is None else []
        ),
    )
    companies = resolve_companies(
        object(),
        companies_from_db=True,
        db_company_limit=5,
        ats_filter=["workable"],
    )
    assert companies == [("workable", "telegraph")]


def test_resolve_companies_passes_ats_exclude_filter(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "get_companies_to_scrape",
        lambda conn, limit, ats_filter=None, ats_exclude_filter=None: (
            [("greenhouse", "figma")]
            if limit == 5 and ats_filter is None and ats_exclude_filter == ["workable"] else []
        ),
    )
    companies = resolve_companies(
        object(),
        companies_from_db=True,
        db_company_limit=5,
        ats_exclude_filter=["workable"],
    )
    assert companies == [("greenhouse", "figma")]


def test_resolve_companies_passes_scrape_status_filter(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "get_companies_to_scrape_by_status",
        lambda conn, limit, ats_filter=None, ats_exclude_filter=None, scrape_statuses=None: (
            [("workable", "loopme")]
            if limit == 5 and ats_filter == ["workable"] and ats_exclude_filter is None and scrape_statuses == ["pending", "error"] else []
        ),
    )
    companies = resolve_companies(
        object(),
        companies_from_db=True,
        db_company_limit=5,
        ats_filter=["workable"],
        scrape_status_filter=["pending", "error"],
    )
    assert companies == [("workable", "loopme")]


def test_resolve_companies_passes_ats_exclude_filter_with_status_selection(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "get_companies_to_scrape_by_status",
        lambda conn, limit, ats_filter=None, ats_exclude_filter=None, scrape_statuses=None: (
            [("greenhouse", "openai")]
            if limit == 5 and ats_filter is None and ats_exclude_filter == ["workable"] and scrape_statuses == ["pending"] else []
        ),
    )
    companies = resolve_companies(
        object(),
        companies_from_db=True,
        db_company_limit=5,
        ats_exclude_filter=["workable"],
        scrape_status_filter=["pending"],
    )
    assert companies == [("greenhouse", "openai")]


def test_step_scrape_reuses_existing_jobvite_descriptions(monkeypatch):
    class FakeScraper:
        def __init__(self, token):
            self.token = token

        def fetch_jobs(self, existing_details=None, refetch_existing_detail=False):
            assert refetch_existing_detail is False
            assert existing_details == {
                "123": {
                    "description": "Stored description",
                    "descriptionHtml": "<p>Stored description</p>",
                    "datePosted": "2026-01-30",
                    "validThrough": None,
                    "inactive": False,
                }
            }
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
        "jobvite__ninjaone__123": {
            "description": "Stored description",
            "descriptionHtml": "<p>Stored description</p>",
            "datePosted": "2026-01-30",
        },
    })
    monkeypatch.setattr(pipeline, "upsert_scraped_jobs", lambda conn, jobs: {
        "new": jobs,
        "changed": [],
        "unchanged": 0,
        "needs_detail_fetch": [],
    })
    monkeypatch.setattr(pipeline, "recompute_job_groups_for_boards", lambda conn, boards: ([], {"groups": 0, "grouped_jobs": 0, "singletons": 0}))
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
    monkeypatch.setattr(pipeline, "recompute_job_groups_for_boards", lambda conn, boards: ([], {"groups": 0, "grouped_jobs": 0, "singletons": 0}))
    monkeypatch.setattr(pipeline, "mark_removed", lambda conn, ats, token, seen_ids: [])
    monkeypatch.setattr(pipeline, "upsert_company", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "time", type("T", (), {"sleep": staticmethod(lambda _: None)}))

    result = pipeline.step_scrape(object(), [("greenhouse", "figma")], max_per_company=None)

    assert result["new_count"] == 3


def test_step_scrape_refreshes_job_groups_for_successful_boards_only(monkeypatch):
    class GoodScraper:
        def __init__(self, token):
            self.token = token

        def fetch_jobs(self):
            yield {
                "id": f"greenhouse__{self.token}__1",
                "ats_name": "greenhouse",
                "board_token": self.token,
                "title": "Engineer",
            }

        def get_company_name(self):
            return self.token

        def get_company_domain(self):
            return f"{self.token}.example.com"

        def get_company_logo_url(self):
            return None

    class BadScraper:
        def __init__(self, token):
            self.token = token

        def fetch_jobs(self):
            raise RuntimeError("boom")

    captured = {}
    monkeypatch.setitem(pipeline.ATS_SCRAPERS, "greenhouse", GoodScraper)
    monkeypatch.setitem(pipeline.ATS_SCRAPERS, "lever", BadScraper)
    monkeypatch.setattr(pipeline, "get_existing_jobs_for_board", lambda conn, ats, token: {})
    monkeypatch.setattr(pipeline, "upsert_scraped_jobs", lambda conn, jobs: {
        "new": jobs,
        "changed": [],
        "unchanged": 0,
        "needs_detail_fetch": [],
    })
    monkeypatch.setattr(pipeline, "mark_removed", lambda conn, ats, token, seen_ids: [])
    monkeypatch.setattr(pipeline, "upsert_company", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        pipeline,
        "recompute_job_groups_for_boards",
        lambda conn, boards: (
            captured.setdefault("boards", list(boards)) and ["greenhouse__figma__1"],
            {"groups": 1, "grouped_jobs": 2, "singletons": 0},
        ),
    )
    monkeypatch.setattr(pipeline, "time", type("T", (), {"sleep": staticmethod(lambda _: None)}))

    result = pipeline.step_scrape(
        object(),
        [("greenhouse", "figma"), ("lever", "spotify")],
        max_per_company=5,
    )

    assert captured["boards"] == [("greenhouse", "figma")]
    assert result["job_group_changed_job_ids"] == {"greenhouse__figma__1"}


def test_step_scrape_recovers_closed_db_connection_between_companies(monkeypatch):
    class FakeConn:
        def __init__(self):
            self.closed = 0
            self.rollback_calls = 0

        def rollback(self):
            if self.closed:
                raise RuntimeError("connection already closed")
            self.rollback_calls += 1

        def close(self):
            self.closed = 1

    class FakeScraper:
        def __init__(self, token):
            self.token = token

        def fetch_jobs(self):
            yield {
                "id": f"greenhouse__{self.token}__1",
                "ats_name": "greenhouse",
                "board_token": self.token,
                "title": f"Job for {self.token}",
            }

        def get_company_name(self):
            return self.token

        def get_company_domain(self):
            return f"{self.token}.example.com"

        def get_company_logo_url(self):
            return None

    reconnected = []
    company_updates = []
    good_conn_seen = []

    def fake_get_connection():
        conn = FakeConn()
        reconnected.append(conn)
        return conn

    monkeypatch.setitem(pipeline.ATS_SCRAPERS, "greenhouse", FakeScraper)
    monkeypatch.setattr(pipeline, "get_connection", fake_get_connection)
    monkeypatch.setattr(pipeline, "init_schema", lambda conn: None)

    def fake_get_existing_jobs_for_board(conn, ats, token):
        if token == "broken":
            conn.closed = 1
            raise RuntimeError("SSL connection has been closed unexpectedly")
        good_conn_seen.append(conn)
        return {}

    monkeypatch.setattr(pipeline, "get_existing_jobs_for_board", fake_get_existing_jobs_for_board)
    monkeypatch.setattr(pipeline, "upsert_scraped_jobs", lambda conn, jobs: {
        "new": jobs,
        "changed": [],
        "unchanged": 0,
        "needs_detail_fetch": [],
    })
    monkeypatch.setattr(pipeline, "recompute_job_groups_for_boards", lambda conn, boards: ([], {"groups": 0, "grouped_jobs": 0, "singletons": 0}))
    monkeypatch.setattr(pipeline, "mark_removed", lambda conn, ats, token, seen_ids: [])
    monkeypatch.setattr(pipeline, "upsert_company", lambda conn, ats, token, **kwargs: company_updates.append((token, conn)))
    monkeypatch.setattr(pipeline, "time", type("T", (), {"sleep": staticmethod(lambda _: None)}))

    initial_conn = FakeConn()
    result = pipeline.step_scrape(
        initial_conn,
        [("greenhouse", "broken"), ("greenhouse", "healthy")],
        max_per_company=5,
    )

    assert result["errors"] == 1
    assert result["new_count"] == 1
    assert len(reconnected) == 1
    assert good_conn_seen == [reconnected[0]]
    assert company_updates == [("healthy", reconnected[0])]
    assert result["conn"] is reconnected[0]


def test_main_loads_job_group_changed_rows(monkeypatch):
    captured = {}

    class FakeConn:
        closed = 0

        def close(self):
            self.closed = 1

    monkeypatch.setattr(sys, "argv", ["pipeline.py", "--companies", "companies.txt", "--skip-parse"])
    monkeypatch.setattr(pipeline, "get_connection", lambda: FakeConn())
    monkeypatch.setattr(pipeline, "init_schema", lambda conn: None)
    monkeypatch.setattr(pipeline, "resolve_companies", lambda *args, **kwargs: [("greenhouse", "figma")])
    monkeypatch.setattr(pipeline, "filter_companies_for_shard", lambda companies, shard_index, total_shards: companies)
    monkeypatch.setattr(
        pipeline,
        "step_scrape",
        lambda conn, companies, max_per_company=None, jobvite_refetch_existing_detail=False: {
            "touched_job_ids": set(),
            "removed_job_ids": set(),
            "job_group_changed_job_ids": {"group-only-1", "group-only-2"},
            "conn": conn,
        },
    )
    monkeypatch.setattr(pipeline, "step_load", lambda conn, **kwargs: captured.setdefault("kwargs", kwargs))

    pipeline.main()

    assert set(captured["kwargs"]["parsed_job_ids"]) == {"group-only-1", "group-only-2"}


def test_build_meili_location_uses_remote_applicant_geography():
    parsed = {
        "office_type": "remote",
        "locations": [],
        "applicant_location_requirements": [
            {"scope": "country", "name": "United States", "country_code": "US"},
            {"scope": "country", "name": "Canada", "country_code": "CA"},
        ],
    }
    assert pipeline._build_meili_location(parsed) == "United States • Canada"


def test_build_meili_locations_all_uses_all_work_locations():
    parsed = {
        "office_type": "hybrid",
        "locations": [
            {"label": "San Francisco, California, United States"},
            {"city": "New York City", "state": "New York", "country_code": "US"},
            {"label": "San Francisco, California, United States"},
        ],
        "applicant_location_requirements": [
            {"scope": "country", "name": "United States", "country_code": "US"},
        ],
    }
    assert pipeline._build_meili_locations_all(parsed) == [
        "San Francisco, California, United States",
        "New York City, New York, US",
    ]


def test_build_meili_locations_all_falls_back_to_remote_requirements():
    parsed = {
        "office_type": "remote",
        "locations": [],
        "applicant_location_requirements": [
            {"scope": "country", "name": "United States", "country_code": "US"},
            {"scope": "country", "name": "Canada", "country_code": "CA"},
            {"scope": "country", "name": "United States", "country_code": "US"},
        ],
    }
    assert pipeline._build_meili_locations_all(parsed) == ["United States", "Canada"]


def test_build_job_geo_fields_uses_country_scoped_admin1_keys():
    parsed = {
        "locations": [
            {"geoname_id": 1, "country_code": "US"},
            {"geoname_id": 2, "country_code": "CA"},
        ],
        "applicant_location_requirements": [
            {"scope": "country", "country_code": "US", "geoname_id": 10},
            {"scope": "state", "country_code": "US", "geoname_id": 11},
            {"scope": "city", "country_code": "CA", "geoname_id": 12},
        ],
    }
    geo_lookup = {
        1: {"country_code": "US", "admin1_code": "CA", "kind": "locality"},
        2: {"country_code": "CA", "admin1_code": "ON", "kind": "locality"},
        10: {"country_code": "US", "admin1_code": None, "kind": "country"},
        11: {"country_code": "US", "admin1_code": "CA", "kind": "admin1"},
        12: {"country_code": "CA", "admin1_code": "ON", "kind": "locality"},
    }
    assert pipeline._build_job_geo_fields(parsed, geo_lookup) == {
        "work_geoname_ids": [1, 2],
        "work_country_codes": ["US", "CA"],
        "work_admin1_keys": ["US-CA", "CA-ON"],
        "applicant_country_codes": ["US", "CA"],
        "applicant_admin1_keys": ["US-CA", "CA-ON"],
    }


def test_build_primary_geo_uses_first_valid_point():
    parsed = {
        "locations": [
            {"label": "United States"},
            {"label": "New York, New York, United States", "lat": 40.7128, "lng": -74.0060},
            {"label": "San Francisco, California, United States", "lat": 37.7749, "lng": -122.4194},
        ],
    }
    assert pipeline._build_primary_geo(parsed) == {"lat": 40.7128, "lng": -74.006}


def test_build_job_geo_fields_normalizes_country_names():
    parsed = {
        "locations": [
            {"label": "Lisbon", "country_code": "Portugal"},
        ],
        "applicant_location_requirements": [
            {"scope": "country", "country_code": "United States"},
        ],
    }

    assert pipeline._build_job_geo_fields(parsed, {}) == {
        "work_geoname_ids": [],
        "work_country_codes": ["PT"],
        "work_admin1_keys": [],
        "applicant_country_codes": ["US"],
        "applicant_admin1_keys": [],
    }


def test_build_job_geojson_includes_all_unique_points():
    parsed = {
        "locations": [
            {"label": "New York, New York, United States", "lat": 40.7128, "lng": -74.0060},
            {"label": "San Francisco, California, United States", "lat": 37.7749, "lng": -122.4194},
            {"label": "San Francisco duplicate", "lat": 37.7749, "lng": -122.4194},
            {"label": "Broad United States"},
        ],
    }
    assert pipeline._build_job_geojson(parsed) == {
        "type": "Feature",
        "geometry": {
            "type": "MultiPoint",
            "coordinates": [
                [-74.006, 40.7128],
                [-122.4194, 37.7749],
            ],
        },
    }


def test_step_load_marks_loaded_and_deleted(monkeypatch):
    captured = {}

    class FakeCursor:
        def execute(self, query, params=None):
            self.query = query
            self.params = params

        def fetchall(self):
            if "FROM pipeline_companies" in self.query:
                return [("greenhouse", "figma", "Figma", "figma", "figma.com", "https://example.com/logo.png")]
            return []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def commit(self):
            return None

    class FakeTask:
        def __init__(self, task_uid):
            self.task_uid = task_uid

    class FakeIndex:
        def __init__(self):
            self.primary_key = "meili_id"

        def update_filterable_attributes(self, attrs):
            self.filterable = attrs

        def update_searchable_attributes(self, attrs):
            self.searchable = attrs

        def update_sortable_attributes(self, attrs):
            self.sortable = attrs

        def update_settings(self, settings):
            self.settings = settings

        def get_primary_key(self):
            return self.primary_key

        def add_documents(self, docs, primary_key="id"):
            self.docs = docs
            self.primary_key_arg = primary_key
            return FakeTask(1)

        def delete_documents(self, ids):
            self.deleted = ids
            return FakeTask(2)

        def get_stats(self):
            return types.SimpleNamespace(number_of_documents=1)

    class FakeClient:
        def __init__(self, host, key):
            self.host = host
            self.key = key
            self._index = FakeIndex()
            captured["client"] = self

        def get_index(self, uid):
            assert uid == "jobs"
            return self._index

        def index(self, name):
            assert name == "jobs"
            return self._index

        def wait_for_task(self, task_uid, timeout_in_ms=0):
            return {"uid": task_uid}

    fake_meili = types.SimpleNamespace(Client=FakeClient)
    monkeypatch.setitem(sys.modules, "meilisearch", fake_meili)

    monkeypatch.setattr(
        pipeline,
        "get_active_jobs_for_meili",
        lambda conn, job_ids=None, include_removed=False: [
            {
                "id": "greenhouse__figma__123",
                "public_job_id": "abc123",
                "ats": "greenhouse",
                "board_token": "figma",
                "title": "Engineer",
                "parsed_json": {
                    "tagline": "Build product quality systems at Figma.",
                    "locations": [],
                    "applicant_location_requirements": [],
                    "office_type": "remote",
                    "job_type": "full_time",
                    "experience_level": "senior",
                    "is_manager": False,
                    "industry_primary": "enterprise_software",
                    "industry_tags": ["enterprise_software"],
                    "salary": None,
                    "salary_transparency": "not_disclosed",
                    "years_experience": {"min": 3, "max": 5},
                    "education_level": "bachelors",
                    "hard_skills": [],
                    "soft_skills": [],
                    "cool_factor": "interesting",
                    "vibe_tags": [],
                    "visa_sponsorship": "unknown",
                    "equity": {"offered": False},
                    "company_stage": "public",
                    "benefits_categories": [],
                    "benefits_highlights": [],
                    "reports_to": "",
                },
                "job_group": None,
                "raw_json": {"first_published": "2026-04-01T12:30:00Z"},
            }
        ],
    )
    monkeypatch.setattr(pipeline, "get_removed_job_ids", lambda conn, job_ids=None: ["greenhouse__figma__gone"])
    monkeypatch.setattr(pipeline, "get_latest_fx_rates", lambda conn: ({}, None))
    monkeypatch.setattr(pipeline, "_load_geo_place_lookup", lambda conn, ids: {})
    monkeypatch.setattr(pipeline, "_build_primary_geo", lambda parsed: None)
    monkeypatch.setattr(pipeline, "_build_job_geojson", lambda parsed: None)
    monkeypatch.setattr(pipeline, "_build_job_geo_fields", lambda parsed, lookup: {})

    loaded_calls = []
    deleted_calls = []
    monkeypatch.setattr(pipeline, "mark_jobs_meili_loaded", lambda conn, ids: loaded_calls.append(list(ids)))
    monkeypatch.setattr(pipeline, "mark_jobs_meili_deleted", lambda conn, ids: deleted_calls.append(list(ids)))

    pipeline.step_load(
        FakeConn(),
        meili_host="http://example.com",
        meili_key="key",
        parsed_job_ids=["greenhouse__figma__123"],
        removed_job_ids=["greenhouse__figma__gone"],
    )

    assert loaded_calls == [["greenhouse__figma__123"]]
    assert deleted_calls == [["greenhouse__figma__gone"]]
    assert captured["client"]._index.primary_key_arg == "meili_id"
    assert captured["client"]._index.docs[0]["meili_id"] == meili_safe_job_id("greenhouse__figma__123")
    assert captured["client"]._index.docs[0]["posted_at_ts"] == 1775046600
    assert captured["client"]._index.docs[0]["years_experience_min"] == 3
    assert captured["client"]._index.docs[0]["years_experience_max"] == 5
    assert captured["client"]._index.docs[0]["years_experience_buckets"] == ["3_5"]
    assert captured["client"]._index.docs[0]["education_level"] == "bachelors"
    assert not hasattr(captured["client"]._index, "filterable")
    assert not hasattr(captured["client"]._index, "searchable")
    assert not hasattr(captured["client"]._index, "sortable")
    assert captured["client"]._index.deleted == [meili_safe_job_id("greenhouse__figma__gone")]


def test_step_load_full_reload_refreshes_index_settings(monkeypatch):
    captured = {}

    class FakeCursor:
        def execute(self, query, params=None):
            self.query = query
            self.params = params

        def fetchall(self):
            if "FROM pipeline_companies" in self.query:
                return [("greenhouse", "figma", "Figma", "figma", "figma.com", "https://example.com/logo.png")]
            return []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    class FakeTask:
        def __init__(self, task_uid):
            self.task_uid = task_uid

    class FakeIndex:
        def __init__(self):
            self.primary_key = "meili_id"

        def update_filterable_attributes(self, attrs):
            self.filterable = attrs

        def update_searchable_attributes(self, attrs):
            self.searchable = attrs

        def update_sortable_attributes(self, attrs):
            self.sortable = attrs

        def update_settings(self, settings):
            self.settings = settings

        def get_primary_key(self):
            return self.primary_key

        def add_documents(self, docs, primary_key="id"):
            self.docs = docs
            self.primary_key_arg = primary_key
            return FakeTask(1)

        def delete_documents(self, ids):
            self.deleted = ids
            return FakeTask(2)

        def get_stats(self):
            return types.SimpleNamespace(number_of_documents=1)

    class FakeClient:
        def __init__(self, host, key):
            self.host = host
            self.key = key
            self._index = FakeIndex()
            captured["client"] = self

        def get_index(self, uid):
            assert uid == "jobs"
            return self._index

        def index(self, name):
            assert name == "jobs"
            return self._index

        def wait_for_task(self, task_uid, timeout_in_ms=0):
            return {"uid": task_uid}

    fake_meili = types.SimpleNamespace(Client=FakeClient)
    monkeypatch.setitem(sys.modules, "meilisearch", fake_meili)

    monkeypatch.setattr(
        pipeline,
        "get_active_jobs_for_meili",
        lambda conn, job_ids=None, include_removed=False: [
            {
                "id": "greenhouse__figma__123",
                "public_job_id": "abc123",
                "ats": "greenhouse",
                "board_token": "figma",
                "title": "Engineer",
                "parsed_json": {
                    "tagline": "Build product quality systems at Figma.",
                    "locations": [],
                    "applicant_location_requirements": [],
                    "office_type": "remote",
                    "job_type": "full_time",
                    "experience_level": "senior",
                    "is_manager": False,
                    "industry_primary": "enterprise_software",
                    "industry_tags": ["enterprise_software"],
                    "salary": None,
                    "salary_transparency": "not_disclosed",
                    "years_experience": {"min": 3, "max": 5},
                    "education_level": "bachelors",
                    "hard_skills": [],
                    "soft_skills": [],
                    "cool_factor": "interesting",
                    "vibe_tags": [],
                    "visa_sponsorship": "unknown",
                    "equity": {"offered": False},
                    "company_stage": "public",
                    "benefits_categories": [],
                    "benefits_highlights": [],
                    "reports_to": "",
                },
                "job_group": None,
                "raw_json": {"first_published": "2026-04-01T12:30:00Z"},
            }
        ],
    )
    monkeypatch.setattr(pipeline, "get_removed_job_ids", lambda conn, job_ids=None: [])
    monkeypatch.setattr(pipeline, "get_latest_fx_rates", lambda conn: ({}, None))
    monkeypatch.setattr(pipeline, "_load_geo_place_lookup", lambda conn, ids: {})
    monkeypatch.setattr(pipeline, "_build_primary_geo", lambda parsed: None)
    monkeypatch.setattr(pipeline, "_build_job_geojson", lambda parsed: None)
    monkeypatch.setattr(pipeline, "_build_job_geo_fields", lambda parsed, lookup: {})
    monkeypatch.setattr(pipeline, "mark_jobs_meili_loaded", lambda conn, ids: None)
    monkeypatch.setattr(pipeline, "mark_jobs_meili_deleted", lambda conn, ids: None)

    pipeline.step_load(
        FakeConn(),
        meili_host="http://example.com",
        meili_key="key",
        full_reload=True,
    )

    assert "posted_at_ts" in captured["client"]._index.filterable
    assert "years_experience_min" in captured["client"]._index.filterable
    assert "years_experience_max" in captured["client"]._index.filterable
    assert "years_experience_buckets" in captured["client"]._index.filterable
    assert "education_level" in captured["client"]._index.filterable


def test_step_load_stops_submitting_batches_after_timeout(monkeypatch):
    captured = {}

    class FakeCursor:
        def execute(self, query, params=None):
            self.query = query
            self.params = params

        def fetchall(self):
            if "FROM pipeline_companies" in self.query:
                return [("greenhouse", "figma", "Figma", "figma", "figma.com", "https://example.com/logo.png")]
            return []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def cursor(self):
            return FakeCursor()

    class FakeTask:
        def __init__(self, task_uid):
            self.task_uid = task_uid

    class FakeIndex:
        def __init__(self):
            self.primary_key = "meili_id"
            self.add_calls = 0

        def get_primary_key(self):
            return self.primary_key

        def add_documents(self, docs, primary_key="id"):
            self.add_calls += 1
            self.docs = docs
            self.primary_key_arg = primary_key
            return FakeTask(100 + self.add_calls)

        def get_stats(self):
            return types.SimpleNamespace(number_of_documents=1)

    class FakeClient:
        def __init__(self, host, key):
            self._index = FakeIndex()
            captured["client"] = self

        def get_index(self, uid):
            return self._index

        def index(self, name):
            return self._index

        def wait_for_task(self, task_uid, timeout_in_ms=0):
            return {"uid": task_uid, "status": "processing"}

    fake_meili = types.SimpleNamespace(Client=FakeClient)
    monkeypatch.setitem(sys.modules, "meilisearch", fake_meili)
    import geo_resolver
    monkeypatch.setattr(
        geo_resolver,
        "GeoResolver",
        lambda conn: types.SimpleNamespace(resolve_parsed_geo=lambda parsed: parsed),
    )
    monkeypatch.setattr(
        pipeline,
        "get_active_jobs_for_meili",
        lambda conn, job_ids=None, include_removed=False: [
            {
                "id": f"greenhouse__figma__{i}",
                "public_job_id": f"abc{i}",
                "ats": "greenhouse",
                "board_token": "figma",
                "title": f"Engineer {i}",
                "parsed_json": {
                    "tagline": "Build product quality systems at Figma.",
                    "locations": [],
                    "applicant_location_requirements": [],
                    "office_type": "remote",
                    "job_type": "full_time",
                    "experience_level": "senior",
                    "is_manager": False,
                    "industry_primary": "enterprise_software",
                    "industry_tags": ["enterprise_software"],
                    "salary": None,
                    "salary_transparency": "not_disclosed",
                    "years_experience": {"min": 3, "max": 5},
                    "education_level": "bachelors",
                    "hard_skills": [],
                    "soft_skills": [],
                    "cool_factor": "interesting",
                    "vibe_tags": [],
                    "visa_sponsorship": "unknown",
                    "equity": {"offered": False},
                    "company_stage": "public",
                    "benefits_categories": [],
                    "benefits_highlights": [],
                    "reports_to": "",
                },
                "job_group": None,
                "raw_json": {"first_published": "2026-04-01T12:30:00Z"},
            }
            for i in range(2)
        ],
    )
    monkeypatch.setattr(pipeline, "get_removed_job_ids", lambda conn, job_ids=None: [])
    monkeypatch.setattr(pipeline, "get_latest_fx_rates", lambda conn: ({}, None))
    monkeypatch.setattr(pipeline, "_load_geo_place_lookup", lambda conn, ids: {})
    monkeypatch.setattr(pipeline, "_build_primary_geo", lambda parsed: None)
    monkeypatch.setattr(pipeline, "_build_job_geojson", lambda parsed: None)
    monkeypatch.setattr(pipeline, "_build_job_geo_fields", lambda parsed, lookup: {})
    monkeypatch.setattr(pipeline, "mark_jobs_meili_loaded", lambda conn, ids: None)
    monkeypatch.setattr(pipeline, "mark_jobs_meili_deleted", lambda conn, ids: None)

    try:
        pipeline.step_load(
            FakeConn(),
            meili_host="http://example.com",
            meili_key="key",
            parsed_job_ids=["greenhouse__figma__0", "greenhouse__figma__1"],
            removed_job_ids=[],
            meili_batch_size=1,
        )
    except RuntimeError as exc:
        assert "Timed out waiting for Meili task" in str(exc)
    else:
        raise AssertionError("Expected step_load to stop after a timed-out Meili task")

    assert captured["client"]._index.add_calls == 1


def test_step_load_includes_unparsed_active_jobs_with_ats_metadata(monkeypatch):
    captured = {}

    class FakeCursor:
        def execute(self, query, params=None):
            self.query = query
            self.params = params

        def fetchone(self):
            return ([],)

        def fetchall(self):
            if "FROM pipeline_companies" in self.query:
                return [("ashby", "acme", "Acme", "acme", "acme.com", "https://example.com/logo.png")]
            return []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    class FakeTask:
        def __init__(self, task_uid):
            self.task_uid = task_uid

    class FakeIndex:
        def __init__(self):
            self.primary_key = "meili_id"

        def get_primary_key(self):
            return self.primary_key

        def add_documents(self, docs, primary_key="id"):
            self.docs = docs
            self.primary_key_arg = primary_key
            return FakeTask(1)

        def get_stats(self):
            return types.SimpleNamespace(number_of_documents=1)

    class FakeClient:
        def __init__(self, host, key):
            self._index = FakeIndex()
            captured["client"] = self

        def get_index(self, uid):
            return self._index

        def index(self, name):
            return self._index

        def wait_for_task(self, task_uid, timeout_in_ms=0):
            return {"uid": task_uid, "status": "succeeded"}

    fake_meili = types.SimpleNamespace(Client=FakeClient)
    monkeypatch.setitem(sys.modules, "meilisearch", fake_meili)
    import geo_resolver
    monkeypatch.setattr(
        geo_resolver,
        "GeoResolver",
        lambda conn: types.SimpleNamespace(resolve_parsed_geo=lambda parsed: parsed),
    )
    monkeypatch.setattr(
        pipeline,
        "get_active_jobs_for_meili",
        lambda conn, job_ids=None, include_removed=False: [
            {
                "id": "ashby__acme__1",
                "public_job_id": "pub1",
                "ats": "ashby",
                "board_token": "acme",
                "title": "Backend Engineer",
                "parsed_json": None,
                "job_group": None,
                "raw_json": {
                    "description": "Build APIs for our platform.",
                    "employmentType": "full-time",
                    "workplaceType": "remote",
                    "locationName": "United States",
                    "locationCountry": "United States",
                    "allLocations": ["United States"],
                    "compensationSalarySummary": "$150K - $190K",
                    "compensationTierSummary": "Base salary plus equity",
                    "experience": "Mid-Senior level",
                    "education": "Bachelor's Degree",
                    "jobUrl": "https://jobs.example.com/1",
                    "publishedDate": "2026-04-01T12:30:00Z",
                },
            }
        ],
    )
    monkeypatch.setattr(pipeline, "get_removed_job_ids", lambda conn, job_ids=None: [])
    monkeypatch.setattr(pipeline, "get_latest_fx_rates", lambda conn: ({}, None))
    monkeypatch.setattr(pipeline, "_load_geo_place_lookup", lambda conn, ids: {})
    monkeypatch.setattr(pipeline, "_build_primary_geo", lambda parsed: None)
    monkeypatch.setattr(pipeline, "_build_job_geojson", lambda parsed: None)
    monkeypatch.setattr(pipeline, "_build_job_geo_fields", lambda parsed, lookup: {})
    monkeypatch.setattr(pipeline, "mark_jobs_meili_loaded", lambda conn, ids: None)
    monkeypatch.setattr(pipeline, "mark_jobs_meili_deleted", lambda conn, ids: None)

    pipeline.step_load(
        FakeConn(),
        meili_host="http://example.com",
        meili_key="key",
        parsed_job_ids=["ashby__acme__1"],
        removed_job_ids=[],
    )

    doc = captured["client"]._index.docs[0]
    assert doc["id"] == "ashby__acme__1"
    assert doc["is_enriched"] is False
    assert doc["office_type"] == "remote"
    assert doc["job_type"] == "full-time"
    assert doc["experience_level"] == "senior"
    assert doc["education_level"] == "bachelors"
    assert doc["salary_min"] == 150000.0
    assert doc["salary_max"] == 190000.0
    assert doc["equity_offered"] is True
    assert doc["location"] == "United States"
    assert doc["apply_url"] == "https://jobs.example.com/1"
