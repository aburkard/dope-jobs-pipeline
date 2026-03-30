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

        def fetch_jobs(self, existing_details=None, refetch_existing_detail=False):
            assert refetch_existing_detail is False
            assert existing_details == {
                "123": {
                    "description": "Stored description",
                    "descriptionHtml": "<p>Stored description</p>",
                    "datePosted": "2026-01-30",
                    "validThrough": None,
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
