"""Regression checks for avoiding full raw-job downloads on each scrape."""
import pytest
from db import get_existing_jobs_for_board


@pytest.mark.parametrize("ats", ["lever", "ashby", "workable"])
def test_scrapers_without_reused_detail_do_not_query_database(ats):
    class NoDatabase:
        def cursor(self):
            raise AssertionError("This scraper has no stored detail to reuse")
    assert get_existing_jobs_for_board(NoDatabase(), ats, "board") == {}


@pytest.mark.parametrize("ats,fields", [
    ("greenhouse", ("pay_input_ranges",)),
    ("jobvite", ("description", "descriptionHtml", "datePosted", "validThrough", "inactive")),
])
def test_existing_detail_query_projects_only_reused_fields(ats, fields):
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *exc): pass
        def execute(self, sql, params):
            self.sql, self.params = sql, params
        def fetchall(self): return [("historical-job", {fields[0]: "saved"})]
    cursor = Cursor()
    class Connection:
        def cursor(self): return cursor
    assert get_existing_jobs_for_board(Connection(), ats, "board") == {
        "historical-job": {fields[0]: "saved"},
    }
    assert "SELECT id, raw_json" not in cursor.sql
    assert "jsonb_build_object" in cursor.sql
    assert cursor.params == tuple(part for field in fields for part in (field, field)) + (ats, "board")
    # Returning jobs still get their stored detail; no removed-at filter.
    assert "removed_at" not in cursor.sql
