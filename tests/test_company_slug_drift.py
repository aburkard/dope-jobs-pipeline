import db
from company_slug_drift import plan_drift


class Cursor:
    def __init__(self, existing):
        self.existing = existing
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.existing


class Conn:
    def __init__(self, existing):
        self.cur = Cursor(existing)

    def cursor(self):
        return self.cur

    def commit(self):
        pass


def _upsert(monkeypatch, existing, **kwargs):
    marked = []
    monkeypatch.setattr(db, "mark_company_jobs_for_meili_reload", lambda cur, companies: marked.extend(companies))
    conn = Conn(existing)
    db.upsert_company(conn, kwargs.pop("ats"), kwargs.pop("board_token"), **kwargs)
    insert_params = next(params for sql, params in conn.cur.executed if "INSERT INTO pipeline_companies" in sql)
    return insert_params[3], marked  # company_slug


def test_failed_scrape_keeps_slug_and_reloads_nothing(monkeypatch):
    # The scrape-error path passes no name or domain; that used to reset the slug to the board token.
    slug, marked = _upsert(monkeypatch, ("komoju", "KOMOJU", None), ats="workable", board_token="degica-hiring",
                           job_count=0, job_count_exact=False, scrape_status="error")
    assert slug == "komoju"
    assert marked == []


def test_slug_change_marks_company_jobs_for_reload(monkeypatch):
    slug, marked = _upsert(monkeypatch, ("nuvance-health", "Nuvance Health", None), ats="jobvite",
                           board_token="nuvance", company_name="Northwell Health", job_count=5)
    assert slug == "northwell-health"
    assert marked == [("jobvite", "nuvance")]


def test_unchanged_slug_and_new_company_mark_nothing(monkeypatch):
    assert _upsert(monkeypatch, ("acme", "Acme", None), ats="greenhouse", board_token="acme",
                   company_name="Acme", job_count=3) == ("acme", [])
    assert _upsert(monkeypatch, None, ats="greenhouse", board_token="newco",
                   company_name="New Co", job_count=3) == ("new-co", [])


def test_null_slug_counts_as_board_token_for_indexed_docs(monkeypatch):
    # Docs loaded while the slug was NULL carry the raw board token.
    assert _upsert(monkeypatch, (None, None, None), ats="lever", board_token="Acme_Inc",
                   company_name="Acme", job_count=1) == ("acme", [("lever", "Acme_Inc")])


def test_plan_drift_restores_reset_slugs_and_reloads_moved_companies():
    companies = {
        ("workable", "degica-hiring"): {"slug": "degica-hiring", "name": "KOMOJU", "domain": None},  # reset
        ("jobvite", "nuvance"): {"slug": "northwell-health", "name": "Northwell Health", "domain": None},  # renamed
        ("greenhouse", "acme"): {"slug": "acme", "name": "Acme", "domain": None},  # fine
    }
    job_companies = {
        "w1": ("workable", "degica-hiring"), "w2": ("workable", "degica-hiring"),
        "n1": ("jobvite", "nuvance"), "a1": ("greenhouse", "acme"),
    }
    indexed = {
        "w1": "komoju",          # matches the name: restore Postgres, no reload
        "w2": "degica-hiring",   # loaded while reset: reload after the restore
        "n1": "nuvance-health",  # renamed: reload
        "a1": "acme",
        "gone": "x",             # unknown to Postgres: left to reconcile-meili
    }
    plan = plan_drift(indexed, job_companies, companies)
    assert plan["restore"] == {("workable", "degica-hiring"): "komoju"}
    assert plan["reload_ids"] == ["n1", "w2"]
    assert set(plan["drifted"]) == {("workable", "degica-hiring", "komoju"), ("jobvite", "nuvance", "nuvance-health")}
