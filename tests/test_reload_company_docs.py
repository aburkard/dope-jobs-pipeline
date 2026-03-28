import argparse

import pytest

from reload_company_docs import (
    get_indexed_job_ids_for_companies,
    meili_filter_for_company,
    resolve_companies,
)


def test_resolve_companies_accepts_single_company_arg():
    args = argparse.Namespace(company="greenhouse:reddit", companies=None)
    assert resolve_companies(args) == [("greenhouse", "reddit")]


def test_resolve_companies_rejects_invalid_single_company_arg():
    args = argparse.Namespace(company="reddit", companies=None)
    with pytest.raises(ValueError, match="ats:board_token"):
        resolve_companies(args)


def test_resolve_companies_reads_companies_file(tmp_path):
    companies_file = tmp_path / "companies.txt"
    companies_file.write_text("greenhouse:reddit\nlever:spotify\n")
    args = argparse.Namespace(company=None, companies=str(companies_file))
    assert resolve_companies(args) == [("greenhouse", "reddit"), ("lever", "spotify")]


def test_meili_filter_for_company_uses_slug_and_ats():
    assert meili_filter_for_company({"company_slug": "reddit", "ats": "greenhouse"}) == (
        'company_slug = "reddit" AND ats_type = "greenhouse"'
    )


def test_get_indexed_job_ids_for_companies_paginates_and_dedupes():
    class FakeIndex:
        def __init__(self):
            self.calls = []

        def search(self, query, params):
            self.calls.append((query, params))
            if params["filter"] == 'company_slug = "reddit" AND ats_type = "greenhouse"':
                if params["offset"] == 0:
                    return {"hits": [{"id": "job-1"}, {"id": "job-2"}]}
                return {"hits": []}
            if params["filter"] == 'company_slug = "spotify" AND ats_type = "lever"':
                if params["offset"] == 0:
                    return {"hits": [{"id": "job-2"}, {"id": "job-3"}]}
                return {"hits": []}
            return {"hits": []}

    index = FakeIndex()
    job_ids = get_indexed_job_ids_for_companies(
        index,
        [
            {"company_slug": "reddit", "ats": "greenhouse"},
            {"company_slug": "spotify", "ats": "lever"},
        ],
    )

    assert job_ids == ["job-1", "job-2", "job-3"]
