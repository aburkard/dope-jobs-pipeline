import argparse

import pytest

from reload_company_docs import resolve_companies


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
