"""Reload parsed/removed MeiliSearch docs for one company or a small company slice."""

from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv

from db import get_connection, init_schema
from pipeline import parse_companies_file, step_load


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reload MeiliSearch docs for selected companies")
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--companies", help="Companies file (ats:token per line)")
    source_group.add_argument("--company", help="Single company in ats:board_token format")
    parser.add_argument("--meili-host", default=None, help="MeiliSearch host")
    parser.add_argument("--meili-key", default=None, help="MeiliSearch master key")
    return parser


def resolve_companies(args: argparse.Namespace) -> list[tuple[str, str]]:
    if args.company:
        if ":" not in args.company:
            raise ValueError("--company must be in ats:board_token format")
        ats, board_token = args.company.split(":", 1)
        return [(ats.strip(), board_token.strip())]
    return parse_companies_file(args.companies)


def get_company_job_ids(conn, companies: list[tuple[str, str]]) -> list[str]:
    if not companies:
        return []

    clauses = []
    params: list[str] = []
    for ats, board_token in companies:
        clauses.append("(ats = %s AND board_token = %s)")
        params.extend([ats, board_token])

    query = f"""
        SELECT id
        FROM pipeline_jobs
        WHERE {" OR ".join(clauses)}
    """
    with conn.cursor() as cur:
        cur.execute(query, params)
        return [row[0] for row in cur.fetchall()]


def main() -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args()

    try:
        companies = resolve_companies(args)
    except ValueError as exc:
        parser.error(str(exc))

    conn = get_connection()
    try:
        init_schema(conn)
        job_ids = get_company_job_ids(conn, companies)
        if not job_ids:
            print("No jobs found for selection")
            return 0

        print(f"Reloading {len(job_ids)} jobs for {len(companies)} companies")
        step_load(
            conn,
            meili_host=args.meili_host or os.environ.get("MEILISEARCH_HOST", "http://localhost:7700"),
            meili_key=args.meili_key,
            parsed_job_ids=job_ids,
            removed_job_ids=job_ids,
            full_reload=False,
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
