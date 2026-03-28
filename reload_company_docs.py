"""Reload parsed/removed MeiliSearch docs for one company or a small company slice."""

from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv
import meilisearch

from db import get_connection, init_schema
from pipeline import parse_companies_file, step_load


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reload MeiliSearch docs for selected companies")
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--companies", help="Companies file (ats:token per line)")
    source_group.add_argument("--company", help="Single company in ats:board_token format")
    parser.add_argument(
        "--all-company-docs",
        action="store_true",
        help="Reload all parsed/removed docs for the selected companies, not just docs already present in MeiliSearch",
    )
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


def get_company_slug_rows(conn, companies: list[tuple[str, str]]) -> list[dict]:
    if not companies:
        return []

    clauses = []
    params: list[str] = []
    for ats, board_token in companies:
        clauses.append("(ats = %s AND board_token = %s)")
        params.extend([ats, board_token])

    query = f"""
        SELECT ats, board_token, company_slug
        FROM pipeline_companies
        WHERE {" OR ".join(clauses)}
    """
    with conn.cursor() as cur:
        cur.execute(query, params)
        return [
            {"ats": row[0], "board_token": row[1], "company_slug": row[2]}
            for row in cur.fetchall()
            if row[2]
        ]


def meili_filter_for_company(company_row: dict) -> str:
    slug = str(company_row["company_slug"]).replace("\\", "\\\\").replace('"', '\\"')
    ats = str(company_row["ats"]).replace("\\", "\\\\").replace('"', '\\"')
    return f'company_slug = "{slug}" AND ats_type = "{ats}"'


def get_indexed_job_ids_for_companies(index, company_rows: list[dict]) -> list[str]:
    seen: set[str] = set()
    ordered_ids: list[str] = []
    for company_row in company_rows:
        offset = 0
        while True:
            result = index.search(
                "",
                {
                    "filter": meili_filter_for_company(company_row),
                    "attributesToRetrieve": ["id"],
                    "limit": 1000,
                    "offset": offset,
                },
            )
            hits = result.get("hits", [])
            if not hits:
                break
            for hit in hits:
                job_id = hit.get("id")
                if job_id and job_id not in seen:
                    seen.add(job_id)
                    ordered_ids.append(job_id)
            offset += len(hits)
    return ordered_ids


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
        meili_host = args.meili_host or os.environ.get("MEILISEARCH_HOST", "http://localhost:7700")
        if args.all_company_docs:
            job_ids = get_company_job_ids(conn, companies)
        else:
            company_rows = get_company_slug_rows(conn, companies)
            client = meilisearch.Client(meili_host, args.meili_key or os.environ.get("MEILISEARCH_MASTER_KEY", ""))
            index = client.index("jobs")
            job_ids = get_indexed_job_ids_for_companies(index, company_rows)
        if not job_ids:
            if args.all_company_docs:
                print("No jobs found for selection")
            else:
                print("No currently indexed docs found for selection")
            return 0

        print(f"Reloading {len(job_ids)} jobs for {len(companies)} companies")
        step_load(
            conn,
            meili_host=meili_host,
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
