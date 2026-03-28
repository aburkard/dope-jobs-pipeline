"""Backfill public-facing company slugs and job IDs."""

from __future__ import annotations

import argparse

from dotenv import load_dotenv

from db import (
    backfill_company_slugs,
    backfill_public_job_ids,
    get_connection,
    init_schema,
)


def main():
    parser = argparse.ArgumentParser(description="Backfill public company/job identifiers")
    parser.add_argument("--all", action="store_true", help="Rewrite all rows, not just missing ones")
    parser.add_argument("--company-chunk-size", type=int, default=1000)
    parser.add_argument("--job-chunk-size", type=int, default=5000)
    args = parser.parse_args()

    load_dotenv()
    conn = get_connection()
    init_schema(conn)

    only_missing = not args.all
    company_updates = backfill_company_slugs(conn, only_missing=only_missing, chunk_size=args.company_chunk_size)
    job_updates = backfill_public_job_ids(conn, only_missing=only_missing, chunk_size=args.job_chunk_size)

    print(f"company_slugs_updated={company_updates}")
    print(f"public_job_ids_updated={job_updates}")

    conn.close()


if __name__ == "__main__":
    main()
