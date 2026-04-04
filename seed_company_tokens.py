"""Seed board tokens into pipeline_companies without network validation."""

from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from db import get_connection, init_schema, upsert_company

load_dotenv()


def load_tokens(path: str | Path) -> list[str]:
    seen: set[str] = set()
    tokens: list[str] = []
    for raw_line in Path(path).read_text().splitlines():
        token = raw_line.strip()
        if not token or token.startswith("#"):
            continue
        if token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def main():
    parser = argparse.ArgumentParser(description="Seed board tokens into pipeline_companies")
    parser.add_argument("--ats", required=True)
    parser.add_argument("--tokens-file", required=True)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    tokens = load_tokens(args.tokens_file)
    if args.limit is not None:
        tokens = tokens[: args.limit]

    conn = get_connection()
    init_schema(conn)
    try:
        for idx, token in enumerate(tokens, start=1):
            upsert_company(
                conn,
                args.ats,
                token,
                job_count=0,
                job_count_exact=False,
                scrape_status="pending",
            )
            if idx % 100 == 0 or idx == len(tokens):
                print(f"{idx}/{len(tokens)} seeded", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
