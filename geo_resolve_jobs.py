from __future__ import annotations

"""Backfill canonical geo resolution into existing parsed_json job rows."""

import argparse

from dotenv import load_dotenv

from db import get_connection, init_schema, update_parsed_json_bulk
from geo_resolver import GeoResolver

load_dotenv()


def _needs_geo_resolution(parsed_json: dict) -> bool:
    for location in parsed_json.get("locations", []) or []:
        if not isinstance(location, dict):
            continue
        if location.get("geoname_id") is None:
            return True
    for requirement in parsed_json.get("applicant_location_requirements", []) or []:
        if not isinstance(requirement, dict):
            continue
        if requirement.get("scope") != "region_group" and requirement.get("geoname_id") is None:
            return True
    return False


def backfill_geo(conn, limit: int | None = None, job_ids: list[str] | None = None,
                 missing_only: bool = True, chunk_size: int = 200) -> dict[str, int]:
    resolver = GeoResolver(conn)
    query = """
        SELECT id, parsed_json
        FROM pipeline_jobs
        WHERE parsed_json IS NOT NULL
          AND removed_at IS NULL
    """
    params: list[object] = []
    if job_ids:
        query += " AND id = ANY(%s)"
        params.append(job_ids)
    query += " ORDER BY last_parsed_at DESC NULLS LAST, id"
    if limit:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    scanned = 0
    updated = 0
    skipped = 0
    pending_updates: list[tuple[str, dict]] = []

    def flush():
        nonlocal updated, pending_updates
        if not pending_updates:
            return
        update_parsed_json_bulk(conn, pending_updates)
        updated += len(pending_updates)
        pending_updates = []

    for job_id, parsed_json in rows:
        scanned += 1
        if not isinstance(parsed_json, dict):
            skipped += 1
            continue
        if missing_only and not _needs_geo_resolution(parsed_json):
            skipped += 1
            continue

        resolved = resolver.resolve_parsed_geo(parsed_json)
        if resolved != parsed_json:
            pending_updates.append((job_id, resolved))
            if len(pending_updates) >= chunk_size:
                flush()
        else:
            skipped += 1

    flush()
    return {"scanned": scanned, "updated": updated, "skipped": skipped}


def main():
    parser = argparse.ArgumentParser(description="Backfill parsed job geography from geo_places")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--job-id", dest="job_ids", action="append", help="Specific job ID(s) to backfill")
    parser.add_argument("--include-already-resolved", action="store_true")
    parser.add_argument("--chunk-size", type=int, default=200)
    args = parser.parse_args()

    conn = get_connection()
    try:
        init_schema(conn)
        result = backfill_geo(
            conn,
            limit=args.limit,
            job_ids=args.job_ids,
            missing_only=not args.include_already_resolved,
            chunk_size=args.chunk_size,
        )
        print(result)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
