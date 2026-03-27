"""
Pipeline state database (Neon Postgres).

Tracks companies, jobs, content hashes, and parse state.
Used for incremental pipeline runs — only re-parse jobs that actually changed.
"""

import hashlib
import json
import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values, Json

from utils.html_utils import remove_html_markup


def get_connection():
    """Get a Postgres connection using DATABASE_URL from environment."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set. Add it to .env")
    return psycopg2.connect(url)


def init_schema(conn):
    """Create tables if they don't exist."""
    create_statements = [
        """CREATE TABLE IF NOT EXISTS pipeline_companies (
            ats TEXT NOT NULL,
            board_token TEXT NOT NULL,
            company_name TEXT,
            domain TEXT,
            logo_url TEXT,
            boilerplate_hashes JSONB,
            last_scraped_at TIMESTAMPTZ,
            job_count INTEGER DEFAULT 0,
            active BOOLEAN DEFAULT TRUE,
            PRIMARY KEY (ats, board_token)
        )""",
        """CREATE TABLE IF NOT EXISTS pipeline_jobs (
            id TEXT PRIMARY KEY,
            ats TEXT NOT NULL,
            board_token TEXT NOT NULL,
            title TEXT,
            content_hash TEXT,
            first_seen_at TIMESTAMPTZ DEFAULT NOW(),
            last_seen_at TIMESTAMPTZ DEFAULT NOW(),
            last_parsed_at TIMESTAMPTZ,
            removed_at TIMESTAMPTZ,
            needs_parse BOOLEAN DEFAULT TRUE,
            parse_error_count INTEGER DEFAULT 0,
            last_parse_error TEXT,
            job_group TEXT,
            raw_json JSONB,
            parsed_json JSONB
        )""",
    ]
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_jobs_needs_parse ON pipeline_jobs (needs_parse) WHERE needs_parse = TRUE",
        "CREATE INDEX IF NOT EXISTS idx_jobs_board ON pipeline_jobs (ats, board_token)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_removed ON pipeline_jobs (removed_at) WHERE removed_at IS NOT NULL",
    ]
    with conn.cursor() as cur:
        for stmt in create_statements:
            cur.execute(stmt)
        cur.execute("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name IN ('pipeline_companies', 'pipeline_jobs')
        """)
        existing_columns = {(row[0], row[1]) for row in cur.fetchall()}

        alter_statements = []
        if ("pipeline_companies", "logo_url") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_companies ADD COLUMN logo_url TEXT")
        if ("pipeline_companies", "boilerplate_hashes") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_companies ADD COLUMN boilerplate_hashes JSONB")
        if ("pipeline_jobs", "job_group") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_jobs ADD COLUMN job_group TEXT")

        for stmt in alter_statements:
            cur.execute(stmt)
        for stmt in index_statements:
            cur.execute(stmt)
    conn.commit()


def content_hash(raw_job: dict) -> str:
    """Compute SHA256 of cleaned job text (title + description)."""
    title = raw_job.get("title", "") or ""
    content = (
        raw_job.get("content", "")
        or raw_job.get("description", "")
        or raw_job.get("descriptionHtml", "")
        or ""
    )
    if content:
        content = remove_html_markup(content, double_unescape=True)
    text = f"{title}\n{content}".strip()
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def job_id(raw_job: dict) -> str:
    """Get the compound job ID. The scraper's normalize_job already builds
    the format ats__board_token__job_id in the 'id' field."""
    jid = raw_job.get("id", "")
    if jid and "__" in jid:
        return jid
    # Fallback: build it ourselves
    ats = raw_job.get("ats_name", "")
    board = raw_job.get("board_token", "")
    return f"{ats}__{board}__{jid}"


def upsert_scraped_jobs(conn, scraped_jobs: list[dict]) -> dict:
    """
    Compare scraped jobs against DB state. Returns dict with:
      - new: list of raw jobs that are new
      - changed: list of raw jobs whose content changed
      - unchanged: count of jobs that didn't change
      - needs_detail_fetch: list of raw jobs that need per-job API call
        (new jobs, or jobs whose source updated_at > our last_seen_at)
    """
    if not scraped_jobs:
        return {"new": [], "changed": [], "unchanged": 0, "needs_detail_fetch": []}

    now = datetime.now(timezone.utc)

    # Get all job IDs and hashes for this batch
    job_data = []
    for raw in scraped_jobs:
        jid = job_id(raw)
        h = content_hash(raw)
        ats = raw.get("ats_name", "")
        board = raw.get("board_token", "")
        title = raw.get("title", "")
        job_data.append((jid, ats, board, title, h, raw))

    # Fetch existing hashes + last_seen_at from DB
    ids = [jd[0] for jd in job_data]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, content_hash, last_seen_at FROM pipeline_jobs WHERE id = ANY(%s)",
            (ids,)
        )
        existing = {row[0]: {"hash": row[1], "last_seen_at": row[2]} for row in cur.fetchall()}

    new_jobs = []
    changed_jobs = []
    needs_detail = []
    unchanged = 0

    for jid, ats, board, title, h, raw in job_data:
        if jid not in existing:
            # New job — always needs detail fetch
            new_jobs.append(raw)
            needs_detail.append(raw)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipeline_jobs (id, ats, board_token, title, content_hash, first_seen_at, last_seen_at, needs_parse, raw_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        last_seen_at = EXCLUDED.last_seen_at,
                        content_hash = EXCLUDED.content_hash,
                        needs_parse = TRUE,
                        removed_at = NULL,
                        raw_json = EXCLUDED.raw_json
                """, (jid, ats, board, title, h, now, now, Json(raw)))
        elif existing[jid]["hash"] != h:
            # Content changed — needs detail fetch
            changed_jobs.append(raw)
            needs_detail.append(raw)
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pipeline_jobs SET
                        content_hash = %s,
                        title = %s,
                        last_seen_at = %s,
                        needs_parse = TRUE,
                        removed_at = NULL,
                        raw_json = %s
                    WHERE id = %s
                """, (h, title, now, Json(raw), jid))
        else:
            # Content unchanged — still update raw_json (metadata may have changed)
            # and check if source updated since our last scrape
            unchanged += 1
            source_updated = raw.get("updated_at")
            last_seen = existing[jid]["last_seen_at"]
            if source_updated and last_seen:
                from dateutil.parser import parse as parse_date
                try:
                    source_dt = parse_date(source_updated)
                    if source_dt.tzinfo is None:
                        source_dt = source_dt.replace(tzinfo=timezone.utc)
                    if source_dt > last_seen:
                        needs_detail.append(raw)
                except (ValueError, TypeError):
                    pass

            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE pipeline_jobs SET last_seen_at = %s, removed_at = NULL, raw_json = %s WHERE id = %s",
                    (now, Json(raw), jid)
                )

    conn.commit()
    return {"new": new_jobs, "changed": changed_jobs, "unchanged": unchanged, "needs_detail_fetch": needs_detail}


def mark_removed(conn, ats: str, board_token: str, seen_ids: set[str]) -> list[str]:
    """Mark jobs as removed if they weren't seen in the latest scrape."""
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_jobs SET removed_at = %s
            WHERE ats = %s AND board_token = %s
                AND removed_at IS NULL
                AND id != ALL(%s)
            RETURNING id
        """, (now, ats, board_token, list(seen_ids)))
        removed_ids = [row[0] for row in cur.fetchall()]
    conn.commit()
    return removed_ids


def get_jobs_needing_parse(conn, limit: int | None = None,
                           companies: list[tuple[str, str]] | None = None) -> list[dict]:
    """Get jobs that need LLM parsing, including raw_json for text preparation."""
    query = "SELECT id, ats, board_token, title, raw_json FROM pipeline_jobs WHERE needs_parse = TRUE AND removed_at IS NULL"
    params = []
    if companies is not None:
        if not companies:
            return []
        clauses = []
        for ats, board_token in companies:
            clauses.append("(ats = %s AND board_token = %s)")
            params.extend([ats, board_token])
        query += " AND (" + " OR ".join(clauses) + ")"
    if limit:
        query += f" LIMIT {limit}"
    with conn.cursor() as cur:
        cur.execute(query, params)
        return [{"id": r[0], "ats": r[1], "board_token": r[2], "title": r[3], "raw_json": r[4]} for r in cur.fetchall()]


def save_parsed_result(conn, job_id: str, parsed_json: dict):
    """Save LLM extraction result and clear needs_parse flag."""
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_jobs SET
                parsed_json = %s,
                last_parsed_at = %s,
                needs_parse = FALSE
            WHERE id = %s
        """, (Json(parsed_json), now, job_id))
    conn.commit()


def record_parse_error(conn, job_id: str, error: str):
    """Record a parse failure. After 3 failures, stop retrying."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_jobs SET
                parse_error_count = COALESCE(parse_error_count, 0) + 1,
                last_parse_error = %s,
                needs_parse = (COALESCE(parse_error_count, 0) + 1) < 3
            WHERE id = %s
        """, (error[:500], job_id))
    conn.commit()


def get_parsed_jobs(conn, include_removed: bool = False, job_ids: list[str] | None = None) -> list[dict]:
    """Get all parsed jobs for loading into MeiliSearch."""
    query = "SELECT id, ats, board_token, title, parsed_json, job_group, raw_json FROM pipeline_jobs WHERE parsed_json IS NOT NULL"
    params = []
    if not include_removed:
        query += " AND removed_at IS NULL"
    if job_ids is not None:
        if not job_ids:
            return []
        query += " AND id = ANY(%s)"
        params.append(job_ids)
    with conn.cursor() as cur:
        cur.execute(query, params)
        return [
            {"id": r[0], "ats": r[1], "board_token": r[2], "title": r[3], "parsed_json": r[4], "job_group": r[5], "raw_json": r[6]}
            for r in cur.fetchall()
        ]


def get_removed_job_ids(conn, job_ids: list[str] | None = None) -> list[str]:
    """Get IDs of jobs that have been removed (for MeiliSearch deletion)."""
    query = "SELECT id FROM pipeline_jobs WHERE removed_at IS NOT NULL"
    params = []
    if job_ids is not None:
        if not job_ids:
            return []
        query += " AND id = ANY(%s)"
        params.append(job_ids)
    with conn.cursor() as cur:
        cur.execute(query, params)
        return [r[0] for r in cur.fetchall()]


def get_companies_to_scrape(conn, limit: int) -> list[tuple[str, str]]:
    """Select a bounded set of active companies for scraping.

    Prioritize companies that have never been scraped, then oldest scrape times.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ats, board_token
            FROM pipeline_companies
            WHERE active = TRUE
            ORDER BY last_scraped_at NULLS FIRST, ats, board_token
            LIMIT %s
        """, (limit,))
        return [(r[0], r[1]) for r in cur.fetchall()]


def upsert_company(conn, ats: str, board_token: str, company_name: str | None = None,
                    domain: str | None = None, logo_url: str | None = None,
                    job_count: int = 0, job_count_exact: bool = True):
    """Upsert a company record."""
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_companies (ats, board_token, company_name, domain, logo_url, last_scraped_at, job_count, active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s > 0)
            ON CONFLICT (ats, board_token) DO UPDATE SET
                company_name = COALESCE(EXCLUDED.company_name, pipeline_companies.company_name),
                domain = COALESCE(EXCLUDED.domain, pipeline_companies.domain),
                logo_url = COALESCE(EXCLUDED.logo_url, pipeline_companies.logo_url),
                last_scraped_at = EXCLUDED.last_scraped_at,
                job_count = CASE
                    WHEN %s THEN EXCLUDED.job_count
                    ELSE GREATEST(pipeline_companies.job_count, EXCLUDED.job_count)
                END,
                active = CASE
                    WHEN %s THEN EXCLUDED.job_count > 0
                    ELSE pipeline_companies.active OR EXCLUDED.job_count > 0
                END
        """, (
            ats, board_token, company_name, domain, logo_url, now, job_count, job_count,
            job_count_exact, job_count_exact,
        ))
    conn.commit()


if __name__ == "__main__":
    """Initialize the schema."""
    from dotenv import load_dotenv
    load_dotenv()

    conn = get_connection()
    init_schema(conn)
    print("Schema initialized.")

    # Show counts
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pipeline_companies")
        print(f"Companies: {cur.fetchone()[0]}")
        cur.execute("SELECT COUNT(*) FROM pipeline_jobs")
        print(f"Jobs: {cur.fetchone()[0]}")
        cur.execute("SELECT COUNT(*) FROM pipeline_jobs WHERE needs_parse = TRUE")
        print(f"Needs parse: {cur.fetchone()[0]}")

    conn.close()
