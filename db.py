"""
Pipeline state database (Neon Postgres).

Tracks companies, jobs, content hashes, and parse state.
Used for incremental pipeline runs — only re-parse jobs that actually changed.
"""

import hashlib
import json
import os
from datetime import date, datetime, timezone

import psycopg2
from psycopg2.extras import execute_values, Json

from public_ids import derive_company_slug_map, short_public_job_id
from utils.html_utils import remove_html_markup

MEILI_DOC_SCHEMA_VERSION = "2026-04-05-ats-geo-v2"


def get_connection():
    """Get a Postgres connection using DATABASE_URL from environment."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set. Add it to .env")
    return psycopg2.connect(url)


def current_meili_doc_version(
    content_hash: str | None,
    last_parsed_at: datetime | None,
    job_group: str | None,
    job_id_value: str,
) -> str:
    """Compute a version hash for fields that materially shape the Meili doc."""
    effective_group = job_group or job_id_value
    parsed_at = last_parsed_at.isoformat() if last_parsed_at is not None else ""
    payload = f"{MEILI_DOC_SCHEMA_VERSION}|{content_hash or ''}|{parsed_at}|{effective_group}"
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def init_schema(conn):
    """Create tables if they don't exist."""
    create_statements = [
        """CREATE TABLE IF NOT EXISTS pipeline_companies (
            ats TEXT NOT NULL,
            board_token TEXT NOT NULL,
            company_name TEXT,
            company_slug TEXT,
            domain TEXT,
            description TEXT,
            logo_url TEXT,
            scraped_logo_url TEXT,
            scrape_status TEXT,
            last_scrape_error TEXT,
            last_http_status INTEGER,
            boilerplate_hashes JSONB,
            last_scraped_at TIMESTAMPTZ,
            job_count INTEGER DEFAULT 0,
            active BOOLEAN DEFAULT TRUE,
            PRIMARY KEY (ats, board_token)
        )""",
        """CREATE TABLE IF NOT EXISTS pipeline_jobs (
            id TEXT PRIMARY KEY,
            public_job_id TEXT,
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
            parse_provider TEXT,
            parse_model TEXT,
            parse_params JSONB,
            meili_loaded_at TIMESTAMPTZ,
            meili_loaded_content_hash TEXT,
            meili_loaded_last_parsed_at TIMESTAMPTZ,
            meili_loaded_doc_version TEXT,
            raw_json JSONB,
            parsed_json JSONB
        )""",
        """CREATE TABLE IF NOT EXISTS pipeline_parse_batches (
            batch_id TEXT PRIMARY KEY,
            model TEXT NOT NULL,
            params JSONB,
            display_name TEXT,
            state TEXT NOT NULL,
            input_file_name TEXT,
            output_file_name TEXT,
            requested_count INTEGER DEFAULT 0,
            succeeded_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            stale_count INTEGER DEFAULT 0,
            submitted_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            completed_at TIMESTAMPTZ,
            last_error TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS pipeline_parse_batch_jobs (
            batch_id TEXT NOT NULL,
            request_index INTEGER NOT NULL,
            job_id TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            PRIMARY KEY (batch_id, request_index),
            UNIQUE (batch_id, job_id)
        )""",
        """CREATE TABLE IF NOT EXISTS pipeline_job_recommendations (
            source_job_id TEXT NOT NULL REFERENCES pipeline_jobs(id) ON DELETE CASCADE,
            recommended_job_id TEXT NOT NULL REFERENCES pipeline_jobs(id) ON DELETE CASCADE,
            rank INTEGER NOT NULL,
            score DOUBLE PRECISION,
            algorithm_version TEXT NOT NULL,
            generated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (source_job_id, recommended_job_id)
        )""",
        """CREATE TABLE IF NOT EXISTS geo_places (
            geoname_id BIGINT PRIMARY KEY,
            kind TEXT NOT NULL,
            canonical_name TEXT NOT NULL,
            ascii_name TEXT,
            display_name TEXT NOT NULL,
            country_code TEXT,
            country_name TEXT,
            admin1_code TEXT,
            admin1_name TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            population BIGINT,
            timezone TEXT,
            feature_class TEXT,
            feature_code TEXT,
            search_names TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS fx_rates (
            currency_code TEXT NOT NULL,
            usd_per_unit DOUBLE PRECISION NOT NULL,
            as_of_date DATE NOT NULL,
            source TEXT NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (currency_code, as_of_date)
        )""",
    ]
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_jobs_needs_parse ON pipeline_jobs (needs_parse) WHERE needs_parse = TRUE",
        "CREATE INDEX IF NOT EXISTS idx_jobs_board ON pipeline_jobs (ats, board_token)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_removed ON pipeline_jobs (removed_at) WHERE removed_at IS NOT NULL",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_public_job_id ON pipeline_jobs (public_job_id) WHERE public_job_id IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_companies_company_slug ON pipeline_companies (company_slug)",
        "CREATE INDEX IF NOT EXISTS idx_parse_batch_jobs_job_id ON pipeline_parse_batch_jobs (job_id)",
        "CREATE INDEX IF NOT EXISTS idx_parse_batches_state ON pipeline_parse_batches (state)",
        "CREATE INDEX IF NOT EXISTS idx_job_recommendations_source_rank ON pipeline_job_recommendations (source_job_id, rank)",
        "CREATE INDEX IF NOT EXISTS idx_job_recommendations_recommended ON pipeline_job_recommendations (recommended_job_id)",
        "CREATE INDEX IF NOT EXISTS idx_geo_places_kind ON geo_places (kind)",
        "CREATE INDEX IF NOT EXISTS idx_geo_places_country_admin1 ON geo_places (country_code, admin1_code)",
        "CREATE INDEX IF NOT EXISTS idx_geo_places_population ON geo_places (population DESC)",
        "CREATE INDEX IF NOT EXISTS idx_geo_places_search_names ON geo_places USING GIN (search_names)",
        "CREATE INDEX IF NOT EXISTS idx_fx_rates_as_of_date ON fx_rates (as_of_date DESC)",
    ]
    with conn.cursor() as cur:
        for stmt in create_statements:
            cur.execute(stmt)
        cur.execute("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name IN ('pipeline_companies', 'pipeline_jobs', 'pipeline_parse_batches')
        """)
        existing_columns = {(row[0], row[1]) for row in cur.fetchall()}

        alter_statements = []
        if ("pipeline_companies", "logo_url") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_companies ADD COLUMN logo_url TEXT")
        added_scraped_logo_url = False
        if ("pipeline_companies", "scraped_logo_url") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_companies ADD COLUMN scraped_logo_url TEXT")
            added_scraped_logo_url = True
        if ("pipeline_companies", "company_slug") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_companies ADD COLUMN company_slug TEXT")
        if ("pipeline_companies", "description") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_companies ADD COLUMN description TEXT")
        if ("pipeline_companies", "scrape_status") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_companies ADD COLUMN scrape_status TEXT")
        if ("pipeline_companies", "last_scrape_error") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_companies ADD COLUMN last_scrape_error TEXT")
        if ("pipeline_companies", "last_http_status") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_companies ADD COLUMN last_http_status INTEGER")
        if ("pipeline_companies", "boilerplate_hashes") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_companies ADD COLUMN boilerplate_hashes JSONB")
        if ("pipeline_jobs", "job_group") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_jobs ADD COLUMN job_group TEXT")
        if ("pipeline_jobs", "public_job_id") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_jobs ADD COLUMN public_job_id TEXT")
        if ("pipeline_jobs", "parse_provider") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_jobs ADD COLUMN parse_provider TEXT")
        if ("pipeline_jobs", "parse_model") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_jobs ADD COLUMN parse_model TEXT")
        if ("pipeline_jobs", "parse_params") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_jobs ADD COLUMN parse_params JSONB")
        if ("pipeline_jobs", "meili_loaded_at") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_jobs ADD COLUMN meili_loaded_at TIMESTAMPTZ")
        if ("pipeline_jobs", "meili_loaded_content_hash") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_jobs ADD COLUMN meili_loaded_content_hash TEXT")
        if ("pipeline_jobs", "meili_loaded_last_parsed_at") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_jobs ADD COLUMN meili_loaded_last_parsed_at TIMESTAMPTZ")
        if ("pipeline_jobs", "meili_loaded_doc_version") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_jobs ADD COLUMN meili_loaded_doc_version TEXT")
        if ("pipeline_jobs", "recommendations_generated_at") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_jobs ADD COLUMN recommendations_generated_at TIMESTAMPTZ")
        if ("pipeline_jobs", "recommendations_algorithm_version") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_jobs ADD COLUMN recommendations_algorithm_version TEXT")
        if ("pipeline_jobs", "recommendations_source_last_parsed_at") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_jobs ADD COLUMN recommendations_source_last_parsed_at TIMESTAMPTZ")
        if ("pipeline_parse_batches", "params") not in existing_columns:
            alter_statements.append("ALTER TABLE pipeline_parse_batches ADD COLUMN params JSONB")

        for stmt in alter_statements:
            cur.execute(stmt)
        if added_scraped_logo_url:
            # Existing logo_url values were historically scraped values.
            # Move them to scraped_logo_url so logo_url can act as a manual override.
            cur.execute("""
                UPDATE pipeline_companies
                SET scraped_logo_url = logo_url,
                    logo_url = NULL
                WHERE scraped_logo_url IS NULL
                  AND logo_url IS NOT NULL
            """)
        for stmt in index_statements:
            cur.execute(stmt)
    conn.commit()


def upsert_fx_rates(conn, rows: list[tuple[str, float, date, str]]) -> None:
    if not rows:
        return

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO fx_rates (currency_code, usd_per_unit, as_of_date, source)
            VALUES %s
            ON CONFLICT (currency_code, as_of_date) DO UPDATE SET
                usd_per_unit = EXCLUDED.usd_per_unit,
                source = EXCLUDED.source,
                updated_at = NOW()
            """,
            rows,
        )
    conn.commit()


def get_latest_fx_rates(conn) -> tuple[dict[str, float], date | None]:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(as_of_date) FROM fx_rates")
        row = cur.fetchone()
        as_of_date = row[0] if row else None
        if as_of_date is None:
            return {}, None

        cur.execute(
            """
            SELECT currency_code, usd_per_unit
            FROM fx_rates
            WHERE as_of_date = %s
            """,
            (as_of_date,),
        )
        rates = {currency_code: float(usd_per_unit) for currency_code, usd_per_unit in cur.fetchall()}
        return rates, as_of_date


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
        public_id = short_public_job_id(jid)
        h = content_hash(raw)
        ats = raw.get("ats_name", "")
        board = raw.get("board_token", "")
        title = raw.get("title", "")
        job_data.append((jid, public_id, ats, board, title, h, raw))

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

    for jid, public_id, ats, board, title, h, raw in job_data:
        if jid not in existing:
            # New job — always needs detail fetch
            new_jobs.append(raw)
            needs_detail.append(raw)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipeline_jobs (id, public_job_id, ats, board_token, title, content_hash, first_seen_at, last_seen_at, needs_parse, raw_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        public_job_id = COALESCE(pipeline_jobs.public_job_id, EXCLUDED.public_job_id),
                        last_seen_at = EXCLUDED.last_seen_at,
                        content_hash = EXCLUDED.content_hash,
                        needs_parse = TRUE,
                        removed_at = NULL,
                        raw_json = EXCLUDED.raw_json
                """, (jid, public_id, ats, board, title, h, now, now, Json(raw)))
        elif existing[jid]["hash"] != h:
            # Content changed — needs detail fetch
            changed_jobs.append(raw)
            needs_detail.append(raw)
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pipeline_jobs SET
                        public_job_id = COALESCE(public_job_id, %s),
                        content_hash = %s,
                        title = %s,
                        last_seen_at = %s,
                        needs_parse = TRUE,
                        removed_at = NULL,
                        raw_json = %s
                    WHERE id = %s
                """, (public_id, h, title, now, Json(raw), jid))
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
                    "UPDATE pipeline_jobs SET public_job_id = COALESCE(public_job_id, %s), last_seen_at = %s, removed_at = NULL, raw_json = %s WHERE id = %s",
                    (public_id, now, Json(raw), jid)
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
    delete_job_recommendations(conn, removed_ids)
    return removed_ids


def get_jobs_needing_parse(conn, limit: int | None = None,
                           companies: list[tuple[str, str]] | None = None) -> list[dict]:
    """Get jobs that need LLM parsing, excluding jobs already reserved for batch parsing."""
    query = """
        SELECT id, ats, board_token, title, raw_json
        FROM pipeline_jobs
        WHERE needs_parse = TRUE
          AND removed_at IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM pipeline_parse_batch_jobs pbj
              WHERE pbj.job_id = pipeline_jobs.id
          )
    """
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


def save_parsed_result(
    conn,
    job_id: str,
    parsed_json: dict,
    parse_provider: str | None = None,
    parse_model: str | None = None,
    parse_params: dict | None = None,
):
    """Save LLM extraction result and clear needs_parse flag."""
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_jobs SET
                parsed_json = %s,
                last_parsed_at = %s,
                needs_parse = FALSE,
                parse_provider = COALESCE(%s, parse_provider),
                parse_model = COALESCE(%s, parse_model),
                parse_params = COALESCE(%s, parse_params)
            WHERE id = %s
        """, (Json(parsed_json), now, parse_provider, parse_model, Json(parse_params) if parse_params is not None else None, job_id))
    conn.commit()


def update_parsed_json(conn, job_id: str, parsed_json: dict):
    """Update parsed_json without mutating parse bookkeeping fields."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_jobs
            SET parsed_json = %s
            WHERE id = %s
        """, (Json(parsed_json), job_id))
    conn.commit()


def update_parsed_json_bulk(conn, rows: list[tuple[str, dict]]):
    """Bulk update parsed_json for many jobs in one transaction."""
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            WITH incoming(job_id, parsed_json) AS (VALUES %s)
            UPDATE pipeline_jobs AS pj
            SET parsed_json = incoming.parsed_json::jsonb
            FROM incoming
            WHERE pj.id = incoming.job_id
            """,
            [(job_id, Json(parsed_json)) for job_id, parsed_json in rows],
            template="(%s, %s)",
        )
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
    query = "SELECT id, public_job_id, ats, board_token, title, parsed_json, job_group, raw_json FROM pipeline_jobs WHERE parsed_json IS NOT NULL"
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
            {"id": r[0], "public_job_id": r[1], "ats": r[2], "board_token": r[3], "title": r[4], "parsed_json": r[5], "job_group": r[6], "raw_json": r[7]}
            for r in cur.fetchall()
        ]


def get_active_jobs_for_meili(conn, include_removed: bool = False, job_ids: list[str] | None = None) -> list[dict]:
    """Get active jobs for Meili loading, including ATS-only rows without parsed_json."""
    query = """
        SELECT id, public_job_id, ats, board_token, title, parsed_json, job_group, raw_json
        FROM pipeline_jobs
        WHERE raw_json IS NOT NULL
    """
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
            {"id": r[0], "public_job_id": r[1], "ats": r[2], "board_token": r[3], "title": r[4], "parsed_json": r[5], "job_group": r[6], "raw_json": r[7]}
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


def get_job_ids_pending_meili_load(conn, batch_id: str | None = None, limit: int | None = None) -> list[str]:
    """Return active jobs whose current ATS/enriched version is not yet loaded into Meili."""
    query = """
        SELECT id
        FROM pipeline_jobs
        WHERE removed_at IS NULL
          AND raw_json IS NOT NULL
          AND (
              (
                  meili_loaded_doc_version IS NULL
                  AND (
                      meili_loaded_content_hash IS DISTINCT FROM content_hash
                      OR meili_loaded_last_parsed_at IS DISTINCT FROM last_parsed_at
                  )
              )
              OR (
                  meili_loaded_doc_version IS NOT NULL
                  AND meili_loaded_doc_version IS DISTINCT FROM md5(
                      concat_ws(
                          '|',
                          COALESCE(%s, ''),
                          COALESCE(content_hash, ''),
                          COALESCE(last_parsed_at::text, ''),
                          COALESCE(job_group, id)
                      )
                  )
              )
          )
    """
    params: list[object] = [MEILI_DOC_SCHEMA_VERSION]
    if batch_id is not None:
        query += " AND parse_params->>'batch_id' = %s"
        params.append(batch_id)
    query += " ORDER BY last_parsed_at ASC NULLS LAST, id"
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)
    with conn.cursor() as cur:
        cur.execute(query, params)
        return [row[0] for row in cur.fetchall()]


def mark_jobs_meili_loaded(conn, job_ids: list[str]) -> None:
    """Stamp jobs with the parsed/content version successfully loaded into Meili."""
    if not job_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_jobs
            SET meili_loaded_at = NOW(),
                meili_loaded_content_hash = content_hash,
                meili_loaded_last_parsed_at = last_parsed_at,
                meili_loaded_doc_version = md5(
                    concat_ws(
                        '|',
                        COALESCE(%s, ''),
                        COALESCE(content_hash, ''),
                        COALESCE(last_parsed_at::text, ''),
                        COALESCE(job_group, id)
                    )
                )
            WHERE id = ANY(%s)
            """,
            (MEILI_DOC_SCHEMA_VERSION, job_ids),
        )
    conn.commit()


def mark_jobs_meili_deleted(conn, job_ids: list[str]) -> None:
    """Clear Meili load tracking after the corresponding docs are deleted."""
    if not job_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_jobs
            SET meili_loaded_at = NULL,
                meili_loaded_content_hash = NULL,
                meili_loaded_last_parsed_at = NULL,
                meili_loaded_doc_version = NULL
            WHERE id = ANY(%s)
            """,
            (job_ids,),
        )
    conn.commit()


def delete_job_recommendations(conn, job_ids: list[str]) -> None:
    """Delete recommendation edges that reference any of the given jobs."""
    if not job_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM pipeline_job_recommendations
            WHERE source_job_id = ANY(%s)
               OR recommended_job_id = ANY(%s)
            """,
            (job_ids, job_ids),
        )
    conn.commit()


def get_jobs_needing_recommendation_refresh(
    conn,
    algorithm_version: str,
    limit: int | None = None,
    job_ids: list[str] | None = None,
) -> list[dict]:
    """Return parsed active jobs whose recommendation set is missing or stale."""
    query = """
        SELECT id, title, parsed_json, last_parsed_at
        FROM pipeline_jobs
        WHERE removed_at IS NULL
          AND parsed_json IS NOT NULL
          AND meili_loaded_last_parsed_at IS NOT DISTINCT FROM last_parsed_at
          AND (
              recommendations_generated_at IS NULL
              OR recommendations_algorithm_version IS DISTINCT FROM %s
              OR recommendations_source_last_parsed_at IS DISTINCT FROM last_parsed_at
          )
    """
    params: list[object] = [algorithm_version]
    if job_ids is not None:
        if not job_ids:
            return []
        query += " AND id = ANY(%s)"
        params.append(job_ids)
    query += " ORDER BY last_parsed_at ASC NULLS LAST, id"
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)
    with conn.cursor() as cur:
        cur.execute(query, params)
        return [
            {"id": row[0], "title": row[1], "parsed_json": row[2] or {}, "last_parsed_at": row[3]}
            for row in cur.fetchall()
        ]


def replace_job_recommendations(
    conn,
    source_job_id: str,
    recommendations: list[dict],
    *,
    algorithm_version: str,
    source_last_parsed_at,
) -> None:
    """Replace the stored recommendation set for a source job."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline_job_recommendations WHERE source_job_id = %s",
            (source_job_id,),
        )
        if recommendations:
            execute_values(
                cur,
                """
                INSERT INTO pipeline_job_recommendations (
                    source_job_id,
                    recommended_job_id,
                    rank,
                    score,
                    algorithm_version,
                    generated_at
                )
                VALUES %s
                """,
                [
                    (
                        source_job_id,
                        row["recommended_job_id"],
                        row["rank"],
                        row.get("score"),
                        algorithm_version,
                        datetime.now(timezone.utc),
                    )
                    for row in recommendations
                ],
                template="(%s, %s, %s, %s, %s, %s)",
            )
        cur.execute(
            """
            UPDATE pipeline_jobs
            SET recommendations_generated_at = NOW(),
                recommendations_algorithm_version = %s,
                recommendations_source_last_parsed_at = %s
            WHERE id = %s
            """,
            (algorithm_version, source_last_parsed_at, source_job_id),
        )
    conn.commit()


def get_existing_jobs_for_board(conn, ats: str, board_token: str) -> dict[str, dict]:
    """Get existing raw jobs for a board keyed by compound job ID."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, raw_json
            FROM pipeline_jobs
            WHERE ats = %s AND board_token = %s
        """, (ats, board_token))
        return {row[0]: (row[1] or {}) for row in cur.fetchall()}


def parse_batch_selection_where(selection: str) -> str:
    """Return the SQL predicate for a parse-batch queue selection."""
    if selection == "needs_parse":
        return "removed_at IS NULL AND needs_parse = TRUE"
    if selection == "failed_once":
        return "removed_at IS NULL AND parsed_json IS NULL AND COALESCE(parse_error_count, 0) > 0"
    if selection == "never_parsed":
        return (
            "removed_at IS NULL AND parsed_json IS NULL AND last_parsed_at IS NULL "
            "AND COALESCE(parse_error_count, 0) = 0 AND last_parse_error IS NULL"
        )
    raise ValueError("selection must be 'needs_parse', 'never_parsed', or 'failed_once'")


def claim_jobs_for_parse_batch(conn, batch_id: str, limit: int,
                               companies: list[tuple[str, str]] | None = None,
                               ats_list: list[str] | None = None,
                               selection: str = "needs_parse") -> list[dict]:
    """Reserve up to ``limit`` jobs for a parse run.

    selection:
      - ``needs_parse``: current incremental queue
      - ``never_parsed``: active jobs with no parsed_json / no previous parse
      - ``failed_once``: active jobs with no parsed_json and at least one parse error
    """
    if limit <= 0:
        raise ValueError("limit must be greater than 0")
    selection_where = parse_batch_selection_where(selection)

    with conn.cursor() as cur:
        query = """
            SELECT id, ats, board_token, title, raw_json, content_hash
            FROM pipeline_jobs
            WHERE removed_at IS NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM pipeline_parse_batch_jobs pbj
                  WHERE pbj.job_id = pipeline_jobs.id
              )
        """
        query += f" AND {selection_where}"
        params: list[object] = []
        if ats_list is not None:
            if not ats_list:
                return []
            query += " AND ats = ANY(%s)"
            params.append(ats_list)
        if companies is not None:
            if not companies:
                return []
            clauses = []
            for ats, board_token in companies:
                clauses.append("(ats = %s AND board_token = %s)")
                params.extend([ats, board_token])
            query += " AND (" + " OR ".join(clauses) + ")"
        query += """
            ORDER BY last_seen_at DESC NULLS LAST, id
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        """
        params.append(limit)
        cur.execute(query, params)
        rows = cur.fetchall()
        if rows:
            cur.execute(
                "SELECT COALESCE(MAX(request_index), -1) + 1 FROM pipeline_parse_batch_jobs WHERE batch_id = %s",
                (batch_id,),
            )
            next_index = cur.fetchone()[0]
            execute_values(
                cur,
                """
                INSERT INTO pipeline_parse_batch_jobs (batch_id, request_index, job_id, content_hash)
                VALUES %s
                """,
                [
                    (batch_id, next_index + index, row[0], row[5] or "")
                    for index, row in enumerate(rows)
                ],
            )
    conn.commit()
    return [
        {
            "id": row[0],
            "ats": row[1],
            "board_token": row[2],
            "title": row[3],
            "raw_json": row[4],
            "content_hash": row[5],
        }
        for row in rows
    ]


def rename_parse_batch(conn, old_batch_id: str, new_batch_id: str):
    """Rename reserved batch mappings after the provider returns the final batch ID."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline_parse_batch_jobs SET batch_id = %s WHERE batch_id = %s",
            (new_batch_id, old_batch_id),
        )
        cur.execute(
            "UPDATE pipeline_parse_batches SET batch_id = %s WHERE batch_id = %s",
            (new_batch_id, old_batch_id),
        )
    conn.commit()


def save_parse_batch(conn, batch_id: str, model: str, state: str, display_name: str | None = None,
                     params: dict | None = None,
                     input_file_name: str | None = None, output_file_name: str | None = None,
                     requested_count: int = 0, succeeded_count: int = 0, failed_count: int = 0,
                     stale_count: int = 0, completed_at=None, last_error: str | None = None):
    """Insert or update batch metadata."""
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_parse_batches (
                batch_id, model, params, display_name, state, input_file_name, output_file_name,
                requested_count, succeeded_count, failed_count, stale_count, submitted_at,
                updated_at, completed_at, last_error
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_id) DO UPDATE SET
                model = EXCLUDED.model,
                params = COALESCE(EXCLUDED.params, pipeline_parse_batches.params),
                display_name = COALESCE(EXCLUDED.display_name, pipeline_parse_batches.display_name),
                state = EXCLUDED.state,
                input_file_name = COALESCE(EXCLUDED.input_file_name, pipeline_parse_batches.input_file_name),
                output_file_name = COALESCE(EXCLUDED.output_file_name, pipeline_parse_batches.output_file_name),
                requested_count = EXCLUDED.requested_count,
                succeeded_count = EXCLUDED.succeeded_count,
                failed_count = EXCLUDED.failed_count,
                stale_count = EXCLUDED.stale_count,
                updated_at = EXCLUDED.updated_at,
                completed_at = COALESCE(EXCLUDED.completed_at, pipeline_parse_batches.completed_at),
                last_error = COALESCE(EXCLUDED.last_error, pipeline_parse_batches.last_error)
            """,
            (
                batch_id, model, Json(params) if params is not None else None, display_name, state, input_file_name, output_file_name,
                requested_count, succeeded_count, failed_count, stale_count, now, now, completed_at, last_error,
            ),
        )
    conn.commit()


def update_parse_batch(conn, batch_id: str, state: str, output_file_name: str | None = None,
                       succeeded_count: int | None = None, failed_count: int | None = None,
                       stale_count: int | None = None, completed_at=None, last_error: str | None = None):
    """Update batch status and aggregate counts."""
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_parse_batches
            SET state = %s,
                output_file_name = COALESCE(%s, output_file_name),
                succeeded_count = COALESCE(%s, succeeded_count),
                failed_count = COALESCE(%s, failed_count),
                stale_count = COALESCE(%s, stale_count),
                updated_at = %s,
                completed_at = COALESCE(%s, completed_at),
                last_error = COALESCE(%s, last_error)
            WHERE batch_id = %s
            """,
            (state, output_file_name, succeeded_count, failed_count, stale_count, now, completed_at, last_error, batch_id),
        )
    conn.commit()


def get_parse_batch(conn, batch_id: str) -> dict | None:
    """Return stored metadata for a parse batch."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT batch_id, model, params, display_name, state, input_file_name, output_file_name,
                   requested_count, succeeded_count, failed_count, stale_count,
                   submitted_at, updated_at, completed_at, last_error
            FROM pipeline_parse_batches
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "batch_id": row[0],
        "model": row[1],
        "params": row[2],
        "display_name": row[3],
        "state": row[4],
        "input_file_name": row[5],
        "output_file_name": row[6],
        "requested_count": row[7],
        "succeeded_count": row[8],
        "failed_count": row[9],
        "stale_count": row[10],
        "submitted_at": row[11],
        "updated_at": row[12],
        "completed_at": row[13],
        "last_error": row[14],
    }


def list_parse_batches(conn, limit: int = 20) -> list[dict]:
    """List recent parse batches."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT batch_id, model, display_name, state, requested_count,
                   succeeded_count, failed_count, stale_count, submitted_at, updated_at
            FROM pipeline_parse_batches
            ORDER BY submitted_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [
        {
            "batch_id": row[0],
            "model": row[1],
            "display_name": row[2],
            "state": row[3],
            "requested_count": row[4],
            "succeeded_count": row[5],
            "failed_count": row[6],
            "stale_count": row[7],
            "submitted_at": row[8],
            "updated_at": row[9],
        }
        for row in rows
    ]


def get_parse_batch_job_rows(conn, batch_id: str) -> list[dict]:
    """Return reserved jobs for a batch in request order."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pbj.request_index, pbj.job_id, pbj.content_hash, pj.raw_json, pj.content_hash
            FROM pipeline_parse_batch_jobs pbj
            LEFT JOIN pipeline_jobs pj ON pj.id = pbj.job_id
            WHERE pbj.batch_id = %s
            ORDER BY pbj.request_index
            """,
            (batch_id,),
        )
        rows = cur.fetchall()
    return [
        {
            "request_index": row[0],
            "job_id": row[1],
            "submitted_content_hash": row[2],
            "raw_json": row[3],
            "current_content_hash": row[4],
        }
        for row in rows
    ]


def apply_parse_batch_chunk(
    conn,
    batch_id: str,
    success_rows: list[tuple[str, str, dict]],
    error_rows: list[tuple[str, str, str]],
    parse_provider: str | None = None,
    parse_model: str | None = None,
    parse_params: dict | None = None,
) -> dict:
    """Apply a chunk of batch parse results with a single transaction."""
    attempted_ids = [row[0] for row in success_rows] + [row[0] for row in error_rows]
    if not attempted_ids:
        return {"applied_success_ids": [], "applied_failure_count": 0, "stale_count": 0}
    applied_success_ids: list[str] = []
    applied_failure_count = 0

    with conn.cursor() as cur:
        if success_rows:
            updated = execute_values(
                cur,
                """
                WITH incoming(job_id, expected_hash, parsed_json, parse_provider, parse_model, parse_params) AS (VALUES %s)
                UPDATE pipeline_jobs AS pj
                SET parsed_json = incoming.parsed_json::jsonb,
                    last_parsed_at = NOW(),
                    needs_parse = FALSE,
                    parse_error_count = 0,
                    last_parse_error = NULL,
                    parse_provider = COALESCE(incoming.parse_provider, pj.parse_provider),
                    parse_model = COALESCE(incoming.parse_model, pj.parse_model),
                    parse_params = COALESCE(incoming.parse_params::jsonb, pj.parse_params)
                FROM incoming
                WHERE pj.id = incoming.job_id
                  AND pj.removed_at IS NULL
                  AND pj.content_hash = incoming.expected_hash
                RETURNING pj.id
                """,
                [
                    (
                        job_id,
                        expected_hash,
                        Json(parsed_json),
                        parse_provider,
                        parse_model,
                        Json(parse_params) if parse_params is not None else None,
                    )
                    for job_id, expected_hash, parsed_json in success_rows
                ],
                template="(%s, %s, %s, %s, %s, %s)",
                fetch=True,
            )
            applied_success_ids = [row[0] for row in updated]

        if error_rows:
            updated = execute_values(
                cur,
                """
                WITH incoming(job_id, expected_hash, error_text) AS (VALUES %s)
                UPDATE pipeline_jobs AS pj
                SET parse_error_count = COALESCE(pj.parse_error_count, 0) + 1,
                    last_parse_error = LEFT(incoming.error_text, 500),
                    needs_parse = (COALESCE(pj.parse_error_count, 0) + 1) < 3
                FROM incoming
                WHERE pj.id = incoming.job_id
                  AND pj.removed_at IS NULL
                  AND pj.content_hash = incoming.expected_hash
                RETURNING pj.id
                """,
                error_rows,
                template="(%s, %s, %s)",
                fetch=True,
            )
            applied_failure_count = len(updated)

        cur.execute(
            "DELETE FROM pipeline_parse_batch_jobs WHERE batch_id = %s AND job_id = ANY(%s)",
            (batch_id, attempted_ids),
        )

    conn.commit()
    stale_count = len(attempted_ids) - len(applied_success_ids) - applied_failure_count
    return {
        "applied_success_ids": applied_success_ids,
        "applied_failure_count": applied_failure_count,
        "stale_count": stale_count,
    }


def save_parsed_batch_result(
    conn,
    batch_id: str,
    job_id: str,
    expected_hash: str,
    parsed_json: dict,
    parse_provider: str | None = None,
    parse_model: str | None = None,
    parse_params: dict | None = None,
) -> bool:
    """Save a batch parse result if the job content has not changed since submission."""
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_jobs
            SET parsed_json = %s,
                last_parsed_at = %s,
                needs_parse = FALSE,
                parse_error_count = 0,
                last_parse_error = NULL,
                parse_provider = COALESCE(%s, parse_provider),
                parse_model = COALESCE(%s, parse_model),
                parse_params = COALESCE(%s, parse_params)
            WHERE id = %s
              AND removed_at IS NULL
              AND content_hash = %s
            """,
            (
                Json(parsed_json),
                now,
                parse_provider,
                parse_model,
                Json(parse_params) if parse_params is not None else None,
                job_id,
                expected_hash,
            ),
        )
        applied = cur.rowcount > 0
        cur.execute(
            "DELETE FROM pipeline_parse_batch_jobs WHERE batch_id = %s AND job_id = %s",
            (batch_id, job_id),
        )
    conn.commit()
    return applied


def record_parse_batch_error(conn, batch_id: str, job_id: str, expected_hash: str, error: str) -> bool:
    """Record a batch parse failure if the job content is still current, then release its reservation."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_jobs
            SET parse_error_count = COALESCE(parse_error_count, 0) + 1,
                last_parse_error = %s,
                needs_parse = (COALESCE(parse_error_count, 0) + 1) < 3
            WHERE id = %s
              AND removed_at IS NULL
              AND content_hash = %s
            """,
            (error[:500], job_id, expected_hash),
        )
        applied = cur.rowcount > 0
        cur.execute(
            "DELETE FROM pipeline_parse_batch_jobs WHERE batch_id = %s AND job_id = %s",
            (batch_id, job_id),
        )
    conn.commit()
    return applied


def delete_parse_batch_jobs(conn, batch_id: str, job_ids: list[str] | None = None):
    """Release reserved jobs for a batch, optionally scoped to specific job IDs."""
    with conn.cursor() as cur:
        if job_ids is None:
            cur.execute("DELETE FROM pipeline_parse_batch_jobs WHERE batch_id = %s", (batch_id,))
        else:
            if not job_ids:
                return
            cur.execute(
                "DELETE FROM pipeline_parse_batch_jobs WHERE batch_id = %s AND job_id = ANY(%s)",
                (batch_id, job_ids),
            )
    conn.commit()


def delete_parse_batch(conn, batch_id: str):
    """Delete local batch metadata and release any reserved jobs."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM pipeline_parse_batch_jobs WHERE batch_id = %s", (batch_id,))
        cur.execute("DELETE FROM pipeline_parse_batches WHERE batch_id = %s", (batch_id,))
    conn.commit()




def upsert_geo_places(conn, places: list[dict], chunk_size: int = 2000) -> int:
    """Upsert canonical place rows into geo_places."""
    if not places:
        return 0

    total = 0
    with conn.cursor() as cur:
        for start in range(0, len(places), chunk_size):
            chunk = places[start:start + chunk_size]
            execute_values(
                cur,
                """
                INSERT INTO geo_places (
                    geoname_id, kind, canonical_name, ascii_name, display_name,
                    country_code, country_name, admin1_code, admin1_name,
                    latitude, longitude, population, timezone,
                    feature_class, feature_code, search_names, updated_at
                )
                VALUES %s
                ON CONFLICT (geoname_id) DO UPDATE SET
                    kind = EXCLUDED.kind,
                    canonical_name = EXCLUDED.canonical_name,
                    ascii_name = EXCLUDED.ascii_name,
                    display_name = EXCLUDED.display_name,
                    country_code = EXCLUDED.country_code,
                    country_name = EXCLUDED.country_name,
                    admin1_code = EXCLUDED.admin1_code,
                    admin1_name = EXCLUDED.admin1_name,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    population = EXCLUDED.population,
                    timezone = EXCLUDED.timezone,
                    feature_class = EXCLUDED.feature_class,
                    feature_code = EXCLUDED.feature_code,
                    search_names = EXCLUDED.search_names,
                    updated_at = EXCLUDED.updated_at
                """,
                [
                    (
                        place["geoname_id"],
                        place["kind"],
                        place["canonical_name"],
                        place.get("ascii_name"),
                        place["display_name"],
                        place.get("country_code"),
                        place.get("country_name"),
                        place.get("admin1_code"),
                        place.get("admin1_name"),
                        place.get("latitude"),
                        place.get("longitude"),
                        place.get("population"),
                        place.get("timezone"),
                        place.get("feature_class"),
                        place.get("feature_code"),
                        place.get("search_names") or [],
                        datetime.now(timezone.utc),
                    )
                    for place in chunk
                ],
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            )
            total += len(chunk)
    conn.commit()
    return total


def get_geo_place_counts(conn) -> dict[str, int]:
    """Return row counts for the canonical geo_places table."""
    with conn.cursor() as cur:
        cur.execute("SELECT kind, COUNT(*) FROM geo_places GROUP BY kind ORDER BY kind")
        return {row[0]: row[1] for row in cur.fetchall()}

def get_companies_to_scrape(
    conn,
    limit: int,
    ats_filter: list[str] | None = None,
    ats_exclude_filter: list[str] | None = None,
) -> list[tuple[str, str]]:
    """Select a bounded set of active companies for scraping.

    Prioritize companies that have never been scraped, then oldest scrape times.
    """
    with conn.cursor() as cur:
        query = """
            SELECT ats, board_token
            FROM pipeline_companies
            WHERE active = TRUE
        """
        params: list[object] = []
        if ats_filter:
            query += " AND ats = ANY(%s)"
            params.append(ats_filter)
        if ats_exclude_filter:
            query += " AND NOT (ats = ANY(%s))"
            params.append(ats_exclude_filter)
        query += """
            ORDER BY last_scraped_at NULLS FIRST, ats, board_token
            LIMIT %s
        """
        params.append(limit)
        cur.execute(query, params)
        return [(r[0], r[1]) for r in cur.fetchall()]


def get_companies_to_scrape_by_status(
    conn,
    limit: int,
    ats_filter: list[str] | None = None,
    ats_exclude_filter: list[str] | None = None,
    scrape_statuses: list[str] | None = None,
) -> list[tuple[str, str]]:
    """Select companies by scrape status, prioritizing pending rows first."""
    with conn.cursor() as cur:
        query = """
            SELECT ats, board_token
            FROM pipeline_companies
            WHERE 1 = 1
        """
        params: list[object] = []
        if ats_filter:
            query += " AND ats = ANY(%s)"
            params.append(ats_filter)
        if ats_exclude_filter:
            query += " AND NOT (ats = ANY(%s))"
            params.append(ats_exclude_filter)
        if scrape_statuses:
            query += " AND COALESCE(scrape_status, 'pending') = ANY(%s)"
            params.append(scrape_statuses)
        query += """
            ORDER BY
                CASE WHEN COALESCE(scrape_status, 'pending') = 'pending' THEN 0 ELSE 1 END,
                last_scraped_at NULLS FIRST,
                ats,
                board_token
            LIMIT %s
        """
        params.append(limit)
        cur.execute(query, params)
        return [(r[0], r[1]) for r in cur.fetchall()]


def backfill_company_slugs(conn, only_missing: bool = True, chunk_size: int = 1000) -> int:
    """Backfill deterministic public company slugs."""
    query = """
        SELECT ats, board_token, company_name, domain, company_slug
        FROM pipeline_companies
    """
    if only_missing:
        query += " WHERE company_slug IS NULL OR company_slug = ''"

    with conn.cursor() as cur:
        cur.execute(query)
        rows = [
            {
                "ats": row[0],
                "board_token": row[1],
                "company_name": row[2],
                "domain": row[3],
                "company_slug": row[4],
            }
            for row in cur.fetchall()
        ]

    if not rows:
        return 0

    with conn.cursor() as cur:
        cur.execute("SELECT ats, board_token, company_name, domain FROM pipeline_companies")
        all_rows = [
            {
                "ats": row[0],
                "board_token": row[1],
                "company_name": row[2],
                "domain": row[3],
            }
            for row in cur.fetchall()
        ]

    slug_map = derive_company_slug_map(all_rows)
    updates = [
        (row["ats"], row["board_token"], slug_map[(row["ats"], row["board_token"])])
        for row in rows
        if slug_map.get((row["ats"], row["board_token"]))
    ]
    if not updates:
        return 0

    total_updated = 0
    for i in range(0, len(updates), chunk_size):
        chunk = updates[i:i + chunk_size]
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                WITH incoming(ats, board_token, company_slug) AS (VALUES %s)
                UPDATE pipeline_companies AS pc
                SET company_slug = incoming.company_slug
                FROM incoming
                WHERE pc.ats = incoming.ats
                  AND pc.board_token = incoming.board_token
                  AND COALESCE(pc.company_slug, '') <> incoming.company_slug
                """,
                chunk,
                template="(%s, %s, %s)",
            )
            total_updated += max(cur.rowcount, 0)
        conn.commit()
    return total_updated


def backfill_public_job_ids(conn, only_missing: bool = True, chunk_size: int = 5000) -> int:
    """Backfill stable public IDs for jobs."""
    query = "SELECT id FROM pipeline_jobs"
    if only_missing:
        query += " WHERE public_job_id IS NULL OR public_job_id = ''"

    with conn.cursor() as cur:
        cur.execute(query)
        rows = [row[0] for row in cur.fetchall()]

    if not rows:
        return 0

    total_updated = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        updates = [(jid, short_public_job_id(jid)) for jid in chunk]
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                WITH incoming(job_id, public_job_id) AS (VALUES %s)
                UPDATE pipeline_jobs AS pj
                SET public_job_id = incoming.public_job_id
                FROM incoming
                WHERE pj.id = incoming.job_id
                  AND COALESCE(pj.public_job_id, '') <> incoming.public_job_id
                """,
                updates,
                template="(%s, %s)",
            )
            total_updated += max(cur.rowcount, 0)
        conn.commit()
    return total_updated


_UNSET = object()


def upsert_company(conn, ats: str, board_token: str, company_name: str | None = None,
                    domain: str | None = None, description: str | None = None,
                    logo_url: str | None = None,
                    scraped_logo_url: str | None = None,
                    job_count: int = 0, job_count_exact: bool = True,
                    scrape_status: str | object = _UNSET,
                    last_scrape_error: str | None | object = _UNSET,
                    last_http_status: int | None | object = _UNSET):
    """Upsert a company record."""
    now = datetime.now(timezone.utc)
    slug_map = derive_company_slug_map([{
        "ats": ats,
        "board_token": board_token,
        "company_name": company_name,
        "domain": domain,
    }])
    company_slug = slug_map[(ats, board_token)]
    scrape_status_is_set = scrape_status is not _UNSET
    last_scrape_error_is_set = last_scrape_error is not _UNSET
    last_http_status_is_set = last_http_status is not _UNSET
    scrape_status_value = None if scrape_status is _UNSET else scrape_status
    last_scrape_error_value = None if last_scrape_error is _UNSET else last_scrape_error
    last_http_status_value = None if last_http_status is _UNSET else last_http_status
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_companies (
                ats, board_token, company_name, company_slug, domain, description,
                logo_url, scraped_logo_url, scrape_status, last_scrape_error, last_http_status,
                last_scraped_at, job_count, active
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s > 0)
            ON CONFLICT (ats, board_token) DO UPDATE SET
                company_name = COALESCE(EXCLUDED.company_name, pipeline_companies.company_name),
                company_slug = COALESCE(EXCLUDED.company_slug, pipeline_companies.company_slug),
                domain = COALESCE(EXCLUDED.domain, pipeline_companies.domain),
                description = COALESCE(EXCLUDED.description, pipeline_companies.description),
                logo_url = COALESCE(EXCLUDED.logo_url, pipeline_companies.logo_url),
                scraped_logo_url = COALESCE(EXCLUDED.scraped_logo_url, pipeline_companies.scraped_logo_url),
                scrape_status = CASE
                    WHEN %s THEN EXCLUDED.scrape_status
                    ELSE pipeline_companies.scrape_status
                END,
                last_scrape_error = CASE
                    WHEN %s THEN EXCLUDED.last_scrape_error
                    ELSE pipeline_companies.last_scrape_error
                END,
                last_http_status = CASE
                    WHEN %s THEN EXCLUDED.last_http_status
                    ELSE pipeline_companies.last_http_status
                END,
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
            ats, board_token, company_name, company_slug, domain, description, logo_url, scraped_logo_url,
            scrape_status_value, last_scrape_error_value, last_http_status_value,
            now, job_count, job_count,
            scrape_status_is_set, last_scrape_error_is_set, last_http_status_is_set,
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
