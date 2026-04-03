"""
End-to-end pipeline: scrape → detect changes → parse new/changed → upsert MeiliSearch.

Uses Neon Postgres for state tracking (content hashes, parse status).
Only re-parses jobs whose content actually changed.

Usage:
  uv run python pipeline.py --companies companies.txt
  uv run python pipeline.py --companies companies.txt --skip-scrape   # re-parse pending + reload
  uv run python pipeline.py --companies companies.txt --skip-parse    # just reload from DB
  uv run python pipeline.py --companies companies.txt --parse-pending # parse only jobs with needs_parse=True
"""

import argparse
import hashlib
import itertools
import json
import math
import os
import time
import sys
import uuid
from collections import defaultdict
from urllib.parse import urlparse

from dotenv import load_dotenv
load_dotenv()
from psycopg2.extras import Json

from scrapers.greenhouse_scraper import GreenhouseScraper
from scrapers.lever_scraper import LeverScraper
from scrapers.ashby_scraper import AshbyScraper
from scrapers.jobvite_scraper import JobviteScraper
from db import (
    get_connection, init_schema, upsert_scraped_jobs, mark_removed,
    job_id, upsert_company, get_jobs_needing_parse, save_parsed_result,
    record_parse_error, get_parsed_jobs, get_removed_job_ids, get_companies_to_scrape,
    get_existing_jobs_for_board, get_latest_fx_rates, mark_jobs_meili_deleted,
    mark_jobs_meili_loaded,
)
from salary_normalization import normalize_salary_annual_usd
from public_ids import meili_safe_job_id


ATS_SCRAPERS = {
    "greenhouse": GreenhouseScraper,
    "lever": LeverScraper,
    "ashby": AshbyScraper,
    "jobvite": JobviteScraper,
}


def _jobs_embedders_settings() -> dict:
    gemini_api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not gemini_api_key:
        return {}

    return {
        "default": {
            "source": "rest",
            "url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:batchEmbedContents",
            "headers": {
                "Content-Type": "application/json",
                "x-goog-api-key": gemini_api_key,
            },
            "request": {
                "requests": [
                    {
                        "model": "models/gemini-embedding-001",
                        "content": {
                            "parts": [
                                {"text": "{{text}}"},
                            ]
                        },
                    },
                    "{{..}}",
                ],
            },
            "response": {
                "embeddings": [
                    {"values": "{{embedding}}"},
                    "{{..}}",
                ]
            },
            "dimensions": 3072,
            "documentTemplateMaxBytes": 10000,
        }
    }


def _connection_is_closed(conn) -> bool:
    closed = getattr(conn, "closed", 0)
    try:
        return bool(closed)
    except Exception:
        return False


def _reconnect_pipeline_connection(conn):
    try:
        if hasattr(conn, "close") and not _connection_is_closed(conn):
            conn.close()
    except Exception:
        pass

    new_conn = get_connection()
    init_schema(new_conn)
    return new_conn


def _recover_pipeline_connection(conn):
    if _connection_is_closed(conn):
        return _reconnect_pipeline_connection(conn)

    try:
        if hasattr(conn, "rollback"):
            conn.rollback()
        return conn
    except Exception:
        return _reconnect_pipeline_connection(conn)


def parse_companies_file(path: str) -> list[tuple[str, str]]:
    """Parse companies.txt → list of (ats, board_token)."""
    companies = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" in line:
                ats, token = line.split(":", 1)
            else:
                ats, token = "greenhouse", line
            companies.append((ats.strip(), token.strip()))
    return companies


def resolve_companies(conn, companies_path: str | None = None,
                      companies_from_db: bool = False,
                      db_company_limit: int | None = None) -> list[tuple[str, str]]:
    """Resolve companies from either a file or a bounded DB query."""
    if companies_from_db:
        if db_company_limit is not None and db_company_limit <= 0:
            raise ValueError("--db-company-limit must be greater than 0")
        limit = db_company_limit if db_company_limit is not None else 10_000_000
        return get_companies_to_scrape(conn, limit=limit)

    if not companies_path:
        raise ValueError("--companies is required unless --companies-from-db is used")

    return parse_companies_file(companies_path)


def shard_for_company(ats: str, token: str, total_shards: int) -> int:
    """Return a stable shard index for a company."""
    key = f"{ats}:{token}".encode("utf-8")
    digest = hashlib.sha256(key).hexdigest()
    return int(digest[:16], 16) % total_shards


def filter_companies_for_shard(companies: list[tuple[str, str]], shard_index: int | None,
                               total_shards: int | None) -> list[tuple[str, str]]:
    """Filter companies to the requested shard."""
    if shard_index is None or total_shards is None:
        return companies
    return [
        (ats, token)
        for ats, token in companies
        if shard_for_company(ats, token, total_shards) == shard_index
    ]


def should_mark_removed(fetched_count: int, max_per_company: int | None) -> bool:
    """Only mark removals when the scrape is likely complete for that company."""
    return max_per_company is None or fetched_count < max_per_company


def step_scrape(conn, companies: list[tuple[str, str]], max_per_company: int | None = None,
                jobvite_refetch_existing_detail: bool = False):
    """Scrape all companies and detect changes via content hashing."""
    print(f"\n--- SCRAPE ({len(companies)} companies) ---")

    total_new = 0
    total_changed = 0
    total_unchanged = 0
    total_removed = 0
    errors = 0
    touched_job_ids = set()
    removed_job_ids = set()

    total_companies = len(companies)
    for idx, (ats, token) in enumerate(companies, start=1):
        if _connection_is_closed(conn):
            conn = _reconnect_pipeline_connection(conn)

        scraper_cls = ATS_SCRAPERS.get(ats)
        if not scraper_cls:
            print(f"  [{idx}/{total_companies}] {ats}:{token} — unknown ATS, skipping")
            continue

        try:
            scraper = scraper_cls(token)
            existing_jobs = get_existing_jobs_for_board(conn, ats, token)
            existing_jobs_by_short_id = {
                jid.split("__")[-1]: raw
                for jid, raw in existing_jobs.items()
            }

            if ats == "jobvite":
                existing_details = {
                    short_id: {
                        "description": raw.get("description", ""),
                        "descriptionHtml": raw.get("descriptionHtml", ""),
                        "datePosted": raw.get("datePosted"),
                        "validThrough": raw.get("validThrough"),
                        "inactive": raw.get("inactive", False),
                    }
                    for short_id, raw in existing_jobs_by_short_id.items()
                    if raw.get("description") or raw.get("descriptionHtml") or raw.get("datePosted") or raw.get("inactive")
                }
                job_iter = scraper.fetch_jobs(
                    existing_details=existing_details,
                    refetch_existing_detail=jobvite_refetch_existing_detail,
                )
                jobs = list(job_iter) if max_per_company is None else list(itertools.islice(job_iter, max_per_company))
            else:
                job_iter = scraper.fetch_jobs()
                jobs = list(job_iter) if max_per_company is None else list(itertools.islice(job_iter, max_per_company))

            # Detect changes against DB
            result = upsert_scraped_jobs(conn, jobs)
            n_new = len(result["new"])
            n_changed = len(result["changed"])
            n_unchanged = result["unchanged"]
            needs_detail = result.get("needs_detail_fetch", [])

            # Fetch per-job detail data for jobs that need it (e.g. Greenhouse pay transparency)
            n_detail = 0
            if ats == "greenhouse" and needs_detail:
                changed_or_new_ids = {job_id(raw) for raw in result["new"]}
                changed_or_new_ids.update(job_id(raw) for raw in result["changed"])
                for raw in needs_detail:
                    jid = job_id(raw)
                    existing_raw = existing_jobs.get(jid, {})
                    if jid not in changed_or_new_ids and existing_raw.get("pay_input_ranges"):
                        continue
                    raw_id = str(raw.get("id", "")).split("__")[-1]
                    try:
                        pay_ranges = scraper.fetch_job_pay(raw_id)
                        if pay_ranges:
                            # Update raw_json in DB with pay data
                            with conn.cursor() as cur:
                                cur.execute("""
                                    UPDATE pipeline_jobs
                                    SET raw_json = raw_json || %s::jsonb
                                    WHERE id = %s
                                """, (Json({"pay_input_ranges": pay_ranges}), jid))
                            n_detail += 1
                    except Exception:
                        pass
                    time.sleep(0.1)
                conn.commit()

            # A bounded scrape only gives a lower bound on board size.
            complete_scrape = should_mark_removed(len(jobs), max_per_company)

            # Mark jobs not seen in this scrape as removed
            seen_ids = {job_id(j) for j in jobs}
            removed_ids = []
            if complete_scrape:
                removed_ids = mark_removed(conn, ats, token, seen_ids)
            n_removed = len(removed_ids)

            # Update company record
            company_name = None
            company_domain = None
            company_logo = None
            try:
                company_name = scraper.get_company_name()
            except Exception:
                pass
            try:
                company_domain = scraper.get_company_domain()
            except Exception:
                pass
            try:
                company_logo = scraper.get_company_logo_url()
            except Exception:
                pass
            upsert_company(
                conn,
                ats,
                token,
                company_name=company_name,
                domain=company_domain,
                scraped_logo_url=company_logo,
                job_count=len(jobs),
                job_count_exact=complete_scrape,
            )

            status_parts = []
            if n_new: status_parts.append(f"{n_new} new")
            if n_changed: status_parts.append(f"{n_changed} changed")
            if n_unchanged: status_parts.append(f"{n_unchanged} unchanged")
            if n_removed: status_parts.append(f"{n_removed} removed")
            if n_detail: status_parts.append(f"{n_detail} pay fetched")
            status = ", ".join(status_parts) or "empty"
            print(f"  [{idx}/{total_companies}] {ats}:{token} — {len(jobs)} total → {status}")

            total_new += n_new
            total_changed += n_changed
            total_unchanged += n_unchanged
            total_removed += n_removed
            touched_job_ids.update(job_id(job) for job in result["new"])
            touched_job_ids.update(job_id(job) for job in result["changed"])
            removed_job_ids.update(removed_ids)

        except Exception as e:
            errors += 1
            print(f"  [{idx}/{total_companies}] {ats}:{token} — ERROR: {e}")
            try:
                conn = _recover_pipeline_connection(conn)
            except Exception as reconnect_error:
                print(f"    failed to recover DB connection: {reconnect_error}")
            # Don't update company record on error — avoid marking as inactive
            continue

        time.sleep(0.3)

    print(f"\nScrape complete: {total_new} new, {total_changed} changed, "
          f"{total_unchanged} unchanged, {total_removed} removed, {errors} errors")
    return {
        "touched_job_ids": touched_job_ids,
        "removed_job_ids": removed_job_ids,
        "new_count": total_new,
        "changed_count": total_changed,
        "removed_count": total_removed,
        "errors": errors,
        "conn": conn,
    }


def step_parse(conn, base_url: str, model: str, api_key: str | None = None,
               limit: int | None = None, concurrency: int = 10,
               companies: list[tuple[str, str]] | None = None):
    """Parse jobs that need extraction (needs_parse=True). Uses concurrent requests."""
    import os
    import concurrent.futures
    from parse import OpenAIBackend, GeminiBackend, prepare_job_text, merge_api_data
    from geo_resolver import GeoResolver

    pending = get_jobs_needing_parse(conn, limit=limit, companies=companies)
    if not pending:
        print("\n--- PARSE (0 jobs pending) ---")
        return []

    print(f"\n--- PARSE ({len(pending)} jobs pending, concurrency={concurrency}) ---")
    # Choose backend based on model name
    if "gemini" in model.lower():
        key = api_key or os.environ.get("GEMINI_API_KEY", "")
        backend = GeminiBackend(model=model, api_key=key)
        parse_provider = "google"
        parse_params = {"method": "direct"}
    else:
        key = api_key or os.environ.get("OPENAI_API_KEY", "not-needed")
        backend = OpenAIBackend(base_url, model, api_key=key)
        parse_provider = "openai"
        parse_params = {"method": "direct", "base_url": base_url}

    successes = 0
    failures = 0
    t0 = time.time()
    parsed_job_ids = []
    geo_resolver = GeoResolver(conn)

    def parse_one(job_row):
        """Parse a single job. Returns (jid, parsed_dict, raw) or (jid, None, raw)."""
        jid = job_row["id"]
        raw = job_row.get("raw_json")
        if not raw:
            return jid, None, None

        try:
            text = prepare_job_text(raw)
            results = backend.extract_batch([text])
            result = results[0]
            if result is not None:
                parsed = result.model_dump(mode="json")
                parsed = merge_api_data(raw, parsed)
                parsed = geo_resolver.resolve_parsed_geo(parsed)
                return jid, parsed, raw
        except Exception as e:
            return jid, None, raw, str(e)

        return jid, None, raw

    # Process in concurrent batches
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {}
        submitted = 0
        completed = 0

        # Submit all jobs
        for job_row in pending:
            future = executor.submit(parse_one, job_row)
            futures[future] = job_row
            submitted += 1

        # Collect results as they complete
        for future in concurrent.futures.as_completed(futures):
            completed += 1
            result_tuple = future.result()

            if len(result_tuple) == 4:
                # Error case
                jid, _, raw, error = result_tuple
                record_parse_error(conn, jid, error)
                failures += 1
            elif result_tuple[1] is not None:
                jid, parsed, raw = result_tuple
                save_parsed_result(
                    conn,
                    jid,
                    parsed,
                    parse_provider=parse_provider,
                    parse_model=model,
                    parse_params=parse_params,
                )
                successes += 1
                parsed_job_ids.append(jid)
            else:
                jid = result_tuple[0]
                if result_tuple[2] is not None:
                    record_parse_error(conn, jid, "parse returned None")
                failures += 1

            if completed % 50 == 0 or completed == len(pending):
                elapsed = time.time() - t0
                rate = completed / elapsed if elapsed > 0 else 0
                eta = (len(pending) - completed) / rate / 60 if rate > 0 else 0
                print(f"  {completed}/{len(pending)} | {successes} ok | {failures} fail | {rate:.1f} jobs/s | ETA {eta:.0f}m")

    print(f"Parsed {successes}/{len(pending)}")
    return parsed_job_ids


def _normalize_company_domain_for_favicon(domain: str | None) -> str | None:
    if not domain:
        return None
    value = domain.strip()
    if not value:
        return None
    parsed = urlparse(value if "://" in value else f"https://{value}")
    return parsed.netloc or parsed.path or None


def _fallback_company_logo(company_domain: str | None, company_slug: str | None) -> str:
    domain = _normalize_company_domain_for_favicon(company_domain) or (f"{company_slug}.com" if company_slug else None)
    if not domain:
        return ""
    return f"https://www.google.com/s2/favicons?domain={domain}&sz=128"


def _admin1_key(country_code: str | None, admin1_code: str | None) -> str | None:
    if not country_code or not admin1_code:
        return None
    return f"{country_code}-{admin1_code}"


def _applicant_requirement_label(requirement: dict) -> str | None:
    if not isinstance(requirement, dict):
        return None
    if requirement.get("scope") == "country":
        return requirement.get("name") or requirement.get("country_code")
    if requirement.get("scope") in {"state", "city"}:
        return requirement.get("name") or requirement.get("region")
    if requirement.get("scope") == "region_group":
        return requirement.get("name")
    return requirement.get("name")


def _work_location_label(location: dict) -> str | None:
    if not isinstance(location, dict):
        return None
    if location.get("label"):
        return location["label"]
    loc_parts = []
    if location.get("city"):
        loc_parts.append(location["city"])
    if location.get("state"):
        loc_parts.append(location["state"])
    if location.get("country_code"):
        loc_parts.append(location["country_code"])
    return ", ".join(loc_parts) or None


def _build_meili_locations_all(parsed_json: dict) -> list[str]:
    labels: list[str] = []
    seen: set[str] = set()

    for location in parsed_json.get("locations", []) or []:
        label = _work_location_label(location)
        if not label or label in seen:
            continue
        seen.add(label)
        labels.append(label)

    if labels:
        return labels

    if parsed_json.get("office_type") == "remote":
        for requirement in parsed_json.get("applicant_location_requirements", []) or []:
            label = _applicant_requirement_label(requirement)
            if not label or label in seen:
                continue
            seen.add(label)
            labels.append(label)

    return labels


def _load_geo_place_lookup(conn, geoname_ids: set[int]) -> dict[int, dict]:
    if not geoname_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT geoname_id, kind, country_code, admin1_code
            FROM geo_places
            WHERE geoname_id = ANY(%s)
            """,
            (list(geoname_ids),),
        )
        return {
            row[0]: {
                "kind": row[1],
                "country_code": row[2],
                "admin1_code": row[3],
            }
            for row in cur.fetchall()
        }


def _dedupe_nonempty(values: list[str | int | None]) -> list[str | int]:
    ordered = []
    seen = set()
    for value in values:
        if value in (None, ""):
            continue
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _location_point(location: dict) -> dict[str, float] | None:
    if not isinstance(location, dict):
        return None
    lat = location.get("lat")
    lng = location.get("lng")
    if lat in (None, "") or lng in (None, ""):
        return None
    try:
        lat_f = float(lat)
        lng_f = float(lng)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(lat_f) or not math.isfinite(lng_f):
        return None
    return {"lat": lat_f, "lng": lng_f}


def _build_primary_geo(parsed_json: dict) -> dict[str, float] | None:
    for location in parsed_json.get("locations", []) or []:
        point = _location_point(location)
        if point:
            return point
    return None


def _build_job_geojson(parsed_json: dict) -> dict | None:
    coordinates: list[list[float]] = []
    seen: set[tuple[float, float]] = set()

    for location in parsed_json.get("locations", []) or []:
        point = _location_point(location)
        if not point:
            continue
        coord = (point["lng"], point["lat"])
        if coord in seen:
            continue
        seen.add(coord)
        coordinates.append([point["lng"], point["lat"]])

    if not coordinates:
        return None

    return {
        "type": "Feature",
        "geometry": {
            "type": "MultiPoint",
            "coordinates": coordinates,
        },
    }


def _build_job_geo_fields(parsed_json: dict, geo_lookup: dict[int, dict]) -> dict[str, list]:
    work_geoname_ids: list[int] = []
    work_country_codes: list[str] = []
    work_admin1_keys: list[str] = []
    applicant_country_codes: list[str] = []
    applicant_admin1_keys: list[str] = []

    for location in parsed_json.get("locations", []) or []:
        geoname_id = location.get("geoname_id")
        place = geo_lookup.get(geoname_id) if geoname_id else None
        country_code = (place or {}).get("country_code") or location.get("country_code")
        admin1_code = (place or {}).get("admin1_code")
        work_geoname_ids.append(geoname_id)
        work_country_codes.append(country_code)
        work_admin1_keys.append(_admin1_key(country_code, admin1_code))

    for requirement in parsed_json.get("applicant_location_requirements", []) or []:
        geoname_id = requirement.get("geoname_id")
        place = geo_lookup.get(geoname_id) if geoname_id else None
        scope = requirement.get("scope")
        country_code = (place or {}).get("country_code") or requirement.get("country_code")
        admin1_code = (place or {}).get("admin1_code")

        if scope == "country":
            applicant_country_codes.append(country_code)
        elif scope in {"state", "city"}:
            applicant_country_codes.append(country_code)
            applicant_admin1_keys.append(_admin1_key(country_code, admin1_code))

    return {
        "work_geoname_ids": _dedupe_nonempty(work_geoname_ids),
        "work_country_codes": _dedupe_nonempty(work_country_codes),
        "work_admin1_keys": _dedupe_nonempty(work_admin1_keys),
        "applicant_country_codes": _dedupe_nonempty(applicant_country_codes),
        "applicant_admin1_keys": _dedupe_nonempty(applicant_admin1_keys),
    }


def _build_meili_location(parsed_json: dict) -> str:
    labels = _build_meili_locations_all(parsed_json)
    if parsed_json.get("locations"):
        return labels[0] if labels else ""

    if parsed_json.get("office_type") == "remote":
        if labels:
            if len(labels) <= 2:
                return " • ".join(labels)
            return f"{' • '.join(labels[:2])} +{len(labels) - 2} more"

    return ""


def step_load(conn, meili_host: str = "http://localhost:7700", meili_key: str | None = None,
              parsed_job_ids: list[str] | None = None, removed_job_ids: list[str] | None = None,
              full_reload: bool = False):
    """Load parsed jobs from DB into MeiliSearch."""
    import meilisearch

    parsed_rows = get_parsed_jobs(conn, job_ids=None if full_reload else parsed_job_ids)
    removed_ids = get_removed_job_ids(conn, job_ids=None if full_reload else removed_job_ids)

    if not parsed_rows and not removed_ids:
        print("\n--- LOAD (nothing to update) ---")
        return

    print(f"\n--- LOAD ({len(parsed_rows)} active, {len(removed_ids)} removed) ---")
    fx_rates, fx_as_of_date = get_latest_fx_rates(conn)
    fx_as_of = fx_as_of_date.isoformat() if fx_as_of_date else None

    # Load company metadata for names, domains, logos
    company_lookup = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                ats,
                board_token,
                company_name,
                company_slug,
                domain,
                COALESCE(logo_url, scraped_logo_url) AS effective_logo_url
            FROM pipeline_companies
        """)
        for r in cur.fetchall():
            company_lookup[(r[0], r[1])] = {"name": r[2], "slug": r[3], "domain": r[4], "logo_url": r[5]}

    geoname_ids: set[int] = set()
    for row in parsed_rows:
        parsed = row.get("parsed_json") or {}
        for location in parsed.get("locations", []) or []:
            geoname_id = location.get("geoname_id")
            if isinstance(geoname_id, int):
                geoname_ids.add(geoname_id)
        for requirement in parsed.get("applicant_location_requirements", []) or []:
            geoname_id = requirement.get("geoname_id")
            if isinstance(geoname_id, int):
                geoname_ids.add(geoname_id)
    geo_lookup = _load_geo_place_lookup(conn, geoname_ids)

    # Count locations per job_group for "Also in N locations" display
    group_counts = {}
    for row in parsed_rows:
        g = row.get("job_group")
        if g:
            group_counts[g] = group_counts.get(g, 0) + 1

    boilerplate_cache: dict[str, set[str]] = {}

    # Build MeiliSearch documents
    docs = []
    for row in parsed_rows:
        m = row["parsed_json"]
        if not m:
            continue

        board = row["board_token"]
        co = company_lookup.get((row["ats"], board), {})
        company = co.get("name") or board.replace("-", " ").replace("_", " ").title()
        company_slug = co.get("slug") or board
        company_domain = co.get("domain", "")
        company_logo = co.get("logo_url", "") or _fallback_company_logo(company_domain, company_slug)

        # Location string from parsed metadata / remote applicant geography
        locs = m.get("locations", [])
        location_str = _build_meili_location(m)

        # Salary
        sal = m.get("salary")
        normalized_salary = normalize_salary_annual_usd(sal, fx_rates)

        # Geo
        geo = _build_primary_geo(m)
        geojson = _build_job_geojson(m)
        geo_fields = _build_job_geo_fields(m, geo_lookup)

        # Build description from raw_json — raw content only, no metadata injection
        raw = row.get("raw_json") or {}
        raw_desc = (
            raw.get("description", "")
            or raw.get("descriptionPlain", "")
            or ""
        )
        if not raw_desc and raw.get("content"):
            from utils.html_utils import remove_html_markup
            raw_desc = remove_html_markup(raw["content"], double_unescape=True)
        # Remove boilerplate
        if raw_desc:
            from detect_boilerplate import get_boilerplate_hashes, remove_boilerplate
            bp_hashes = boilerplate_cache.get(board)
            if bp_hashes is None:
                bp_hashes = get_boilerplate_hashes(conn, board)
                boilerplate_cache[board] = bp_hashes
            description = remove_boilerplate(raw_desc, bp_hashes)
        else:
            description = ""

        # String versions of arrays for embedding template
        skills_text = ', '.join(m.get("hard_skills", []) + m.get("soft_skills", []))
        vibes_text = ', '.join(v.replace('_', ' ') for v in m.get("vibe_tags", []))
        sal_text = ""
        if sal and sal.get("min"):
            sal_text = f"${sal['min']:,.0f}"
            if sal.get("max") and sal["max"] != sal["min"]:
                sal_text += f"-${sal['max']:,.0f}"
            sal_text += f" {sal.get('period', 'annually')}"

        primary_industry = m.get("industry_primary") or m.get("industry", "")
        secondary_industry_tags = m.get("industry_tags") or []
        all_industry_tags = []
        for value in [primary_industry, *secondary_industry_tags]:
            if isinstance(value, str) and value and value not in all_industry_tags:
                all_industry_tags.append(value)

        doc = {
            "meili_id": meili_safe_job_id(row["id"]),
            "id": row["id"],
            "public_job_id": row.get("public_job_id"),
            "title": row["title"],
            "tagline": m.get("tagline", ""),
            "company": company,
            "company_slug": company_slug,
            "company_domain": company_domain,
            "company_logo": company_logo,
            "description": description[:3000],
            "location": location_str,
            "locations_all": _build_meili_locations_all(m),
            **geo_fields,
            "office_type": m.get("office_type", ""),
            "job_type": m.get("job_type", ""),
            "experience_level": m.get("experience_level", ""),
            "is_manager": m.get("is_manager", False),
            "industry": primary_industry,
            "industry_tags": all_industry_tags,
            "salary_min": sal["min"] if sal else None,
            "salary_max": sal["max"] if sal else None,
            "salary_currency": sal["currency"] if sal else None,
            "salary_period": sal["period"] if sal else None,
            "salary_annual_min_usd": normalized_salary["salary_annual_min_usd"] if normalized_salary else None,
            "salary_annual_max_usd": normalized_salary["salary_annual_max_usd"] if normalized_salary else None,
            "salary_fx_currency": normalized_salary["salary_fx_currency"] if normalized_salary else None,
            "salary_fx_usd_per_unit": normalized_salary["salary_fx_usd_per_unit"] if normalized_salary else None,
            "salary_fx_as_of": fx_as_of,
            "salary_transparency": m.get("salary_transparency", "not_disclosed"),
            "hard_skills": m.get("hard_skills", []),
            "soft_skills": m.get("soft_skills", []),
            "cool_factor": m.get("cool_factor", "standard"),
            "vibe_tags": [v for v in m.get("vibe_tags", [])],
            "visa_sponsorship": m.get("visa_sponsorship", "unknown"),
            "equity_offered": m.get("equity", {}).get("offered", False),
            "company_stage": m.get("company_stage"),
            "benefits_categories": [b for b in m.get("benefits_categories", [])],
            "benefits_highlights": m.get("benefits_highlights", []),
            "reports_to": m.get("reports_to"),
            "ats_type": row["ats"],
            "job_group": row.get("job_group") or row["id"],  # ungrouped jobs use their own ID
            "location_count": group_counts.get(row.get("job_group"), 1),
        }
        if geo is not None:
            doc["_geo"] = geo
        if geojson is not None:
            doc["_geojson"] = geojson
        docs.append(doc)

    key = meili_key or os.environ.get("MEILISEARCH_MASTER_KEY", "")
    client = meilisearch.Client(meili_host, key)

    def configure_jobs_index(index):
        index.update_filterable_attributes([
        "office_type", "job_type", "experience_level", "is_manager",
        "industry", "industry_tags", "company_slug", "ats_type",
        "cool_factor", "vibe_tags", "visa_sponsorship", "equity_offered",
        "company_stage", "benefits_categories", "salary_transparency",
        "salary_min", "salary_max",
        "salary_annual_min_usd", "salary_annual_max_usd",
        "work_geoname_ids", "work_country_codes", "work_admin1_keys",
        "applicant_country_codes", "applicant_admin1_keys",
        "job_group", "location_count",
        "_geo", "_geojson",
        ])
        index.update_searchable_attributes([
        "title", "tagline", "company", "description", "location", "locations_all",
        "hard_skills", "soft_skills", "benefits_highlights",
        ])
        index.update_sortable_attributes([
        "salary_min", "salary_max",
        "salary_annual_min_usd", "salary_annual_max_usd",
        "_geo",
        ])
        settings = {"pagination": {"maxTotalHits": 500000}}
        embedders = _jobs_embedders_settings()
        if embedders:
            settings["embedders"] = embedders
        index.update_settings(settings)

    def wait_for_task(task_uid: int, timeout_in_ms: int = 60000) -> bool:
        try:
            result = client.wait_for_task(task_uid, timeout_in_ms=timeout_in_ms)
            status = getattr(result, "status", None)
            if status is None and isinstance(result, dict):
                status = result.get("status")
            return status == "succeeded" or status is None
        except Exception:
            return False

    target_index_uid = "jobs"
    jobs_index_exists = True
    try:
        jobs_index = client.get_index(target_index_uid)
        current_primary_key = jobs_index.get_primary_key()
    except Exception:
        jobs_index_exists = False
        current_primary_key = None

    recreate_index = full_reload and jobs_index_exists and current_primary_key not in {None, "meili_id"}
    active_index_uid = target_index_uid
    if recreate_index:
        active_index_uid = f"{target_index_uid}_rebuild_{uuid.uuid4().hex[:8]}"
        task = client.create_index(active_index_uid, {"primaryKey": "meili_id"})
        if not wait_for_task(task.task_uid):
            raise RuntimeError(f"Timed out creating temporary Meili index {active_index_uid}")
    elif not jobs_index_exists:
        task = client.create_index(target_index_uid, {"primaryKey": "meili_id"})
        if not wait_for_task(task.task_uid):
            raise RuntimeError("Timed out creating Meili jobs index")
    elif current_primary_key not in {None, "meili_id"}:
        raise RuntimeError(
            f"jobs index primary key is {current_primary_key!r}; run a full reload to migrate to 'meili_id'"
        )

    index = client.index(active_index_uid)
    configure_jobs_index(index)

    # Upsert documents in batches
    BATCH_SIZE = 1000
    if docs:
        for i in range(0, len(docs), BATCH_SIZE):
            batch = docs[i:i + BATCH_SIZE]
            task = index.add_documents(batch, primary_key="meili_id")
            print(f"  Upserting batch {i//BATCH_SIZE + 1} ({len(batch)} docs)... (task {task.task_uid})")
            if wait_for_task(task.task_uid):
                mark_jobs_meili_loaded(conn, [doc["id"] for doc in batch])
            else:
                print("  (waiting for index timed out, but task is queued)")

    # Delete removed jobs in batches
    if removed_ids and active_index_uid == target_index_uid:
        for i in range(0, len(removed_ids), BATCH_SIZE):
            batch = removed_ids[i:i + BATCH_SIZE]
            task = index.delete_documents(ids=[meili_safe_job_id(job_id) for job_id in batch])
            print(f"  Deleting batch ({len(batch)} removed jobs)...")
            if wait_for_task(task.task_uid, timeout_in_ms=30000):
                mark_jobs_meili_deleted(conn, batch)
            else:
                pass

    if recreate_index:
        swap_task = client.swap_indexes([{"indexes": [active_index_uid, target_index_uid]}])
        if not wait_for_task(swap_task.task_uid):
            raise RuntimeError("Timed out swapping Meili jobs indexes")
        delete_task = client.delete_index(active_index_uid)
        wait_for_task(delete_task.task_uid, timeout_in_ms=30000)
        mark_jobs_meili_loaded(conn, [row["id"] for row in parsed_rows])
        if removed_ids:
            mark_jobs_meili_deleted(conn, removed_ids)
        index = client.index(target_index_uid)

    stats = index.get_stats()
    print(f"  Index: {stats.number_of_documents} documents")


def main():
    parser = argparse.ArgumentParser(description="dopejobs pipeline")
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--companies", help="Companies file (ats:token per line)")
    source_group.add_argument("--companies-from-db", action="store_true",
                              help="Select companies from pipeline_companies instead of a file")
    parser.add_argument("--db-company-limit", type=int, default=None,
                        help="Required safety cap when using --companies-from-db")
    parser.add_argument("--skip-scrape", action="store_true")
    parser.add_argument("--skip-parse", action="store_true")
    parser.add_argument("--skip-load", action="store_true")
    parser.add_argument("--parse-pending", action="store_true", help="Only parse jobs with needs_parse=True")
    parser.add_argument("--base-url", default="https://api.openai.com/v1", help="LLM API base URL")
    parser.add_argument("--model", default="gemini-3.1-flash-lite-preview", help="LLM model name (gemini-* uses Gemini API, others use OpenAI API)")
    parser.add_argument("--max-per-company", type=int, default=None,
                        help="Optional max jobs fetched per company (default: no cap)")
    parser.add_argument("--parse-limit", type=int, default=None, help="Max jobs to parse per run")
    parser.add_argument("--allow-full-parse", action="store_true",
                        help="Allow parse step without a parse limit when using --companies-from-db")
    parser.add_argument("--meili-host", default=None, help="MeiliSearch host (default: MEILISEARCH_HOST env var or localhost)")
    parser.add_argument("--meili-key", default=None, help="MeiliSearch master key (default: MEILISEARCH_MASTER_KEY env var)")
    parser.add_argument("--full-load", action="store_true", help="Rebuild and upsert all parsed jobs into MeiliSearch")
    parser.add_argument("--allow-load", action="store_true",
                        help="Allow MeiliSearch load step when using --companies-from-db")
    parser.add_argument("--shard-index", type=int, default=None, help="0-based shard index for company selection")
    parser.add_argument("--total-shards", type=int, default=None, help="Total number of shards for company selection")
    parser.add_argument("--jobvite-refetch-existing-detail", action="store_true",
                        help="For Jobvite, backfill missing stored detail HTML/date metadata on existing jobs")
    args = parser.parse_args()

    if (args.shard_index is None) != (args.total_shards is None):
        parser.error("--shard-index and --total-shards must be provided together")
    if args.total_shards is not None and args.total_shards <= 0:
        parser.error("--total-shards must be greater than 0")
    if args.shard_index is not None and not (0 <= args.shard_index < args.total_shards):
        parser.error("--shard-index must be in [0, total_shards)")
    if args.companies_from_db and not args.skip_parse and args.parse_limit is None and not args.allow_full_parse:
        parser.error("--parse-limit is required with --companies-from-db unless --allow-full-parse is set")
    if args.companies_from_db and not args.skip_load and not args.allow_load:
        parser.error("--skip-load or --allow-load is required with --companies-from-db")

    conn = get_connection()
    init_schema(conn)

    try:
        companies = resolve_companies(
            conn,
            companies_path=args.companies,
            companies_from_db=args.companies_from_db,
            db_company_limit=args.db_company_limit,
        )
    except ValueError as e:
        parser.error(str(e))

    if args.companies_from_db:
        if args.db_company_limit is None:
            print(f"Using DB company selection: {len(companies)} companies (no company cap)")
        else:
            print(f"Using DB company selection: {len(companies)} companies (cap {args.db_company_limit})")
    else:
        print(f"Using file company selection: {len(companies)} companies from {args.companies}")

    selected_companies = filter_companies_for_shard(companies, args.shard_index, args.total_shards)
    if args.total_shards is not None:
        print(f"Using shard {args.shard_index}/{args.total_shards}: {len(selected_companies)} of {len(companies)} companies")

    scrape_result = {"touched_job_ids": set(), "removed_job_ids": set()}
    parsed_job_ids = []

    # Step 1: Scrape + detect changes
    if not args.skip_scrape:
        scrape_result = step_scrape(
            conn,
            selected_companies,
            max_per_company=args.max_per_company,
            jobvite_refetch_existing_detail=args.jobvite_refetch_existing_detail,
        )
        conn = scrape_result.get("conn", conn)
    else:
        print("Skipping scrape")

    if _connection_is_closed(conn):
        conn = _reconnect_pipeline_connection(conn)

    # Step 2: Parse new/changed jobs
    if not args.skip_parse:
        parsed_job_ids = step_parse(
            conn,
            args.base_url,
            args.model,
            limit=args.parse_limit,
            companies=selected_companies,
        )
    else:
        print("Skipping parse")

    # Step 3: Load to MeiliSearch
    if not args.skip_load:
        meili_host = args.meili_host or os.environ.get("MEILISEARCH_HOST", "http://localhost:7700")
        step_load(
            conn,
            meili_host=meili_host,
            meili_key=args.meili_key,
            parsed_job_ids=parsed_job_ids,
            removed_job_ids=list(scrape_result["removed_job_ids"]),
            full_reload=args.full_load,
        )
    else:
        print("Skipping load")

    conn.close()
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
