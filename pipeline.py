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
from datetime import datetime, timezone
from urllib.parse import urlparse

from dotenv import load_dotenv
load_dotenv()
from psycopg2.extras import Json

from scrapers.greenhouse_scraper import GreenhouseScraper
from scrapers.lever_scraper import LeverScraper
from scrapers.ashby_scraper import AshbyScraper
from scrapers.jobvite_scraper import JobviteScraper
from scrapers.workable_scraper import WorkableScraper
from db import (
    get_connection, init_schema, upsert_scraped_jobs, mark_removed,
    job_id, upsert_company, get_jobs_needing_parse, save_parsed_result,
    record_parse_error, get_active_jobs_for_meili, get_removed_job_ids, get_companies_to_scrape,
    get_companies_to_scrape_by_status,
    get_existing_jobs_for_board, get_latest_fx_rates, mark_jobs_meili_deleted,
    mark_jobs_meili_loaded, get_job_ids_pending_meili_load,
)
from salary_normalization import normalize_salary_annual_usd
from public_ids import meili_safe_job_id
from job_groups import recompute_job_groups_for_boards


ATS_SCRAPERS = {
    "greenhouse": GreenhouseScraper,
    "lever": LeverScraper,
    "ashby": AshbyScraper,
    "jobvite": JobviteScraper,
    "workable": WorkableScraper,
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
                      db_company_limit: int | None = None,
                      ats_filter: list[str] | None = None,
                      ats_exclude_filter: list[str] | None = None,
                      scrape_status_filter: list[str] | None = None) -> list[tuple[str, str]]:
    """Resolve companies from either a file or a bounded DB query."""
    if companies_from_db:
        if db_company_limit is not None and db_company_limit <= 0:
            raise ValueError("--db-company-limit must be greater than 0")
        limit = db_company_limit if db_company_limit is not None else 10_000_000
        if scrape_status_filter:
            return get_companies_to_scrape_by_status(
                conn,
                limit=limit,
                ats_filter=ats_filter,
                ats_exclude_filter=ats_exclude_filter,
                scrape_statuses=scrape_status_filter,
            )
        return get_companies_to_scrape(
            conn,
            limit=limit,
            ats_filter=ats_filter,
            ats_exclude_filter=ats_exclude_filter,
        )

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
    successful_boards: set[tuple[str, str]] = set()

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

            try:
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
            except Exception as scrape_error:
                http_status = getattr(scrape_error, "status_code", None)
                scrape_status = "blocked" if getattr(scrape_error, "blocked", False) else "error"
                upsert_company(
                    conn,
                    ats,
                    token,
                    job_count=0,
                    job_count_exact=False,
                    scrape_status=scrape_status,
                    last_scrape_error=str(scrape_error),
                    last_http_status=http_status,
                )
                print(f"  [{idx}/{total_companies}] {ats}:{token} — {scrape_status}: {scrape_error}")
                errors += 1
                continue

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
            company_description = None
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
                company_description = scraper.get_company_description()
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
                description=company_description,
                scraped_logo_url=company_logo,
                job_count=len(jobs),
                job_count_exact=complete_scrape,
                scrape_status="active" if jobs else "empty",
                last_scrape_error=None,
                last_http_status=200,
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
            successful_boards.add((ats, token))

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

    job_group_changed_job_ids: set[str] = set()
    job_group_stats = {"groups": 0, "grouped_jobs": 0, "singletons": 0}
    if successful_boards:
        changed_ids, job_group_stats = recompute_job_groups_for_boards(conn, list(successful_boards))
        job_group_changed_job_ids = set(changed_ids)
        if changed_ids:
            print(
                "Job groups refreshed: "
                f"{job_group_stats['groups']} groups, "
                f"{job_group_stats['grouped_jobs']} grouped jobs, "
                f"{len(changed_ids)} changed rows"
            )
        else:
            print("Job groups refreshed: no changes")

    print(f"\nScrape complete: {total_new} new, {total_changed} changed, "
          f"{total_unchanged} unchanged, {total_removed} removed, {errors} errors")
    return {
        "touched_job_ids": touched_job_ids,
        "removed_job_ids": removed_job_ids,
        "job_group_changed_job_ids": job_group_changed_job_ids,
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


def _normalize_country_code_for_filters(value) -> str | None:
    if value in (None, ""):
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    if len(cleaned) == 2:
        return cleaned.upper()

    try:
        from parse import _country_code_from_value
    except Exception:
        return None

    normalized = _country_code_from_value(cleaned)
    if isinstance(normalized, str) and len(normalized) == 2:
        return normalized
    return None


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
        country_code = _normalize_country_code_for_filters(
            (place or {}).get("country_code") or location.get("country_code")
        )
        admin1_code = (place or {}).get("admin1_code")
        work_geoname_ids.append(geoname_id)
        work_country_codes.append(country_code)
        work_admin1_keys.append(_admin1_key(country_code, admin1_code))

    for requirement in parsed_json.get("applicant_location_requirements", []) or []:
        geoname_id = requirement.get("geoname_id")
        place = geo_lookup.get(geoname_id) if geoname_id else None
        scope = requirement.get("scope")
        country_code = _normalize_country_code_for_filters(
            (place or {}).get("country_code") or requirement.get("country_code")
        )
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


def _extract_apply_url(raw_json: dict) -> str:
    if not isinstance(raw_json, dict):
        return ""

    candidates = (
        "applyUrl",
        "apply_url",
        "url",
        "hostedUrl",
        "jobUrl",
        "absolute_url",
    )
    for key in candidates:
        value = raw_json.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _extract_posted_at(raw_json: dict) -> tuple[str | None, int | None]:
    if not isinstance(raw_json, dict):
        return None, None

    for key in ("publishedAt", "first_published", "datePosted", "createdAt"):
        value = raw_json.get(key)
        if value in (None, ""):
            continue

        dt: datetime | None = None
        if isinstance(value, (int, float)):
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp /= 1000.0
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        elif isinstance(value, str):
            candidate = value.strip()
            if not candidate:
                continue

            if candidate.isdigit():
                timestamp = float(candidate)
                if timestamp > 10_000_000_000:
                    timestamp /= 1000.0
                dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            else:
                normalized = candidate.replace("Z", "+00:00")
                try:
                    dt = datetime.fromisoformat(normalized)
                except ValueError:
                    continue
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)

        if dt is not None:
            return dt.isoformat(), int(dt.timestamp())

    return None, None


def _build_years_experience_buckets(parsed_json: dict) -> list[str]:
    years_experience = parsed_json.get("years_experience") or {}
    min_years = years_experience.get("min")
    max_years = years_experience.get("max")

    if not isinstance(min_years, int):
        min_years = None
    if not isinstance(max_years, int):
        max_years = None

    buckets: list[str] = []

    def overlaps(bucket_min: int, bucket_max: int) -> bool:
        min_ok = min_years is None or min_years <= bucket_max
        max_ok = max_years is None or max_years >= bucket_min
        return min_ok and max_ok

    if overlaps(0, 2):
        buckets.append("0_2")
    if overlaps(3, 5):
        buckets.append("3_5")
    if overlaps(6, 9):
        buckets.append("6_9")
    if (max_years is None and min_years is not None and min_years >= 10) or (
        max_years is not None and max_years >= 10
    ):
        buckets.append("10_plus")

    return buckets



def _build_docs_for_chunk(rows, merge_api_data, geo_resolver, company_lookup,
                          fx_rates, fx_as_of, geo_lookup, group_counts,
                          boilerplate_cache, conn) -> list[dict]:
    """Build Meili docs for a chunk of DB rows, resolving metadata and geo."""
    docs = []
    # Collect geoname_ids for this chunk, then extend geo_lookup
    chunk_geoname_ids: set[int] = set()
    metadata_by_id: dict[str, dict] = {}
    for row in rows:
        parsed = row.get("parsed_json")
        if parsed:
            metadata_by_id[row["id"]] = parsed
        else:
            metadata_by_id[row["id"]] = geo_resolver.resolve_parsed_geo(
                merge_api_data(row.get("raw_json") or {}, {})
            )
        m = metadata_by_id[row["id"]]
        for loc in m.get("locations", []) or []:
            gid = loc.get("geoname_id")
            if isinstance(gid, int):
                chunk_geoname_ids.add(gid)
        for req in m.get("applicant_location_requirements", []) or []:
            gid = req.get("geoname_id")
            if isinstance(gid, int):
                chunk_geoname_ids.add(gid)

    # Load any new geoname_ids not already in the lookup
    missing_geonames = chunk_geoname_ids - set(geo_lookup.keys())
    if missing_geonames:
        geo_lookup.update(_load_geo_place_lookup(conn, missing_geonames))

    for row in rows:
        doc = _build_meili_doc(row, metadata_by_id[row["id"]], company_lookup, fx_rates,
                               fx_as_of, geo_lookup, group_counts, boilerplate_cache, conn)
        docs.append(doc)
    return docs


def _build_meili_doc(row: dict, metadata: dict, company_lookup: dict, fx_rates: dict,
                     fx_as_of: str | None, geo_lookup: dict, group_counts: dict,
                     boilerplate_cache: dict, conn) -> dict:
    """Build a single MeiliSearch document from a DB row and its metadata."""
    raw = row.get("raw_json") or {}
    is_enriched = bool(row.get("parsed_json"))
    m = metadata

    board = row["board_token"]
    co = company_lookup.get((row["ats"], board), {})
    company = co.get("name") or board.replace("-", " ").replace("_", " ").title()
    company_slug = co.get("slug") or board
    company_domain = co.get("domain", "")
    company_logo = co.get("logo_url", "") or _fallback_company_logo(company_domain, company_slug)

    location_str = _build_meili_location(m)
    sal = m.get("salary")
    normalized_salary = normalize_salary_annual_usd(sal, fx_rates)
    geo = _build_primary_geo(m)
    geojson = _build_job_geojson(m)
    geo_fields = _build_job_geo_fields(m, geo_lookup)

    raw_desc = raw.get("description", "") or raw.get("descriptionPlain", "") or ""
    if not raw_desc and raw.get("content"):
        from utils.html_utils import remove_html_markup
        raw_desc = remove_html_markup(raw["content"], double_unescape=True)
    if raw_desc:
        from detect_boilerplate import get_boilerplate_hashes, remove_boilerplate
        bp_hashes = boilerplate_cache.get(board)
        if bp_hashes is None:
            bp_hashes = get_boilerplate_hashes(conn, board)
            boilerplate_cache[board] = bp_hashes
        description = remove_boilerplate(raw_desc, bp_hashes)
    else:
        description = ""

    apply_url = _extract_apply_url(raw)
    posted_at, posted_at_ts = _extract_posted_at(raw)
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
    years_experience = m.get("years_experience") or {}
    years_experience_buckets = _build_years_experience_buckets(m)

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
        "apply_url": apply_url,
        "posted_at": posted_at,
        "posted_at_ts": posted_at_ts,
        "description": description[:3000],
        "location": location_str,
        "locations_all": _build_meili_locations_all(m),
        **geo_fields,
        "office_type": m.get("office_type", ""),
        "job_type": m.get("job_type", ""),
        "experience_level": m.get("experience_level", ""),
        "is_manager": m.get("is_manager", False),
        "is_enriched": is_enriched,
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
        "years_experience_min": years_experience.get("min"),
        "years_experience_max": years_experience.get("max"),
        "years_experience_buckets": years_experience_buckets,
        "education_level": m.get("education_level"),
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
        "job_group": row.get("job_group") or row["id"],
        "location_count": group_counts.get(row.get("job_group"), 1),
    }
    if geo is not None:
        doc["_geo"] = geo
    if geojson is not None:
        doc["_geojson"] = geojson
    return doc


def step_load(conn, meili_host: str = "http://localhost:7700", meili_key: str | None = None,
              parsed_job_ids: list[str] | None = None, removed_job_ids: list[str] | None = None,
              full_reload: bool = False, meili_batch_size: int = 1000):
    """Load active jobs from DB into MeiliSearch, using ATS-only metadata when needed.

    Processes jobs in chunks to avoid loading all raw_json blobs into memory at once.
    """
    import meilisearch
    from parse import merge_api_data
    from geo_resolver import GeoResolver

    # Determine which job IDs to load (lightweight — no raw_json)
    if full_reload:
        # Get all active job IDs (just the IDs, no blobs)
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM pipeline_jobs WHERE raw_json IS NOT NULL AND removed_at IS NULL ORDER BY id")
            all_job_ids = [r[0] for r in cur.fetchall()]
    elif parsed_job_ids is not None:
        all_job_ids = parsed_job_ids
    else:
        all_job_ids = []

    removed_ids = get_removed_job_ids(conn, job_ids=None if full_reload else removed_job_ids)

    if not all_job_ids and not removed_ids:
        print("\n--- LOAD (nothing to update) ---")
        return

    total_active = len(all_job_ids)

    print(f"\n--- LOAD ({total_active} active, {len(removed_ids)} removed) ---")

    # Load shared lookups (small, load once)
    fx_rates, fx_as_of_date = get_latest_fx_rates(conn)
    fx_as_of = fx_as_of_date.isoformat() if fx_as_of_date else None
    geo_resolver = GeoResolver(conn)

    company_lookup = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ats, board_token, company_name, company_slug, domain,
                   COALESCE(logo_url, scraped_logo_url) AS effective_logo_url
            FROM pipeline_companies
        """)
        for r in cur.fetchall():
            company_lookup[(r[0], r[1])] = {"name": r[2], "slug": r[3], "domain": r[4], "logo_url": r[5]}

    # Load job_group counts (lightweight query, no raw_json)
    group_counts = {}
    if all_job_ids:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT job_group, COUNT(*) FROM pipeline_jobs
                WHERE removed_at IS NULL AND job_group IS NOT NULL AND id = ANY(%s)
                GROUP BY job_group
            """, (all_job_ids,))
            for r in cur.fetchall():
                group_counts[r[0]] = r[1]

    # Set up Meili client
    key = meili_key or os.environ.get("MEILISEARCH_MASTER_KEY", "")
    custom_headers = {}
    cf_client_id = os.environ.get("CF_ACCESS_CLIENT_ID")
    cf_client_secret = os.environ.get("CF_ACCESS_CLIENT_SECRET")
    if cf_client_id:
        custom_headers["CF-Access-Client-Id"] = cf_client_id
    if cf_client_secret:
        custom_headers["CF-Access-Client-Secret"] = cf_client_secret
    client = meilisearch.Client(meili_host, key, custom_headers=custom_headers or None)

    filterable_attributes = [
        "id", "public_job_id",
        "office_type", "job_type", "experience_level", "is_manager", "is_enriched",
        "industry", "industry_tags", "company_slug", "ats_type",
        "cool_factor", "vibe_tags", "visa_sponsorship", "equity_offered",
        "company_stage", "benefits_categories", "salary_transparency",
        "salary_min", "salary_max",
        "salary_annual_min_usd", "salary_annual_max_usd",
        "posted_at_ts",
        "years_experience_min", "years_experience_max", "years_experience_buckets", "education_level",
        "work_geoname_ids", "work_country_codes", "work_admin1_keys",
        "applicant_country_codes", "applicant_admin1_keys",
        "job_group", "location_count",
        "_geo", "_geojson",
    ]
    searchable_attributes = [
        "title", "tagline", "company", "description", "location", "locations_all",
        "hard_skills", "soft_skills", "benefits_highlights",
    ]
    sortable_attributes = [
        "salary_min", "salary_max",
        "salary_annual_min_usd", "salary_annual_max_usd",
        "posted_at_ts",
        "_geo",
    ]

    def configure_jobs_index(index):
        index.update_filterable_attributes(filterable_attributes)
        index.update_searchable_attributes(searchable_attributes)
        index.update_sortable_attributes(sortable_attributes)
        # Embedder settings are managed externally via apply_perplexity_meili_embedder.py
        # — don't touch them here to avoid clobbering the composite Perplexity config.
        settings = {"pagination": {"maxTotalHits": 500000}}
        index.update_settings(settings)

    def wait_for_task(task_uid: int, timeout_in_ms: int = 300000) -> bool:
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
    should_configure_index = recreate_index or not jobs_index_exists or full_reload
    if should_configure_index:
        configure_jobs_index(index)
    else:
        print("  Skipping Meili index settings refresh for incremental load")

    # Upsert documents in batches
    def _add_documents_with_retry(batch: list[dict], primary_key: str = "meili_id",
                                 retries: int = 3, backoff_seconds: float = 1.5) -> object:
        last_error = None
        for attempt in range(retries):
            try:
                return index.add_documents(batch, primary_key=primary_key)
            except Exception as exc:  # network hiccup handling for shared hosting / transient failures
                last_error = exc
                if attempt == retries - 1:
                    break
                sleep_seconds = backoff_seconds * (attempt + 1)
                print(f"  Warning: failed to start add_documents for batch ({len(batch)} docs): {type(exc).__name__}; retrying in {sleep_seconds:.1f}s")
                time.sleep(sleep_seconds)
        raise RuntimeError(f"Failed to submit documents for batch of {len(batch)} docs") from last_error

    # Process active jobs in chunks: fetch from DB → build docs → submit to Meili
    BATCH_SIZE = max(1, int(meili_batch_size))
    boilerplate_cache: dict[str, set[str]] = {}
    geo_lookup: dict = {}
    total_loaded = 0

    for i in range(0, len(all_job_ids), BATCH_SIZE):
        chunk_ids = all_job_ids[i:i + BATCH_SIZE]
        chunk_rows = get_active_jobs_for_meili(conn, job_ids=chunk_ids)
        if not chunk_rows:
            continue
        docs = _build_docs_for_chunk(chunk_rows, merge_api_data, geo_resolver, company_lookup,
                                     fx_rates, fx_as_of, geo_lookup, group_counts, boilerplate_cache, conn)
        if docs:
            task = _add_documents_with_retry(docs)
            batch_num = i // BATCH_SIZE + 1
            print(f"  Upserting batch {batch_num} ({len(docs)} docs)... (task {task.task_uid})")
            if wait_for_task(task.task_uid):
                mark_jobs_meili_loaded(conn, [doc["id"] for doc in docs])
                total_loaded += len(docs)
            else:
                raise RuntimeError(
                    f"Timed out waiting for Meili task {task.task_uid}; "
                    "stopping further document submissions to avoid overloading the queue"
                )

    print(f"  Loaded {total_loaded} documents")

    # Delete removed jobs in batches (larger than upsert batches since no embeddings needed)
    DELETE_BATCH_SIZE = max(BATCH_SIZE, 2000)
    total_deleted = 0
    if removed_ids and active_index_uid == target_index_uid:
        for i in range(0, len(removed_ids), DELETE_BATCH_SIZE):
            batch = removed_ids[i:i + DELETE_BATCH_SIZE]
            task = index.delete_documents(ids=[meili_safe_job_id(job_id) for job_id in batch])
            batch_num = i // DELETE_BATCH_SIZE + 1
            total_batches = (len(removed_ids) + DELETE_BATCH_SIZE - 1) // DELETE_BATCH_SIZE
            print(f"  Deleting batch {batch_num}/{total_batches} ({len(batch)} removed jobs)... (task {task.task_uid})")
            if wait_for_task(task.task_uid, timeout_in_ms=120000):
                mark_jobs_meili_deleted(conn, batch)
                total_deleted += len(batch)
            else:
                print(f"  Warning: timed out on delete task {task.task_uid}, stopping deletes ({total_deleted} deleted so far)")
                break
        print(f"  Deleted {total_deleted}/{len(removed_ids)} removed jobs")

    if recreate_index:
        swap_task = client.swap_indexes([{"indexes": [active_index_uid, target_index_uid]}])
        if not wait_for_task(swap_task.task_uid):
            raise RuntimeError("Timed out swapping Meili jobs indexes")
        delete_task = client.delete_index(active_index_uid)
        wait_for_task(delete_task.task_uid, timeout_in_ms=120000)
        # For full reload, all active jobs were already marked in the chunked loop above
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
    source_group.add_argument("--load-pending", action="store_true",
                              help="Standalone load: push all stale/removed jobs to MeiliSearch (no scrape or parse)")
    parser.add_argument("--db-company-limit", type=int, default=None,
                        help="Required safety cap when using --companies-from-db")
    parser.add_argument("--ats-filter", nargs="+", default=None,
                        help="Restrict DB company selection to one or more ATS names")
    parser.add_argument("--ats-exclude-filter", nargs="+", default=None,
                        help="Exclude one or more ATS names from DB company selection")
    parser.add_argument("--scrape-status-filter", nargs="+", default=None,
                        help="Restrict DB company selection to one or more scrape statuses")
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
    parser.add_argument("--load-limit", type=int, default=None,
                        help="Max number of jobs to load into MeiliSearch (for testing)")
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

    # --load-pending is a standalone mode: skip scrape/parse, go straight to load
    if args.load_pending:
        args.skip_scrape = True
        args.skip_parse = True

    conn = get_connection()
    init_schema(conn)

    selected_companies = []
    if not args.load_pending:
        try:
            companies = resolve_companies(
                conn,
                companies_path=args.companies,
                companies_from_db=args.companies_from_db,
                db_company_limit=args.db_company_limit,
                ats_filter=args.ats_filter,
                ats_exclude_filter=args.ats_exclude_filter,
                scrape_status_filter=args.scrape_status_filter,
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

    scrape_result = {"touched_job_ids": set(), "removed_job_ids": set(), "job_group_changed_job_ids": set()}
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
        if args.load_pending:
            # Standalone load: query DB for all jobs needing a Meili refresh
            jobs_to_load = get_job_ids_pending_meili_load(conn, limit=args.load_limit)
            # Only delete removed jobs that were previously loaded into Meili
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM pipeline_jobs
                    WHERE removed_at IS NOT NULL AND meili_loaded_at IS NOT NULL
                """)
                removed_to_load = [r[0] for r in cur.fetchall()]
            print(f"\n--- LOAD-PENDING: {len(jobs_to_load)} stale, {len(removed_to_load)} removed ---")
        else:
            jobs_to_load = list(set(parsed_job_ids) | set(scrape_result.get("job_group_changed_job_ids", set())))
            removed_to_load = list(scrape_result["removed_job_ids"])
        step_load(
            conn,
            meili_host=meili_host,
            meili_key=args.meili_key,
            parsed_job_ids=jobs_to_load,
            removed_job_ids=removed_to_load,
            full_reload=args.full_load,
        )
    else:
        print("Skipping load")

    conn.close()
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
