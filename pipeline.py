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
import os
import time
import sys
from collections import defaultdict

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
    get_existing_jobs_for_board,
)


ATS_SCRAPERS = {
    "greenhouse": GreenhouseScraper,
    "lever": LeverScraper,
    "ashby": AshbyScraper,
    "jobvite": JobviteScraper,
}


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
                existing_descriptions = {
                    short_id: raw.get("description", "")
                    for short_id, raw in existing_jobs_by_short_id.items()
                    if raw.get("description")
                }
                job_iter = scraper.fetch_jobs(
                    existing_descriptions=existing_descriptions,
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
                logo_url=company_logo,
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
    else:
        key = api_key or os.environ.get("OPENAI_API_KEY", "not-needed")
        backend = OpenAIBackend(base_url, model, api_key=key)

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
                save_parsed_result(conn, jid, parsed)
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

    # Load company metadata for names, domains, logos
    company_lookup = {}
    with conn.cursor() as cur:
        cur.execute("SELECT ats, board_token, company_name, company_slug, domain, logo_url FROM pipeline_companies")
        for r in cur.fetchall():
            company_lookup[(r[0], r[1])] = {"name": r[2], "slug": r[3], "domain": r[4], "logo_url": r[5]}

    # Count locations per job_group for "Also in N locations" display
    group_counts = {}
    for row in parsed_rows:
        g = row.get("job_group")
        if g:
            group_counts[g] = group_counts.get(g, 0) + 1

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
        company_logo = co.get("logo_url", "")

        # Location string from parsed metadata
        locs = m.get("locations", [])
        location_str = ""
        if locs:
            loc = locs[0]
            if loc.get("label"):
                location_str = loc["label"]
            else:
                loc_parts = []
                if loc.get("city"): loc_parts.append(loc["city"])
                if loc.get("state"): loc_parts.append(loc["state"])
                if loc.get("country_code"): loc_parts.append(loc["country_code"])
                location_str = ", ".join(loc_parts)

        # Salary
        sal = m.get("salary")

        # Geo
        geo = None
        if locs and locs[0].get("lat") and locs[0].get("lng"):
            geo = {"lat": locs[0]["lat"], "lng": locs[0]["lng"]}

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
            bp_hashes = get_boilerplate_hashes(conn, board)
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

        docs.append({
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
            "_geo": geo,
            "office_type": m.get("office_type", ""),
            "job_type": m.get("job_type", ""),
            "experience_level": m.get("experience_level", ""),
            "is_manager": m.get("is_manager", False),
            "industry": m.get("industry", ""),
            "salary_min": sal["min"] if sal else None,
            "salary_max": sal["max"] if sal else None,
            "salary_currency": sal["currency"] if sal else None,
            "salary_period": sal["period"] if sal else None,
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
        })

    key = meili_key or os.environ.get("MEILISEARCH_MASTER_KEY", "")
    client = meilisearch.Client(meili_host, key)
    index = client.index("jobs")

    # Configure index settings (idempotent)
    index.update_filterable_attributes([
        "office_type", "job_type", "experience_level", "is_manager",
        "industry", "company_slug", "ats_type",
        "cool_factor", "vibe_tags", "visa_sponsorship", "equity_offered",
        "company_stage", "benefits_categories", "salary_transparency",
        "salary_min", "salary_max",
        "job_group", "location_count",
    ])
    index.update_searchable_attributes([
        "title", "tagline", "company", "description", "location",
        "hard_skills", "soft_skills", "benefits_highlights",
    ])
    index.update_sortable_attributes(["salary_min", "salary_max"])
    index.update_settings({"pagination": {"maxTotalHits": 500000}})

    # Upsert documents in batches
    BATCH_SIZE = 1000
    if docs:
        for i in range(0, len(docs), BATCH_SIZE):
            batch = docs[i:i + BATCH_SIZE]
            task = index.add_documents(batch, primary_key="id")
            print(f"  Upserting batch {i//BATCH_SIZE + 1} ({len(batch)} docs)... (task {task.task_uid})")
            try:
                client.wait_for_task(task.task_uid, timeout_in_ms=60000)
            except Exception:
                print("  (waiting for index timed out, but task is queued)")

    # Delete removed jobs in batches
    if removed_ids:
        for i in range(0, len(removed_ids), BATCH_SIZE):
            batch = removed_ids[i:i + BATCH_SIZE]
            task = index.delete_documents(ids=batch)
            print(f"  Deleting batch ({len(batch)} removed jobs)...")
            try:
                client.wait_for_task(task.task_uid, timeout_in_ms=30000)
            except Exception:
                pass

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
                        help="Force Jobvite detail HTML refetch for existing jobs")
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
    else:
        print("Skipping scrape")

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
