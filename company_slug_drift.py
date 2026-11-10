"""Find indexed job docs whose company_slug no longer matches Postgres, and repair them.

The loader writes `company_slug = pipeline_companies.company_slug or board_token` at load time, and the
web app resolves /companies/<slug> from Postgres, so a slug that moves after a load leaves search hits
linking to a 404. Two causes:

- reset: a failed scrape used to re-derive the slug from the board token alone (fixed in upsert_company).
  When the stored name/domain still derive the indexed slug, the Postgres slug is restored; no reload.
- moved: the company really changed (rename, backfill). Its drifted jobs are marked so the next
  load-pending re-sends them; that load also revalidates their `company:{slug}` cache tags.

usage: company_slug_drift.py [--dry-run]
"""
import argparse
import os
from collections import Counter, defaultdict

from dotenv import load_dotenv

from db import get_connection
from meili_reconcile import JOBS_INDEX, DOCUMENT_PAGE_SIZE, make_meili_client
from public_ids import derive_company_slug_map, slugify


def fetch_indexed_slugs(index) -> dict[str, str]:
    """Return {job id: company_slug} for every document in the index."""
    out: dict[str, str] = {}
    offset = 0
    while True:
        page = index.get_documents({"fields": ["id", "company_slug"], "limit": DOCUMENT_PAGE_SIZE, "offset": offset})
        for doc in page.results:
            values = dict(doc)
            if values.get("id"):
                out[values["id"]] = values.get("company_slug") or ""
        offset += len(page.results)
        if not page.results or offset >= page.total:
            return out


def plan_drift(indexed: dict[str, str], job_companies: dict[str, tuple[str, str]], companies: dict) -> dict:
    """indexed: {job id: indexed slug}; job_companies: {job id: (ats, board)};
    companies: {(ats, board): {"slug", "name", "domain"}}."""
    by_company: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    drifted: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for job_id, indexed_slug in indexed.items():
        key = job_companies.get(job_id)
        if key is None:
            continue  # unknown or removed in Postgres: reconcile-meili's job
        by_company[key].append((job_id, indexed_slug))
        co = companies.get(key) or {}
        if indexed_slug != (co.get("slug") or key[1]):
            drifted[(key[0], key[1], indexed_slug)].append(job_id)
    restore: dict[tuple[str, str], str] = {}
    for ats, board, indexed_slug in drifted:
        co = companies.get((ats, board)) or {}
        derived = derive_company_slug_map([{"ats": ats, "board_token": board,
                                            "company_name": co.get("name"), "domain": co.get("domain")}])[(ats, board)]
        if co.get("slug") == slugify(board) and derived != co.get("slug") and derived == indexed_slug:
            restore[(ats, board)] = derived
    # Reload every doc that won't match Postgres afterwards (for restored companies, that includes
    # docs loaded while the slug was reset).
    reload_ids = [job_id for key, docs in by_company.items() for job_id, indexed_slug in docs
                  if indexed_slug != (restore.get(key) or (companies.get(key) or {}).get("slug") or key[1])]
    return {"drifted": dict(drifted), "restore": restore, "reload_ids": sorted(reload_ids)}


def _load_postgres(conn):
    companies = {}
    with conn.cursor() as cur:
        cur.execute("SELECT ats, board_token, company_slug, company_name, domain FROM pipeline_companies")
        for ats, board, slug, name, domain in cur.fetchall():
            companies[(ats, board)] = {"slug": slug, "name": name, "domain": domain}
    job_companies = {}
    with conn.cursor(name="slug_drift_jobs") as cur:
        cur.itersize = 50000
        cur.execute("SELECT id, ats, board_token FROM pipeline_jobs WHERE removed_at IS NULL")
        for job_id, ats, board in cur:
            job_companies[job_id] = (ats, board)
    conn.commit()
    return companies, job_companies


def apply_plan(conn, plan: dict) -> None:
    with conn.cursor() as cur:
        for (ats, board), slug in plan["restore"].items():
            cur.execute("UPDATE pipeline_companies SET company_slug = %s WHERE ats = %s AND board_token = %s",
                        (slug, ats, board))
        if plan["reload_ids"]:
            cur.execute("UPDATE pipeline_jobs SET meili_loaded_doc_version = NULL WHERE id = ANY(%s)",
                        (plan["reload_ids"],))
    conn.commit()


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--dry-run", action="store_true", help="Report without changing anything")
    args = parser.parse_args()

    client = make_meili_client(os.environ.get("MEILISEARCH_HOST", "http://localhost:7700"))
    indexed = fetch_indexed_slugs(client.index(JOBS_INDEX))
    conn = get_connection()
    try:
        companies, job_companies = _load_postgres(conn)
        plan = plan_drift(indexed, job_companies, companies)
        drifted = plan["drifted"]
        n_docs = sum(len(v) for v in drifted.values())
        print(f"Indexed docs: {len(indexed)}; companies with indexed docs: "
              f"{len({job_companies[j] for j in indexed if j in job_companies})}")
        print(f"Drifted: {n_docs} docs across {len({(a, b) for a, b, _ in drifted})} companies")
        print(f"  reset by a failed scrape (restore the Postgres slug, no reload): {len(plan['restore'])} companies, "
              f"{sum(len(v) for (a, b, _), v in drifted.items() if (a, b) in plan['restore'])} docs")
        print(f"  docs to reload (moved, or loaded while a slug was reset): {len(plan['reload_ids'])}")
        resets_all = sum(
            1 for (ats, board), co in companies.items()
            if co["slug"] == slugify(board) and derive_company_slug_map([{
                "ats": ats, "board_token": board, "company_name": co["name"], "domain": co["domain"]}])[(ats, board)] != co["slug"]
        )
        print(f"All companies whose Postgres slug is the board token although name/domain derive another: {resets_all}")
        dupes = Counter(co["slug"] for co in companies.values() if co["slug"])
        print(f"Postgres slugs shared by more than one company row: {sum(1 for c in dupes.values() if c > 1)}")
        for (ats, board, indexed_slug), job_ids in sorted(drifted.items(), key=lambda kv: -len(kv[1]))[:40]:
            co = companies.get((ats, board)) or {}
            action = "restore" if (ats, board) in plan["restore"] else "reload"
            print(f"  {len(job_ids):6} {ats}:{board} indexed={indexed_slug} postgres={co.get('slug')} "
                  f"name={co.get('name')!r} domain={co.get('domain')!r} -> {action}")
        if args.dry_run:
            print("Dry run: no changes made")
            return 0
        apply_plan(conn, plan)
        print(f"Restored {len(plan['restore'])} company slugs; marked {len(plan['reload_ids'])} jobs for reload "
              "(the next load-pending re-sends them)")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
