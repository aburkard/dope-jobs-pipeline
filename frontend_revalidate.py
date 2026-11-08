"""Invalidate cached dopejobs.xyz pages for jobs the load step changed.

The frontend caches job and company pages with per-entity tags. After the load
step deletes removed jobs or upserts changed ones, call the frontend's
revalidation endpoint so those pages refresh instead of waiting for the
backstop revalidate window. Failures only warn: the backstop still applies.
"""
import json
import os
import urllib.request

TAG_BATCH_SIZE = 500
SITEMAP_TAG = "sitemaps"


def build_revalidation_tags(conn, job_ids: list[str]) -> list[str]:
    """Return job and company cache tags for the given canonical job ids."""
    if not job_ids:
        return []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT j.public_job_id, COALESCE(c.company_slug, c.board_token, j.board_token)
            FROM pipeline_jobs j
            LEFT JOIN pipeline_companies c
              ON c.ats = j.ats AND c.board_token = j.board_token
            WHERE j.id = ANY(%s)
        """, (job_ids,))
        rows = cur.fetchall()
    conn.commit()
    tags: set[str] = set()
    for public_job_id, company_slug in rows:
        if public_job_id:
            tags.add(f"job:{public_job_id}")
        if company_slug:
            tags.add(f"company:{company_slug}")
    return sorted(tags)


def post_revalidation(tags: list[str], url: str | None = None, secret: str | None = None,
                      timeout_seconds: float = 30) -> int:
    """POST tags to the frontend in batches. Returns the number of tags accepted."""
    url = url or os.environ.get("REVALIDATE_URL")
    secret = secret or os.environ.get("REVALIDATE_SECRET")
    if not url or not secret:
        print("  Revalidation skipped: REVALIDATE_URL/REVALIDATE_SECRET not set")
        return 0
    accepted = 0
    for start in range(0, len(tags), TAG_BATCH_SIZE):
        batch = tags[start:start + TAG_BATCH_SIZE]
        request = urllib.request.Request(
            url,
            data=json.dumps({"tags": batch}).encode("utf-8"),
            headers={"Authorization": f"Bearer {secret}", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                accepted += int(json.load(response).get("revalidated", len(batch)))
        except Exception as exc:
            print(f"  Warning: revalidation batch failed ({type(exc).__name__}: {exc}); backstop window applies")
            break
    print(f"  Revalidated {accepted}/{len(tags)} cache tags")
    return accepted


def revalidate_changed_jobs(conn, job_ids: list[str]) -> int:
    tags = build_revalidation_tags(conn, job_ids)
    if job_ids:
        tags.append(SITEMAP_TAG)
    return post_revalidation(tags) if tags else 0
