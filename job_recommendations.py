"""Generate and store precomputed job recommendations from Meili similar-documents."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any
from urllib import error, request

from db import (
    get_connection,
    get_jobs_needing_recommendation_refresh,
    init_schema,
    replace_job_recommendations,
)
from public_ids import meili_safe_job_id

DEFAULT_ALGORITHM_VERSION = "meili-similar-v1"
DEFAULT_RECOMMENDATIONS_PER_JOB = 10
DEFAULT_CANDIDATE_LIMIT = 24
DEFAULT_SCORE_THRESHOLD = 0.89
DEFAULT_TIMEOUT_SECONDS = 10.0


def get_access_headers(
    client_id: str | None = None,
    client_secret: str | None = None,
) -> dict[str, str]:
    resolved_client_id = client_id or os.environ.get("CF_ACCESS_CLIENT_ID")
    resolved_client_secret = client_secret or os.environ.get("CF_ACCESS_CLIENT_SECRET")
    headers: dict[str, str] = {}
    if resolved_client_id:
        headers["CF-Access-Client-Id"] = resolved_client_id
    if resolved_client_secret:
        headers["CF-Access-Client-Secret"] = resolved_client_secret
    return headers


def normalize_string_list(values: list[Any] | None) -> list[str]:
    if not isinstance(values, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        item = value.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        normalized.append(item)
    return normalized


def escape_filter_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def build_exact_value_clause(field: str, value: str | None) -> str | None:
    if not value:
        return None
    return f'{field} = "{escape_filter_value(value)}"'


def build_array_equals_clause(field: str, values: list[str]) -> str | None:
    normalized = [value for value in values if value]
    if not normalized:
        return None
    if len(normalized) == 1:
        return f'{field} = "{escape_filter_value(normalized[0])}"'
    return " OR ".join(f'{field} = "{escape_filter_value(value)}"' for value in normalized)


def build_location_filter_passes(parsed_json: dict) -> list[list[str]]:
    office_type = parsed_json.get("office_type")
    if office_type == "remote":
        applicant_country_codes = normalize_string_list([
            item.get("country_code", "")
            for item in (parsed_json.get("applicant_location_requirements") or [])
            if isinstance(item, dict)
        ])
        applicant_country_clause = build_array_equals_clause("applicant_country_codes", applicant_country_codes)
        return [[f"({applicant_country_clause})"], []] if applicant_country_clause else [[]]

    work_geoname_ids = sorted({
        item.get("geoname_id")
        for item in (parsed_json.get("locations") or [])
        if isinstance(item, dict) and isinstance(item.get("geoname_id"), int)
    })
    work_country_codes = normalize_string_list([
        item.get("country_code", "")
        for item in (parsed_json.get("locations") or [])
        if isinstance(item, dict)
    ])
    exact_geo_clause = (
        " OR ".join(f"work_geoname_ids = {value}" for value in work_geoname_ids)
        if work_geoname_ids
        else None
    )
    country_clause = build_array_equals_clause("work_country_codes", work_country_codes)

    passes: list[list[str]] = []
    if exact_geo_clause:
        passes.append([f"({exact_geo_clause})"])
    if country_clause:
        passes.append([f"({country_clause})"])
    passes.append([])
    return passes


def build_similar_filter_passes(parsed_json: dict) -> list[str]:
    job_type_clause = build_exact_value_clause("job_type", parsed_json.get("job_type"))
    office_type_clause = build_exact_value_clause("office_type", parsed_json.get("office_type"))
    experience_clause = build_exact_value_clause("experience_level", parsed_json.get("experience_level"))
    passes: list[str] = []
    for location_clauses in build_location_filter_passes(parsed_json):
        clauses = [job_type_clause, experience_clause, office_type_clause, *location_clauses]
        filter_value = " AND ".join(clause for clause in clauses if clause)
        if filter_value:
            passes.append(filter_value)
    return passes


def post_json(
    url: str,
    payload: dict[str, Any],
    *,
    api_key: str | None,
    access_headers: dict[str, str] | None,
    timeout_seconds: float,
) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "dopejobs-recommendations/1.0",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    if access_headers:
        headers.update(access_headers)
    req = request.Request(url, data=data, headers=headers, method="POST")
    with request.urlopen(req, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_similar_hits(
    meili_host: str,
    meili_key: str | None,
    access_headers: dict[str, str] | None,
    *,
    reference_id: str,
    filter_value: str,
    limit: int,
    score_threshold: float,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    payload = {
        "id": reference_id,
        "embedder": "default",
        "filter": filter_value,
        "limit": limit,
        "attributesToRetrieve": ["id", "company_slug"],
        "showRankingScore": True,
        "rankingScoreThreshold": score_threshold,
    }
    response = post_json(
        f"{meili_host.rstrip('/')}/indexes/jobs/similar",
        payload,
        api_key=meili_key,
        access_headers=access_headers,
        timeout_seconds=timeout_seconds,
    )
    hits = response.get("hits")
    return hits if isinstance(hits, list) else []


def build_recommendations_for_job(
    source_job: dict,
    *,
    meili_host: str,
    meili_key: str | None,
    access_headers: dict[str, str] | None,
    recommendations_per_job: int,
    candidate_limit: int,
    score_threshold: float,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen_job_ids = {source_job["id"]}
    seen_companies: set[str] = set()
    reference_id = meili_safe_job_id(source_job["id"])

    for filter_value in build_similar_filter_passes(source_job.get("parsed_json") or {}):
        hits = fetch_similar_hits(
            meili_host,
            meili_key,
            access_headers,
            reference_id=reference_id,
            filter_value=filter_value,
            limit=candidate_limit,
            score_threshold=score_threshold,
            timeout_seconds=timeout_seconds,
        )
        for hit in hits:
            recommended_job_id = hit.get("id")
            if not isinstance(recommended_job_id, str) or not recommended_job_id or recommended_job_id in seen_job_ids:
                continue
            company_key = hit.get("company_slug") if isinstance(hit.get("company_slug"), str) else recommended_job_id
            if company_key in seen_companies:
                continue
            seen_job_ids.add(recommended_job_id)
            seen_companies.add(company_key)
            selected.append(
                {
                    "recommended_job_id": recommended_job_id,
                    "rank": len(selected) + 1,
                    "score": float(hit.get("_rankingScore")) if isinstance(hit.get("_rankingScore"), (int, float)) else None,
                }
            )
            if len(selected) >= recommendations_per_job:
                return selected
    return selected


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate job recommendations from Meili similar-documents")
    parser.add_argument("--meili-host", default=os.environ.get("MEILISEARCH_HOST"), help="MeiliSearch host")
    parser.add_argument("--meili-key", default=os.environ.get("MEILISEARCH_MASTER_KEY"), help="MeiliSearch master key")
    parser.add_argument("--cf-access-client-id", default=os.environ.get("CF_ACCESS_CLIENT_ID"))
    parser.add_argument("--cf-access-client-secret", default=os.environ.get("CF_ACCESS_CLIENT_SECRET"))
    parser.add_argument("--algorithm-version", default=DEFAULT_ALGORITHM_VERSION)
    parser.add_argument("--limit", type=int, default=100, help="Maximum source jobs to refresh")
    parser.add_argument("--job-id", action="append", dest="job_ids", help="Specific source job id to refresh")
    parser.add_argument("--recommendations-per-job", type=int, default=DEFAULT_RECOMMENDATIONS_PER_JOB)
    parser.add_argument("--candidate-limit", type=int, default=DEFAULT_CANDIDATE_LIMIT)
    parser.add_argument("--score-threshold", type=float, default=DEFAULT_SCORE_THRESHOLD)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.meili_host:
        print("MEILISEARCH_HOST is required", file=sys.stderr)
        return 1
    access_headers = get_access_headers(args.cf_access_client_id, args.cf_access_client_secret)

    conn = get_connection()
    init_schema(conn)
    source_jobs = get_jobs_needing_recommendation_refresh(
        conn,
        args.algorithm_version,
        limit=args.limit,
        job_ids=args.job_ids,
    )
    total = len(source_jobs)
    print(f"Refreshing recommendations for {total} jobs")

    refreshed = 0
    failed = 0
    for index, source_job in enumerate(source_jobs, start=1):
        try:
            recommendations = build_recommendations_for_job(
                source_job,
                meili_host=args.meili_host,
                meili_key=args.meili_key,
                access_headers=access_headers,
                recommendations_per_job=args.recommendations_per_job,
                candidate_limit=args.candidate_limit,
                score_threshold=args.score_threshold,
                timeout_seconds=args.timeout_seconds,
            )
            replace_job_recommendations(
                conn,
                source_job["id"],
                recommendations,
                algorithm_version=args.algorithm_version,
                source_last_parsed_at=source_job["last_parsed_at"],
            )
            refreshed += 1
            print(
                f"[{index}/{total}] refreshed {source_job['id']} "
                f"({len(recommendations)} recommendations)"
            )
        except error.HTTPError as exc:
            failed += 1
            body = exc.read().decode("utf-8", errors="replace")
            print(f"[{index}/{total}] failed {source_job['id']}: HTTP {exc.code} {body}", file=sys.stderr)
        except Exception as exc:  # pragma: no cover - defensive runtime guard
            failed += 1
            print(f"[{index}/{total}] failed {source_job['id']}: {exc}", file=sys.stderr)

    print(f"Done. Refreshed={refreshed} failed={failed}")
    conn.close()
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
