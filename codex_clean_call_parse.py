#!/usr/bin/env python3
"""Parse pipeline jobs through Codex-auth OpenAI calls and persist results."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from codex_clean_call_eval import build_request_artifacts, load_codex_clean_call_module
from db import (
    apply_parse_batch_chunk,
    claim_jobs_for_parse_batch,
    delete_parse_batch_jobs,
    get_connection,
    init_schema,
    parse_batch_selection_where,
    save_parse_batch,
    update_parse_batch,
)
from geo_resolver import GeoResolver
from parse import PREPARE_JOB_TEXT_MAX_CHARS, SYSTEM_PROMPT, _parse_response, merge_api_data

MAX_429_RETRIES = 6
BASE_429_BACKOFF_SECONDS = 2.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parse jobs through codex-clean-call and persist to Postgres.")
    parser.add_argument("--limit", type=int, required=True, help="Number of jobs to parse.")
    parser.add_argument("--model", default="gpt-5.4", help="Codex/OpenAI model id.")
    parser.add_argument(
        "--reasoning-effort",
        choices=["none", "low", "medium", "high", "xhigh"],
        default="medium",
        help="Reasoning effort for Codex clean call.",
    )
    parser.add_argument(
        "--reasoning-summary",
        choices=["auto", "concise", "detailed"],
        help="Optional reasoning summary mode.",
    )
    parser.add_argument(
        "--verbosity",
        choices=["low", "medium", "high"],
        default="low",
        help="Text verbosity for Codex clean call.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=8,
        help="Concurrent Codex requests to run.",
    )
    parser.add_argument(
        "--selection",
        choices=["never_parsed", "needs_parse", "failed_once"],
        default="never_parsed",
        help="Which queue slice to claim.",
    )
    parser.add_argument(
        "--balanced-by-ats",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="When claiming a fresh slice, distribute the limit proportionally across ATSes.",
    )
    parser.add_argument(
        "--prompt-max-chars",
        type=int,
        default=PREPARE_JOB_TEXT_MAX_CHARS,
        help="Character cap passed into prepare_job_text().",
    )
    parser.add_argument(
        "--variant",
        choices=["current", "hybrid_descriptions", "schema_descriptions"],
        default="current",
        help="Prompt/schema variant.",
    )
    parser.add_argument(
        "--flush-size",
        type=int,
        default=25,
        help="How many parsed rows to buffer before writing to Postgres.",
    )
    parser.add_argument(
        "--display-name",
        help="Optional display name recorded in pipeline_parse_batches.",
    )
    parser.add_argument(
        "--auth-file",
        default=str(Path.home() / ".codex" / "auth.json"),
        help="Path to Codex auth.json.",
    )
    return parser.parse_args()


def _build_payload_args(schema_path: Path, args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(
        model=args.model,
        verbosity=args.verbosity,
        reasoning_effort=args.reasoning_effort,
        reasoning_summary=args.reasoning_summary,
        schema_file=str(schema_path),
        schema_name="job_metadata",
        json_object=False,
        image_url=[],
        web_search=False,
    )


def _build_parse_params(args: argparse.Namespace, batch_id: str) -> dict[str, Any]:
    return {
        "method": "codex_clean_call",
        "transport": "codex_auth_wrapper",
        "batch_id": batch_id,
        "reasoning_effort": args.reasoning_effort,
        "reasoning_summary": args.reasoning_summary,
        "verbosity": args.verbosity,
        "variant": args.variant,
        "prompt_max_chars": args.prompt_max_chars,
        "selection": args.selection,
        "balanced_by_ats": args.balanced_by_ats,
    }


def _call_codex(module, auth_path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(MAX_429_RETRIES + 1):
        try:
            auth = module.ensure_fresh_auth(auth_path)
            return module.stream_call(auth_path, auth, payload)
        except Exception as exc:
            if not _is_rate_limit_error(exc):
                raise
            last_exc = exc
            if attempt >= MAX_429_RETRIES:
                break
            time.sleep(BASE_429_BACKOFF_SECONDS * (2 ** attempt))
    assert last_exc is not None
    raise last_exc


def _is_rate_limit_error(exc: Exception) -> bool:
    text = str(exc)
    return "HTTP 429" in text or "Rate limit exceeded" in text


def _ats_counts_for_selection(conn, selection: str) -> list[tuple[str, int]]:
    where = parse_batch_selection_where(selection)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT ats, COUNT(*)
            FROM pipeline_jobs
            WHERE {where}
            GROUP BY ats
            ORDER BY ats
            """
        )
        return [(row[0], row[1]) for row in cur.fetchall()]


def _proportional_ats_targets(counts: list[tuple[str, int]], total_limit: int) -> list[tuple[str, int]]:
    total_available = sum(count for _, count in counts)
    if total_available <= 0 or total_limit <= 0:
        return []

    exact = []
    assigned = 0
    for ats, count in counts:
        raw = total_limit * count / total_available
        floor = min(count, int(raw))
        assigned += floor
        exact.append((ats, count, floor, raw - floor))

    remaining = total_limit - assigned
    for ats, count, floor, frac in sorted(exact, key=lambda item: (-item[3], item[0])):
        if remaining <= 0:
            break
        if floor >= count:
            continue
        idx = next(i for i, row in enumerate(exact) if row[0] == ats)
        exact[idx] = (ats, count, floor + 1, frac)
        remaining -= 1

    return [(ats, floor) for ats, _, floor, _ in exact if floor > 0]


def claim_jobs_slice(conn, batch_id: str, limit: int, selection: str, balanced_by_ats: bool) -> list[dict]:
    if not balanced_by_ats:
        return claim_jobs_for_parse_batch(
            conn,
            batch_id=batch_id,
            limit=limit,
            selection=selection,
        )

    ats_targets = _proportional_ats_targets(_ats_counts_for_selection(conn, selection), limit)
    claimed: list[dict] = []
    for ats, target in ats_targets:
        if target <= 0:
            continue
        claimed.extend(
            claim_jobs_for_parse_batch(
                conn,
                batch_id=batch_id,
                limit=target,
                ats_list=[ats],
                selection=selection,
            )
        )

    remaining = limit - len(claimed)
    if remaining > 0:
        claimed.extend(
            claim_jobs_for_parse_batch(
                conn,
                batch_id=batch_id,
                limit=remaining,
                selection=selection,
            )
        )
    return claimed


def main() -> int:
    args = parse_args()
    auth_path = Path(args.auth_file).expanduser()
    batch_id = f"local-codex-{uuid.uuid4().hex[:12]}"
    display_name = args.display_name or f"codex-{args.model}-{args.selection}-{args.limit}"
    parse_params = _build_parse_params(args, batch_id)

    conn = get_connection()
    init_schema(conn)
    jobs = claim_jobs_slice(
        conn,
        batch_id=batch_id,
        limit=args.limit,
        selection=args.selection,
        balanced_by_ats=args.balanced_by_ats,
    )
    if not jobs:
        print(json.dumps({"batch_id": batch_id, "status": "no_jobs"}))
        conn.close()
        return 0

    save_parse_batch(
        conn,
        batch_id=batch_id,
        model=args.model,
        params=parse_params,
        display_name=display_name,
        state="running",
        requested_count=len(jobs),
    )

    geo = GeoResolver(conn)
    module = load_codex_clean_call_module()
    _, _, schema = build_request_artifacts({}, prompt_max_chars=args.prompt_max_chars, variant=args.variant)
    schema_path = Path(f"/tmp/{batch_id}-schema.json")
    schema_path.write_text(json.dumps(schema))
    payload_args = _build_payload_args(schema_path, args)

    success_buffer: list[tuple[str, str, dict]] = []
    error_buffer: list[tuple[str, str, str]] = []
    transient_release_ids: list[str] = []
    applied_success = 0
    applied_failures = 0
    stale_count = 0
    transient_release_count = 0
    completed = 0
    usage_totals = Counter()
    ats_counts = Counter(job["ats"] for job in jobs)
    started = time.time()
    completed_normally = False

    def flush_buffers() -> None:
        nonlocal applied_success, applied_failures, stale_count, transient_release_count
        nonlocal success_buffer, error_buffer, transient_release_ids
        if not success_buffer and not error_buffer and not transient_release_ids:
            return
        outcome = apply_parse_batch_chunk(
            conn,
            batch_id=batch_id,
            success_rows=success_buffer,
            error_rows=error_buffer,
            parse_provider="openai",
            parse_model=args.model,
            parse_params=parse_params,
        )
        applied_success += len(outcome["applied_success_ids"])
        applied_failures += outcome["applied_failure_count"]
        stale_count += outcome["stale_count"]
        if transient_release_ids:
            delete_parse_batch_jobs(conn, batch_id, transient_release_ids)
            transient_release_count += len(transient_release_ids)
        success_buffer = []
        error_buffer = []
        transient_release_ids = []
        update_parse_batch(
            conn,
            batch_id=batch_id,
            state="running",
            succeeded_count=applied_success,
            failed_count=applied_failures,
            stale_count=stale_count,
        )

    def parse_one(job: dict[str, Any]) -> dict[str, Any]:
        prepared_job_text, prompt, _ = build_request_artifacts(
            job["raw_json"] or {},
            prompt_max_chars=args.prompt_max_chars,
            variant=args.variant,
        )
        payload = module.build_payload(payload_args, prompt, SYSTEM_PROMPT)
        result = _call_codex(module, auth_path, payload)
        text = result.get("text")
        if not isinstance(text, str) or not text.strip():
            return {
                "ok": False,
                "job_id": job["id"],
                "expected_hash": job.get("content_hash") or "",
                "error": "empty response text",
            }
        parsed = _parse_response(text, use_flat=True)
        if parsed is None:
            parsed = _parse_response(text, use_flat=False)
        if parsed is None:
            return {
                "ok": False,
                "job_id": job["id"],
                "expected_hash": job.get("content_hash") or "",
                "error": f"parse_failed: {text[:500]}",
            }
        return {
            "ok": True,
            "job_id": job["id"],
            "expected_hash": job.get("content_hash") or "",
            "raw_json": job["raw_json"],
            "parsed": parsed.model_dump(mode="json"),
            "usage": result.get("usage") or {},
            "prepared_job_text_chars": len(prepared_job_text),
            "prompt_chars": len(prompt),
        }

    try:
        print(
            json.dumps(
                {
                    "batch_id": batch_id,
                    "status": "started",
                    "requested_count": len(jobs),
                    "selection": args.selection,
                    "model": args.model,
                    "reasoning_effort": args.reasoning_effort,
                    "verbosity": args.verbosity,
                    "variant": args.variant,
                    "prompt_max_chars": args.prompt_max_chars,
                    "concurrency": args.concurrency,
                    "ats_counts": dict(ats_counts),
                },
                indent=2,
            )
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            futures = {executor.submit(parse_one, job): job for job in jobs}
            for future in concurrent.futures.as_completed(futures):
                job = futures[future]
                completed += 1
                try:
                    result = future.result()
                except Exception as exc:
                    if _is_rate_limit_error(exc):
                        transient_release_ids.append(job["id"])
                    else:
                        error_buffer.append((job["id"], job.get("content_hash") or "", str(exc)))
                else:
                    if result["ok"]:
                        merged = merge_api_data(result["raw_json"] or {}, result["parsed"])
                        merged = geo.resolve_parsed_geo(merged)
                        success_buffer.append((result["job_id"], result["expected_hash"], merged))
                        usage = result.get("usage") or {}
                        usage_totals["input_tokens"] += usage.get("input_tokens", 0) or 0
                        usage_totals["output_tokens"] += usage.get("output_tokens", 0) or 0
                        details = usage.get("output_tokens_details") or {}
                        usage_totals["reasoning_tokens"] += details.get("reasoning_tokens", 0) or 0
                    else:
                        error_buffer.append((result["job_id"], result["expected_hash"], result["error"]))

                if len(success_buffer) + len(error_buffer) >= args.flush_size:
                    flush_buffers()

                if completed % 25 == 0 or completed == len(jobs):
                    elapsed = max(time.time() - started, 0.001)
                    rate = completed / elapsed
                    eta_min = (len(jobs) - completed) / rate / 60 if rate > 0 else None
                    print(
                        json.dumps(
                            {
                                "batch_id": batch_id,
                                "completed": completed,
                                "requested_count": len(jobs),
                                "applied_success": applied_success + len(success_buffer),
                                "applied_failures": applied_failures + len(error_buffer),
                                "stale_count": stale_count,
                                "released_transient": transient_release_count + len(transient_release_ids),
                                "jobs_per_sec": round(rate, 3),
                                "eta_min": round(eta_min, 1) if eta_min is not None else None,
                                "usage_totals": dict(usage_totals),
                            }
                        )
                    )

        flush_buffers()
        update_parse_batch(
            conn,
            batch_id=batch_id,
            state="completed",
            succeeded_count=applied_success,
            failed_count=applied_failures,
            stale_count=stale_count,
            completed_at=datetime.now(timezone.utc),
        )
        completed_normally = True
        print(
            json.dumps(
                {
                    "batch_id": batch_id,
                    "status": "completed",
                    "requested_count": len(jobs),
                    "applied_success": applied_success,
                    "applied_failures": applied_failures,
                    "stale_count": stale_count,
                    "released_transient": transient_release_count,
                    "usage_totals": dict(usage_totals),
                },
                indent=2,
            )
        )
        return 0
    finally:
        schema_path.unlink(missing_ok=True)
        if not completed_normally:
            conn.rollback()
            delete_parse_batch_jobs(conn, batch_id)
            update_parse_batch(
                conn,
                batch_id=batch_id,
                state="failed",
                succeeded_count=applied_success,
                failed_count=applied_failures,
                stale_count=stale_count,
                last_error="interrupted",
            )
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
