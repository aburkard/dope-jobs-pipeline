#!/usr/bin/env python3
"""Run parser prompts through codex-clean-call for one or more jobs."""

from __future__ import annotations

import argparse
import copy
import json
import subprocess
import time
from pathlib import Path
from typing import Any

from db import get_connection
from geo_resolver import GeoResolver
from parse import (
    FLAT_JSON_SCHEMA,
    SYSTEM_PROMPT,
    _parse_response,
    build_user_prompt,
    merge_api_data,
    prepare_job_text,
)


STRUCTURED_FIELDS = {
    "locations",
    "applicant_location_requirements",
    "salary",
    "office_type",
    "hybrid_days",
    "job_type",
    "experience_level",
    "is_manager",
    "industry_primary",
    "industry_tags",
    "industry_other_hint",
    "visa_sponsorship",
    "visa_sponsorship_types",
    "equity",
    "company_stage",
    "company_size",
    "team_size",
    "reports_to",
    "remote_timezone_range",
    "education_level",
    "years_experience",
    "travel_percent",
    "interview_stages",
    "posting_language",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate codex-clean-call on pipeline jobs.")
    parser.add_argument("--job-id", action="append", required=True, help="Pipeline job id. Repeatable.")
    parser.add_argument("--model", default="gpt-5.4", help="codex-clean-call model id.")
    parser.add_argument(
        "--reasoning-effort",
        choices=["none", "low", "medium", "high", "xhigh"],
        help="Reasoning effort for codex-clean-call.",
    )
    parser.add_argument(
        "--reasoning-summary",
        choices=["auto", "concise", "detailed"],
        help="Reasoning summary mode for codex-clean-call.",
    )
    parser.add_argument(
        "--verbosity",
        choices=["low", "medium", "high"],
        help="Text verbosity for codex-clean-call.",
    )
    parser.add_argument(
        "--output-dir",
        default="/tmp/codex-clean-call-runs",
        help="Directory for request/response artifacts.",
    )
    parser.add_argument(
        "--compare-stored",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Compare merged result against stored parsed_json.",
    )
    return parser.parse_args()


def build_codex_json_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Normalize to the stricter schema dialect accepted by codex-clean-call."""
    normalized = copy.deepcopy(schema)

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            if node.get("type") == "object":
                props = node.get("properties", {})
                node["additionalProperties"] = False
                node["required"] = list(props.keys())
                for child in props.values():
                    visit(child)
            elif node.get("type") == "array":
                visit(node.get("items"))
            else:
                for value in node.values():
                    visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(normalized)
    return normalized


def canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: canonical(val) for key, val in sorted(value.items())}
    if isinstance(value, list):
        return [canonical(item) for item in value]
    return value


def compute_diffs(baseline: dict[str, Any], current: dict[str, Any]) -> tuple[dict[str, Any], int, int]:
    diffs: dict[str, Any] = {}
    structured_count = 0
    descriptive_count = 0
    for key in sorted(set(baseline.keys()) | set(current.keys())):
        if baseline.get(key) == current.get(key):
            continue
        diffs[key] = {"baseline": baseline.get(key), "current": current.get(key)}
        if key in STRUCTURED_FIELDS:
            structured_count += 1
        else:
            descriptive_count += 1
    return diffs, structured_count, descriptive_count


def fetch_jobs(job_ids: list[str]) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, ats, board_token, title, raw_json, parsed_json
            FROM pipeline_jobs
            WHERE id = ANY(%s)
            ORDER BY id
            """,
            (job_ids,),
        )
        rows = cur.fetchall()
        cur.close()
        found = {
            row[0]: {
                "id": row[0],
                "ats": row[1],
                "board_token": row[2],
                "title": row[3],
                "raw_json": row[4],
                "parsed_json": row[5],
            }
            for row in rows
        }
        missing = [job_id for job_id in job_ids if job_id not in found]
        if missing:
            raise SystemExit(f"Missing jobs: {', '.join(missing)}")
        return [found[job_id] for job_id in job_ids]
    finally:
        conn.close()


def build_command(run_dir: Path, args: argparse.Namespace) -> list[str]:
    command = [
        "codex-clean-call",
        "--model",
        args.model,
        "--instructions-file",
        str(run_dir / "instructions.txt"),
        "--input-file",
        str(run_dir / "prompt.txt"),
        "--schema-file",
        str(run_dir / "schema.codex.json"),
        "--schema-name",
        "job_metadata",
        "--json-envelope",
    ]
    if args.reasoning_effort:
        command.extend(["--reasoning-effort", args.reasoning_effort])
    if args.reasoning_summary:
        command.extend(["--reasoning-summary", args.reasoning_summary])
    if args.verbosity:
        command.extend(["--verbosity", args.verbosity])
    return command


def preview_fields(parsed: dict[str, Any]) -> dict[str, Any]:
    salary = parsed.get("salary") or {}
    locations = parsed.get("locations") or []
    first_location = locations[0] if locations else {}
    return {
        "tagline": parsed.get("tagline"),
        "office_type": parsed.get("office_type"),
        "experience_level": parsed.get("experience_level"),
        "industry_primary": parsed.get("industry_primary"),
        "industry_tags": parsed.get("industry_tags"),
        "cool_factor": parsed.get("cool_factor"),
        "salary": salary,
        "posting_language": parsed.get("posting_language"),
        "first_location": first_location,
    }


def run_job(job: dict[str, Any], args: argparse.Namespace, output_root: Path, geo: GeoResolver) -> dict[str, Any]:
    run_dir = output_root / job["id"]
    run_dir.mkdir(parents=True, exist_ok=True)

    prompt = build_user_prompt(prepare_job_text(job["raw_json"] or {}))
    schema = build_codex_json_schema(FLAT_JSON_SCHEMA)
    (run_dir / "instructions.txt").write_text(SYSTEM_PROMPT)
    (run_dir / "prompt.txt").write_text(prompt)
    (run_dir / "schema.codex.json").write_text(json.dumps(schema, indent=2))
    (run_dir / "job.json").write_text(
        json.dumps(
            {
                "id": job["id"],
                "ats": job["ats"],
                "board_token": job["board_token"],
                "title": job["title"],
            },
            indent=2,
        )
    )

    command = build_command(run_dir, args)
    start = time.perf_counter()
    completed = subprocess.run(command, capture_output=True, text=True)
    elapsed = round(time.perf_counter() - start, 3)

    result: dict[str, Any] = {
        "job_id": job["id"],
        "title": job["title"],
        "model": args.model,
        "reasoning_effort": args.reasoning_effort,
        "reasoning_summary": args.reasoning_summary,
        "verbosity": args.verbosity,
        "latency_s": elapsed,
        "command": command,
    }

    if completed.returncode != 0:
        result["ok"] = False
        result["stderr"] = completed.stderr
        result["stdout"] = completed.stdout
        (run_dir / "error.txt").write_text(
            f"STDOUT:\n{completed.stdout}\n\nSTDERR:\n{completed.stderr}"
        )
        return result

    envelope = json.loads(completed.stdout)
    (run_dir / "envelope.json").write_text(json.dumps(envelope, indent=2))
    text = envelope.get("text")
    parsed = _parse_response(text, use_flat=True) if isinstance(text, str) else None
    if parsed is None:
        result["ok"] = False
        result["error"] = "parse_failed"
        result["text_preview"] = text[:800] if isinstance(text, str) else None
        return result

    merged = merge_api_data(job["raw_json"] or {}, parsed.model_dump(mode="json"))
    merged = geo.resolve_parsed_geo(merged)
    (run_dir / "merged.json").write_text(json.dumps(merged, indent=2))

    result["ok"] = True
    result["usage"] = envelope.get("usage")
    result["preview"] = preview_fields(merged)

    if args.compare_stored and job.get("parsed_json") is not None:
        baseline = canonical(job["parsed_json"] or {})
        current = canonical(merged)
        diffs, structured_count, descriptive_count = compute_diffs(baseline, current)
        result["diff_count"] = len(diffs)
        result["structured_diff_count"] = structured_count
        result["descriptive_diff_count"] = descriptive_count
        result["diff_fields"] = sorted(diffs.keys())
        (run_dir / "baseline.json").write_text(json.dumps(job["parsed_json"], indent=2))
        (run_dir / "diffs.json").write_text(json.dumps(diffs, indent=2))

    (run_dir / "summary.json").write_text(json.dumps(result, indent=2))
    return result


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    jobs = fetch_jobs(args.job_id)
    conn = get_connection()
    try:
        geo = GeoResolver(conn)
        results = [run_job(job, args, output_root, geo) for job in jobs]
    finally:
        conn.close()

    print(json.dumps({"results": results, "output_dir": str(output_root)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
