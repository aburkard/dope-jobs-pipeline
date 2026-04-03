#!/usr/bin/env python3
"""Run parser prompts through codex-clean-call for one or more jobs."""

from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import subprocess
import time
from pathlib import Path
from typing import Any

from db import get_connection
from geo_resolver import GeoResolver
from parse import (
    COMPACT_SCHEMA,
    FLAT_JSON_SCHEMA,
    PREPARE_JOB_TEXT_MAX_CHARS,
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

SCHEMA_VARIANTS = ["current", "hybrid_descriptions", "schema_descriptions"]
DESCRIPTIVE_SCHEMA_ROOT = COMPACT_SCHEMA
SCHEMA_FIELD_DESCRIPTIONS = {
    "tagline": "Catchy one-sentence summary of the job, not the title.",
    "applicant_location_requirements": "Only for remote jobs with explicit applicant geography restrictions. Use [] when unrestricted or unknown.",
    "salary_currency": 'ISO 4217 currency code. Use "" if not disclosed.',
    "salary_period": 'Compensation period from the posting: hourly, weekly, monthly, or annually.',
    "salary_transparency": "Whether the posting shows a full range, only a minimum, or no salary at all.",
    "office_type": "remote, hybrid, or onsite. Prefer the actual work arrangement described in the posting.",
    "hybrid_days": "Days in office per week for hybrid roles. Use 0 if not hybrid or unknown.",
    "experience_level": "entry, mid, senior, staff, principal, or executive based on title and required experience.",
    "is_manager": "True only for people-managing roles, not senior ICs.",
    "industry_primary": "One primary industry from the enum that best reflects the company's core business, not the specific job function.",
    "industry_tags": "Additional strong secondary industries from the same enum list. Use [] if none apply.",
    "industry_other_hint": 'Short freeform hint only when industry_primary is "other". Otherwise use "".',
    "cool_factor": "How unusually interesting the job is to a general job seeker. Most jobs should be standard or interesting.",
    "vibe_tags": "Only include tags supported by specific evidence in the text.",
    "visa_sponsorship": 'yes, no, or unknown based only on explicit text.',
    "visa_sponsorship_types": "Specific sponsorship types only when visa_sponsorship is yes; [] otherwise.",
    "benefits_categories": "Standardized benefit categories explicitly supported by the posting.",
    "benefits_highlights": "At most 3 genuinely unusual perks, not standard benefits like health insurance or PTO.",
    "remote_timezone_earliest": 'Earliest allowed remote timezone like "UTC-8". Use "" if unknown.',
    "remote_timezone_latest": 'Latest allowed remote timezone like "UTC+1". Use "" if unknown.',
    "posting_language": "ISO 639-1 code of the language the posting is written in, not the candidate's required language.",
}
APPLICANT_LOCATION_ITEM_DESCRIPTIONS = {
    "scope": 'Geography level: country, state, city, or region_group.',
    "name": "Human-readable place name.",
    "country_code": "ISO alpha-2 country code when known, else empty string.",
    "region": "Admin region or group label when relevant, else empty string.",
}

_CODEX_CLEAN_CALL_MODULE = None


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
        "--prompt-max-chars",
        type=int,
        default=PREPARE_JOB_TEXT_MAX_CHARS,
        help="Character cap passed into prepare_job_text().",
    )
    parser.add_argument(
        "--variant",
        choices=SCHEMA_VARIANTS,
        default="current",
        help="Prompt/schema variant to evaluate.",
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


def build_descriptive_codex_json_schema() -> dict[str, Any]:
    schema = build_codex_json_schema(FLAT_JSON_SCHEMA)
    schema["description"] = DESCRIPTIVE_SCHEMA_ROOT
    properties = schema.get("properties", {})
    for field, description in SCHEMA_FIELD_DESCRIPTIONS.items():
        if field in properties:
            properties[field]["description"] = description
    location_items = properties.get("applicant_location_requirements", {}).get("items", {})
    if isinstance(location_items, dict):
        location_items["description"] = "Remote applicant geography restriction entry."
        for field, description in APPLICANT_LOCATION_ITEM_DESCRIPTIONS.items():
            if field in location_items.get("properties", {}):
                location_items["properties"][field]["description"] = description
    return schema


def build_request_artifacts(raw_job: dict[str, Any], prompt_max_chars: int, variant: str) -> tuple[str, str, dict[str, Any]]:
    prepared_job_text = prepare_job_text(raw_job, max_chars=prompt_max_chars)
    if variant == "schema_descriptions":
        prompt = f"Job posting:\n{prepared_job_text}"
        schema = build_descriptive_codex_json_schema()
    elif variant == "hybrid_descriptions":
        prompt = build_user_prompt(prepared_job_text)
        schema = build_descriptive_codex_json_schema()
    else:
        prompt = build_user_prompt(prepared_job_text)
        schema = build_codex_json_schema(FLAT_JSON_SCHEMA)
    return prepared_job_text, prompt, schema


def load_codex_clean_call_module():
    global _CODEX_CLEAN_CALL_MODULE
    if _CODEX_CLEAN_CALL_MODULE is not None:
        return _CODEX_CLEAN_CALL_MODULE

    script_path = Path.home() / ".agents" / "skills" / "codex-clean-call" / "scripts" / "codex_clean_call.py"
    spec = importlib.util.spec_from_file_location("codex_clean_call_script", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load codex-clean-call wrapper from {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _CODEX_CLEAN_CALL_MODULE = module
    return module


def build_codex_payload(run_dir: Path, args: argparse.Namespace, prompt: str) -> dict[str, Any]:
    module = load_codex_clean_call_module()
    payload_args = argparse.Namespace(
        model=args.model,
        verbosity=args.verbosity,
        reasoning_effort=args.reasoning_effort,
        reasoning_summary=args.reasoning_summary,
        schema_file=str(run_dir / "schema.codex.json"),
        schema_name="job_metadata",
        json_object=False,
        image_url=[],
        web_search=False,
    )
    return module.build_payload(payload_args, prompt, SYSTEM_PROMPT)


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

    prepared_job_text, prompt, schema = build_request_artifacts(
        job["raw_json"] or {},
        prompt_max_chars=args.prompt_max_chars,
        variant=args.variant,
    )
    (run_dir / "instructions.txt").write_text(SYSTEM_PROMPT)
    (run_dir / "prepared_job_text.txt").write_text(prepared_job_text)
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
    payload = build_codex_payload(run_dir, args, prompt)
    (run_dir / "payload.json").write_text(json.dumps(payload, indent=2))

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
        "variant": args.variant,
        "prompt_max_chars": args.prompt_max_chars,
        "prepared_job_text_chars": len(prepared_job_text),
        "prompt_chars": len(prompt),
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
