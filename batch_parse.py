"""Submit and collect Gemini Batch API parse jobs.

Usage:
  uv run python batch_parse.py submit --limit 1000
  uv run python batch_parse.py submit --companies companies.txt --limit 1000
  uv run python batch_parse.py status batches/123
  uv run python batch_parse.py collect batches/123
  uv run python batch_parse.py list
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import requests
from dotenv import load_dotenv

from db import (
    apply_parse_batch_chunk,
    claim_jobs_for_parse_batch,
    delete_parse_batch,
    delete_parse_batch_jobs,
    get_connection,
    get_parse_batch,
    get_parse_batch_job_rows,
    init_schema,
    list_parse_batches,
    rename_parse_batch,
    save_parse_batch,
    update_parse_batch,
)
from geo_resolver import GeoResolver
from parse import GeminiBackend, merge_api_data, prepare_job_text

load_dotenv()

GEMINI_API_BASE = "https://generativelanguage.googleapis.com"


def normalize_batch_resource(data: dict) -> dict:
    metadata = data.get("metadata")
    if isinstance(metadata, dict) and str(metadata.get("name", "")).startswith("batches/"):
        return metadata
    response = data.get("response")
    if isinstance(response, dict) and str(response.get("name", "")).startswith("batches/"):
        return response
    return data


def _redact_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


class GeminiBatchClient:
    def __init__(self, model: str, api_key: str | None = None):
        self._session = requests.Session()
        self._model = model
        self._api_key = api_key or os.environ.get("GEMINI_API_KEY", "")
        if not self._api_key:
            raise RuntimeError("GEMINI_API_KEY not set")

    def _request(self, method: str, url: str, *, retries: int = 5, **kwargs) -> requests.Response:
        delay = 2.0
        last_response = None
        for attempt in range(retries):
            resp = self._session.request(method, url, **kwargs)
            if resp.status_code not in {429, 500, 502, 503, 504}:
                if resp.ok:
                    return resp
                message = (
                    f"Gemini API {resp.status_code} for {_redact_url(url)}: "
                    f"{(resp.text or '')[:500]}"
                )
                raise RuntimeError(message)
            last_response = resp
            if attempt == retries - 1:
                break
            retry_after = resp.headers.get("Retry-After")
            try:
                sleep_for = float(retry_after) if retry_after else delay
            except ValueError:
                sleep_for = delay
            time.sleep(sleep_for)
            delay = min(delay * 2, 60.0)

        message = (
            f"Gemini API {last_response.status_code} for {_redact_url(url)}: "
            f"{(last_response.text or '')[:500]}"
        )
        raise RuntimeError(message)

    def upload_jsonl(self, path: Path, display_name: str) -> dict:
        size = path.stat().st_size
        start_resp = self._request(
            "POST",
            f"{GEMINI_API_BASE}/upload/v1beta/files",
            params={"key": self._api_key},
            headers={
                "X-Goog-Upload-Protocol": "resumable",
                "X-Goog-Upload-Command": "start",
                "X-Goog-Upload-Header-Content-Length": str(size),
                "X-Goog-Upload-Header-Content-Type": "application/jsonl",
                "Content-Type": "application/json",
            },
            json={"file": {"displayName": display_name}},
            timeout=60,
        )
        start_resp.raise_for_status()
        upload_url = start_resp.headers.get("X-Goog-Upload-URL") or start_resp.headers.get("x-goog-upload-url")
        if not upload_url:
            raise RuntimeError("Gemini upload response missing resumable upload URL")

        upload_resp = self._request(
            "POST",
            upload_url,
            headers={
                "Content-Length": str(size),
                "X-Goog-Upload-Offset": "0",
                "X-Goog-Upload-Command": "upload, finalize",
            },
            data=path.read_bytes(),
            timeout=300,
        )
        upload_resp.raise_for_status()
        data = upload_resp.json()
        file_info = data.get("file") or {}
        if not file_info.get("name"):
            raise RuntimeError(f"Gemini upload response missing file name: {data}")
        return file_info

    def create_batch(self, requests_file_name: str, display_name: str) -> str:
        resp = self._request(
            "POST",
            f"{GEMINI_API_BASE}/v1beta/models/{self._model}:batchGenerateContent",
            params={"key": self._api_key},
            json={
                "batch": {
                    "displayName": display_name,
                    "inputConfig": {"fileName": requests_file_name},
                }
            },
            timeout=60,
        )
        data = resp.json()
        batch = normalize_batch_resource(data)
        for candidate in (
            batch.get("name"),
            data.get("name"),
            (data.get("response") or {}).get("name"),
            (data.get("metadata") or {}).get("name"),
        ):
            if isinstance(candidate, str) and candidate.startswith("batches/"):
                return candidate
        raise RuntimeError(f"Could not determine Gemini batch name from response: {data}")

    def get_batch(self, batch_name: str) -> dict:
        resp = self._request(
            "GET",
            f"{GEMINI_API_BASE}/v1beta/{batch_name}",
            params={"key": self._api_key},
            timeout=60,
        )
        return normalize_batch_resource(resp.json())

    def get_file_metadata(self, file_name: str) -> dict:
        resp = self._request(
            "GET",
            f"{GEMINI_API_BASE}/v1beta/{file_name}",
            params={"key": self._api_key},
            timeout=60,
        )
        return resp.json()

    def download_file_text(self, file_name: str) -> str:
        metadata = self.get_file_metadata(file_name)
        download_url = metadata.get("downloadUri")
        if not download_url:
            raise RuntimeError(f"Gemini file metadata missing downloadUri: {metadata}")

        attempts = [
            {"url": download_url, "kwargs": {"params": {"key": self._api_key}, "timeout": 300}},
            {"url": download_url, "kwargs": {"headers": {"x-goog-api-key": self._api_key}, "timeout": 300}},
            {
                "url": f"{GEMINI_API_BASE}/v1beta/{file_name}:download",
                "kwargs": {"params": {"key": self._api_key}, "timeout": 300},
            },
            {"url": download_url, "kwargs": {"timeout": 300}},
        ]

        last_error = None
        for attempt in attempts:
            try:
                resp = self._request("GET", attempt["url"], **attempt["kwargs"])
                return resp.text
            except Exception as exc:  # pragma: no cover - fallback path
                last_error = exc

        raise RuntimeError(f"Unable to download Gemini batch output file {file_name}: {last_error}")


def build_batch_request_entry(backend: GeminiBackend, job_id: str, job_text: str, max_tokens: int) -> dict:
    return {
        "request": backend.build_request(job_text, max_tokens=max_tokens),
        "metadata": {"job_id": job_id},
    }


def extract_batch_output_entry(payload: dict) -> tuple[dict | None, str | None]:
    if "response" in payload:
        return payload["response"], None
    if "error" in payload:
        error = payload["error"]
        return None, error.get("message") or json.dumps(error)

    output = payload.get("output")
    if isinstance(output, dict):
        if "response" in output:
            return output["response"], None
        if "error" in output:
            error = output["error"]
            return None, error.get("message") or json.dumps(error)

    if "candidates" in payload or "promptFeedback" in payload:
        return payload, None

    return None, f"Unrecognized batch output payload keys: {sorted(payload.keys())}"


def extract_batch_output_job_id(payload: dict) -> str | None:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        job_id = metadata.get("job_id")
        if isinstance(job_id, str) and job_id.strip():
            return job_id.strip()

    output = payload.get("output")
    if isinstance(output, dict):
        metadata = output.get("metadata")
        if isinstance(metadata, dict):
            job_id = metadata.get("job_id")
            if isinstance(job_id, str) and job_id.strip():
                return job_id.strip()

    return None


def _batch_counts(batch: dict) -> tuple[int | None, int | None, int | None]:
    stats = batch.get("batchStats") or {}
    requested = stats.get("requestCount")
    succeeded = stats.get("successfulRequestCount")
    failed = stats.get("failedRequestCount")
    return (
        int(requested) if requested is not None else None,
        int(succeeded) if succeeded is not None else None,
        int(failed) if failed is not None else None,
    )


def parse_companies_file(path: str) -> list[tuple[str, str]]:
    companies: list[tuple[str, str]] = []
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


def submit_batch(conn, limit: int, model: str, max_tokens: int,
                 display_name: str | None = None,
                 companies: list[tuple[str, str]] | None = None) -> str | None:
    temp_batch_id = f"pending-{uuid.uuid4()}"
    jobs = claim_jobs_for_parse_batch(conn, temp_batch_id, limit, companies=companies)
    if not jobs:
        print("No eligible jobs need parsing.")
        return None

    backend = GeminiBackend(model=model)
    geo_resolver = GeoResolver(conn)
    client = GeminiBatchClient(model=model)
    display_name = display_name or f"parse-{int(time.time())}-{len(jobs)}"

    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", encoding="utf-8", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        for job in jobs:
            text = prepare_job_text(job["raw_json"] or {})
            entry = build_batch_request_entry(backend, job["id"], text, max_tokens=max_tokens)
            tmp.write(json.dumps(entry, separators=(",", ":")) + "\n")

    try:
        input_file = client.upload_jsonl(tmp_path, display_name=display_name)
        batch_name = client.create_batch(input_file["name"], display_name=display_name)
        batch = client.get_batch(batch_name)
        rename_parse_batch(conn, temp_batch_id, batch_name)
        requested_count, _, _ = _batch_counts(batch)
        save_parse_batch(
            conn,
            batch_name,
            model=model,
            params={"method": "batch", "max_output_tokens": max_tokens},
            state=batch.get("state", "STATE_UNSPECIFIED"),
            display_name=batch.get("displayName", display_name),
            input_file_name=input_file["name"],
            output_file_name=(batch.get("output") or {}).get("responsesFile"),
            requested_count=requested_count or len(jobs),
            completed_at=batch.get("endTime"),
        )
        print(f"Submitted {len(jobs)} jobs as {batch_name} ({batch.get('state', 'unknown')})")
        return batch_name
    except Exception:
        delete_parse_batch(conn, temp_batch_id)
        raise
    finally:
        tmp_path.unlink(missing_ok=True)


def status_batch(conn, batch_name: str, model: str) -> dict:
    client = GeminiBatchClient(model=model)
    batch = client.get_batch(batch_name)
    requested_count, succeeded_count, failed_count = _batch_counts(batch)
    save_parse_batch(
        conn,
        batch_name,
        model=model,
        state=batch.get("state", "STATE_UNSPECIFIED"),
        display_name=batch.get("displayName"),
        input_file_name=((batch.get("inputConfig") or {}).get("fileName")),
        output_file_name=((batch.get("output") or {}).get("responsesFile")),
        requested_count=requested_count or 0,
        succeeded_count=succeeded_count or 0,
        failed_count=failed_count or 0,
        completed_at=batch.get("endTime"),
        last_error=(batch.get("error") or {}).get("message"),
    )
    return batch


def collect_batch(conn, batch_name: str, model: str) -> list[str]:
    backend = GeminiBackend(model=model)
    client = GeminiBatchClient(model=model)
    geo_resolver = GeoResolver(conn)
    batch = status_batch(conn, batch_name, model)
    batch_record = get_parse_batch(conn, batch_name) or {}
    state = batch.get("state", "STATE_UNSPECIFIED")

    if state not in {"JOB_STATE_SUCCEEDED", "BATCH_STATE_SUCCEEDED", "SUCCEEDED"}:
        if state in {"JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "BATCH_STATE_FAILED", "BATCH_STATE_CANCELLED", "FAILED", "CANCELLED"}:
            delete_parse_batch_jobs(conn, batch_name)
            update_parse_batch(conn, batch_name, state=state, last_error=(batch.get("error") or {}).get("message"))
        print(f"Batch {batch_name} is {state}; nothing to collect yet.")
        return []

    responses_file = ((batch.get("output") or {}).get("responsesFile"))
    if not responses_file:
        raise RuntimeError(f"Batch {batch_name} succeeded but did not expose an output file")

    lines = [line for line in client.download_file_text(responses_file).splitlines() if line.strip()]
    reserved_jobs = get_parse_batch_job_rows(conn, batch_name)
    reserved_jobs_by_id = {row["job_id"]: row for row in reserved_jobs}

    parsed_ids: list[str] = []
    successes = 0
    failures = 0
    stale = 0
    mismatch_messages: list[str] = []
    chunk_success_rows: list[tuple[str, str, dict]] = []
    chunk_error_rows: list[tuple[str, str, str]] = []
    chunk_size = 200
    matched_job_ids: set[str] = set()

    if len(lines) != len(reserved_jobs):
        mismatch_messages.append(
            f"Batch output line count mismatch: got {len(lines)} lines for {len(reserved_jobs)} reserved jobs"
        )

    parse_provider = "google"
    parse_params = dict(batch_record.get("params") or {})
    parse_params["method"] = "batch"
    parse_params["batch_id"] = batch_name

    def flush_chunk():
        nonlocal successes, failures, stale, chunk_success_rows, chunk_error_rows, parsed_ids
        if not chunk_success_rows and not chunk_error_rows:
            return
        result = apply_parse_batch_chunk(
            conn,
            batch_name,
            chunk_success_rows,
            chunk_error_rows,
            parse_provider=parse_provider,
            parse_model=model,
            parse_params=parse_params,
        )
        successes += len(result["applied_success_ids"])
        failures += result["applied_failure_count"]
        stale += result["stale_count"]
        parsed_ids.extend(result["applied_success_ids"])
        chunk_success_rows = []
        chunk_error_rows = []

    for index, line in enumerate(lines):
        payload = json.loads(line)
        output_job_id = extract_batch_output_job_id(payload)
        if not output_job_id:
            mismatch_messages.append(f"Output line {index} missing metadata.job_id")
            continue
        if output_job_id in matched_job_ids:
            mismatch_messages.append(f"Duplicate output for job_id {output_job_id}")
            continue
        job_row = reserved_jobs_by_id.get(output_job_id)
        if job_row is None:
            mismatch_messages.append(f"Unexpected output for unknown job_id {output_job_id}")
            continue
        matched_job_ids.add(output_job_id)
        response_payload, response_error = extract_batch_output_entry(payload)
        if response_error:
            chunk_error_rows.append(
                (job_row["job_id"], job_row["submitted_content_hash"], response_error)
            )
            if len(chunk_success_rows) + len(chunk_error_rows) >= chunk_size:
                flush_chunk()
            continue

        parsed, parse_error = backend.parse_response_payload(response_payload or {})
        if parse_error or parsed is None:
            chunk_error_rows.append(
                (
                    job_row["job_id"],
                    job_row["submitted_content_hash"],
                    parse_error or "Gemini batch returned no parsed content",
                )
            )
            if len(chunk_success_rows) + len(chunk_error_rows) >= chunk_size:
                flush_chunk()
            continue

        merged = merge_api_data(job_row["raw_json"] or {}, parsed.model_dump(mode="json"))
        merged = geo_resolver.resolve_parsed_geo(merged)
        chunk_success_rows.append(
            (job_row["job_id"], job_row["submitted_content_hash"], merged)
        )
        if len(chunk_success_rows) + len(chunk_error_rows) >= chunk_size:
            flush_chunk()

    flush_chunk()

    missing_job_ids = [job_id for job_id in reserved_jobs_by_id if job_id not in matched_job_ids]
    if missing_job_ids:
        stale += len(missing_job_ids)
        delete_parse_batch_jobs(conn, batch_name, job_ids=missing_job_ids)

    mismatch_error = None
    if mismatch_messages:
        preview = "; ".join(mismatch_messages[:5])
        extra = len(mismatch_messages) - 5
        mismatch_error = preview if extra <= 0 else f"{preview}; and {extra} more"

    update_parse_batch(
        conn,
        batch_name,
        state=state,
        output_file_name=responses_file,
        succeeded_count=successes,
        failed_count=failures,
        stale_count=stale,
        completed_at=batch.get("endTime"),
        last_error=mismatch_error,
    )
    print(f"Collected {batch_name}: {successes} applied, {failures} failed, {stale} stale")
    return parsed_ids


def main():
    parser = argparse.ArgumentParser(description="Gemini batch parsing for dopejobs")
    parser.add_argument("--model", default="gemini-3.1-flash-lite-preview", help="Gemini model name")
    subparsers = parser.add_subparsers(dest="command", required=True)

    submit_parser = subparsers.add_parser("submit", help="Reserve jobs and submit a Gemini batch")
    submit_parser.add_argument("--limit", type=int, required=True, help="Number of jobs to submit in this batch")
    submit_parser.add_argument("--max-output-tokens", type=int, default=2000, help="Gemini max output tokens")
    submit_parser.add_argument("--display-name", default=None, help="Optional human-readable batch name")
    submit_parser.add_argument("--companies", default=None, help="Optional companies file to scope the batch")

    status_parser = subparsers.add_parser("status", help="Fetch current batch state")
    status_parser.add_argument("batch_name", help="Gemini batch resource name, e.g. batches/123")

    collect_parser = subparsers.add_parser("collect", help="Apply completed batch results to Postgres")
    collect_parser.add_argument("batch_name", help="Gemini batch resource name, e.g. batches/123")

    subparsers.add_parser("list", help="List recent locally tracked parse batches")

    args = parser.parse_args()

    conn = get_connection()
    init_schema(conn)
    try:
        if args.command == "submit":
            companies = parse_companies_file(args.companies) if args.companies else None
            submit_batch(
                conn,
                limit=args.limit,
                model=args.model,
                max_tokens=args.max_output_tokens,
                display_name=args.display_name,
                companies=companies,
            )
        elif args.command == "status":
            batch = status_batch(conn, args.batch_name, args.model)
            print(json.dumps(batch, indent=2, sort_keys=True))
        elif args.command == "collect":
            collect_batch(conn, args.batch_name, args.model)
        elif args.command == "list":
            print(json.dumps(list_parse_batches(conn), indent=2, default=str))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
