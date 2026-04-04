from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

import requests


BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}


def load_tokens(path: str | Path) -> list[str]:
    tokens: list[str] = []
    seen: set[str] = set()
    for raw_line in Path(path).read_text().splitlines():
        token = raw_line.strip()
        if not token or token.startswith("#"):
            continue
        if token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def extract_title(text: str) -> str | None:
    match = re.search(r"<title>(.*?)</title>", text, re.I | re.S)
    if not match:
        return None
    return re.sub(r"\s+", " ", match.group(1)).strip()


def classify_response(response: requests.Response) -> tuple[str, dict]:
    details = {
        "status_code": response.status_code,
        "content_type": response.headers.get("content-type"),
        "cf_mitigated": response.headers.get("cf-mitigated"),
        "server": response.headers.get("server"),
    }

    if response.status_code == 200:
        try:
            payload = response.json()
        except Exception:
            title = extract_title(response.text)
            details["title"] = title
            return "non_json_200", details
        if isinstance(payload, dict) and "jobs" in payload:
            details["job_count"] = len(payload.get("jobs") or [])
            details["name"] = payload.get("name")
            return "ok", details
        details["payload_keys"] = sorted(payload.keys()) if isinstance(payload, dict) else None
        return "unexpected_json_200", details

    title = extract_title(response.text)
    details["title"] = title
    if response.headers.get("cf-mitigated") == "challenge" or title == "Security challenge":
        return "challenge", details
    return "error", details


def probe_token(session: requests.Session, token: str, timeout_seconds: float) -> dict:
    url = f"https://apply.workable.com/api/v1/widget/accounts/{token}?details=true"
    started = time.perf_counter()
    try:
        response = session.get(url, timeout=timeout_seconds)
    except Exception as exc:
        return {
            "token": token,
            "url": url,
            "classification": "request_error",
            "error": repr(exc),
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }

    classification, details = classify_response(response)
    return {
        "token": token,
        "url": url,
        "classification": classification,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        **details,
    }


def main():
    parser = argparse.ArgumentParser(description="Probe Workable widget endpoints.")
    parser.add_argument("--tokens-file", required=True)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.25)
    parser.add_argument("--output-json", default="workable-widget-results.json")
    args = parser.parse_args()

    tokens = load_tokens(args.tokens_file)[: args.limit]
    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    results = []
    summary: dict[str, int] = {}
    for idx, token in enumerate(tokens, start=1):
        result = probe_token(session, token, args.timeout_seconds)
        results.append(result)
        classification = result["classification"]
        summary[classification] = summary.get(classification, 0) + 1
        extra = ""
        if "job_count" in result:
            extra = f" jobs={result['job_count']}"
        elif "title" in result and result["title"]:
            extra = f" title={result['title']!r}"
        print(
            f"[{idx}/{len(tokens)}] {token} -> {classification} "
            f"status={result.get('status_code')}{extra}",
            flush=True,
        )
        if args.sleep_seconds > 0 and idx < len(tokens):
            time.sleep(args.sleep_seconds)

    payload = {"summary": summary, "results": results}
    Path(args.output_json).write_text(json.dumps(payload, indent=2))
    print("Summary:", json.dumps(summary, sort_keys=True))
    print(f"Wrote {args.output_json}")


if __name__ == "__main__":
    main()
