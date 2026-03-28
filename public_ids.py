"""Stable public-facing identifiers for jobs and companies."""

from __future__ import annotations

import base64
import hashlib
import re
from collections import defaultdict


def slugify(text: str) -> str:
    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")[:120]


def short_public_job_id(job_id: str) -> str:
    digest = hashlib.blake2b(job_id.encode("utf-8"), digest_size=10, person=b"dopejobs").digest()
    return base64.b32encode(digest).decode("ascii").rstrip("=").lower()


def _short_suffix(text: str, length: int = 6) -> str:
    digest = hashlib.blake2b(text.encode("utf-8"), digest_size=5, person=b"dopeco").digest()
    return base64.b32encode(digest).decode("ascii").rstrip("=").lower()[:length]


def _normalize_domain(domain: str | None) -> str:
    if not domain:
        return ""
    host = domain.strip().lower()
    host = re.sub(r"^https?://", "", host)
    host = host.split("/")[0]
    host = host.split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    return host


def _domain_root(domain: str | None) -> str:
    host = _normalize_domain(domain)
    if not host:
        return ""
    parts = [part for part in host.split(".") if part]
    if len(parts) == 1:
        return parts[0]
    if len(parts) >= 3 and len(parts[-1]) == 2 and len(parts[-2]) <= 3:
        return parts[-3]
    return parts[-2]


def base_company_slug(company_name: str | None, domain: str | None, board_token: str) -> str:
    candidate = slugify(company_name or "")
    if candidate:
        return candidate
    domain_candidate = slugify(_domain_root(domain))
    if domain_candidate:
        return domain_candidate
    board_candidate = slugify(board_token)
    return board_candidate or "company"


def identity_key(company_name: str | None, domain: str | None, ats: str, board_token: str) -> str:
    host = _normalize_domain(domain)
    if host:
        return f"domain:{host}"
    name = slugify(company_name or "")
    if name:
        return f"name:{name}"
    return f"row:{ats}:{board_token}"


def derive_company_slug_map(rows: list[dict]) -> dict[tuple[str, str], str]:
    """Derive stable company slugs for pipeline_companies rows.

    Rows belonging to the same apparent company identity share a slug. Conflicts
    are disambiguated using domain roots or a short deterministic suffix.
    """
    by_identity: dict[str, list[dict]] = defaultdict(list)
    identity_meta: dict[str, dict] = {}

    for row in rows:
        ident = identity_key(row.get("company_name"), row.get("domain"), row["ats"], row["board_token"])
        by_identity[ident].append(row)
        if ident not in identity_meta:
            identity_meta[ident] = {
                "base": base_company_slug(row.get("company_name"), row.get("domain"), row["board_token"]),
                "domain_root": slugify(_domain_root(row.get("domain"))),
                "identity": ident,
            }

    assigned: dict[str, str] = {}
    used: dict[str, str] = {}

    for ident in sorted(identity_meta):
        meta = identity_meta[ident]
        base = meta["base"] or "company"
        candidate = base

        if candidate in used and used[candidate] != ident:
            domain_root = meta["domain_root"]
            preferred = f"{base}-{domain_root}" if domain_root and domain_root != base else ""
            if preferred and preferred not in used:
                candidate = preferred
            else:
                candidate = f"{base}-{_short_suffix(ident)}"
                while candidate in used and used[candidate] != ident:
                    candidate = f"{base}-{_short_suffix(ident, length=len(candidate.split('-')[-1]) + 1)}"

        used[candidate] = ident
        assigned[ident] = candidate

    slug_map: dict[tuple[str, str], str] = {}
    for ident, members in by_identity.items():
        slug = assigned[ident]
        for row in members:
            slug_map[(row["ats"], row["board_token"])] = slug
    return slug_map
