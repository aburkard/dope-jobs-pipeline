"""Detect and assign job groups for multi-location deduplication.

Groups jobs that are the same role posted in different locations.
Uses exact title match + content similarity (>95%) to avoid false positives.
"""
import hashlib
import difflib
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()

from db import get_connection, content_hash as content_hash_str
from parse import prepare_job_text


SIMILARITY_THRESHOLD = 0.95


def strip_metadata_lines(text: str, n: int = 5) -> str:
    """Strip first N lines which are typically location/department metadata."""
    lines = text.split("\n")[n:]
    return "\n".join(lines)


def content_similarity(text_a: str, text_b: str) -> float:
    """Compute similarity ratio between two job descriptions,
    ignoring the first few lines of metadata."""
    a = strip_metadata_lines(text_a)
    b = strip_metadata_lines(text_b)
    return difflib.SequenceMatcher(None, a, b).ratio()


def _cluster_candidate_jobs(candidate_ids: list[str], hashes: dict[str, str], texts: dict[str, str]) -> list[list[str]]:
    """Build connected components for duplicate candidates.

    Two jobs are connected when they share a non-empty content hash or when
    their prepared texts meet the similarity threshold. Using graph components
    avoids representative-order bugs where A matches B and A matches C, but
    B/C fall just below the threshold.
    """
    if len(candidate_ids) < 2:
        return []

    adjacency: dict[str, set[str]] = {jid: set() for jid in candidate_ids}

    for idx, jid in enumerate(candidate_ids):
        for other_jid in candidate_ids[idx + 1:]:
            same_hash = hashes.get(jid) and hashes.get(jid) == hashes.get(other_jid)
            similar = False
            if not same_hash and jid in texts and other_jid in texts:
                similar = content_similarity(texts[jid], texts[other_jid]) >= SIMILARITY_THRESHOLD
            if same_hash or similar:
                adjacency[jid].add(other_jid)
                adjacency[other_jid].add(jid)

    clusters: list[list[str]] = []
    visited: set[str] = set()
    for jid in candidate_ids:
        if jid in visited or not adjacency[jid]:
            continue
        stack = [jid]
        component: list[str] = []
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            component.append(current)
            stack.extend(sorted(adjacency[current] - visited))
        if len(component) > 1:
            clusters.append(sorted(component))

    return clusters


def compute_job_groups(conn) -> dict:
    """Compute job_group assignments for all parsed jobs.

    Returns dict of {job_id: job_group_hash} for jobs that belong to a group.
    Jobs with no group (unique postings) are not included.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, board_token, title, raw_json, content_hash
            FROM pipeline_jobs
            WHERE parsed_json IS NOT NULL AND removed_at IS NULL
            ORDER BY board_token, title
        """)
        rows = cur.fetchall()

    # Group candidates by company + normalized title
    by_key = defaultdict(list)
    for job_id, board, title, raw_json, stored_hash in rows:
        normalized_title = (title or "").strip()
        by_key[(board, normalized_title)].append((job_id, raw_json, stored_hash))

    groups = {}
    group_stats = {"groups": 0, "grouped_jobs": 0, "singletons": 0}

    for (board, title), candidates in by_key.items():
        if len(candidates) == 1:
            group_stats["singletons"] += 1
            continue

        # Prepare texts and collect content hashes
        texts = {}
        hashes = {}
        for job_id, raw_json, stored_hash in candidates:
            if raw_json:
                texts[job_id] = prepare_job_text(raw_json)
            hashes[job_id] = stored_hash or ""

        if len(texts) < 2:
            continue

        merged_clusters = _cluster_candidate_jobs(
            [job_id for job_id, _, _ in candidates],
            hashes,
            texts,
        )

        # Assign group hashes
        for ci, cluster in enumerate(merged_clusters):
            suffix = f"__{ci}" if ci > 0 else ""
            group_hash = hashlib.sha256(f"{board}__{title}{suffix}".encode()).hexdigest()[:16]
            for jid in cluster:
                groups[jid] = group_hash
            group_stats["groups"] += 1
            group_stats["grouped_jobs"] += len(cluster)

    return groups, group_stats


def save_job_groups(conn, groups: dict):
    """Save job_group assignments to DB and update MeiliSearch documents."""
    # Add job_group column if not exists
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE pipeline_jobs ADD COLUMN IF NOT EXISTS job_group TEXT")
    conn.commit()

    # Clear existing groups
    with conn.cursor() as cur:
        cur.execute("UPDATE pipeline_jobs SET job_group = NULL")

    # Set new groups
    for job_id, group_hash in groups.items():
        with conn.cursor() as cur:
            cur.execute("UPDATE pipeline_jobs SET job_group = %s WHERE id = %s", (group_hash, job_id))
    conn.commit()


def get_group_summary(conn) -> list[dict]:
    """Get summary of all job groups for display."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT job_group, board_token, title, COUNT(*) as cnt,
                   array_agg(parsed_json->'locations'->0->>'city') as cities
            FROM pipeline_jobs
            WHERE job_group IS NOT NULL AND removed_at IS NULL
            GROUP BY job_group, board_token, title
            ORDER BY cnt DESC
        """)
        return [
            {"group": r[0], "company": r[1], "title": r[2], "count": r[3], "cities": r[4]}
            for r in cur.fetchall()
        ]


if __name__ == "__main__":
    conn = get_connection()

    print("Computing job groups...")
    groups, stats = compute_job_groups(conn)
    print(f"  Groups: {stats['groups']}")
    print(f"  Grouped jobs: {stats['grouped_jobs']}")
    print(f"  Singleton jobs: {stats['singletons']}")

    print("\nSaving to DB...")
    save_job_groups(conn, groups)

    print("\nTop groups:")
    summary = get_group_summary(conn)
    for g in summary[:15]:
        cities = [c for c in g["cities"] if c][:5]
        print(f"  {g['count']:3d}x  {g['company']:20s}  {g['title'][:40]:40s}  {', '.join(cities)}")

    conn.close()
    print("\nDone!")
