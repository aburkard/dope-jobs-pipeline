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

        # First pass: group by identical content hash (guaranteed same content)
        by_hash = defaultdict(list)
        for jid, h in hashes.items():
            if h:
                by_hash[h].append(jid)

        # Second pass: merge hash groups that are similar (>threshold)
        # This catches cases where content differs slightly (localized salary/legal)
        hash_groups = list(by_hash.values())
        merged_clusters = []

        while hash_groups:
            current = hash_groups.pop(0)
            # Only need similarity check if texts are available
            ref_jid = current[0]
            if ref_jid not in texts:
                if len(current) > 1:
                    merged_clusters.append(current)
                continue
            ref_text = texts[ref_jid]
            i = 0
            while i < len(hash_groups):
                other_jid = hash_groups[i][0]
                # Same hash = same content, auto-merge
                if hashes.get(other_jid) == hashes.get(ref_jid) and hashes.get(ref_jid):
                    current.extend(hash_groups.pop(i))
                elif other_jid in texts:
                    sim = content_similarity(ref_text, texts[other_jid])
                    if sim >= SIMILARITY_THRESHOLD:
                        current.extend(hash_groups.pop(i))
                    else:
                        i += 1
                else:
                    i += 1
            if len(current) > 1:
                merged_clusters.append(current)

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
