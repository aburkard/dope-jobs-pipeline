"""Detect and assign job groups for multi-location deduplication.

Groups jobs that are the same role posted in different locations.
Uses exact title match + content similarity (>95%) to avoid false positives.
"""
import hashlib
import difflib
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()
from psycopg2.extras import execute_values

from db import get_connection
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


def _length_similarity_upper_bound(text_a: str, text_b: str) -> float:
    """Return a safe upper bound on SequenceMatcher ratio from string lengths alone."""
    len_a = len(text_a)
    len_b = len(text_b)
    if len_a == 0 and len_b == 0:
        return 1.0
    if len_a == 0 or len_b == 0:
        return 0.0
    return (2 * min(len_a, len_b)) / (len_a + len_b)


def _cluster_candidate_jobs(candidate_ids: list[str], hashes: dict[str, str], texts: dict[str, str]) -> list[list[str]]:
    """Build connected components for duplicate candidates.

    Two jobs are connected when they share a non-empty content hash or when
    their prepared texts meet the similarity threshold. Using graph components
    avoids representative-order bugs where A matches B and A matches C, but
    B/C fall just below the threshold.
    """
    if len(candidate_ids) < 2:
        return []

    parent: dict[str, str] = {jid: jid for jid in candidate_ids}

    def find(jid: str) -> str:
        root = jid
        while parent[root] != root:
            root = parent[root]
        while parent[jid] != jid:
            next_jid = parent[jid]
            parent[jid] = root
            jid = next_jid
        return root

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    hash_buckets: dict[str, list[str]] = defaultdict(list)
    for jid in candidate_ids:
        group_hash = hashes.get(jid) or ""
        if group_hash:
            hash_buckets[group_hash].append(jid)
    for bucket in hash_buckets.values():
        first = bucket[0]
        for other_jid in bucket[1:]:
            union(first, other_jid)

    comparison_ids = [jid for jid in candidate_ids if jid in texts]
    if len(comparison_ids) >= 2:
        stripped_texts = {jid: strip_metadata_lines(texts[jid]) for jid in comparison_ids}
        exact_text_buckets: dict[str, list[str]] = defaultdict(list)
        for jid in comparison_ids:
            exact_text_buckets[stripped_texts[jid]].append(jid)

        representatives: list[str] = []
        for bucket in exact_text_buckets.values():
            first = bucket[0]
            representatives.append(first)
            for other_jid in bucket[1:]:
                union(first, other_jid)

        for idx, jid in enumerate(representatives):
            stripped_jid = stripped_texts[jid]
            for other_jid in representatives[idx + 1:]:
                if find(jid) == find(other_jid):
                    continue
                stripped_other = stripped_texts[other_jid]
                if _length_similarity_upper_bound(stripped_jid, stripped_other) < SIMILARITY_THRESHOLD:
                    continue
                if content_similarity(texts[jid], texts[other_jid]) >= SIMILARITY_THRESHOLD:
                    union(jid, other_jid)

    clusters_by_root: dict[str, list[str]] = defaultdict(list)
    for jid in candidate_ids:
        clusters_by_root[find(jid)].append(jid)

    return sorted(
        (sorted(cluster) for cluster in clusters_by_root.values() if len(cluster) > 1),
        key=lambda cluster: cluster[0],
    )


def _board_scope_sql(boards: list[tuple[str, str]] | None) -> tuple[str, list[str]]:
    if not boards:
        return "", []

    clauses = []
    params: list[str] = []
    for ats, board_token in boards:
        clauses.append("(ats = %s AND board_token = %s)")
        params.extend([ats, board_token])
    return " AND (" + " OR ".join(clauses) + ")", params


def compute_job_groups(conn, boards: list[tuple[str, str]] | None = None) -> tuple[dict, dict]:
    """Compute job_group assignments for live jobs in the selected scope.

    Returns dict of {job_id: job_group_hash} for jobs that belong to a group.
    Jobs with no group (unique postings) are not included.
    """
    where_scope, params = _board_scope_sql(boards)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id, ats, board_token, title, raw_json, content_hash
            FROM pipeline_jobs
            WHERE raw_json IS NOT NULL AND removed_at IS NULL
            {where_scope}
            ORDER BY board_token, title
        """, params)
        rows = cur.fetchall()

    # Group candidates by ATS board + normalized title
    by_key = defaultdict(list)
    for job_id, ats_name, board, title, raw_json, stored_hash in rows:
        normalized_title = (title or "").strip()
        by_key[(ats_name, board, normalized_title)].append((job_id, raw_json, stored_hash))

    groups = {}
    group_stats = {"groups": 0, "grouped_jobs": 0, "singletons": 0}

    for (ats_name, board, title), candidates in by_key.items():
        if len(candidates) == 1:
            group_stats["singletons"] += 1
            continue

        # Only prepare text for representatives that may need text similarity.
        texts = {}
        hashes = {}
        raw_by_id: dict[str, dict] = {}
        seen_non_empty_hashes: set[str] = set()
        non_empty_hash_representatives = 0
        empty_hash_candidates = 0
        for job_id, raw_json, stored_hash in candidates:
            group_hash = stored_hash or ""
            hashes[job_id] = group_hash
            if raw_json:
                raw_by_id[job_id] = raw_json
            if group_hash:
                if group_hash not in seen_non_empty_hashes:
                    non_empty_hash_representatives += 1
                    seen_non_empty_hashes.add(group_hash)
            else:
                empty_hash_candidates += 1

        needs_text_similarity = empty_hash_candidates > 0 or non_empty_hash_representatives > 1
        if needs_text_similarity:
            seen_non_empty_hashes.clear()
            for job_id, _, stored_hash in candidates:
                raw_json = raw_by_id.get(job_id)
                if not raw_json:
                    continue
                group_hash = stored_hash or ""
                if group_hash:
                    if group_hash in seen_non_empty_hashes:
                        continue
                    seen_non_empty_hashes.add(group_hash)
                texts[job_id] = prepare_job_text(raw_json)

        merged_clusters = _cluster_candidate_jobs(
            [job_id for job_id, _, _ in candidates],
            hashes,
            texts,
        )

        # Assign group hashes
        for ci, cluster in enumerate(merged_clusters):
            suffix = f"__{ci}" if ci > 0 else ""
            group_hash = hashlib.sha256(f"{ats_name}__{board}__{title}{suffix}".encode()).hexdigest()[:16]
            for jid in cluster:
                groups[jid] = group_hash
            group_stats["groups"] += 1
            group_stats["grouped_jobs"] += len(cluster)

    return groups, group_stats


def save_job_groups(conn, groups: dict, boards: list[tuple[str, str]] | None = None) -> list[str]:
    """Save job_group assignments to DB for the selected scope.

    Returns job ids whose persisted job_group changed.
    """
    # Add job_group column if not exists
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE pipeline_jobs ADD COLUMN IF NOT EXISTS job_group TEXT")
    conn.commit()

    where_scope, params = _board_scope_sql(boards)
    existing_query = f"""
        SELECT id, job_group
        FROM pipeline_jobs
        WHERE removed_at IS NULL
        {where_scope}
    """
    with conn.cursor() as cur:
        cur.execute(existing_query, params)
        existing = {job_id: job_group for job_id, job_group in cur.fetchall()}

    previous_counts: dict[str, int] = defaultdict(int)
    for previous_group in existing.values():
        if previous_group:
            previous_counts[previous_group] += 1

    new_counts: dict[str, int] = defaultdict(int)
    for new_group in groups.values():
        if new_group:
            new_counts[new_group] += 1

    changed_ids = []
    for job_id, previous_group in existing.items():
        new_group = groups.get(job_id)
        previous_count = previous_counts.get(previous_group, 1 if previous_group is None else 0)
        new_count = new_counts.get(new_group, 1 if new_group is None else 0)
        if previous_group != new_group or previous_count != new_count:
            changed_ids.append(job_id)

    clear_query = f"""
        UPDATE pipeline_jobs
        SET job_group = NULL
        WHERE removed_at IS NULL
        {where_scope}
    """
    with conn.cursor() as cur:
        cur.execute(clear_query, params)
        if groups:
            execute_values(
                cur,
                """
                WITH incoming(job_id, group_hash) AS (VALUES %s)
                UPDATE pipeline_jobs AS pj
                SET job_group = incoming.group_hash
                FROM incoming
                WHERE pj.id = incoming.job_id
                """,
                [(job_id, group_hash) for job_id, group_hash in groups.items()],
                template="(%s, %s)",
            )
    conn.commit()
    return changed_ids


def recompute_job_groups_for_boards(conn, boards: list[tuple[str, str]]) -> tuple[list[str], dict]:
    """Recompute job groups for a bounded set of boards.

    Returns (changed_job_ids, stats).
    """
    normalized = sorted({(ats, board_token) for ats, board_token in boards})
    if not normalized:
        return [], {"groups": 0, "grouped_jobs": 0, "singletons": 0}

    groups, stats = compute_job_groups(conn, boards=normalized)
    changed_ids = save_job_groups(conn, groups, boards=normalized)
    return changed_ids, stats


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
