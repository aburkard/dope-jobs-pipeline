"""Detect and remove boilerplate text from job descriptions.

Finds repeated content across jobs at the same company using sentence-level hashing.
Results are cached in pipeline_companies.boilerplate_hashes.
"""
import hashlib
import json
import re
from collections import Counter
from dotenv import load_dotenv
load_dotenv()

from db import get_connection
from parse import prepare_job_text
from psycopg2.extras import Json


def normalize_sentence(s: str) -> str:
    """Normalize a sentence for comparison."""
    s = s.lower().strip()
    s = re.sub(r'https?://\S+', '', s)
    s = re.sub(r'\s+', ' ', s)
    return s


def sentence_hash(s: str) -> str:
    """Hash a normalized sentence for fast comparison."""
    return hashlib.md5(normalize_sentence(s).encode()).hexdigest()[:12]


def split_sentences(text: str, min_length: int = 30) -> list[str]:
    """Split text into lines, filtering out short ones."""
    return [l.strip() for l in text.split('\n') if len(l.strip()) >= min_length]


def compute_boilerplate(conn, board_token: str, sample_size: int = 15) -> list[str]:
    """Compute boilerplate hashes for a company by comparing job descriptions.
    Returns list of sentence hashes that appear in >40% of sampled jobs."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT raw_json FROM pipeline_jobs
            WHERE board_token = %s AND raw_json IS NOT NULL AND removed_at IS NULL
            ORDER BY random() LIMIT %s
        """, (board_token, sample_size))
        rows = cur.fetchall()

    if len(rows) < 3:
        return []

    hash_counts = Counter()
    for (raw,) in rows:
        text = prepare_job_text(raw)
        seen = set()
        for sentence in split_sentences(text):
            h = sentence_hash(sentence)
            if h not in seen:
                seen.add(h)
                hash_counts[h] += 1

    threshold = len(rows) * 0.4
    return [h for h, count in hash_counts.items() if count > threshold]


def update_company_boilerplate(conn, board_token: str):
    """Compute and cache boilerplate hashes for a company."""
    bp_hashes = compute_boilerplate(conn, board_token)
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline_companies SET boilerplate_hashes = %s WHERE board_token = %s",
            (Json(bp_hashes), board_token)
        )
    conn.commit()
    return bp_hashes


def get_boilerplate_hashes(conn, board_token: str) -> set[str]:
    """Get cached boilerplate hashes for a company. Computes if not cached."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT boilerplate_hashes FROM pipeline_companies WHERE board_token = %s",
            (board_token,)
        )
        row = cur.fetchone()

    if row and row[0]:
        return set(row[0])

    # Not cached — compute and store
    bp_hashes = update_company_boilerplate(conn, board_token)
    return set(bp_hashes)


def remove_boilerplate(text: str, bp_hashes: set[str]) -> str:
    """Remove boilerplate lines from a job description."""
    if not bp_hashes:
        return text

    cleaned = []
    for line in text.split('\n'):
        stripped = line.strip()
        if len(stripped) < 30:
            cleaned.append(line)
            continue
        if sentence_hash(stripped) in bp_hashes:
            continue
        cleaned.append(line)

    return '\n'.join(cleaned)


def clean_description(conn, board_token: str, raw_job: dict) -> str:
    """Get a cleaned job description with boilerplate removed.
    This is the main function to use from the pipeline."""
    text = prepare_job_text(raw_job)
    bp_hashes = get_boilerplate_hashes(conn, board_token)
    return remove_boilerplate(text, bp_hashes)


def update_all_boilerplate(conn):
    """Compute and cache boilerplate for all active companies."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT board_token FROM pipeline_jobs
            WHERE raw_json IS NOT NULL AND removed_at IS NULL
        """)
        boards = [r[0] for r in cur.fetchall()]

    print(f"Computing boilerplate for {len(boards)} companies...")
    for i, board in enumerate(boards):
        bp = update_company_boilerplate(conn, board)
        if (i + 1) % 10 == 0:
            print(f"  {i+1}/{len(boards)} ({len(bp)} hashes for {board})")
    print("Done!")


if __name__ == "__main__":
    conn = get_connection()
    update_all_boilerplate(conn)
    conn.close()
