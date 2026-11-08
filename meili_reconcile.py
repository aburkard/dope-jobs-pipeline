"""Meili status report and DB <-> Meili bookkeeping reconciliation.

`meili_loaded_at` is only cleared after a delete task is acknowledged, so
timed-out runs leave removed jobs flagged as loaded even after Meili applied
the delete. Reconcile compares the flags against the documents Meili actually
holds and fixes both sides:

- flagged in DB but absent from Meili -> clear the flags
- removed in DB but present in Meili   -> delete from Meili, then clear flags
- present in Meili but unknown to DB    -> delete from Meili (orphans)
"""
import os

from db import mark_jobs_meili_deleted
from public_ids import meili_safe_job_id

JOBS_INDEX = "jobs"
DOCUMENT_PAGE_SIZE = 10000
DELETE_BATCH_SIZE = 2000
FLAG_BATCH_SIZE = 10000


def make_meili_client(meili_host: str, meili_key: str | None = None):
    import meilisearch

    key = meili_key or os.environ.get("MEILISEARCH_MASTER_KEY", "")
    headers = {}
    if os.environ.get("CF_ACCESS_CLIENT_ID"):
        headers["CF-Access-Client-Id"] = os.environ["CF_ACCESS_CLIENT_ID"]
    if os.environ.get("CF_ACCESS_CLIENT_SECRET"):
        headers["CF-Access-Client-Secret"] = os.environ["CF_ACCESS_CLIENT_SECRET"]
    return meilisearch.Client(meili_host, key, custom_headers=headers or None)


def _task_field(task, name):
    return task.get(name) if isinstance(task, dict) else getattr(task, name, None)


def print_meili_status(client) -> None:
    index = client.index(JOBS_INDEX)
    stats = index.get_stats()
    print(f"Index '{JOBS_INDEX}': numberOfDocuments={stats.number_of_documents} "
          f"isIndexing={getattr(stats, 'is_indexing', None)}")
    documents = index.get_documents({"fields": ["meili_id"], "limit": 1})
    print(f"Documents endpoint total: {documents.total}")
    search = index.search("", {"limit": 0})
    print(f"Placeholder search estimatedTotalHits: {search.get('estimatedTotalHits')}")

    for status in ("processing", "enqueued", "failed"):
        tasks = client.get_tasks({"statuses": [status], "limit": 1000})
        by_type: dict[str, int] = {}
        for task in tasks.results:
            task_type = _task_field(task, "type") or "unknown"
            by_type[task_type] = by_type.get(task_type, 0) + 1
        uids = [_task_field(task, "uid") for task in tasks.results]
        span = f" uids {min(uids)}..{max(uids)}" if uids else ""
        print(f"Tasks {status}: total={tasks.total} sample_by_type={by_type}{span}")

    try:
        embedders = index.get_embedders()
        embedders = embedders if isinstance(embedders, dict) else getattr(embedders, "embedders", {}) or {}
        for name, config in embedders.items():
            config = config if isinstance(config, dict) else dict(getattr(config, "__dict__", {}))
            redacted = {k: v for k, v in config.items() if "key" not in k.lower() and "header" not in k.lower()}
            print(f"Embedder {name}: {redacted}")
    except Exception as exc:
        print(f"Embedders: unavailable ({type(exc).__name__})")


def fetch_meili_job_ids(index) -> dict[str, str]:
    """Return {meili_id: job id} for every document in the index."""
    docs: dict[str, str] = {}
    offset = 0
    while True:
        page = index.get_documents({"fields": ["meili_id", "id"], "limit": DOCUMENT_PAGE_SIZE, "offset": offset})
        for doc in page.results:
            values = dict(doc)
            docs[values["meili_id"]] = values.get("id") or ""
        offset += len(page.results)
        if not page.results or offset >= page.total:
            return docs


def plan_reconcile(meili_docs: dict[str, str], db_rows) -> dict[str, list[str]]:
    """Diff Meili documents against (job_id, is_removed, is_flagged) DB rows."""
    present = set(meili_docs)
    known_meili_ids: set[str] = set()
    plan = {
        "clear_removed_absent": [],
        "clear_active_absent": [],
        "delete_removed_present": [],
        "active_present_unflagged": [],
        "delete_orphans": [],
    }
    for job_id, is_removed, is_flagged in db_rows:
        meili_id = meili_safe_job_id(job_id)
        known_meili_ids.add(meili_id)
        in_meili = meili_id in present
        if is_removed and in_meili:
            plan["delete_removed_present"].append(job_id)
        elif is_removed and is_flagged:
            plan["clear_removed_absent"].append(job_id)
        elif not is_removed and is_flagged and not in_meili:
            plan["clear_active_absent"].append(job_id)
        elif not is_removed and not is_flagged and in_meili:
            plan["active_present_unflagged"].append(job_id)
    plan["delete_orphans"] = sorted(present - known_meili_ids)
    return plan


def _iter_db_rows(conn):
    with conn.cursor(name="reconcile_job_flags") as cur:
        cur.itersize = 50000
        cur.execute("""
            SELECT id, removed_at IS NOT NULL, meili_loaded_at IS NOT NULL
            FROM pipeline_jobs
        """)
        yield from cur


def _wait(client, task_uid: int, timeout_in_ms: int = 600000) -> bool:
    try:
        task = client.wait_for_task(task_uid, timeout_in_ms=timeout_in_ms)
    except Exception as exc:
        print(f"  Warning: task {task_uid} not confirmed ({type(exc).__name__})")
        return False
    status = _task_field(task, "status")
    if status != "succeeded":
        print(f"  Warning: task {task_uid} ended with status {status}")
    return status == "succeeded"


def reconcile_meili(conn, client, dry_run: bool = True) -> dict[str, list[str]]:
    index = client.index(JOBS_INDEX)
    meili_docs = fetch_meili_job_ids(index)
    print(f"Meili documents listed: {len(meili_docs)}")
    plan = plan_reconcile(meili_docs, _iter_db_rows(conn))
    conn.commit()
    for name, ids in plan.items():
        print(f"  {name}: {len(ids)}")
    if dry_run:
        print("Dry run: no changes made")
        return plan

    # Remove documents first, one acknowledged batch at a time.
    to_delete = [meili_safe_job_id(job_id) for job_id in plan["delete_removed_present"]] + plan["delete_orphans"]
    deleted_removed: list[str] = []
    for start in range(0, len(to_delete), DELETE_BATCH_SIZE):
        batch = to_delete[start:start + DELETE_BATCH_SIZE]
        task = index.delete_documents(ids=batch)
        print(f"  Delete batch {start // DELETE_BATCH_SIZE + 1} ({len(batch)} docs, task {task.task_uid})")
        if not _wait(client, task.task_uid):
            print("  Stopping deletes; rerun reconcile to continue")
            break
        batch_set = set(batch)
        deleted_removed.extend(
            job_id for job_id in plan["delete_removed_present"] if meili_safe_job_id(job_id) in batch_set
        )

    to_clear = plan["clear_removed_absent"] + plan["clear_active_absent"] + deleted_removed
    for start in range(0, len(to_clear), FLAG_BATCH_SIZE):
        mark_jobs_meili_deleted(conn, to_clear[start:start + FLAG_BATCH_SIZE])
    print(f"Cleared Meili flags on {len(to_clear)} jobs; deleted {len(deleted_removed)} removed jobs "
          f"and attempted {len(plan['delete_orphans'])} orphans")
    return plan


def cancel_enqueued_deletions(client) -> None:
    """Cancel queued document deletions, e.g. stale bulk deletes from failed loads."""
    task = client.cancel_tasks({"statuses": ["enqueued"], "types": ["documentDeletion"]})
    print(f"Cancel task submitted: {task.task_uid}")
    _wait(client, task.task_uid, timeout_in_ms=120000)
