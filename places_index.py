from __future__ import annotations

"""Sync canonical places from Postgres geo_places into a MeiliSearch places index."""

import argparse
import os

from dotenv import load_dotenv

load_dotenv()


def _admin1_key(country_code: str | None, admin1_code: str | None) -> str | None:
    if not country_code or not admin1_code:
        return None
    return f"{country_code}-{admin1_code}"


def build_place_doc(row: tuple) -> dict:
    (
        geoname_id,
        kind,
        canonical_name,
        ascii_name,
        display_name,
        country_code,
        country_name,
        admin1_code,
        admin1_name,
        latitude,
        longitude,
        population,
        feature_code,
        search_names,
    ) = row

    doc = {
        "id": str(geoname_id),
        "geoname_id": geoname_id,
        "kind": kind,
        "kind_priority": {"country": 0, "admin1": 1, "metro": 2, "locality": 3}.get(kind, 9),
        "canonical_name": canonical_name,
        "ascii_name": ascii_name,
        "display_name": display_name,
        "country_code": country_code,
        "country_name": country_name,
        "admin1_code": admin1_code,
        "admin1_name": admin1_name,
        "admin1_key": _admin1_key(country_code, admin1_code),
        "population": population,
        "feature_code": feature_code,
        "search_names": search_names or [],
        "supports_radius": kind in {"locality", "metro"},
    }
    if latitude is not None and longitude is not None:
        doc["_geo"] = {"lat": latitude, "lng": longitude}
    return doc


def sync_places_index(conn, meili_host: str, meili_key: str | None = None,
                      index_name: str = "places", limit: int | None = None,
                      batch_size: int = 5000):
    import meilisearch

    query = """
        SELECT geoname_id, kind, canonical_name, ascii_name, display_name,
               country_code, country_name, admin1_code, admin1_name,
               latitude, longitude, population, feature_code, search_names
        FROM geo_places
        WHERE kind IN ('country', 'admin1', 'locality')
        ORDER BY
            CASE kind
                WHEN 'country' THEN 0
                WHEN 'admin1' THEN 1
                ELSE 2
            END,
            population DESC NULLS LAST,
            display_name ASC
    """
    params: list[object] = []
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    docs = [build_place_doc(row) for row in rows]
    print(f"places_docs {len(docs)}")

    client = meilisearch.Client(meili_host, meili_key or os.environ.get("MEILISEARCH_MASTER_KEY", ""))
    index = client.index(index_name)
    index.update_filterable_attributes(["kind", "country_code", "admin1_key", "supports_radius"])
    index.update_searchable_attributes([
        "display_name",
        "canonical_name",
        "ascii_name",
        "search_names",
        "country_name",
        "admin1_name",
    ])
    index.update_sortable_attributes(["population"])
    index.update_ranking_rules([
        "words",
        "typo",
        "proximity",
        "attributeRank",
        "sort",
        "wordPosition",
        "exactness",
        "kind_priority:asc",
        "population:desc",
    ])

    for i in range(0, len(docs), batch_size):
        batch = docs[i:i + batch_size]
        task = index.add_documents(batch, primary_key="id")
        print(f"upserting_batch {i // batch_size + 1} size={len(batch)} task={task.task_uid}")
        try:
            client.wait_for_task(task.task_uid, timeout_in_ms=60000)
        except Exception as exc:
            print(f"wait_timeout task={task.task_uid} error={exc.__class__.__name__}")

    try:
        stats = index.get_stats()
        print(f"index_stats {stats.number_of_documents}")
    except Exception as exc:
        print(f"index_stats_unavailable error={exc.__class__.__name__}")


def main():
    from db import get_connection, init_schema

    parser = argparse.ArgumentParser(description="Sync canonical places into MeiliSearch")
    parser.add_argument("--meili-host", default=None, help="MeiliSearch host")
    parser.add_argument("--meili-key", default=None, help="MeiliSearch master key")
    parser.add_argument("--index-name", default="places", help="MeiliSearch index name")
    parser.add_argument("--limit", type=int, default=None, help="Optional document cap")
    parser.add_argument("--batch-size", type=int, default=5000, help="MeiliSearch upsert batch size")
    args = parser.parse_args()

    conn = get_connection()
    init_schema(conn)
    try:
        sync_places_index(
            conn,
            meili_host=args.meili_host or os.environ.get("MEILISEARCH_HOST", "http://localhost:7700"),
            meili_key=args.meili_key,
            index_name=args.index_name,
            limit=args.limit,
            batch_size=args.batch_size,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
