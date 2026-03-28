from __future__ import annotations

"""Download and sync canonical geo_places rows from the official GeoNames dump."""

import argparse
import csv
import re
import unicodedata
import zipfile
from pathlib import Path

import requests

from db import get_connection, get_geo_place_counts, init_schema, upsert_geo_places

GEONAMES_BASE_URL = "https://download.geonames.org/export/dump"
GEONAMES_FILES = {
    "countryInfo.txt": f"{GEONAMES_BASE_URL}/countryInfo.txt",
    "admin1CodesASCII.txt": f"{GEONAMES_BASE_URL}/admin1CodesASCII.txt",
    "cities1000.zip": f"{GEONAMES_BASE_URL}/cities1000.zip",
}

COUNTRY_CODE_ALIASES = {
    "US": ["USA", "United States of America"],
    "GB": ["UK", "Great Britain", "Britain"],
    "AE": ["UAE"],
}


def normalize_geo_text(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = ascii_text.lower()
    ascii_text = re.sub(r"[^a-z0-9]+", " ", ascii_text)
    return re.sub(r"\s+", " ", ascii_text).strip()


def _unique(values: list[str | None]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _country_aliases(country_code: str | None, country_name: str | None) -> list[str]:
    aliases = [country_name, country_code]
    if country_code:
        aliases.extend(COUNTRY_CODE_ALIASES.get(country_code, []))
    return _unique(aliases)


def _admin1_aliases(admin1_code: str | None, admin1_name: str | None) -> list[str]:
    aliases = [admin1_name, admin1_code]
    if admin1_name:
        stripped = re.sub(r"\b(city|province|region|state|prefecture|district)\b", "", admin1_name, flags=re.IGNORECASE)
        stripped = re.sub(r"\s+", " ", stripped).strip(" ,")
        if stripped:
            aliases.append(stripped)
    return _unique(aliases)


def build_display_name(name: str, country_name: str | None = None, admin1_name: str | None = None) -> str:
    parts = [name]
    if admin1_name and normalize_geo_text(admin1_name) != normalize_geo_text(name):
        parts.append(admin1_name)
    if country_name and normalize_geo_text(country_name) not in {normalize_geo_text(part) for part in parts}:
        parts.append(country_name)
    return ", ".join(parts)


def build_search_names(name: str, ascii_name: str | None, display_name: str,
                       country_code: str | None = None, country_name: str | None = None,
                       admin1_code: str | None = None, admin1_name: str | None = None,
                       extra_names: list[str] | None = None) -> list[str]:
    variants = [
        name,
        ascii_name,
        display_name,
        *(extra_names or []),
    ]

    country_aliases = _country_aliases(country_code, country_name)
    admin1_aliases = _admin1_aliases(admin1_code, admin1_name)

    for country in country_aliases:
        variants.extend([
            f"{name}, {country}",
            f"{name} {country}",
        ])
    for admin1 in admin1_aliases:
        variants.extend([
            f"{name}, {admin1}",
            f"{name} {admin1}",
        ])
        for country in country_aliases:
            variants.extend([
                f"{name}, {admin1}, {country}",
                f"{name} {admin1} {country}",
            ])

    normalized = [normalize_geo_text(value) for value in variants]
    return _unique([value for value in normalized if value])


def build_country_row(fields: list[str]) -> dict | None:
    if len(fields) < 17:
        return None
    country_code = fields[0].strip()
    country_name = fields[4].strip()
    geoname_id = fields[16].strip()
    if not country_code or not country_name or not geoname_id:
        return None
    return {
        "geoname_id": int(geoname_id),
        "kind": "country",
        "canonical_name": country_name,
        "ascii_name": country_name,
        "display_name": country_name,
        "country_code": country_code,
        "country_name": country_name,
        "admin1_code": None,
        "admin1_name": None,
        "latitude": None,
        "longitude": None,
        "population": int(fields[7]) if fields[7] else None,
        "timezone": None,
        "feature_class": "A",
        "feature_code": "PCLI",
        "search_names": build_search_names(
            country_name,
            country_name,
            country_name,
            country_code=country_code,
            country_name=country_name,
            extra_names=COUNTRY_CODE_ALIASES.get(country_code, []),
        ),
    }


def build_admin1_row(fields: list[str], country_names: dict[str, str]) -> dict | None:
    if len(fields) < 4 or "." not in fields[0]:
        return None
    code = fields[0].strip()
    country_code, admin1_code = code.split(".", 1)
    name = fields[1].strip()
    ascii_name = fields[2].strip() or None
    geoname_id = fields[3].strip()
    if not name or not geoname_id:
        return None
    country_name = country_names.get(country_code)
    display_name = build_display_name(name, country_name=country_name)
    return {
        "geoname_id": int(geoname_id),
        "kind": "admin1",
        "canonical_name": name,
        "ascii_name": ascii_name,
        "display_name": display_name,
        "country_code": country_code,
        "country_name": country_name,
        "admin1_code": admin1_code,
        "admin1_name": name,
        "latitude": None,
        "longitude": None,
        "population": None,
        "timezone": None,
        "feature_class": "A",
        "feature_code": "ADM1",
        "search_names": build_search_names(
            name,
            ascii_name,
            display_name,
            country_code=country_code,
            country_name=country_name,
            admin1_code=admin1_code,
            admin1_name=name,
        ),
    }


def build_city_row(fields: list[str], country_names: dict[str, str], admin1_names: dict[tuple[str, str], str]) -> dict | None:
    if len(fields) < 19:
        return None
    feature_class = fields[6].strip()
    if feature_class != "P":
        return None

    geoname_id = fields[0].strip()
    name = fields[1].strip()
    ascii_name = fields[2].strip() or None
    latitude = fields[4].strip()
    longitude = fields[5].strip()
    feature_code = fields[7].strip() or None
    country_code = fields[8].strip() or None
    admin1_code = fields[10].strip() or None
    population = fields[14].strip()
    timezone = fields[17].strip() or None

    if not geoname_id or not name:
        return None

    country_name = country_names.get(country_code or "")
    admin1_name = admin1_names.get((country_code or "", admin1_code or ""))
    display_name = build_display_name(name, country_name=country_name, admin1_name=admin1_name)
    alternate_names = []
    for alt in fields[3].split(','):
        alt = alt.strip()
        if not alt:
            continue
        normalized_alt = normalize_geo_text(alt)
        if not normalized_alt:
            continue
        alternate_names.append(alt)
        if len(_unique(alternate_names)) >= 12:
            break
    alternate_names = _unique(alternate_names)
    return {
        "geoname_id": int(geoname_id),
        "kind": "locality",
        "canonical_name": name,
        "ascii_name": ascii_name,
        "display_name": display_name,
        "country_code": country_code,
        "country_name": country_name,
        "admin1_code": admin1_code,
        "admin1_name": admin1_name,
        "latitude": float(latitude) if latitude else None,
        "longitude": float(longitude) if longitude else None,
        "population": int(population) if population else None,
        "timezone": timezone,
        "feature_class": feature_class,
        "feature_code": feature_code,
        "search_names": build_search_names(
            name,
            ascii_name,
            display_name,
            country_code=country_code,
            country_name=country_name,
            admin1_code=admin1_code,
            admin1_name=admin1_name,
            extra_names=alternate_names,
        ),
    }


def download_geonames_files(cache_dir: Path, force: bool = False) -> dict[str, Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    for name, url in GEONAMES_FILES.items():
        path = cache_dir / name
        paths[name] = path
        if path.exists() and not force:
            continue
        print(f"Downloading {url} -> {path}")
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        with path.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    fh.write(chunk)
    return paths


def load_country_rows(path: Path) -> tuple[list[dict], dict[str, str]]:
    rows: list[dict] = []
    country_names: dict[str, str] = {}
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="	")
        for fields in reader:
            if not fields or fields[0].startswith("#"):
                continue
            row = build_country_row(fields)
            if not row:
                continue
            rows.append(row)
            country_names[row["country_code"]] = row["country_name"]
    return rows, country_names


def load_admin1_rows(path: Path, country_names: dict[str, str]) -> tuple[list[dict], dict[tuple[str, str], str]]:
    rows: list[dict] = []
    admin1_names: dict[tuple[str, str], str] = {}
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="	")
        for fields in reader:
            if not fields or fields[0].startswith("#"):
                continue
            row = build_admin1_row(fields, country_names)
            if not row:
                continue
            rows.append(row)
            admin1_names[(row["country_code"], row["admin1_code"])] = row["admin1_name"]
    return rows, admin1_names


def iter_city_rows(path: Path, country_names: dict[str, str], admin1_names: dict[tuple[str, str], str], limit: int | None = None):
    with zipfile.ZipFile(path) as zf:
        member = next(name for name in zf.namelist() if name.endswith('.txt'))
        with zf.open(member) as fh:
            reader = csv.reader((line.decode('utf-8') for line in fh), delimiter='	')
            count = 0
            for fields in reader:
                row = build_city_row(fields, country_names, admin1_names)
                if not row:
                    continue
                yield row
                count += 1
                if limit is not None and count >= limit:
                    return


def sync_geo_places(cache_dir: Path, force_download: bool = False, city_limit: int | None = None):
    paths = download_geonames_files(cache_dir, force=force_download)
    conn = get_connection()
    try:
        init_schema(conn)

        country_rows, country_names = load_country_rows(paths['countryInfo.txt'])
        upsert_geo_places(conn, country_rows)
        print(f"Loaded {len(country_rows)} country rows")

        admin1_rows, admin1_names = load_admin1_rows(paths['admin1CodesASCII.txt'], country_names)
        upsert_geo_places(conn, admin1_rows)
        print(f"Loaded {len(admin1_rows)} admin1 rows")

        city_batch: list[dict] = []
        city_count = 0
        for row in iter_city_rows(paths['cities1000.zip'], country_names, admin1_names, limit=city_limit):
            city_batch.append(row)
            if len(city_batch) >= 2000:
                upsert_geo_places(conn, city_batch)
                city_count += len(city_batch)
                print(f"Loaded {city_count} locality rows")
                city_batch = []
        if city_batch:
            upsert_geo_places(conn, city_batch)
            city_count += len(city_batch)
            print(f"Loaded {city_count} locality rows")

        print(f"Geo place counts: {get_geo_place_counts(conn)}")
    finally:
        conn.close()


def lookup_geo_places(query: str, limit: int = 10, kind: str | None = None, country_code: str | None = None):
    normalized = normalize_geo_text(query)
    if not normalized:
        raise ValueError("query must not be empty")

    conn = get_connection()
    try:
        init_schema(conn)
        sql = """
            SELECT geoname_id, kind, display_name, country_code, admin1_code, population
            FROM geo_places
            WHERE search_names @> ARRAY[%s]
        """
        params: list[object] = [normalized]
        if kind:
            sql += " AND kind = %s"
            params.append(kind)
        if country_code:
            sql += " AND country_code = %s"
            params.append(country_code)
        sql += """
            ORDER BY
                CASE kind
                    WHEN 'locality' THEN 0
                    WHEN 'admin1' THEN 1
                    ELSE 2
                END,
                population DESC NULLS LAST,
                display_name ASC
            LIMIT %s
        """
        params.append(limit)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    finally:
        conn.close()

    for row in rows:
        print(f"{row[0]}	{row[1]}	{row[2]}	country={row[3]}	admin1={row[4]}	population={row[5]}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync canonical geo_places rows from GeoNames")
    sub = parser.add_subparsers(dest="command", required=True)

    sync_parser = sub.add_parser("sync", help="Download GeoNames dumps and load geo_places")
    sync_parser.add_argument("--cache-dir", default="/tmp/dopejobs-geonames")
    sync_parser.add_argument("--force-download", action="store_true")
    sync_parser.add_argument("--city-limit", type=int)

    lookup_parser = sub.add_parser("lookup", help="Lookup a normalized place in geo_places")
    lookup_parser.add_argument("query")
    lookup_parser.add_argument("--limit", type=int, default=10)
    lookup_parser.add_argument("--kind", choices=["country", "admin1", "locality"])
    lookup_parser.add_argument("--country-code")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "sync":
        sync_geo_places(Path(args.cache_dir), force_download=args.force_download, city_limit=args.city_limit)
    elif args.command == "lookup":
        lookup_geo_places(args.query, limit=args.limit, kind=args.kind, country_code=args.country_code)
    else:
        parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
