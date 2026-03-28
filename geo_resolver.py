from __future__ import annotations

"""Resolve denormalized parsed job geography against canonical geo_places rows."""

import re
from copy import deepcopy

from geo_places import normalize_geo_text
from parse import _country_code_from_value


REMOTEISH_RE = re.compile(r"\b(remote|hybrid|on-?site|in[- ]office)\b", re.IGNORECASE)


def _clean_str(value) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _normalize_country_code(value) -> str | None:
    cleaned = _clean_str(value)
    if not cleaned:
        return None
    return _country_code_from_value(cleaned) or cleaned


def _location_identity(location: dict) -> tuple:
    geoname_id = location.get("geoname_id")
    if geoname_id:
        return ("geoname", geoname_id)

    city = normalize_geo_text(_clean_str(location.get("city")) or "")
    state = normalize_geo_text(_clean_str(location.get("state")) or "")
    country_code = _normalize_country_code(location.get("country_code"))
    if city or state or country_code:
        return ("structured", city, state, country_code)

    label = normalize_geo_text(_clean_str(location.get("label")) or "")
    return ("label", label)


def _dedupe_resolved_locations(locations: list[dict]) -> list[dict]:
    seen = set()
    deduped = []
    for location in locations:
        key = _location_identity(location)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(location)
    return deduped


class GeoResolver:
    def __init__(self, conn):
        self.conn = conn
        self._cache: dict[tuple[str, str | None, str | None], dict | None] = {}

    def _lookup_candidate(self, candidate: str, *, kind: str | None = None,
                          country_code: str | None = None) -> dict | None:
        normalized = normalize_geo_text(candidate)
        if not normalized:
            return None
        cache_key = (normalized, kind, country_code)
        if cache_key in self._cache:
            return self._cache[cache_key]

        query = """
            SELECT geoname_id, kind, canonical_name, display_name,
                   country_code, country_name, admin1_code, admin1_name,
                   latitude, longitude, population
            FROM geo_places
            WHERE search_names @> ARRAY[%s]
        """
        params: list[object] = [normalized]
        if kind:
            query += " AND kind = %s"
            params.append(kind)
        if country_code:
            query += " AND country_code = %s"
            params.append(country_code)

        query += """
            ORDER BY
                CASE kind
                    WHEN 'locality' THEN 0
                    WHEN 'admin1' THEN 1
                    ELSE 2
                END,
                population DESC NULLS LAST,
                display_name ASC
            LIMIT 1
        """

        with self.conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()

        match = None
        if row:
            match = {
                "geoname_id": row[0],
                "kind": row[1],
                "canonical_name": row[2],
                "display_name": row[3],
                "country_code": row[4],
                "country_name": row[5],
                "admin1_code": row[6],
                "admin1_name": row[7],
                "latitude": row[8],
                "longitude": row[9],
                "population": row[10],
            }

        self._cache[cache_key] = match
        return match

    def _resolve_country(self, value: str | None, country_code: str | None = None) -> dict | None:
        for candidate in [value, country_code]:
            if not candidate:
                continue
            match = self._lookup_candidate(candidate, kind="country")
            if match:
                return match
        return None

    def resolve_work_location(self, location: dict, office_type: str | None = None) -> dict | None:
        if not isinstance(location, dict):
            return None

        resolved = deepcopy(location)
        label = _clean_str(resolved.get("label"))
        city = _clean_str(resolved.get("city"))
        state = _clean_str(resolved.get("state"))
        country_code = _normalize_country_code(resolved.get("country_code"))

        if label and REMOTEISH_RE.search(label) and not city:
            return None
        if office_type == "remote" and not city and not state and country_code:
            return None

        match = None

        exact_candidates: list[tuple[str, str | None, str | None]] = []
        if label:
            exact_candidates.append((label, None, country_code))
        if city and state and country_code:
            exact_candidates.append((f"{city}, {state}, {country_code}", "locality", country_code))
        if city and state:
            exact_candidates.append((f"{city}, {state}", "locality", country_code))
        if city and country_code:
            exact_candidates.append((f"{city}, {country_code}", "locality", country_code))
        if city:
            exact_candidates.append((city, "locality", country_code))
        if state and country_code:
            exact_candidates.append((f"{state}, {country_code}", "admin1", country_code))
        if state:
            exact_candidates.append((state, "admin1", country_code))
        if country_code and not city:
            exact_candidates.append((country_code, "country", None))

        seen = set()
        for candidate, kind, cc in exact_candidates:
            key = (candidate, kind, cc)
            if key in seen:
                continue
            seen.add(key)
            match = self._lookup_candidate(candidate, kind=kind, country_code=cc)
            if match:
                break

        if not match:
            return resolved

        resolved["geoname_id"] = match["geoname_id"]
        resolved["label"] = match["display_name"]
        resolved["country_code"] = match["country_code"] or country_code

        if match["kind"] == "locality":
            resolved["city"] = match["canonical_name"]
            resolved["state"] = match["admin1_name"] or state
            resolved["lat"] = match["latitude"]
            resolved["lng"] = match["longitude"]
        elif match["kind"] == "admin1":
            resolved["state"] = match["canonical_name"]
            if not city:
                resolved["city"] = None
            if match["latitude"] is not None:
                resolved["lat"] = match["latitude"]
            if match["longitude"] is not None:
                resolved["lng"] = match["longitude"]
        elif match["kind"] == "country":
            if not city:
                resolved["city"] = None
                resolved["state"] = None

        return resolved

    def resolve_applicant_requirement(self, requirement: dict) -> dict:
        if not isinstance(requirement, dict):
            return requirement

        resolved = deepcopy(requirement)
        scope = _clean_str(resolved.get("scope"))
        name = _clean_str(resolved.get("name"))
        country_code = _normalize_country_code(resolved.get("country_code"))
        region = _clean_str(resolved.get("region"))

        if scope == "region_group":
            return resolved

        match = None
        if scope == "country":
            match = self._resolve_country(name, country_code)
            if match:
                resolved["name"] = match["canonical_name"]
                resolved["country_code"] = match["country_code"]
                resolved["geoname_id"] = match["geoname_id"]
            return resolved

        if scope == "state":
            for candidate in [name, region]:
                if not candidate:
                    continue
                match = self._lookup_candidate(candidate, kind="admin1", country_code=country_code)
                if match:
                    resolved["name"] = match["canonical_name"]
                    resolved["region"] = match["canonical_name"]
                    resolved["country_code"] = match["country_code"] or country_code
                    resolved["geoname_id"] = match["geoname_id"]
                    return resolved
            return resolved

        if scope == "city":
            city_candidates = []
            if name and region and country_code:
                city_candidates.append((f"{name}, {region}, {country_code}", "locality", country_code))
            if name and country_code:
                city_candidates.append((f"{name}, {country_code}", "locality", country_code))
            if name:
                city_candidates.append((name, "locality", country_code))
            for candidate, kind, cc in city_candidates:
                match = self._lookup_candidate(candidate, kind=kind, country_code=cc)
                if match:
                    resolved["name"] = match["canonical_name"]
                    resolved["region"] = match["admin1_name"] or region
                    resolved["country_code"] = match["country_code"] or country_code
                    resolved["geoname_id"] = match["geoname_id"]
                    return resolved
            return resolved

        return resolved

    def resolve_parsed_geo(self, parsed_json: dict) -> dict:
        parsed = deepcopy(parsed_json)
        office_type = _clean_str(parsed.get("office_type"))

        locations = []
        for location in parsed.get("locations", []) or []:
            resolved = self.resolve_work_location(location, office_type=office_type)
            if resolved:
                locations.append(resolved)
        parsed["locations"] = _dedupe_resolved_locations(locations)

        requirements = []
        for requirement in parsed.get("applicant_location_requirements", []) or []:
            requirements.append(self.resolve_applicant_requirement(requirement))
        parsed["applicant_location_requirements"] = requirements

        return parsed
