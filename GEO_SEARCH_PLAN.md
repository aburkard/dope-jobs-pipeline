# Geo Search Plan

This document records the current geo-search decisions so we do not lose the contract between:

- `geo_places` in Postgres
- the `places` Meili index used for autocomplete
- the `jobs` Meili index used for filtering results
- the frontend UI behavior

## Goals

- Let users search near a place with autocomplete and typo tolerance.
- Treat `city`/`locality` searches differently from `state`/`country` searches.
- Keep remote eligibility separate from physical work location.
- Avoid brittle string matching for geographic filters.

## Core Decisions

### 1. Separate `places` autocomplete index

We will use a dedicated Meili `places` index, not the `jobs` index, for autocomplete.

Reasons:

- Meili gives us prefix search and typo tolerance out of the box.
- The source of truth is canonical `geo_places`, not noisy job-derived location strings.
- The autocomplete corpus should not depend on the currently indexed job subset.

### 2. Jobs need structured geo filter fields

The `jobs` Meili index must expose normalized geo fields beyond:

- `location`
- `locations_all`
- `_geo`

We also need exact-matchable fields:

- `work_geoname_ids`
- `work_country_codes`
- `work_admin1_keys`
- `applicant_country_codes`
- `applicant_admin1_keys`

These fields support deterministic filters for non-point places.

### 3. `admin1_code` is not globally unique

We must not filter on `admin1_code` alone.

Use:

- `admin1_key = "{country_code}-{admin1_code}"`

Examples:

- `US-CA`
- `CA-ON`

### 4. Search behavior depends on selected place kind

#### Locality / point-like place

Use:

- `_geoRadius(lat, lng, radius)`

Optional:

- `include remote` adds applicant-geography matches in the same country/admin1

#### Admin1 / state / province

Use exact filter:

- `work_admin1_keys = "{country_code}-{admin1_code}"`

Optional:

- `include remote` ORs in `applicant_admin1_keys = "{country_code}-{admin1_code}"`

#### Country

Use exact filter:

- `work_country_codes = "{country_code}"`

Optional:

- `include remote` ORs in `applicant_country_codes = "{country_code}"`

### 5. No distance sorting for v1

We only have city-level resolution for most work locations.

For v1:

- support radius filtering
- do not support sorting by nearest

### 6. Remote stays separate from physical work location

Remote eligibility and work location are different concepts.

Examples:

- remote eligible in `US`
- work location in `San Francisco`

We will not collapse them into one field.

### 7. Radius controls only for point-like places

If the selected place is:

- `locality`
- or later `metro`

show radius controls.

If the selected place is:

- `admin1`
- `country`

use exact matching only and hide radius controls.

## V1 Data Model

### `places` index

Suggested fields:

- `id`
- `display_name`
- `canonical_name`
- `kind`
- `country_code`
- `country_name`
- `admin1_code`
- `admin1_name`
- `admin1_key`
- `_geo`
- `population`
- `feature_code`
- `search_names`
- `supports_radius`

Notes:

- `_geo` is useful for point-like places and future proximity features.
- `supports_radius` should be `true` for `locality` and `false` for `admin1` / `country`.

### `jobs` index

Add:

- `locations_all`
- `work_geoname_ids`
- `work_country_codes`
- `work_admin1_keys`
- `applicant_country_codes`
- `applicant_admin1_keys`

## Edge Cases We Are Explicitly Handling

### Overlapping admin1 codes

`admin1_code` can overlap across countries. Always use `admin1_key`.

### Duplicate city names

Autocomplete must show full context, e.g.:

- `Springfield, Illinois, United States`

The selected place identity comes from the place doc, not the raw string.

### Multi-location jobs

A job can have:

- one primary `location`
- many `locations_all`

For v1 exact state/country filtering:

- match any stored work/admin/country key

For v1 radius search:

- use the primary `_geo` only

This is approximate for multi-location jobs and acceptable for v1.

### Remote applicant geography

Remote jobs should match only when eligibility overlaps the selected geography.

Example:

- searching near San Francisco with `include remote`
- should include `US-only` remote jobs
- should not include `Canada-only` remote jobs

### Region groups like `EMEA` / `APAC`

These are not part of v1 autocomplete.

They may be supported later as `region_group`, but they complicate semantics and should not block v1.

### Hybrid roles with broad geography

Examples like:

- `Remote - United States`

should be treated as broad geography, not a fake point.

## Phase Split

## Phase 1

- Add structured geo fields to `jobs`
- Create and sync the `places` Meili index
- Add frontend autocomplete UI
- Support:
  - locality radius
  - admin1 exact match
  - country exact match
  - separate `include remote` toggle

## Phase 2

- Improve radius handling for multi-location jobs
- Add `metro` support
- Consider `region_group` support
- Consider browser geolocation / “near me”
- Consider better geo ranking and map-like experiences
