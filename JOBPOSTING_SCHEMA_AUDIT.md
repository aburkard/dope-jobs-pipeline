# Google JobPosting Audit

This document maps the current `dope-jobs-pipeline` data model to Google's supported `JobPosting` properties and separates fields into:

- already available
- derivable at render time
- genuinely missing

The goal is to avoid storing duplicate "presentation" fields when a static mapping or helper can derive them.

## Source of truth

Current pipeline storage has two real sources of truth:

- `pipeline_jobs.raw_json`
  - ATS-native job payload, normalized by scraper
- `pipeline_jobs.parsed_json`
  - semantic metadata extracted by the LLM and overlaid with ATS-native structured fields

Company metadata lives in `pipeline_companies`:

- `company_name`
- `domain`
- `logo_url`

For Google job pages, we should prefer this rule:

- store ATS-native facts in `raw_json`
- store semantic meaning in `parsed_json`
- derive Google-specific formatting at render time

We should not add dedicated stored fields for things like:

- Google `employmentType`
- Google `jobLocationType`
- Google `baseSalary.unitText`
- slugs
- JSON-LD blobs

Those are all deterministic mappings.

## Property Audit

### Required properties

| Google property | Current source | Status | Storage decision | Notes / action |
| --- | --- | --- | --- | --- |
| `title` | `pipeline_jobs.title` / `raw_json.title` | Available | Derive at render time from stored title | Use ATS title as-is except whitespace cleanup. Do not rewrite for SEO. |
| `description` | Greenhouse: `raw_json.content` HTML; Ashby: `raw_json.descriptionHtml`; Lever: only flattened plain text today; Jobvite: only flattened plain text today | Partially available | Keep canonical description in `raw_json`; render HTML from it | We should preserve richer HTML for Lever and Jobvite instead of only plain text. |
| `datePosted` | Greenhouse: `raw_json.first_published`; Ashby: `raw_json.publishedAt`; Lever API supports `createdAt` / `updatedAt` but scraper does not persist them yet; Jobvite currently missing | Partially available | Do not store a separate Google field; derive from raw ATS timestamps | Add missing ATS-native posted timestamp capture, especially Lever. Do not fall back to `first_seen_at` in schema output. |
| `hiringOrganization` | `pipeline_companies.company_name`, `domain`, `logo_url` | Available | Derive at render time | `name`, `sameAs`, and optional `logo` should come from company table. |
| `jobLocation` | `parsed_json.locations`; plus ATS-native location fields in Greenhouse/Ashby/Lever/Jobvite | Available but lossy | Derive at render time | For physical or hybrid jobs, emit `Place` / `PostalAddress` from parsed locations. Must include `addressCountry` when known. |

### Remote-job required/recommended properties

| Google property | Current source | Status | Storage decision | Notes / action |
| --- | --- | --- | --- | --- |
| `jobLocationType` | `parsed_json.office_type` | Available | Static mapping at render time | `office_type == "remote"` -> `TELECOMMUTE`. Do not store separately. |
| `applicantLocationRequirements` | No dedicated field today | Missing | Add new semantic field in `parsed_json` only if we can extract real geography | Current `remote_timezone_range` is not equivalent. We need geographic eligibility like countries/states. |

### Recommended properties

| Google property | Current source | Status | Storage decision | Notes / action |
| --- | --- | --- | --- | --- |
| `baseSalary` | `parsed_json.salary`, `salary_transparency`; ATS salary overlay already exists | Available | Static mapping at render time | Only emit when employer-provided/disclosed. Map period -> `HOUR/WEEK/MONTH/YEAR`. |
| `employmentType` | `parsed_json.job_type`; ATS overlay from Ashby / Lever already normalizes it | Available | Static mapping at render time | `full-time -> FULL_TIME`, `part-time -> PART_TIME`, `contract -> CONTRACTOR`, `internship -> INTERN`, `temporary -> TEMPORARY`, `freelance -> OTHER`. |
| `identifier` | `pipeline_jobs.id` and ATS-specific trailing id inside it | Available | Derive at render time | Use ATS job id as `value`; company name as `name`. No extra storage needed. |
| `validThrough` | No reliable cross-ATS field today | Missing / optional | Only store if genuinely sourced | Do not guess. If ATS provides expiry/app deadline, use it. Otherwise omit. |
| `directApply` | We link out to ATS apply pages | Usually not applicable | Omit by default | Google's definition is strict. An ATS redirect flow usually should not be marked direct apply. |

### Beta properties

| Google property | Current source | Status | Storage decision | Notes / action |
| --- | --- | --- | --- | --- |
| `educationRequirements` | `parsed_json.education_level` | Available | Static mapping at render time | No extra storage needed. |
| `experienceRequirements` | `parsed_json.years_experience` | Available | Static mapping at render time | Convert years -> months for Google's example shape if we choose to emit it. |

## ATS-specific audit

### Greenhouse

Already captured:

- title
- HTML description via `raw_json.content`
- canonical job URL via `raw_json.url`
- `first_published`
- `updated_at`
- structured departments / offices
- pay transparency data via `pay_input_ranges`

Gaps relative to Google job pages:

- no explicit normalized apply URL field
  - but `url` is usually sufficient as the apply destination
- remote eligibility geography is still not explicitly extracted

Recommendation:

- no schema changes needed for the basics
- use `first_published` as `datePosted`
- derive `description` HTML directly from `content`

### Ashby

Already captured:

- `descriptionHtml`
- `descriptionPlain`
- `jobUrl`
- `applyUrl`
- `publishedAt`
- structured `employmentType`
- structured `workplaceType`
- structured location fields
- structured compensation fields

Gaps relative to Google job pages:

- remote applicant geography is not normalized into a dedicated field
- no explicit expiry / `validThrough`

Recommendation:

- Ashby is already the cleanest source
- use `publishedAt` as `datePosted`
- use `applyUrl` as CTA, not `directApply`

### Lever

Currently captured:

- title
- plain description text
- `applyUrl`
- `hostedUrl`
- `workplaceType`
- commitment / department / team / locations

Missing but available from Lever:

- `createdAt`
- `updatedAt`
- richer structured description content

Source:

- Lever's posting docs list posting `createdAt` and `updatedAt` fields.

Recommendation:

- update scraper to persist `createdAt` and `updatedAt`
- preserve richer description HTML instead of only flattened text
- derive `datePosted` from `createdAt`

### Jobvite

Currently captured:

- title
- location
- job URL
- flattened plain-text description from the detail page

Missing or weak:

- rich HTML description is discarded
- no reliable posted date is stored
- no explicit structured apply URL distinct from the job URL

Recommendation:

- preserve `descriptionHtml` from the detail page
- inspect job pages for any machine-readable posted date; if present, store it in `raw_json`
- if Jobvite does not expose a trustworthy posted date, omit `datePosted` for those pages until we have a real source

## What we should not store

These should be computed when we build the job page or JSON-LD:

- job slug
- company slug
- Google `employmentType`
- Google `jobLocationType`
- Google `baseSalary` object
- Google `identifier` object
- Google `hiringOrganization` object
- final JSON-LD payload

Reason:

- they are deterministic transformations of already stored fields
- storing them would create drift and duplicate state

## What we probably do need to add

### 1. Preserve richer description HTML where available

Needed because Google wants the structured-data `description` to be a full HTML representation of the job.

Low-risk scraper changes:

- Lever: store a normalized `descriptionHtml`
- Jobvite: store a normalized `descriptionHtml`

Greenhouse and Ashby already have a usable HTML source.

### 2. Capture real posted timestamps where the ATS exposes them

Needed because `datePosted` is required.

Low-risk scraper changes:

- Lever: add `createdAt` and `updatedAt`
- Jobvite: investigate whether detail pages expose a published date in structured data or metadata

Do not use:

- `first_seen_at`
- `last_seen_at`

Those are pipeline dates, not employer posting dates.

### 3. Add geographic remote eligibility

Needed for fully remote jobs where Google expects `applicantLocationRequirements`.

This is the main genuinely missing semantic field today.

Candidate shape in `parsed_json`:

```json
{
  "applicant_location_requirements": [
    {"type": "country", "name": "United States"},
    {"type": "state", "name": "California, USA"}
  ]
}
```

Notes:

- this is not the same as `remote_timezone_range`
- it should only be populated when the job text or ATS metadata actually restricts geography
- it belongs in `parsed_json`, not `raw_json`, unless an ATS provides it structurally

### 4. Optional later: explicit application deadline

Only if we can source it truthfully.

If we do add it, keep it as a semantic field like `application_deadline` or ATS-native raw field, then derive Google `validThrough`.

Do not infer expiry dates.

## Render-time mapping plan

The right long-term shape is a helper that takes:

- `job row`
- `company row`

and returns:

- page model
- `JobPosting` JSON-LD payload

without introducing new persisted duplicate columns.

Suggested helper outputs:

- `title`
- `description_html`
- `date_posted`
- `apply_url`
- `hiring_organization`
- `job_locations`
- `job_location_type`
- `applicant_location_requirements`
- `employment_type`
- `base_salary`
- `identifier`

## Implementation priority

1. Preserve Lever and Jobvite HTML descriptions.
2. Capture Lever `createdAt` / `updatedAt`.
3. Investigate Jobvite posted-date availability.
4. Add semantic remote geography extraction.
5. Build a render-time `JobPosting` mapper instead of storing Google-shaped fields.

