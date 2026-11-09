"""Meili index profiles: which document vectors, stored fields and index settings a load writes.

- "pplx512" (default): today's `jobs` index. pplx-embed-v1-0.6b 512d vectors, full documents,
  settings as configured by pipeline.configure_jobs_index; embedders managed outside the pipeline.
- "v2": the voyage/student index. voyage-4-lite 1024d document vectors projected to 256d with the
  pinned clean projection P (models/proj_clean_voy4_1024_to_256.npy), slim documents, lean
  settings (proximityPrecision byAttribute, granular filterable attributes, posted_at_ts sort) and a
  composite REST embedder with no DistributionShift. Queries are embedded by the box proxy's
  /openai-search-tiny route (the tiny student, aburkard/dope-jobs PR 1) into the same 256d space.

The profile is chosen per run with PIPELINE_INDEX_PROFILE (load.yml: repo variable, or the dispatch
input). Never write "v2" vectors into a "pplx512" index or the reverse: Meili cannot detect a
vector-space mismatch. Changing anything in a profile's vector space means a new index plus a swap.
"""
import os

PROFILE_ENV = "PIPELINE_INDEX_PROFILE"
DEFAULT_PROFILE = "pplx512"
PROFILES = ("pplx512", "v2")


def active_profile() -> str:
    profile = os.environ.get(PROFILE_ENV) or DEFAULT_PROFILE
    if profile not in PROFILES:
        raise ValueError(f"{PROFILE_ENV}={profile!r}; expected one of {PROFILES}")
    return profile


# Stored fields the public app never reads from Meili (verified against dopejobs-front main with the
# web-app session, 2026-11-08). They stay in Postgres. `ats_type` is read only by /mockups.
V2_DROP_FIELDS = (
    "_geojson", "salary_fx_currency", "salary_fx_usd_per_unit", "salary_fx_as_of", "reports_to",
    "is_enriched", "industry", "salary_transparency", "years_experience_min", "years_experience_max",
    "posted_at", "ats_type",
)


def slim_doc(doc: dict) -> dict:
    return {key: value for key, value in doc.items() if key not in V2_DROP_FIELDS}


# Filter/facet/sort needs of dopejobs-front (23 attributes + distinct + geo), per the web-app session.
_V2_EQUALITY = [
    "id", "office_type", "experience_level", "industry_tags", "cool_factor", "years_experience_buckets",
    "education_level", "job_type", "visa_sponsorship", "company_stage", "equity_offered", "is_manager",
    "work_geoname_ids", "work_country_codes", "work_admin1_keys", "applicant_country_codes",
    "applicant_admin1_keys", "job_group",
]
_V2_FACET_SEARCH = ["vibe_tags", "benefits_categories"]
_V2_COMPARISON = ["salary_annual_min_usd", "salary_annual_max_usd", "posted_at_ts"]
V2_SEARCHABLE = [
    "title", "tagline", "company", "description", "location", "locations_all",
    "hard_skills", "soft_skills", "benefits_highlights",
]

# Must stay byte-identical to embeddings.render_document_text (only used if a document arrives
# without vectors and Meili has to embed it through the indexing route).
DOCUMENT_TEMPLATE = (
    "{% if doc.title %}Job title: {{ doc.title }}.{% endif %}\n"
    "{% if doc.company %} Company: {{ doc.company }}.{% endif %}\n"
    "{% if doc.office_type %} Work setup: {{ doc.office_type }}.{% endif %}\n"
    "{% if doc.tagline %} Summary: {{ doc.tagline | truncatewords: 16 }}.{% endif %}\n"
    "{% if doc.description %} Description: {{ doc.description | truncatewords: 90 }}{% endif %}"
)
V2_DIMENSIONS = 256
_OPENAI_SHAPED = {
    "request": {"input": ["{{text}}", "{{..}}"]},
    "response": {"data": [{"embedding": "{{embedding}}"}, "{{..}}"]},
}


def v2_embedder() -> dict:
    """Composite REST embedder for the v2 index.

    source "rest" on purpose: an `openAi` source with an OpenAI model name makes Meili apply that
    model's built-in DistributionShift (this is what prod's pplx index silently has). The gate
    measured v2 best with no shift, so `distribution` is explicitly null.
    """
    search_url = os.environ.get("MEILI_V2_SEARCH_EMBED_URL", "http://127.0.0.1:8087/openai-search-tiny/v1/embeddings")
    index_url = os.environ.get("MEILI_V2_INDEX_EMBED_URL", "http://127.0.0.1:8087/openai-index-tiny/v1/embeddings")
    return {
        "source": "composite",
        "distribution": None,
        "searchEmbedder": {"source": "rest", "url": search_url, "dimensions": V2_DIMENSIONS, **_OPENAI_SHAPED},
        "indexingEmbedder": {
            "source": "rest", "url": index_url, "dimensions": V2_DIMENSIONS, **_OPENAI_SHAPED,
            "documentTemplate": DOCUMENT_TEMPLATE, "documentTemplateMaxBytes": 8000,
        },
    }


def v2_settings() -> dict:
    """Full settings for a new v2 index (applied once, when the index is created / fully reloaded)."""
    return {
        "searchableAttributes": V2_SEARCHABLE,
        "filterableAttributes": [
            {"attributePatterns": _V2_EQUALITY,
             "features": {"facetSearch": False, "filter": {"equality": True, "comparison": False}}},
            {"attributePatterns": _V2_FACET_SEARCH,
             "features": {"facetSearch": True, "filter": {"equality": True, "comparison": False}}},
            {"attributePatterns": _V2_COMPARISON,
             "features": {"facetSearch": False, "filter": {"equality": True, "comparison": True}}},
            "_geo",
        ],
        "sortableAttributes": ["posted_at_ts"],
        "proximityPrecision": "byAttribute",
        "pagination": {"maxTotalHits": 500000},
        "embedders": {"default": v2_embedder()},
    }
