"""
Modernized job posting metadata extraction using LLMs.

Supports multiple backends:
  - OpenAI-compatible API (OpenAI, LM Studio, vLLM)
  - Local via mlx-lm batch_generate

Usage:
  uv run python parse.py data/raw/greenhouse.jsonl.bz2 --backend openai --base-url http://10.0.0.158:1234/v1 --model qwen/qwen3.5-35b-a3b --batch-size 4
  uv run python parse.py data/raw/greenhouse.jsonl.bz2 --backend local --model mlx-community/Qwen3.5-35B-A3B-4bit --batch-size 16
"""

from __future__ import annotations

import argparse
import bz2
import json
import os
import re
import sys
import time
from functools import lru_cache
from enum import Enum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field
from utils.html_utils import remove_html_markup


# ---------------------------------------------------------------------------
# Enums & Literals
# ---------------------------------------------------------------------------

INDUSTRY_VALUES = [
    "ai_ml",
    "developer_tools",
    "cloud_data_infra",
    "enterprise_software",
    "cybersecurity",
    "payments_banking",
    "investing_trading",
    "insurance",
    "crypto_web3",
    "healthcare_services",
    "biotech_life_sciences",
    "pharma",
    "education_edtech",
    "consumer_social",
    "media_entertainment",
    "gaming",
    "advertising_marketing",
    "commerce_marketplaces",
    "consumer_goods_brands",
    "food_beverage",
    "travel_hospitality",
    "climate_sustainability",
    "energy_utilities",
    "transportation_logistics",
    "manufacturing_industrials",
    "robotics_autonomy",
    "semiconductors_hardware",
    "space_aerospace",
    "defense_public_safety",
    "government_public_sector",
    "real_estate_proptech",
    "construction_built_environment",
    "telecommunications_networking",
    "agriculture",
    "legal",
    "consulting_professional_services",
    "nonprofit_philanthropy",
    "staffing_recruiting",
    "bpo_outsourcing",
    "other",
]
INDUSTRY_VALUE_SET = set(INDUSTRY_VALUES)
INDUSTRY_ORDER = {value: index for index, value in enumerate(INDUSTRY_VALUES)}

Industry = Enum("Industry", {value: value for value in INDUSTRY_VALUES}, type=str)


class VibeTag(str, Enum):
    mission_driven = "mission_driven"
    high_growth = "high_growth"
    small_team = "small_team"
    cutting_edge_tech = "cutting_edge_tech"
    strong_culture = "strong_culture"
    high_autonomy = "high_autonomy"
    work_life_balance = "work_life_balance"
    well_funded = "well_funded"
    public_benefit = "public_benefit"
    creative_role = "creative_role"
    data_intensive = "data_intensive"
    global_team = "global_team"
    diverse_inclusive = "diverse_inclusive"
    fast_paced = "fast_paced"
    customer_facing = "customer_facing"
    research_focused = "research_focused"


class BenefitCategory(str, Enum):
    health = "health"
    dental = "dental"
    vision = "vision"
    life_insurance = "life_insurance"
    disability = "disability"
    four_01k = "401k"
    pension = "pension"
    equity_comp = "equity_comp"
    bonus = "bonus"
    unlimited_pto = "unlimited_pto"
    generous_pto = "generous_pto"
    parental_leave = "parental_leave"
    remote_stipend = "remote_stipend"
    home_office = "home_office"
    relocation = "relocation"
    learning_budget = "learning_budget"
    tuition_reimbursement = "tuition_reimbursement"
    gym_fitness = "gym_fitness"
    wellness = "wellness"
    meals = "meals"
    commuter = "commuter"
    mental_health = "mental_health"
    childcare = "childcare"
    pet_friendly = "pet_friendly"
    sabbatical = "sabbatical"
    stock_purchase = "stock_purchase"


class VisaType(str, Enum):
    h1b = "h1b"
    h1b_transfer = "h1b_transfer"
    o1 = "o1"
    l1 = "l1"
    j1 = "j1"
    green_card = "green_card"
    other = "other"


OfficeType = Literal["remote", "hybrid", "onsite"]
JobType = Literal["full-time", "part-time", "contract", "internship", "temporary", "freelance"]
ExperienceLevel = Literal["entry", "mid", "senior", "staff", "principal", "executive"]
CompanyStage = Literal[
    "pre-seed", "seed", "series-a", "series-b", "series-c-plus",
    "public", "bootstrapped", "government", "nonprofit",
]
SalaryPeriod = Literal["hourly", "weekly", "monthly", "annually"]
EducationLevel = Literal["none", "high-school", "bachelors", "masters", "phd"]
SalaryTransparency = Literal["full_range", "minimum_only", "not_disclosed"]
VisaSponsorship = Literal["yes", "no", "unknown"]


# ---------------------------------------------------------------------------
# Nested models
# ---------------------------------------------------------------------------

class Location(BaseModel):
    label: str | None = Field(None, description="Raw human-readable location label")
    geoname_id: int | None = Field(None, description="Canonical GeoNames place ID when resolved")
    city: str | None = None
    state: str | None = None
    country_code: str | None = Field(None, description="ISO 3166-1 alpha-2 country code")
    lat: float | None = Field(None, description="Approximate latitude, best-effort")
    lng: float | None = Field(None, description="Approximate longitude, best-effort")


class ApplicantLocationRequirement(BaseModel):
    scope: Literal["country", "state", "city", "region_group"]
    name: str
    geoname_id: int | None = Field(None, description="Canonical GeoNames place ID when resolved")
    country_code: str | None = Field(None, description="ISO 3166-1 alpha-2 country code when known")
    region: str | None = Field(None, description="Administrative region/state/province when known")


class Salary(BaseModel):
    min: float | None = None
    max: float | None = None
    currency: str = Field("USD", description="ISO 4217 currency code")
    period: SalaryPeriod = "annually"


class Equity(BaseModel):
    offered: bool = False
    min_pct: float | None = Field(None, description="Minimum equity percentage if disclosed")
    max_pct: float | None = Field(None, description="Maximum equity percentage if disclosed")


class MinMax(BaseModel):
    min: int | None = None
    max: int | None = None


class TimezoneRange(BaseModel):
    earliest: str = Field(description="e.g. UTC-8")
    latest: str = Field(description="e.g. UTC+1")


# ---------------------------------------------------------------------------
# Main schema
# ---------------------------------------------------------------------------

class JobMetadata(BaseModel):
    """Structured metadata extracted from a job posting."""

    # Tier 1: Core
    tagline: str = Field(description="One catchy sentence describing what makes this job interesting. Must be extracted or derived from the posting, not generic.")
    locations: list[Location] = Field(default_factory=list, description="Job locations. For remote jobs with no specific location, use an empty list.")
    applicant_location_requirements: list[ApplicantLocationRequirement] = Field(
        default_factory=list,
        description="For remote jobs, geographic restrictions on where the applicant may be based. Leave empty if unrestricted or unknown.",
    )
    salary: Salary | None = Field(None, description="Compensation range if mentioned. Set to null if not disclosed.")
    office_type: OfficeType = Field(description="Whether the job is remote, hybrid, or onsite")
    hybrid_days: int | None = Field(None, description="Days per week in office if hybrid. Null otherwise.")
    job_type: JobType = Field(description="Employment type")
    experience_level: ExperienceLevel = Field(description="Seniority level of the role")
    is_manager: bool = Field(description="Is this a people management role?")
    industry_primary: Industry = Field(description="Primary industry of the company")
    industry_tags: list[Industry] = Field(
        default_factory=list,
        description="Additional applicable industries from the same enum list, excluding the primary industry when possible.",
    )
    industry_other_hint: str | None = Field(
        None,
        description="Short freeform hint only when industry_primary is 'other' and none of the enum values fit well.",
    )
    hard_skills: list[str] = Field(default_factory=list, description="Technical and domain-specific skills required: programming languages, tools, frameworks, domain knowledge")
    soft_skills: list[str] = Field(default_factory=list, description="Interpersonal and transferable skills: communication, leadership, teamwork, problem-solving")

    # Tier 2: Differentiating
    cool_factor: Literal["boring", "standard", "interesting", "compelling", "exceptional"] = Field(
        description=(
            "How interesting/desirable is this job? "
            "boring: routine/generic role. standard: normal job, nothing special. "
            "interesting: notable role or company. compelling: strong mission, unique tech, standout culture. "
            "exceptional: dream job territory, rare opportunity. Most jobs are standard or interesting."
        ),
    )
    vibe_tags: list[VibeTag] = Field(default_factory=list, description="What makes this job/company interesting? Only include tags with clear evidence in the posting.")
    visa_sponsorship: VisaSponsorship = Field("unknown", description="Does the company offer visa sponsorship?")
    visa_sponsorship_types: list[VisaType] | None = Field(None, description="Specific visa types sponsored, if mentioned. Only populate when visa_sponsorship is 'yes'.")
    equity: Equity = Field(default_factory=Equity, description="Equity compensation details")
    company_stage: CompanyStage | None = Field(None, description="Company funding stage or type. Null if not determinable.")
    company_size: MinMax | None = Field(None, description="Approximate company size in employees, if mentioned or inferable")
    team_size: MinMax | None = Field(None, description="Size of the specific team this role joins, if mentioned")
    reports_to: str | None = Field(None, description="Who this role reports to, e.g. 'VP of Engineering'. Null if not mentioned.")
    benefits_categories: list[BenefitCategory] = Field(default_factory=list, description="Categorized benefits offered")
    benefits_highlights: list[str] = Field(default_factory=list, description="Up to 3 standout/unusual benefits. Only list perks that are ABOVE AVERAGE or UNUSUAL. Do not list standard health/dental/vision.")
    remote_timezone_range: TimezoneRange | None = Field(None, description="For remote jobs, the allowed timezone range (e.g. UTC-8 to UTC+1). Null if not specified or not remote.")

    # Tier 3: Nice-to-have
    years_experience: MinMax | None = Field(None, description="Required years of experience, if mentioned")
    education_level: EducationLevel | None = Field(None, description="Minimum education required, if mentioned")
    certifications: list[str] = Field(default_factory=list, description="Required or preferred certifications")
    languages: list[str] = Field(default_factory=list, description="Required spoken languages as ISO 639-1 codes (e.g. 'en', 'es', 'zh')")
    travel_percent: int | None = Field(None, ge=0, le=100, description="Percentage of time traveling if mentioned. Null if not mentioned.")
    salary_transparency: SalaryTransparency = Field("not_disclosed", description="Did the posting actually disclose salary information?")
    interview_stages: int | None = Field(None, description="Number of interview rounds, if mentioned")
    posting_language: str | None = Field(
        None,
        description="ISO 639-1 code for the language the job posting text is written in (e.g. 'en', 'fr', 'de'). This is NOT the candidate's required spoken language.",
    )


def _canonicalize_industry_fields(data: dict) -> None:
    primary_industry = _to_str(data.get("industry_primary"))
    tags = data.get("industry_tags") or []

    ordered_tags = []
    seen = set()

    if primary_industry and primary_industry in INDUSTRY_VALUE_SET:
        ordered_tags.append(primary_industry)
        seen.add(primary_industry)

    remaining = []
    for tag in tags:
        tag_value = _to_str(tag)
        if not tag_value or tag_value not in INDUSTRY_VALUE_SET or tag_value in seen:
            continue
        seen.add(tag_value)
        remaining.append(tag_value)

    remaining.sort(key=lambda value: INDUSTRY_ORDER[value])
    data["industry_tags"] = ordered_tags + remaining

    other_hint = _to_str(data.get("industry_other_hint"))
    data["industry_other_hint"] = other_hint if primary_industry == "other" else None


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are extracting metadata from a job posting for dopejobs, a job board that helps people find roles they'll actually be excited about.

Your extractions should help job seekers quickly decide: "Is this worth my time?"

TAGLINE: One sentence, under 120 characters. Write like a friend telling you about a cool job — specific, vivid, human. Mention what you'd actually WORK ON and why it matters. Include the company name.
Good: "You'll 3D-print rocket engines at Relativity Space"
Good: "Own Duolingo's Gen-Z marketing blitz in Beijing"
Good: "Build the AI safety evals that decide if frontier models ship at Anthropic"
OK but boring: "Develop advanced additive manufacturing processes for Terran R"
Bad: "Join a fast-growing company as a software engineer"

COOL FACTOR: Calibrate carefully. ~10% boring, ~40% standard, ~30% interesting, ~15% compelling, ~5% exceptional.
- boring: generic back-office role at unremarkable company (data entry, temp admin)
- standard: decent job but nothing particularly stands out. This is the DEFAULT. SDR/BDR, account exec, coordinator, associate, ops/logistics, PM/TPM, sourcing, junior analyst roles are almost always "standard" even at good companies. Ask: "would my friend who works in a different industry think this is cool?" If no → standard.
- interesting: notable company OR genuinely unique role OR clearly above-average compensation.
- compelling: notable company AND unique/impactful role AND strong signals. Examples: AI safety researcher at a frontier lab, rocket engineer at a space startup, lead designer at a top consumer product.
- exceptional: once-in-a-career. EXTREMELY rare, maybe 1 in 200 jobs.
A temp/contract role is almost never above "interesting".

INDUSTRY: Classify by what the company SELLS to end users, not the function of this specific role.
- Choose ONE primary industry that best describes the company.
- Use industry_tags for real secondary overlaps from the same enum list.
- industry_primary should usually be company-level and stable across most roles at that company.
- Use industry_tags for team-level/product-line overlaps like payments, advertising, AI features, developer tools, or marketplaces inside a broader company.
- Example: Airbnb payments/ML roles should usually stay travel_hospitality or commerce_marketplaces as primary, with payments_banking as a tag if relevant.
- Example: Spotify ads roles should usually stay media_entertainment as primary, with advertising_marketing as a tag if relevant.
- Example: Sitecore roles should usually stay enterprise_software as primary, not ai_ml, unless the company itself is primarily an AI company.
- Example: Vercel roles should usually stay developer_tools as primary, with cloud_data_infra or ai_ml as tags if relevant.
- Example: Datadog, MongoDB, Snowflake, Databricks, and similar platform/database/observability companies should usually be cloud_data_infra as primary, not enterprise_software.
- Example: Cloudflare and Okta-style security/identity/network-edge companies should usually be cybersecurity or cloud_data_infra as primary, not enterprise_software.
- AI labs, model platforms, AI safety orgs → ai_ml
- Developer tools, SDKs, APIs, testing tools, CI/CD, code collaboration → developer_tools
- Cloud infra, databases, observability, data platforms, networking infrastructure → cloud_data_infra
- Business/productivity/collaboration/CRM/HR/CMS/DXP software for non-engineering workflows → enterprise_software
- Do NOT use enterprise_software as a catch-all for every B2B software company. Infra/platform/database/observability/networking products should usually be developer_tools, cloud_data_infra, or cybersecurity instead.
- Payments, banking, lending, expense management, card/treasury platforms → payments_banking
- Trading, hedge funds, market infrastructure, investing platforms → investing_trading
- Security companies, IAM, auth, access control, identity platforms → cybersecurity
- Social networks, forums, consumer communities → consumer_social
- Music/video/news/publishing/streaming → media_entertainment
- Biotech tools, diagnostics, research platforms, life sciences → biotech_life_sciences
- Drug makers, therapeutics, pharma companies → pharma
- Travel and lodging platforms → travel_hospitality
- Climate tech, decarbonization, carbon, sustainability software/services → climate_sustainability
- Utilities, grid, power, traditional energy infrastructure/operators → energy_utilities
- Robotics, autonomy, drones, AV → robotics_autonomy
- Chips, compute hardware, electronics → semiconductors_hardware
- Space companies → space_aerospace
- Defense, public safety, police/fire tech → defense_public_safety
- Proptech, real estate platforms, brokerage/property software → real_estate_proptech
- Builders, contractors, construction operations, built environment tech → construction_built_environment
- Recruiting and staffing firms → staffing_recruiting
- BPO, outsourcing, managed back-office/contact center services → bpo_outsourcing
- Do NOT classify by the job function. An accountant at a gaming company is "gaming". A recruiter at a travel company is "travel_hospitality".
- Do NOT use biotechnology for AI companies unless the company actually sells biotech/pharma products.

EXPERIENCE LEVEL:
- entry: intern, new grad, associate, coordinator, 0-2 years
- mid: 2-5 years, no "senior" in title
- senior: "Senior" in title, or 5+ years required
- staff: "Staff" in title
- principal: "Principal" or "Distinguished" in title
- executive: VP, Director, C-suite, Head of

IS MANAGER: true ONLY if the role manages people (Director of X, Engineering Manager, Team Lead, Head of). Individual contributors are false, even if senior/staff/principal.

VIBE TAGS: Only include tags where you can point to specific text evidence. "We value diversity" is NOT evidence for diverse_inclusive — look for concrete programs, ERGs, specific policies. Each tag needs a real signal in the text.

BENEFITS HIGHLIGHTS: EXACTLY 0-3 perks that would make someone say "wow, really?"
NOT highlights (never list): health/dental/vision, 401k/pension, PTO/vacation (even if "generous"/"unlimited"), standard parental leave, remote/hybrid, equity/stock options.
ARE highlights: "$10K learning budget", "6-month parental leave", "4-day work weeks", "sabbatical", "fertility benefits $10K+", pro-bono programs, on-site childcare, pet insurance. If nothing unusual → empty array [].

VISA SPONSORSHIP: "yes" if mentions sponsorship/visa support. "no" if "must be authorized to work" or "no sponsorship". "unknown" if not mentioned.

Sentinel values (no nulls): Use 0 for unknown numbers, "" for unknown strings, "unknown" for unknown company_stage, "not_specified" for unknown education_level.

LANGUAGE: ALL output MUST be in English, regardless of posting language. Translate everything."""


COMPACT_SCHEMA = """Extract these fields as JSON:
- tagline: catchy one-sentence summary (NOT the title)
- location_city, location_state, location_country (ISO alpha-2), location_lat, location_lng: from ATS metadata or text. "" if remote/unknown. Geocode lat/lng from city.
- applicant_location_requirements (array): ONLY for remote jobs with explicit applicant geography restrictions. Each item is {scope: "country"|"state"|"city"|"region_group", name: string, country_code: string, region: string}. Use [] when unrestricted or unknown.
- salary_min, salary_max: numbers, 0 if not disclosed. Can be decimal for hourly rates (e.g. 18.50).
- salary_currency: ISO 4217 code. "" if not disclosed.
- salary_period: "hourly"|"weekly"|"monthly"|"annually". Match the period from the posting.
- salary_transparency: "full_range"|"minimum_only"|"not_disclosed"
- office_type: "remote"|"hybrid"|"onsite"
- hybrid_days: number, 0 if not hybrid or unknown
- job_type: "full-time"|"part-time"|"contract"|"internship"|"temporary"|"freelance"
- experience_level: "entry"|"mid"|"senior"|"staff"|"principal"|"executive"
- is_manager: boolean
- industry_primary: one of [ai_ml, developer_tools, cloud_data_infra, enterprise_software, cybersecurity, payments_banking, investing_trading, insurance, crypto_web3, healthcare_services, biotech_life_sciences, pharma, education_edtech, consumer_social, media_entertainment, gaming, advertising_marketing, commerce_marketplaces, consumer_goods_brands, food_beverage, travel_hospitality, climate_sustainability, energy_utilities, transportation_logistics, manufacturing_industrials, robotics_autonomy, semiconductors_hardware, space_aerospace, defense_public_safety, government_public_sector, real_estate_proptech, construction_built_environment, telecommunications_networking, agriculture, legal, consulting_professional_services, nonprofit_philanthropy, staffing_recruiting, bpo_outsourcing, other]. This should usually reflect the COMPANY'S core business, not the specific team.
- industry_tags (array): zero or more additional applicable values from the SAME industry list. Use [] if no strong secondary industries apply. Prefer not to repeat the primary industry here. Use tags for adjacent business lines, monetization models, or product overlaps.
- industry_other_hint: short freeform string ONLY when industry_primary is "other" and none of the enum values fit. "" otherwise.
- hard_skills (array): ALL technical/domain skills mentioned
- soft_skills (array): ALL interpersonal skills mentioned
- cool_factor: "boring"|"standard"|"interesting"|"compelling"|"exceptional". Most jobs are standard/interesting.
- vibe_tags (array): from [mission_driven, high_growth, small_team, cutting_edge_tech, strong_culture, high_autonomy, work_life_balance, well_funded, public_benefit, creative_role, data_intensive, global_team, diverse_inclusive, fast_paced, customer_facing, research_focused]
- visa_sponsorship: "yes"|"no"|"unknown"
- visa_sponsorship_types (array): if yes, from [h1b, h1b_transfer, o1, l1, j1, green_card, other]. [] otherwise.
- equity_offered: boolean
- equity_min_pct, equity_max_pct: numbers, 0 if unknown
- company_stage: "pre-seed"|"seed"|"series-a"|"series-b"|"series-c-plus"|"public"|"bootstrapped"|"government"|"nonprofit"|"unknown"
- company_size_min, company_size_max: numbers, 0 if unknown (employees)
- team_size_min, team_size_max: numbers, 0 if unknown
- reports_to: string, "" if unknown
- benefits_categories (array): from [health, dental, vision, life_insurance, disability, 401k, pension, equity_comp, bonus, unlimited_pto, generous_pto, parental_leave, remote_stipend, home_office, relocation, learning_budget, tuition_reimbursement, gym_fitness, wellness, meals, commuter, mental_health, childcare, pet_friendly, sabbatical, stock_purchase]
- benefits_highlights (array, max 3): only UNUSUAL perks, not standard ones
- remote_timezone_earliest, remote_timezone_latest: strings like "UTC-8", "UTC+1". "" if unknown.
- years_experience_min, years_experience_max: numbers, 0 if unknown
- education_level: "none"|"high-school"|"bachelors"|"masters"|"phd"|"not_specified"
- certifications (array): required/preferred certifications
- languages (array): ISO 639-1 codes of required spoken languages
- travel_percent: 0-100, 0 if unknown
- interview_stages: number, 0 if unknown
- posting_language: ISO 639-1 code of the language the POSTING ITSELF is written in, e.g. "en", "fr", "de". This is not the candidate's required language."""


def build_user_prompt(job_text: str) -> str:
    return f"{COMPACT_SCHEMA}\n\nJob posting:\n{job_text}"


# ---------------------------------------------------------------------------
# Flat JSON schema for constrained decoding (no $defs/$ref/anyOf bloat)
# ---------------------------------------------------------------------------

FLAT_JSON_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "tagline": {"type": "string"},
        "location_city": {"type": "string"},
        "location_state": {"type": "string"},
        "location_country": {"type": "string"},
        "location_lat": {"type": "number"},
        "location_lng": {"type": "number"},
        "applicant_location_requirements": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "scope": {"type": "string", "enum": ["country", "state", "city", "region_group"]},
                    "name": {"type": "string"},
                    "country_code": {"type": "string"},
                    "region": {"type": "string"},
                },
                "required": ["scope", "name"],
            },
        },
        "salary_min": {"type": "number"},
        "salary_max": {"type": "number"},
        "salary_currency": {"type": "string"},
        "salary_period": {"type": "string", "enum": ["hourly", "weekly", "monthly", "annually"]},
        "salary_transparency": {"type": "string", "enum": ["full_range", "minimum_only", "not_disclosed"]},
        "office_type": {"type": "string", "enum": ["remote", "hybrid", "onsite"]},
        "hybrid_days": {"type": "integer"},
        "job_type": {"type": "string", "enum": ["full-time", "part-time", "contract", "internship", "temporary", "freelance"]},
        "experience_level": {"type": "string", "enum": ["entry", "mid", "senior", "staff", "principal", "executive"]},
        "is_manager": {"type": "boolean"},
        "industry_primary": {"type": "string", "enum": INDUSTRY_VALUES},
        "industry_tags": {"type": "array", "items": {"type": "string", "enum": INDUSTRY_VALUES}},
        "industry_other_hint": {"type": "string"},
        "hard_skills": {"type": "array", "items": {"type": "string"}},
        "soft_skills": {"type": "array", "items": {"type": "string"}},
        "cool_factor": {"type": "string", "enum": [
            "boring", "standard", "interesting", "compelling", "exceptional",
        ]},
        "vibe_tags": {"type": "array", "items": {"type": "string", "enum": [
            "mission_driven", "high_growth", "small_team", "cutting_edge_tech",
            "strong_culture", "high_autonomy", "work_life_balance", "well_funded",
            "public_benefit", "creative_role", "data_intensive", "global_team",
            "diverse_inclusive", "fast_paced", "customer_facing", "research_focused",
        ]}},
        "visa_sponsorship": {"type": "string", "enum": ["yes", "no", "unknown"]},
        "visa_sponsorship_types": {"type": "array", "items": {"type": "string", "enum": [
            "h1b", "h1b_transfer", "o1", "l1", "j1", "green_card", "other",
        ]}},
        "equity_offered": {"type": "boolean"},
        "equity_min_pct": {"type": "number"},
        "equity_max_pct": {"type": "number"},
        "company_stage": {"type": "string", "enum": [
            "pre-seed", "seed", "series-a", "series-b", "series-c-plus",
            "public", "bootstrapped", "government", "nonprofit", "unknown",
        ]},
        "company_size_min": {"type": "integer"},
        "company_size_max": {"type": "integer"},
        "team_size_min": {"type": "integer"},
        "team_size_max": {"type": "integer"},
        "reports_to": {"type": "string"},
        "benefits_categories": {"type": "array", "items": {"type": "string", "enum": [
            "health", "dental", "vision", "life_insurance", "disability", "401k",
            "pension", "equity_comp", "bonus", "unlimited_pto", "generous_pto",
            "parental_leave", "remote_stipend", "home_office", "relocation",
            "learning_budget", "tuition_reimbursement", "gym_fitness", "wellness",
            "meals", "commuter", "mental_health", "childcare", "pet_friendly",
            "sabbatical", "stock_purchase",
        ]}},
        "benefits_highlights": {"type": "array", "items": {"type": "string"}},
        "remote_timezone_earliest": {"type": "string"},
        "remote_timezone_latest": {"type": "string"},
        "years_experience_min": {"type": "integer"},
        "years_experience_max": {"type": "integer"},
        "education_level": {"type": "string", "enum": [
            "none", "high-school", "bachelors", "masters", "phd", "not_specified",
        ]},
        "certifications": {"type": "array", "items": {"type": "string"}},
        "languages": {"type": "array", "items": {"type": "string"}},
        "travel_percent": {"type": "integer"},
        "interview_stages": {"type": "integer"},
        "posting_language": {"type": "string"},
    },
    "required": list({
        "tagline", "location_city", "location_state", "location_country",
        "location_lat", "location_lng", "salary_min", "salary_max",
        "salary_currency", "salary_period", "salary_transparency",
        "office_type", "hybrid_days", "job_type", "experience_level", "is_manager",
        "applicant_location_requirements",
        "industry_primary", "industry_tags", "industry_other_hint", "hard_skills", "soft_skills", "cool_factor", "vibe_tags",
        "visa_sponsorship", "visa_sponsorship_types", "equity_offered",
        "equity_min_pct", "equity_max_pct", "company_stage",
        "company_size_min", "company_size_max", "team_size_min", "team_size_max",
        "reports_to", "benefits_categories", "benefits_highlights",
        "remote_timezone_earliest", "remote_timezone_latest",
        "years_experience_min", "years_experience_max", "education_level",
        "certifications", "languages", "travel_percent", "interview_stages",
        "posting_language",
    }),
}


def _to_str(val) -> str | None:
    """Convert empty string or None to None."""
    if val is None or val == "":
        return None
    return str(val)

def _to_int(val) -> int | None:
    """Convert 0, None, or empty to None."""
    if val is None or val == "" or val == 0:
        return None
    return int(val)

def _to_float(val) -> float | None:
    """Convert 0, None, or empty to None."""
    if val is None or val == "" or val == 0:
        return None
    return float(val)


_LANGUAGE_CODE_ALIASES = {
    "EN": "en",
    "ENG": "en",
    "ENGLISH": "en",
    "FR": "fr",
    "FRA": "fr",
    "FRE": "fr",
    "FRENCH": "fr",
    "FRANCAIS": "fr",
    "FRANÇAIS": "fr",
    "DE": "de",
    "DEU": "de",
    "GER": "de",
    "GERMAN": "de",
    "DEUTSCH": "de",
    "ES": "es",
    "SPA": "es",
    "SPANISH": "es",
    "ESPANOL": "es",
    "ESPAÑOL": "es",
    "IT": "it",
    "ITALIAN": "it",
    "ITALIANO": "it",
    "PT": "pt",
    "POR": "pt",
    "PORTUGUESE": "pt",
    "PORTUGUÊS": "pt",
    "PT-BR": "pt",
    "PT_BR": "pt",
    "JA": "ja",
    "JPN": "ja",
    "JAPANESE": "ja",
    "日本語": "ja",
    "ZH": "zh",
    "ZHO": "zh",
    "CHI": "zh",
    "CHINESE": "zh",
    "ZH-CN": "zh",
    "ZH_CN": "zh",
    "ZH-TW": "zh",
    "ZH_TW": "zh",
    "KO": "ko",
    "KOR": "ko",
    "KOREAN": "ko",
    "한국어": "ko",
    "NL": "nl",
    "DUTCH": "nl",
    "NEDERLANDS": "nl",
    "PL": "pl",
    "POLISH": "pl",
    "POLSKI": "pl",
    "SV": "sv",
    "SWE": "sv",
    "SWEDISH": "sv",
    "SVENSKA": "sv",
    "DA": "da",
    "DANISH": "da",
    "DANSK": "da",
    "NO": "no",
    "NORWEGIAN": "no",
    "NORSK": "no",
    "FI": "fi",
    "FINNISH": "fi",
    "SUOMI": "fi",
}


def _normalize_language_code(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    alias = _LANGUAGE_CODE_ALIASES.get(normalized.upper())
    if alias:
        return alias
    locale_match = re.match(r"^([A-Za-z]{2,3})[-_][A-Za-z0-9]{2,}$", normalized)
    if locale_match:
        base = locale_match.group(1)
        return _normalize_language_code(base)
    if re.fullmatch(r"[A-Za-z]{2}", normalized):
        return normalized.lower()
    if re.fullmatch(r"[A-Za-z]{3}", normalized):
        return _LANGUAGE_CODE_ALIASES.get(normalized.upper())
    return None

def _flat_to_job_metadata(data: dict) -> JobMetadata:
    """Convert flat schema output to nested JobMetadata model."""
    # Reconstruct salary (0 = not disclosed)
    s_min = _to_int(data.pop("salary_min", 0))
    s_max = _to_int(data.pop("salary_max", 0))
    s_cur = _to_str(data.pop("salary_currency", ""))
    s_per = _to_str(data.pop("salary_period", ""))
    data["salary"] = Salary(min=s_min, max=s_max, currency=s_cur or "USD", period=s_per or "annually") if (s_min or s_max) else None

    # Reconstruct location ("" = unknown)
    l_city = _to_str(data.pop("location_city", ""))
    l_state = _to_str(data.pop("location_state", ""))
    l_country_raw = _to_str(data.pop("location_country", ""))
    l_country = _country_code_from_value(l_country_raw) or l_country_raw
    l_lat = _to_float(data.pop("location_lat", 0))
    l_lng = _to_float(data.pop("location_lng", 0))
    label_parts = [part for part in (l_city, l_state, l_country) if part]
    location_label = ", ".join(label_parts) if label_parts else None
    data["locations"] = [Location(label=location_label, city=l_city, state=l_state, country_code=l_country, lat=l_lat, lng=l_lng)] if (l_city or l_state or l_country) else []

    reqs = []
    for req in data.get("applicant_location_requirements", []) or []:
        if not isinstance(req, dict):
            continue
        scope = _to_str(req.get("scope"))
        name = _to_str(req.get("name"))
        if not scope or not name:
            continue
        reqs.append({
            "scope": scope,
            "name": name,
            "country_code": _to_str(req.get("country_code")),
            "region": _to_str(req.get("region")),
        })
    data["applicant_location_requirements"] = reqs

    # Reconstruct equity
    data["equity"] = Equity(
        offered=data.pop("equity_offered", False),
        min_pct=_to_float(data.pop("equity_min_pct", 0)),
        max_pct=_to_float(data.pop("equity_max_pct", 0)),
    )

    # Deduplicate vibe_tags
    if data.get("vibe_tags"):
        seen = set()
        deduped = []
        for tag in data["vibe_tags"]:
            if tag not in seen:
                seen.add(tag)
                deduped.append(tag)
        data["vibe_tags"] = deduped

    _canonicalize_industry_fields(data)

    # company_stage ("unknown" = not known)
    if data.get("company_stage") == "unknown":
        data["company_stage"] = None

    # company_size (0 = unknown)
    cs_min = _to_int(data.pop("company_size_min", 0))
    cs_max = _to_int(data.pop("company_size_max", 0))
    data["company_size"] = MinMax(min=cs_min, max=cs_max) if (cs_min or cs_max) else None

    # team_size (0 = unknown)
    ts_min = _to_int(data.pop("team_size_min", 0))
    ts_max = _to_int(data.pop("team_size_max", 0))
    data["team_size"] = MinMax(min=ts_min, max=ts_max) if (ts_min or ts_max) else None

    # years_experience (0 = unknown)
    ye_min = _to_int(data.pop("years_experience_min", 0))
    ye_max = _to_int(data.pop("years_experience_max", 0))
    data["years_experience"] = MinMax(min=ye_min, max=ye_max) if (ye_min or ye_max) else None

    # remote_timezone_range ("" = unknown)
    tz_earliest = _to_str(data.pop("remote_timezone_earliest", ""))
    tz_latest = _to_str(data.pop("remote_timezone_latest", ""))
    data["remote_timezone_range"] = TimezoneRange(earliest=tz_earliest, latest=tz_latest) if (tz_earliest and tz_latest) else None

    # reports_to ("" = unknown)
    if data.get("reports_to") == "":
        data["reports_to"] = None

    # education_level ("not_specified" = unknown)
    if data.get("education_level") == "not_specified":
        data["education_level"] = None

    # interview_stages / travel_percent (0 = unknown)
    if data.get("interview_stages") == 0:
        data["interview_stages"] = None
    if data.get("travel_percent") == 0:
        data["travel_percent"] = None
    # hybrid_days (0 = not hybrid)
    if data.get("hybrid_days") == 0:
        data["hybrid_days"] = None

    posting_language = _normalize_language_code(_to_str(data.get("posting_language")))
    data["posting_language"] = posting_language

    return JobMetadata.model_validate(data)


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------

def _parse_response(content: str, use_flat: bool = False) -> JobMetadata | None:
    """Parse LLM response text into a JobMetadata object."""
    content = content.strip()
    # Strip markdown code fences if present
    if content.startswith("```"):
        content = content.split("```")[1]
        if content.startswith("json"):
            content = content[4:]
        content = content.strip()
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return None

    # Try flat schema conversion first, then direct Pydantic validation
    if use_flat:
        try:
            return _flat_to_job_metadata(data)
        except Exception:
            pass
    # Fall back to direct validation (handles both nested and partially flat)
    try:
        return JobMetadata.model_validate(data)
    except Exception:
        return None


class OpenAIBackend:
    """Backend using OpenAI-compatible chat completions API."""

    def __init__(self, base_url: str, model: str, api_key: str = "not-needed",
                 use_constrained: bool = True):
        import requests
        self._session = requests.Session()
        self._base_url = base_url.rstrip("/")
        self._model = model
        self._api_key = api_key
        self._use_constrained = use_constrained

    def _max_tokens_param(self, max_tokens: int) -> dict:
        """Newer OpenAI models use max_completion_tokens instead of max_tokens."""
        if "api.openai.com" in self._base_url:
            return {"max_completion_tokens": max_tokens}
        return {"max_tokens": max_tokens}

    def extract_batch(self, job_texts: list[str], max_tokens: int = 2000) -> list[JobMetadata | None]:
        results = []
        for text in job_texts:
            try:
                payload = {
                    "model": self._model,
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": build_user_prompt(text)},
                    ],
                    **self._max_tokens_param(max_tokens),
                    "temperature": 0.1,
                }
                if self._use_constrained:
                    payload["response_format"] = {
                        "type": "json_schema",
                        "json_schema": {"name": "job_metadata", "schema": FLAT_JSON_SCHEMA},
                    }
                resp = self._session.post(
                    f"{self._base_url}/chat/completions",
                    headers={"Authorization": f"Bearer {self._api_key}"},
                    json=payload,
                    timeout=300,
                )
                data = resp.json()
                content = data["choices"][0]["message"]["content"]
                # Always try flat first (our prompt asks for flat fields),
                # then fall back to nested Pydantic validation
                parsed = _parse_response(content, use_flat=True)
                if parsed is None:
                    print(f"  Failed to parse response: {content[:200]}...", file=sys.stderr)
                results.append(parsed)
            except Exception as e:
                print(f"  Error: {e}", file=sys.stderr)
                results.append(None)
        return results


class GeminiBackend:
    """Backend using Google Gemini API with structured output."""

    def __init__(
        self,
        model: str = "gemini-3.1-flash-lite-preview",
        api_key: str | None = None,
        service_tier: str | None = None,
        timeout_seconds: int | None = None,
        max_retries: int | None = None,
    ):
        import requests
        self._session = requests.Session()
        self._model = model
        self._api_key = api_key or os.environ.get("GEMINI_API_KEY", "")
        self._service_tier = (service_tier or os.environ.get("GEMINI_SERVICE_TIER", "flex")).strip().lower() or None
        self._timeout_seconds = timeout_seconds or int(os.environ.get("GEMINI_TIMEOUT_SECONDS", "900"))
        self._max_retries = max_retries or int(os.environ.get("GEMINI_MAX_RETRIES", "5"))
        self._schema = {
            "type": "OBJECT",
            "properties": {
                "tagline": {"type": "STRING", "description": "One sentence that makes a job seeker stop scrolling"},
                "location_city": {"type": "STRING"}, "location_state": {"type": "STRING"},
                "location_country": {"type": "STRING"}, "location_lat": {"type": "NUMBER"}, "location_lng": {"type": "NUMBER"},
                "applicant_location_requirements": {
                    "type": "ARRAY",
                    "items": {
                        "type": "OBJECT",
                        "properties": {
                            "scope": {"type": "STRING", "enum": ["country", "state", "city", "region_group"]},
                            "name": {"type": "STRING"},
                            "country_code": {"type": "STRING"},
                            "region": {"type": "STRING"},
                        },
                        "required": ["scope", "name"],
                    },
                },
                "salary_min": {"type": "NUMBER"}, "salary_max": {"type": "NUMBER"},
                "salary_currency": {"type": "STRING"},
                "salary_period": {"type": "STRING", "enum": ["hourly", "weekly", "monthly", "annually"]},
                "salary_transparency": {"type": "STRING", "enum": ["full_range", "minimum_only", "not_disclosed"]},
                "office_type": {"type": "STRING", "enum": ["remote", "hybrid", "onsite"]},
                "hybrid_days": {"type": "INTEGER"},
                "job_type": {"type": "STRING", "enum": ["full-time", "part-time", "contract", "internship", "temporary", "freelance"]},
                "experience_level": {"type": "STRING", "enum": ["entry", "mid", "senior", "staff", "principal", "executive"]},
                "is_manager": {"type": "BOOLEAN"},
                "industry_primary": {"type": "STRING", "enum": [e.value for e in Industry]},
                "industry_tags": {"type": "ARRAY", "items": {"type": "STRING", "enum": [e.value for e in Industry]}},
                "industry_other_hint": {"type": "STRING"},
                "hard_skills": {"type": "ARRAY", "items": {"type": "STRING"}},
                "soft_skills": {"type": "ARRAY", "items": {"type": "STRING"}},
                "cool_factor": {"type": "STRING", "enum": ["boring", "standard", "interesting", "compelling", "exceptional"]},
                "vibe_tags": {"type": "ARRAY", "items": {"type": "STRING", "enum": [e.value for e in VibeTag]}},
                "visa_sponsorship": {"type": "STRING", "enum": ["yes", "no", "unknown"]},
                "visa_sponsorship_types": {"type": "ARRAY", "items": {"type": "STRING", "enum": [
                    "h1b", "h1b_transfer", "o1", "l1", "j1", "green_card", "other",
                ]}},
                "equity_offered": {"type": "BOOLEAN"},
                "equity_min_pct": {"type": "NUMBER"},
                "equity_max_pct": {"type": "NUMBER"},
                "company_stage": {"type": "STRING", "enum": [
                    "pre-seed", "seed", "series-a", "series-b", "series-c-plus",
                    "public", "bootstrapped", "government", "nonprofit", "unknown"]},
                "company_size_min": {"type": "INTEGER"}, "company_size_max": {"type": "INTEGER"},
                "team_size_min": {"type": "INTEGER"}, "team_size_max": {"type": "INTEGER"},
                "reports_to": {"type": "STRING"},
                "benefits_categories": {"type": "ARRAY", "items": {"type": "STRING", "enum": [e.value for e in BenefitCategory]}},
                "benefits_highlights": {"type": "ARRAY", "items": {"type": "STRING"}},
                "remote_timezone_earliest": {"type": "STRING"},
                "remote_timezone_latest": {"type": "STRING"},
                "education_level": {"type": "STRING", "enum": ["none", "high-school", "bachelors", "masters", "phd", "not_specified"]},
                "years_experience_min": {"type": "INTEGER"}, "years_experience_max": {"type": "INTEGER"},
                "certifications": {"type": "ARRAY", "items": {"type": "STRING"}},
                "languages": {"type": "ARRAY", "items": {"type": "STRING"}},
                "travel_percent": {"type": "INTEGER"},
                "interview_stages": {"type": "INTEGER"},
                "posting_language": {"type": "STRING"},
            },
            "required": list(FLAT_JSON_SCHEMA["required"]),
        }

    def build_request(self, job_text: str, max_tokens: int = 2000) -> dict:
        prompt = f"{SYSTEM_PROMPT}\n\n{build_user_prompt(job_text)}"
        request = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": max_tokens,
                "responseMimeType": "application/json",
                "responseSchema": self._schema,
            },
        }
        if self._service_tier:
            request["service_tier"] = self._service_tier
        return request


    def _extract_text(self, data: dict) -> str | None:
        """Best-effort extraction of text from Gemini generateContent responses."""
        candidates = data.get("candidates")
        if not candidates:
            return None

        content = candidates[0].get("content") or {}
        parts = content.get("parts") or []
        texts = [part.get("text", "") for part in parts if isinstance(part, dict) and part.get("text")]
        if not texts:
            return None
        return "".join(texts)

    def parse_response_payload(self, data: dict) -> tuple[JobMetadata | None, str | None]:
        content = self._extract_text(data)
        if not content:
            error = data.get("error")
            prompt_feedback = data.get("promptFeedback")
            finish_reason = None
            candidates = data.get("candidates") or []
            if candidates:
                finish_reason = candidates[0].get("finishReason")
            return None, (
                "Gemini response missing text: "
                f"error={error!r} prompt_feedback={prompt_feedback!r} finish_reason={finish_reason!r}"
            )

        parsed = _parse_response(content, use_flat=True)
        if parsed is None:
            parsed = _parse_response(content, use_flat=False)
        if parsed is None:
            return None, f"Failed to parse Gemini response: {content[:200]}..."
        return parsed, None

    def extract_batch(self, job_texts: list[str], max_tokens: int = 2000) -> list[JobMetadata | None]:
        import random
        import time

        results = []
        for text in job_texts:
            request_json = self.build_request(text, max_tokens=max_tokens)
            headers = {}
            if self._service_tier == "flex":
                headers["X-Server-Timeout"] = str(min(self._timeout_seconds, 900))

            for attempt in range(self._max_retries):
                try:
                    resp = self._session.post(
                        f"https://generativelanguage.googleapis.com/v1beta/models/{self._model}:generateContent?key={self._api_key}",
                        json=request_json,
                        headers=headers or None,
                        timeout=self._timeout_seconds,
                    )
                    if resp.ok:
                        parsed, error = self.parse_response_payload(resp.json())
                        if error:
                            print(f"  {error}", file=sys.stderr)
                        results.append(parsed)
                        break

                    body = resp.text[:500]
                    if self._service_tier == "flex" and resp.status_code in (429, 503) and attempt < self._max_retries - 1:
                        delay = min(60, 5 * (2 ** attempt)) + random.uniform(0, 1)
                        print(
                            f"  Gemini Flex HTTP {resp.status_code}; retrying in {delay:.1f}s "
                            f"(attempt {attempt + 1}/{self._max_retries}): {body}",
                            file=sys.stderr,
                        )
                        time.sleep(delay)
                        continue

                    print(f"  Gemini HTTP {resp.status_code}: {body}", file=sys.stderr)
                    results.append(None)
                    break
                except Exception as e:
                    if self._service_tier == "flex" and attempt < self._max_retries - 1:
                        delay = min(60, 5 * (2 ** attempt)) + random.uniform(0, 1)
                        print(
                            f"  Gemini Flex error; retrying in {delay:.1f}s "
                            f"(attempt {attempt + 1}/{self._max_retries}): {e}",
                            file=sys.stderr,
                        )
                        time.sleep(delay)
                        continue
                    print(f"  Gemini error: {e}", file=sys.stderr)
                    results.append(None)
                    break
        return results


class LocalBackend:
    """Backend using mlx-lm for local Apple Silicon inference."""

    def __init__(self, model_path: str):
        from mlx_lm import load
        print(f"Loading model {model_path}...")
        self._model, self._tokenizer = load(model_path)
        self._model_path = model_path

    def extract_batch(self, job_texts: list[str], max_tokens: int = 2000, batch_size: int = 16) -> list[JobMetadata | None]:
        from mlx_lm import batch_generate

        # Build prompts
        prompts = []
        for text in job_texts:
            messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": build_user_prompt(text)},
            ]
            try:
                prompt = self._tokenizer.apply_chat_template(
                    messages, add_generation_prompt=True, enable_thinking=False
                )
            except TypeError:
                prompt = self._tokenizer.apply_chat_template(
                    messages, add_generation_prompt=True
                )
            prompts.append(prompt)

        # Process in batches
        results: list[JobMetadata | None] = []
        for i in range(0, len(prompts), batch_size):
            batch = prompts[i : i + batch_size]
            result = batch_generate(
                self._model, self._tokenizer, batch,
                max_tokens=max_tokens, verbose=False,
            )
            for text_out in result.texts:
                parsed = _parse_response(text_out)
                if parsed is None:
                    print(f"  Parse error: {text_out[:200]}...", file=sys.stderr)
                results.append(parsed)

        return results


# ---------------------------------------------------------------------------
# Description-based salary extraction
# ---------------------------------------------------------------------------

# Keywords near a dollar amount that signal salary/compensation.
_SALARY_SIGNAL_RE = re.compile(
    r'salary|base pay|pay range|compensation[:\s]|hourly rate|'
    r'pay rate|starting pay|base hourly|annual base|annualized base|'
    r'target.*?compensation|on.?target earnings|ote\b|'
    r'comp\s+range|starting\s+at\s+\$|earnings\b|'
    r'per\s+(?:hour|annum|year|month|week)|/\s*(?:hr|hour|yr|year)|'
    r'hourly|annually|USD|usd|'
    r'minimum\s+\$.*maximum\s+\$',
    re.IGNORECASE,
)

# Keywords that indicate the dollar amount is NOT salary.
_SALARY_REJECT_RE = re.compile(
    r'revenue|(?<!\w)arr\b|funding|raised|valuation|market\s*cap|'
    r'billion|trillion|assets?\s+under|aum\b|gmv\b|'
    r'series\s+[a-f]\b|processing|customers?|'
    r'stipend|reimbursement|tuition|fsa\b|hsa\b|'
    r'sign.?on\s+bonus|signing\s+bonus|relocation|'
    r'learning.{0,20}budget|development.{0,20}budget|'
    r'work.?from.?home.{0,15}stipend|home\s+office.{0,15}stipend|'
    r'401\s*\(?k\)?|retirement\s+match',
    re.IGNORECASE,
)

# Matches dollar amounts like $150,000  $150,000.00  $150K  $150k  $1.5M
_USD_AMOUNT_RE = re.compile(
    r'\$\s*([\d,]+(?:\.[\d]{1,2})?)\s*([KkMm])?'
)

# --- Non-USD currency support ---

# Map currency symbols to ISO codes
_CURRENCY_SYMBOLS = {
    '€': 'EUR',
    '£': 'GBP',
    '¥': 'JPY',
    '₹': 'INR',
    '₪': 'ILS',
    'zł': 'PLN',
}

# Prefixed currency symbols/codes: €X, £X, C$X, A$X, R$X, NZ$X, CHF X, etc.
_NON_USD_PREFIX_RE = re.compile(
    r'(?:([€£¥₹₪]|(?:zł))\s*([\d.,]+)\s*([KkMm])?|'                      # symbol + number
    r'([CRA]|NZ)\$([\d,]+(?:\.[\d]{1,2})?)\s*([KkMm])?|'                  # C$/R$/A$/NZ$ + number
    r'(EUR|GBP|CAD|AUD|CHF|SEK|NOK|DKK|PLN|BRL|MXN|SGD|NZD|INR|JPY|CZK|HUF)'
    r'\s*([\d.,]+)\s*([KkMm])?)',                                          # CODE + number
    re.IGNORECASE,
)

# Suffixed currency: X EUR, X€, X£, X CAD, X PLN, X Kč, etc.
_NON_USD_SUFFIX_RE = re.compile(
    r'([\d.,]+)\s*([KkMm])?\s*'
    r'(EUR|GBP|CAD|AUD|CHF|SEK|NOK|DKK|PLN|BRL|MXN|SGD|NZD|INR|JPY|CZK|HUF|Kč|[€£¥₹₪])',
    re.IGNORECASE,
)

# $X CAD / $X AUD etc. — dollar sign but non-USD currency code after
_DOLLAR_WITH_CURRENCY_RE = re.compile(
    r'\$\s*([\d,]+(?:\.[\d]{1,2})?)\s*([KkMm])?\s*'
    r'(CAD|AUD|NZD|SGD|MXN|HKD)\b',
    re.IGNORECASE,
)

_PREFIX_SYMBOL_CODES = {'C': 'CAD', 'R': 'BRL', 'NZ': 'NZD', 'A': 'AUD'}  # maps C$/R$/NZ$/A$ prefixes

# Matches a salary range: two amounts separated by dash/to
_RANGE_SEP_RE = re.compile(
    r'[-–—]\s*|to\s+',
    re.IGNORECASE,
)


def _parse_number_intl(num_str: str) -> float | None:
    """Parse a number that may use European formatting (period as thousands sep).

    Heuristic: if the string has periods and the last group after a period is
    exactly 3 digits with no comma, treat periods as thousands separators.
    e.g. '25.500' -> 25500, '25.500,00' -> 25500.00
    But '25.50' -> 25.50 (normal decimal).
    """
    num_str = num_str.strip()
    if not num_str:
        return None

    # European format: 25.500 or 25.500,00 or 158.400
    # Has period(s) AND last segment after period is exactly 3 digits
    # AND no commas before the period (or commas used as decimal sep at end)
    if '.' in num_str:
        parts = num_str.split('.')
        # Check if it looks European: e.g. "25.500" or "158.400"
        if len(parts) == 2 and len(parts[1]) == 3 and parts[1].isdigit():
            # Could be European thousands sep — check for trailing comma decimal
            base = num_str.replace('.', '')
            if ',' in base:
                # "25.500,00" -> base="25500,00" -> "25500.00"
                base = base.replace(',', '.')
            try:
                return float(base)
            except ValueError:
                return None

    # Standard format: commas as thousands, period as decimal
    cleaned = num_str.replace(',', '')
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_dollar_amount(s: str) -> float | None:
    """Parse a string like '$150,000', '$150K', '$1.5M' into a float."""
    m = _USD_AMOUNT_RE.search(s)
    if not m:
        return None
    num_str = m.group(1).replace(',', '')
    suffix = (m.group(2) or '').upper()
    try:
        value = float(num_str)
    except ValueError:
        return None
    if suffix == 'K':
        value *= 1_000
    elif suffix == 'M':
        value *= 1_000_000
    return value


def _apply_suffix(value: float, suffix: str | None) -> float:
    s = (suffix or '').upper()
    if s == 'K':
        return value * 1_000
    if s == 'M':
        return value * 1_000_000
    return value


def _parse_non_usd_amount(s: str) -> tuple[float | None, str | None]:
    """Parse a non-USD currency amount. Returns (value, currency_code).

    Handles:
      - Symbol prefix: €75,000  £65,000  ¥30,000  ₹32,000
      - Country-prefix dollar: C$90,000  R$10,000  A$85,000  NZ$75,000
      - Code prefix: EUR 150,000  CAD 90,000  PLN 9,550
      - Code suffix: 25,500 EUR  160,000 PLN  9,550 PLN
      - Dollar + code suffix: $90,000 CAD  $85,000 AUD
      - European number format: 25.500€  158.400 EUR
    """
    # Try $X CAD/AUD/etc. first (most specific)
    m = _DOLLAR_WITH_CURRENCY_RE.search(s)
    if m:
        value = _parse_number_intl(m.group(1))
        if value is not None:
            return _apply_suffix(value, m.group(2)), m.group(3).upper()

    # Try prefix patterns: €X, C$X, EUR X
    m = _NON_USD_PREFIX_RE.search(s)
    if m:
        if m.group(1):  # Symbol form: €/£/¥/₹/₪/zł
            symbol = m.group(1)
            value = _parse_number_intl(m.group(2))
            suffix = m.group(3)
            currency = _CURRENCY_SYMBOLS.get(symbol)
            if value is not None and currency:
                return _apply_suffix(value, suffix), currency
        elif m.group(4):  # C$/R$/NZ$/A$
            prefix = m.group(4).upper()
            value = _parse_number_intl(m.group(5))
            suffix = m.group(6)
            currency = _PREFIX_SYMBOL_CODES.get(prefix)
            if value is not None and currency:
                return _apply_suffix(value, suffix), currency
        elif m.group(7):  # CODE + number
            currency = m.group(7).upper()
            value = _parse_number_intl(m.group(8))
            suffix = m.group(9)
            if value is not None:
                return _apply_suffix(value, suffix), currency

    # Try suffix pattern: X EUR, X PLN, X Kč, X€, X£
    m = _NON_USD_SUFFIX_RE.search(s)
    if m:
        value = _parse_number_intl(m.group(1))
        suffix = m.group(2)
        code = m.group(3)
        currency = _CURRENCY_SYMBOLS.get(code) or ('CZK' if code == 'Kč' else code.upper())
        if value is not None:
            return _apply_suffix(value, suffix), currency

    return None, None


# Keep backward compat for any direct callers
_EUR_GBP_AMOUNT_RE = _NON_USD_PREFIX_RE

def _parse_eur_gbp_amount(s: str) -> tuple[float | None, str | None]:
    """Parse non-USD currency amount. Legacy name kept for compatibility."""
    return _parse_non_usd_amount(s)


def _infer_salary_period(line: str, amount: float) -> str:
    """Infer salary period from context clues or amount magnitude."""
    lower = line.lower()
    # Explicit period markers — these are unambiguous
    if re.search(r'/\s*(?:hr|hour)|per\s+hour|hourly|per\s+visit', lower):
        return 'hourly'
    if re.search(r'/\s*(?:wk|week)|per\s+week|weekly', lower):
        return 'weekly'
    if re.search(r'/\s*(?:mo|month)|per\s+month|monthly', lower):
        return 'monthly'
    if re.search(r'/\s*(?:yr|year|annum)|per\s+year|annual', lower):
        return 'annually'
    # Magnitude heuristic when no explicit period word
    if amount < 300:
        return 'hourly'
    if amount >= 10_000:
        return 'annually'
    # Ambiguous zone ($300-$10k) — could be weekly/monthly/annual, default annually
    return 'annually'


def _is_salary_plausible(min_val: float, max_val: float, period: str) -> bool:
    """Sanity-check extracted salary values."""
    if min_val <= 0 and max_val <= 0:
        return False
    if min_val < 0 or max_val < 0:
        return False
    # Max should be >= min (allow equal for single values)
    if max_val < min_val:
        return False
    # Range shouldn't be absurdly wide (max > 5x min is suspicious)
    if min_val > 0 and max_val > min_val * 5:
        return False

    if period == 'hourly':
        return 7 <= min_val <= 500 and max_val <= 500
    elif period == 'weekly':
        return 200 <= min_val <= 20_000 and max_val <= 20_000
    elif period == 'monthly':
        return 1_000 <= min_val <= 100_000 and max_val <= 100_000
    else:  # annually
        return 15_000 <= min_val <= 1_000_000 and max_val <= 1_000_000


def _extract_salary_from_description(raw_job: dict) -> dict | None:
    """Extract salary information from job description text using regex.

    Returns a dict with 'salary' and 'salary_transparency' keys, or None
    if no plausible salary is found.

    This is a fallback for when structured ATS salary fields are absent.
    It searches each line of the description for dollar/currency amounts,
    filters out false positives (funding, benefits, stipends), and
    validates the result.
    """
    desc = (
        raw_job.get('description', '')
        or raw_job.get('content', '')
        or raw_job.get('descriptionPlain', '')
        or ''
    )
    if '<' in desc:
        desc = remove_html_markup(desc, double_unescape=True)
    if not desc:
        return None

    best: dict | None = None
    best_score = -1

    for line in desc.split('\n'):
        line = line.strip()
        if not line:
            continue

        # Skip lines that are clearly not salary
        if _SALARY_REJECT_RE.search(line):
            continue

        has_signal = bool(_SALARY_SIGNAL_RE.search(line))

        # --- Try USD amounts first (but skip if line uses a non-USD currency code) ---
        usd_amounts = _USD_AMOUNT_RE.findall(line)
        # Check if line has $ amounts alongside a non-USD currency code
        _non_usd_codes_in_line = re.search(
            r'\b(CAD|AUD|NZD|SGD|MXN|HKD)\b', line, re.IGNORECASE,
        ) if usd_amounts else None
        if usd_amounts and not _non_usd_codes_in_line:
            # Try to find a range pattern: $X - $Y or $X to $Y
            # $X / hour - $Y / hour  (period marker between amounts)
            range_match = re.search(
                r'(\$\s*[\d,]+(?:\.[\d]{1,2})?\s*[KkMm]?)'
                r'\s*/\s*(?:hr|hour|yr|year|mo|month|wk|week)\s*'
                r'[-–—‑]\s*'
                r'(\$\s*[\d,]+(?:\.[\d]{1,2})?\s*[KkMm]?)',
                line,
                re.IGNORECASE,
            )
            # $X - $Y  or  $X – $Y  or  $X — $Y  or  $X ‑ $Y (non-breaking hyphen)
            if not range_match:
                range_match = re.search(
                    r'(\$\s*[\d,]+(?:\.[\d]{1,2})?\s*[KkMm]?)'
                    r'\s*[-–—‑]\s*'
                    r'(\$\s*[\d,]+(?:\.[\d]{1,2})?\s*[KkMm]?)',
                    line,
                )
            # $X to $Y  or  $X ‑to‑ $Y
            if not range_match:
                range_match = re.search(
                    r'(\$\s*[\d,]+(?:\.[\d]{1,2})?\s*[KkMm]?)'
                    r'\s*[-–—‑]?\s*to\s*[-–—‑]?\s*'
                    r'(\$\s*[\d,]+(?:\.[\d]{1,2})?\s*[KkMm]?)',
                    line,
                    re.IGNORECASE,
                )
            # $X and $Y (e.g. "between $130k and $150k")
            if not range_match:
                range_match = re.search(
                    r'(\$\s*[\d,]+(?:\.[\d]{1,2})?\s*[KkMm]?)'
                    r'\s+and\s+'
                    r'(\$\s*[\d,]+(?:\.[\d]{1,2})?\s*[KkMm]?)',
                    line,
                    re.IGNORECASE,
                )
            # "minimum $X – maximum $Y" (words before each amount)
            if not range_match:
                range_match = re.search(
                    r'minimum\s+(\$\s*[\d,]+(?:\.[\d]{1,2})?\s*[KkMm]?)'
                    r'\s*[-–—‑]\s*'
                    r'maximum\s+(\$\s*[\d,]+(?:\.[\d]{1,2})?\s*[KkMm]?)',
                    line,
                    re.IGNORECASE,
                )
            # $X- Y/hour (second missing $, followed by period marker)
            no_dollar_match = None
            if not range_match:
                no_dollar_match = re.search(
                    r'(\$\s*[\d,]+(?:\.[\d]{1,2})?\s*[KkMm]?)'
                    r'\s*[-–—‑]\s*'
                    r'([\d,]+(?:\.[\d]{1,2})?)\s*(?=[/]|\s*per\s)',
                    line,
                )
            # $X - Y (second missing $, salary signal present for confidence)
            if not range_match and not no_dollar_match and has_signal:
                no_dollar_match = re.search(
                    r'(\$\s*[\d,]+(?:\.[\d]{1,2})?\s*[KkMm]?)'
                    r'\s*[-–—‑]\s*'
                    r'([\d,]+(?:\.[\d]{1,2})?\s*[KkMm]?)',
                    line,
                )
            if not range_match and no_dollar_match:
                min_val = _parse_dollar_amount(no_dollar_match.group(1))
                raw_max = no_dollar_match.group(2).strip().replace(',', '')
                suffix = ''
                if raw_max.upper().endswith('K'):
                    suffix = 'K'
                    raw_max = raw_max[:-1]
                elif raw_max.upper().endswith('M'):
                    suffix = 'M'
                    raw_max = raw_max[:-1]
                try:
                    max_val = float(raw_max)
                except ValueError:
                    max_val = None
                if max_val is not None:
                    if suffix == 'K':
                        max_val *= 1_000
                    elif suffix == 'M':
                        max_val *= 1_000_000
                if min_val is not None and max_val is not None:
                    period = _infer_salary_period(line, min_val)
                    if _is_salary_plausible(min_val, max_val, period):
                        score = 10 + (5 if has_signal else 0)
                        if score > best_score:
                            best_score = score
                            best = {
                                'salary': {
                                    'min': min_val,
                                    'max': max_val,
                                    'currency': 'USD',
                                    'period': period,
                                },
                                'salary_transparency': 'full_range',
                            }

            if range_match:
                min_val = _parse_dollar_amount(range_match.group(1))
                max_val = _parse_dollar_amount(range_match.group(2))
                if min_val is not None and max_val is not None:
                    period = _infer_salary_period(line, min_val)
                    if _is_salary_plausible(min_val, max_val, period):
                        # Score: ranges with signal words are best
                        score = 10 + (5 if has_signal else 0)
                        if score > best_score:
                            best_score = score
                            best = {
                                'salary': {
                                    'min': min_val,
                                    'max': max_val,
                                    'currency': 'USD',
                                    'period': period,
                                },
                                'salary_transparency': 'full_range',
                            }
            elif has_signal and len(usd_amounts) == 1:
                # Single amount with salary-signal context
                val = _parse_dollar_amount(line)
                if val is not None:
                    period = _infer_salary_period(line, val)
                    if _is_salary_plausible(val, val, period):
                        score = 5
                        if score > best_score:
                            best_score = score
                            best = {
                                'salary': {
                                    'min': val,
                                    'max': val,
                                    'currency': 'USD',
                                    'period': period,
                                },
                                'salary_transparency': 'minimum_only',
                            }

        # --- Try non-USD currencies if no strong USD match yet ---
        if best_score < 10:
            val1, cur1 = _parse_non_usd_amount(line)
            if val1 is not None and cur1:
                # Try to find a range by looking for two non-USD amounts
                # separated by dash/to/and. We search for the pattern directly
                # rather than splitting (splitting hits word-hyphens like "full-time").
                range_val = None
                # Find all non-USD amounts in the line with their positions
                non_usd_finds = []
                for pat in [_NON_USD_PREFIX_RE, _NON_USD_SUFFIX_RE, _DOLLAR_WITH_CURRENCY_RE]:
                    for m in pat.finditer(line):
                        v, c = _parse_non_usd_amount(m.group(0))
                        if v is not None and c:
                            non_usd_finds.append((m.start(), m.end(), v, c))
                # Dedupe by position and sort
                non_usd_finds = sorted(set(non_usd_finds), key=lambda x: x[0])
                # Check if two consecutive finds are separated by a range separator
                if len(non_usd_finds) >= 2:
                    for i in range(len(non_usd_finds) - 1):
                        _, end1, v1, c1 = non_usd_finds[i]
                        start2, _, v2, c2 = non_usd_finds[i + 1]
                        between = line[end1:start2]
                        if re.fullmatch(r'\s*[-–—‑]+\s*', between) or re.fullmatch(r'\s+to\s+', between, re.I) or re.fullmatch(r'\s+and\s+', between, re.I):
                            cur = c1 or c2
                            if cur:
                                range_val = (v1, v2, cur)
                                break

                # If only one match, check for a bare number on the other side of a separator
                # e.g. "160,000 - 265,000 PLN" or "SGD 165,000 - 195,000"
                if not range_val and len(non_usd_finds) == 1:
                    start1, end1, v1_found, c1_found = non_usd_finds[0]

                    # Look for "NUMBER sep" BEFORE the matched amount
                    prefix = line[:start1]
                    bare_before = re.search(
                        r'([\d.,]+\s*[KkMm]?)\s*[-–—‑]\s*$', prefix,
                    ) or re.search(
                        r'([\d.,]+\s*[KkMm]?)\s+to\s+$', prefix, re.I,
                    )
                    if bare_before:
                        raw_num = bare_before.group(1).strip()
                        suffix_char = raw_num[-1] if raw_num[-1] in 'KkMm' else ''
                        num_part = raw_num.rstrip('KkMm')
                        v_bare = _parse_number_intl(num_part)
                        if v_bare is not None and suffix_char:
                            v_bare = _apply_suffix(v_bare, suffix_char)
                        if v_bare is not None:
                            range_val = (v_bare, v1_found, c1_found)

                    # Look for "sep NUMBER" AFTER the matched amount
                    if not range_val:
                        suffix_text = line[end1:]
                        bare_after = re.search(
                            r'^\s*[-–—‑]\s*([\d.,]+\s*[KkMm]?)', suffix_text,
                        ) or re.search(
                            r'^\s+to\s+([\d.,]+\s*[KkMm]?)', suffix_text, re.I,
                        )
                        if bare_after:
                            raw_num = bare_after.group(1).strip()
                            suffix_char = raw_num[-1] if raw_num[-1] in 'KkMm' else ''
                            num_part = raw_num.rstrip('KkMm')
                            v_bare = _parse_number_intl(num_part)
                            if v_bare is not None and suffix_char:
                                v_bare = _apply_suffix(v_bare, suffix_char)
                            if v_bare is not None:
                                range_val = (v1_found, v_bare, c1_found)

                if range_val:
                    min_val, max_val, currency = range_val
                    period = _infer_salary_period(line, min_val)
                    if _is_salary_plausible(min_val, max_val, period):
                        score = 10 + (5 if has_signal else 0)
                        if score > best_score:
                            best_score = score
                            best = {
                                'salary': {
                                    'min': min_val,
                                    'max': max_val,
                                    'currency': currency,
                                    'period': period,
                                },
                                'salary_transparency': 'full_range',
                            }
                elif has_signal:
                    period = _infer_salary_period(line, val1)
                    if _is_salary_plausible(val1, val1, period):
                        score = 5
                        if score > best_score:
                            best_score = score
                            best = {
                                'salary': {
                                    'min': val1,
                                    'max': val1,
                                    'currency': cur1,
                                    'period': period,
                                },
                                'salary_transparency': 'minimum_only',
                            }

            # Also check for $X CAD/AUD patterns (dollar sign + currency code nearby)
            if best_score < 10:
                # Range: $X - $Y CAD  or  $X - Y CAD  or  $X CAD - $Y CAD
                dollar_range = re.search(
                    r'(\$\s*[\d,]+(?:\.[\d]{1,2})?\s*[KkMm]?)'
                    r'\s*[-–—‑]\s*'
                    r'(\$?\s*[\d,]+(?:\.[\d]{1,2})?\s*[KkMm]?)'
                    r'\s*(CAD|AUD|NZD|SGD|MXN|HKD)\b',
                    line,
                    re.IGNORECASE,
                )
                if dollar_range:
                    min_val = _parse_dollar_amount(dollar_range.group(1))
                    max_s = dollar_range.group(2).strip()
                    if not max_s.startswith('$'):
                        max_s = '$' + max_s
                    max_val = _parse_dollar_amount(max_s)
                    currency = dollar_range.group(3).upper()
                    if min_val is not None and max_val is not None:
                        period = _infer_salary_period(line, min_val)
                        if _is_salary_plausible(min_val, max_val, period):
                            score = 10 + (5 if has_signal else 0)
                            if score > best_score:
                                best_score = score
                                best = {
                                    'salary': {
                                        'min': min_val,
                                        'max': max_val,
                                        'currency': currency,
                                        'period': period,
                                    },
                                    'salary_transparency': 'full_range',
                                }
                # Single: $X CAD
                if best_score < 10:
                    m = _DOLLAR_WITH_CURRENCY_RE.search(line)
                    if m and has_signal:
                        val = _parse_dollar_amount('$' + m.group(1))
                        currency = m.group(3).upper()
                        if val is not None:
                            period = _infer_salary_period(line, val)
                            if _is_salary_plausible(val, val, period):
                                score = 5
                                if score > best_score:
                                    best_score = score
                                    best = {
                                        'salary': {
                                            'min': val,
                                            'max': val,
                                            'currency': currency,
                                            'period': period,
                                        },
                                        'salary_transparency': 'minimum_only',
                                    }

    return best


# ---------------------------------------------------------------------------
# Description-based years-of-experience extraction
# ---------------------------------------------------------------------------

# Context words that indicate the number is about required experience, not age/history.
_EXPERIENCE_CONTEXT_RE = re.compile(
    r'experience|experiencia|expérience|erfahrung',
    re.IGNORECASE,
)

# Lines that mention years but are NOT about required experience.
_YEARS_REJECT_RE = re.compile(
    r'years?\s+(?:old|of\s+age|in\s+(?:a\s+row|business|operation|the\s+(?:industry|market|field|making)))'
    r'|(?:founded|serving|established|operating|running)\s+(?:for\s+)?\d+'
    r'|over\s+\d+\s+years?\s+(?:of\s+)?(?:experience\s+)?(?:serving|helping|providing|building|delivering|protecting|connecting)'
    r'|(?:company|firm|organization|we)\s+(?:has|have)\s+(?:been\s+)?(?:\w+\s+){0,3}for\s+(?:over\s+)?\d+\s+years?'
    r'|(?:more\s+than|over)\s+\d+\s+years?\s+(?:of\s+)?(?:history|heritage|tradition)'
    r'|\d+\s+years?\s+(?:of\s+)?(?:age|old)\b',
    re.IGNORECASE,
)

# The main extraction patterns, ordered by specificity.
# Each returns (min_years, max_years) or None.
_YEARS_PATTERNS = [
    # "5-10 years" / "5–10 years" / "5 - 10 years"
    re.compile(r'(\d{1,2})\s*[-–—]\s*(\d{1,2})\+?\s*years?', re.I),
    # "5 to 10 years"
    re.compile(r'(\d{1,2})\s+to\s+(\d{1,2})\+?\s*years?', re.I),
    # "5+ years"
    re.compile(r'(\d{1,2})\+\s*years?', re.I),
    # "at least 5 years" / "minimum of 5 years" / "minimum 5 years"
    re.compile(r'(?:at\s+least|minimum\s+(?:of\s+)?|min\.?\s+)\s*(\d{1,2})\s*years?', re.I),
    # "5 years of experience" / "5 years experience" / "5 years' experience"
    re.compile(r"(\d{1,2})\s*years?['\u2018\u2019]?\s+(?:of\s+)?(?:\w+\s+){0,3}experience", re.I),
]


def _extract_years_experience_from_description(raw_job: dict) -> dict | None:
    """Extract years of experience requirement from job description text.

    Returns a dict like {"min": 5, "max": 10} or {"min": 5, "max": None},
    or None if no plausible requirement is found.
    """
    desc = (
        raw_job.get('description', '')
        or raw_job.get('content', '')
        or raw_job.get('descriptionPlain', '')
        or ''
    )
    if '<' in desc:
        desc = remove_html_markup(desc, double_unescape=True)
    if not desc:
        return None

    best: dict | None = None
    best_score = -1

    for line in desc.split('\n'):
        line = line.strip()
        if not line:
            continue

        # Skip lines that are clearly not about required experience
        if _YEARS_REJECT_RE.search(line):
            continue

        for i, pattern in enumerate(_YEARS_PATTERNS):
            m = pattern.search(line)
            if not m:
                continue

            groups = m.groups()
            if len(groups) == 2:
                min_years = int(groups[0])
                max_years = int(groups[1])
            else:
                min_years = int(groups[0])
                max_years = None

            # Sanity checks — cap at 20 years; higher values are usually
            # age requirements or company history, not experience requirements
            if min_years > 20:
                continue
            if max_years is not None and max_years > 20:
                continue
            if max_years is not None and max_years < min_years:
                continue

            # For bare "N years" (last pattern), require experience context
            if i == len(_YEARS_PATTERNS) - 1:
                # Already has "experience" in the pattern
                pass
            elif i >= 3:
                # "at least N years" — strong signal
                pass
            else:
                # Range or N+ patterns — check that the line mentions experience
                # or is in a requirements-like context
                has_exp_context = bool(_EXPERIENCE_CONTEXT_RE.search(line))
                has_req_context = bool(re.search(
                    r'require|qualif|must\s+have|minimum|at\s+least|ideally|prefer',
                    line, re.I,
                ))
                if not has_exp_context and not has_req_context:
                    continue

            # Score: ranges > N+ > bare N. Earlier patterns = more specific.
            score = (len(_YEARS_PATTERNS) - i) * 10
            # Prefer higher min_years (more specific requirement)
            score += min_years

            if score > best_score:
                best_score = score
                best = {"min": min_years, "max": max_years}

            break  # Only take first pattern match per line

    return best


# ---------------------------------------------------------------------------
# Description-based visa sponsorship detection
# ---------------------------------------------------------------------------

_VISA_YES_RE = re.compile(
    r'(?:visa\s+sponsorship\s+(?:is\s+)?(?:available|offered|provided|possible))'
    r'|(?:(?:we|will|can|do)\s+(?:offer|provide|sponsor)\s+(?:\w+\s+)?visa)'
    r'|(?:sponsorship\s+(?:is\s+)?available)'
    r'|(?:open\s+to\s+sponsoring)'
    r'|(?:we\s+sponsor\s+visas)',
    re.IGNORECASE,
)

_VISA_NO_RE = re.compile(
    r'(?:(?:no|not|unable|cannot|doesn?.t|does\s+not|will\s+not|won.t|do\s+not)\s+'
    r'(?:\w+\s+){0,2}(?:sponsor|sponsorship|visa\s+sponsor))'
    r'|(?:visa\s+sponsorship\s+(?:is\s+)?(?:not|unavailable))'
    r'|(?:must\s+be\s+(?:legally\s+)?(?:authorized|eligible)\s+to\s+work)'
    r'|(?:(?:legally\s+)?authorized\s+to\s+work\s+in)'
    r'|(?:work\s+authorization\s+(?:is\s+)?required)'
    r'|(?:without\s+(?:the\s+need\s+for|requiring)\s+(?:\w+\s+)?sponsorship)'
    r'|(?:not\s+(?:able|in\s+a\s+position)\s+to\s+(?:\w+\s+)?sponsor)'
    r'|(?:proof\s+of\s+(?:right|eligibility)\s+to\s+work)',
    re.IGNORECASE,
)


def _detect_visa_sponsorship(raw_job: dict) -> str | None:
    """Detect visa sponsorship status from description text.

    Returns 'yes', 'no', or None (unknown).
    """
    desc = (
        raw_job.get('description', '')
        or raw_job.get('content', '')
        or raw_job.get('descriptionPlain', '')
        or ''
    )
    if '<' in desc:
        desc = remove_html_markup(desc, double_unescape=True)
    if not desc:
        return None

    has_yes = bool(_VISA_YES_RE.search(desc))
    has_no = bool(_VISA_NO_RE.search(desc))

    # "No" is more specific/authoritative — if both present, trust "no"
    if has_no:
        return 'no'
    if has_yes:
        return 'yes'
    return None


# ---------------------------------------------------------------------------
# Description-based education level extraction
# ---------------------------------------------------------------------------

_EDUCATION_PATTERNS = [
    # PhD / Doctorate
    (re.compile(
        r'\b(?:ph\.?d|doctorate|doctoral)\b'
        r'(?:\s+(?:degree|required|preferred|in\b))?',
        re.I,
    ), 'phd'),
    # Master's / MBA
    (re.compile(
        r"\b(?:master'?s?\s+(?:degree|of)|mba|m\.?s\.?\s+(?:in|degree)|m\.?a\.?\s+(?:in|degree))\b",
        re.I,
    ), 'masters'),
    # Bachelor's
    (re.compile(
        r"\b(?:bachelor'?s?\s+(?:degree|of)|b\.?s\.?\s+(?:in|degree)|b\.?a\.?\s+(?:in|degree)"
        r"|undergraduate\s+degree)\b",
        re.I,
    ), 'bachelors'),
    # Associate's
    (re.compile(
        r"\b(?:associate'?s?\s+degree)\b",
        re.I,
    ), 'associates'),
    # High school / GED
    (re.compile(
        r'\b(?:high\s+school\s+(?:diploma|degree|graduate|education)|ged)\b',
        re.I,
    ), 'high-school'),
    # No degree required
    (re.compile(
        r'\b(?:no\s+degree\s+(?:required|necessary|needed)|degree\s+not\s+required)\b',
        re.I,
    ), 'none'),
]

# Context words that distinguish required vs preferred education
_EDUCATION_REQUIRED_RE = re.compile(
    r'require|must\s+have|minimum|needed|necessary|mandatory',
    re.IGNORECASE,
)
_EDUCATION_PREFERRED_RE = re.compile(
    r'prefer|ideal|nice\s+to\s+have|desired|bonus|plus|advantage|asset',
    re.IGNORECASE,
)


def _extract_education_from_description(raw_job: dict) -> str | None:
    """Extract minimum education requirement from description.

    Strategy: find education levels with "required" context first. If none,
    fall back to any mentioned level. When multiple levels are mentioned,
    pick the LOWEST required level (that's the minimum requirement).
    e.g. "Bachelor's required, Master's preferred" -> "bachelors"
    """
    desc = (
        raw_job.get('description', '')
        or raw_job.get('content', '')
        or raw_job.get('descriptionPlain', '')
        or ''
    )
    if '<' in desc:
        desc = remove_html_markup(desc, double_unescape=True)
    if not desc:
        return None

    level_priority = {'phd': 6, 'masters': 5, 'bachelors': 4, 'associates': 3, 'high-school': 2, 'none': 1}

    required_levels = []  # Levels with "required" context
    mentioned_levels = []  # All mentioned levels

    for line in desc.split('\n'):
        line = line.strip()
        if not line:
            continue

        for pattern, level in _EDUCATION_PATTERNS:
            if pattern.search(line):
                # "no degree" only counts with explicit requirement context
                if level == 'none' and not _EDUCATION_REQUIRED_RE.search(line):
                    continue

                is_required = bool(_EDUCATION_REQUIRED_RE.search(line))
                is_preferred = bool(_EDUCATION_PREFERRED_RE.search(line))

                if is_required and not is_preferred:
                    required_levels.append(level)
                else:
                    # "preferred" or neutral context — lower confidence
                    mentioned_levels.append(level)

    # Pick the minimum required level if any; otherwise minimum mentioned level
    candidates = required_levels or mentioned_levels
    if not candidates:
        return None

    # Return the lowest level (the minimum requirement)
    return min(candidates, key=lambda l: level_priority.get(l, 0))


# ---------------------------------------------------------------------------
# Workable industry field mapping
# ---------------------------------------------------------------------------

_WORKABLE_INDUSTRY_MAP = {
    'information technology and services': 'enterprise_software',
    'computer software': 'enterprise_software',
    'internet': 'enterprise_software',
    'computer & network security': 'cybersecurity',
    'hospital & health care': 'healthcare_services',
    'mental health care': 'healthcare_services',
    'health, wellness and fitness': 'healthcare_services',
    'medical devices': 'biotech_life_sciences',
    'biotechnology': 'biotech_life_sciences',
    'pharmaceuticals': 'pharma',
    'financial services': 'payments_banking',
    'banking': 'payments_banking',
    'insurance': 'insurance',
    'investment management': 'investing_trading',
    'capital markets': 'investing_trading',
    'venture capital & private equity': 'investing_trading',
    'marketing and advertising': 'advertising_marketing',
    'market research': 'advertising_marketing',
    'public relations and communications': 'advertising_marketing',
    'retail': 'commerce_marketplaces',
    'e-commerce': 'commerce_marketplaces',
    'consumer goods': 'consumer_goods_brands',
    'luxury goods & jewelry': 'consumer_goods_brands',
    'food & beverages': 'food_beverage',
    'restaurants': 'food_beverage',
    'food production': 'food_beverage',
    'hospitality': 'travel_hospitality',
    'leisure, travel & tourism': 'travel_hospitality',
    'education management': 'education_edtech',
    'e-learning': 'education_edtech',
    'primary/secondary education': 'education_edtech',
    'higher education': 'education_edtech',
    'entertainment': 'media_entertainment',
    'media production': 'media_entertainment',
    'online media': 'media_entertainment',
    'broadcast media': 'media_entertainment',
    'computer games': 'gaming',
    'consumer electronics': 'semiconductors_hardware',
    'semiconductors': 'semiconductors_hardware',
    'electrical/electronic manufacturing': 'semiconductors_hardware',
    'automotive': 'transportation_logistics',
    'transportation/trucking/railroad': 'transportation_logistics',
    'logistics and supply chain': 'transportation_logistics',
    'construction': 'construction_built_environment',
    'building materials': 'construction_built_environment',
    'architecture & planning': 'construction_built_environment',
    'real estate': 'real_estate_proptech',
    'renewables & environment': 'climate_sustainability',
    'environmental services': 'climate_sustainability',
    'oil & energy': 'energy_utilities',
    'utilities': 'energy_utilities',
    'mining & metals': 'energy_utilities',
    'defense & space': 'defense_public_safety',
    'military': 'defense_public_safety',
    'government administration': 'government_public_sector',
    'government relations': 'government_public_sector',
    'law practice': 'legal',
    'legal services': 'legal',
    'management consulting': 'consulting_professional_services',
    'professional training & coaching': 'consulting_professional_services',
    'human resources': 'staffing_recruiting',
    'staffing and recruiting': 'staffing_recruiting',
    'nonprofit organization management': 'nonprofit_philanthropy',
    'philanthropy': 'nonprofit_philanthropy',
    'telecommunications': 'telecommunications_networking',
    'wireless': 'telecommunications_networking',
    'farming': 'agriculture',
    'manufacturing': 'manufacturing_industrials',
    'industrial automation': 'manufacturing_industrials',
    'mechanical or industrial engineering': 'manufacturing_industrials',
    'machinery': 'manufacturing_industrials',
    'facilities services': 'manufacturing_industrials',
    'aviation & aerospace': 'space_aerospace',
    'outsourcing/offshoring': 'bpo_outsourcing',
    'sports': 'media_entertainment',
}


def _map_workable_industry(raw_job: dict) -> str | None:
    """Map Workable 'industry' field to our industry enum value."""
    industry = raw_job.get('industry')
    if not isinstance(industry, str) or not industry.strip():
        return None
    return _WORKABLE_INDUSTRY_MAP.get(industry.strip().lower())


# ---------------------------------------------------------------------------
# Title/description-based manager detection
# ---------------------------------------------------------------------------

# Title patterns that strongly indicate a people-management role.
_MANAGER_TITLE_POSITIVE_RE = re.compile(
    r'\b(?:'
    # DOMAIN + manager/director/lead: "Engineering Manager", "Data Science Director"
    # Only domains where "X Manager" reliably means people management.
    # Excluded: product, content, brand, growth, analytics, performance — often IC roles.
    r'(?:engineering|software|design|data|QA|platform|infrastructure|'
    r'DevOps|SRE|machine\s+learning|ML|AI|frontend|backend|mobile|security|'
    r'science|research|creative|'
    r'revenue|finance|accounting|legal|people|talent|recruiting|'
    r'clinical|nursing|operations|supply\s+chain|logistics|manufacturing|'
    r'quality|facilities|IT|support|delivery)\s+'
    r'(?:manager|mgr|director|dir|lead|leader|head)'
    # "Senior/Associate Manager, DOMAIN": "Senior Manager, AI Engineering"
    r'|(?:senior|associate|assistant|principal|staff|group)\s+(?:manager|mgr|director),?\s+\w+'
    # "Director/Head/VP of X" or "Director, X"
    r'|(?:director|head|vp|vice\s+president)(?:\s+of\b|,)'
    # Physical-location managers: "Store Manager", "Restaurant Manager"
    r'|(?:store|restaurant|warehouse|plant|site|branch|district|regional|general|'
    r'assistant\s+(?:store|site|general))\s+manager'
    # "Team Lead", "Department Head"
    r'|(?:team|group|department|division|section)\s+(?:lead|leader|manager|head)'
    # C-suite, Managing Director
    r'|(?:managing\s+director|chief\s+\w+\s+officer|c[a-z]o)\b'
    r'|supervisor\b'
    r'|superintendent\b'
    r')',
    re.IGNORECASE,
)

# Title patterns that use "manager/lead/director" but are NOT people management.
_MANAGER_TITLE_NEGATIVE_RE = re.compile(
    r'\b(?:'
    r'(?:account|customer\s+success|partner(?:ship)?|relationship|client(?:\s+success)?|'
    r'project|program|product\s+marketing|product|event|community|social\s+media|'
    r'office|case|care|property|campaign|vendor|compliance|risk|'
    r'(?:implementation|integration|migration|onboarding)|'
    r'analytics|digital\s+analytics|devrel|developer\s+relations)\s+'
    r'(?:manager|mgr|lead|director)'
    r'|(?:technical|solution|field)\s+(?:lead|manager)'
    r'|lead\s+(?:engineer|developer|designer|scientist|analyst|architect|counsel|therapist|nurse|technician|mechanic)'
    r')\b',
    re.IGNORECASE,
)

# Description signals that indicate people management.
_MANAGER_DESC_SIGNALS = [
    re.compile(r'(?:manage|lead|mentor|grow|build)\s+(?:a\s+)?team\s+of\b', re.I),
    re.compile(r'\b\d+\s*[-–—+]\s*direct\s+reports?\b', re.I),
    re.compile(r'\bdirect\s+reports?\b', re.I),
    re.compile(r'\bpeople\s+management\b', re.I),
    re.compile(r'\bsupervis(?:e|ing)\s+(?:a\s+)?(?:team|staff|employees|workers)\b', re.I),
    re.compile(r'\bhire\s+and\s+(?:develop|train|mentor)\b', re.I),
    re.compile(r'\bbuild\s+and\s+lead\b', re.I),
    re.compile(r'\bteam\s+of\s+\d+\b', re.I),
    re.compile(r'\breporting\s+to\s+you\b', re.I),
]


def _detect_is_manager(raw_job: dict) -> bool | None:
    """Detect whether a job is a people-management role from title and description.

    Returns True if strong manager signal, False if strong IC signal, None if ambiguous.
    """
    title = raw_job.get('title', '') or ''

    # Check title patterns
    has_positive_title = bool(_MANAGER_TITLE_POSITIVE_RE.search(title))
    has_negative_title = bool(_MANAGER_TITLE_NEGATIVE_RE.search(title))

    # If title is clearly positive and not contradicted by a negative pattern, it's a manager
    if has_positive_title and not has_negative_title:
        return True

    # If title has a negative pattern (Account Manager, Project Manager, etc.), not a manager
    # unless description has strong people-management signals
    if has_negative_title and not has_positive_title:
        # Check description for overriding signals
        desc = (
            raw_job.get('description', '')
            or raw_job.get('content', '')
            or raw_job.get('descriptionPlain', '')
            or ''
        )
        if '<' in desc:
            desc = remove_html_markup(desc, double_unescape=True)
        signal_count = sum(1 for pat in _MANAGER_DESC_SIGNALS if pat.search(desc))
        if signal_count >= 2:
            return True
        return False

    # Title doesn't match either pattern — check description
    desc = (
        raw_job.get('description', '')
        or raw_job.get('content', '')
        or raw_job.get('descriptionPlain', '')
        or ''
    )
    if '<' in desc:
        desc = remove_html_markup(desc, double_unescape=True)
    signal_count = sum(1 for pat in _MANAGER_DESC_SIGNALS if pat.search(desc))
    if signal_count >= 2:
        return True

    return None  # Ambiguous — don't override


# ---------------------------------------------------------------------------
# Job loading helpers
# ---------------------------------------------------------------------------

def merge_api_data(raw_job: dict, llm_metadata: dict) -> dict:
    """Overlay structured API data onto LLM-extracted metadata.
    API data wins for fields it provides — more reliable and free."""

    merged = dict(llm_metadata)

    # --- Posting language: prefer structured/raw hints when present ---
    posting_language = _derive_posting_language(raw_job)
    if posting_language:
        merged["posting_language"] = posting_language
    else:
        merged["posting_language"] = _normalize_language_code(merged.get("posting_language"))

    # --- Salary: Greenhouse pay_input_ranges ---
    pay_ranges = raw_job.get("pay_input_ranges", [])
    if pay_ranges:
        pay = pay_ranges[0]  # use first range
        min_cents = pay.get("min_cents")
        max_cents = pay.get("max_cents")
        currency = pay.get("currency_type", "USD")
        title = (pay.get("title") or "").lower()
        period = "annually"
        if "hour" in title:
            period = "hourly"
        elif "month" in title:
            period = "monthly"
        elif "week" in title:
            period = "weekly"
        if min_cents or max_cents:
            merged["salary"] = {
                "min": min_cents / 100 if min_cents else None,
                "max": max_cents / 100 if max_cents else None,
                "currency": currency,
                "period": period,
            }
            merged["salary_transparency"] = "full_range" if (min_cents and max_cents) else "minimum_only"

    # --- Salary: Ashby compensationSalarySummary ---
    comp_salary = raw_job.get("compensationSalarySummary")
    if comp_salary and not (merged.get("salary") or {}).get("min"):
        import re
        # Parse "$150K - $250K" style
        amounts = re.findall(r'\$[\d,.]+[KkMm]?', comp_salary)
        if amounts:
            def parse_amount(s):
                s = s.replace('$', '').replace(',', '')
                multiplier = 1
                if s.upper().endswith('K'):
                    multiplier = 1000
                    s = s[:-1]
                elif s.upper().endswith('M'):
                    multiplier = 1_000_000
                    s = s[:-1]
                return float(s) * multiplier
            try:
                min_val = parse_amount(amounts[0])
                max_val = parse_amount(amounts[1]) if len(amounts) >= 2 else min_val
                merged["salary"] = {
                    "min": min_val,
                    "max": max_val,
                    "currency": "USD",
                    "period": "annually",
                }
                merged["salary_transparency"] = "full_range" if len(amounts) >= 2 else "minimum_only"
            except (ValueError, IndexError):
                pass

    # --- Salary: description text fallback ---
    if not (merged.get("salary") or {}).get("min"):
        desc_salary = _extract_salary_from_description(raw_job)
        if desc_salary:
            merged["salary"] = desc_salary["salary"]
            merged["salary_transparency"] = desc_salary["salary_transparency"]

    # --- Office type: structured ATS data is strong for remote/hybrid, softer for onsite ---
    merged["office_type"] = _choose_office_type(raw_job, merged)
    if merged.get("office_type") != "hybrid":
        merged["hybrid_days"] = None

    llm_requirements = _dedupe_requirements(merged.get("applicant_location_requirements") or [])
    if merged.get("office_type") != "remote":
        merged["applicant_location_requirements"] = []
    else:
        merged["applicant_location_requirements"] = llm_requirements

    # --- Job type: Ashby employmentType, Lever commitment ---
    emp_type = raw_job.get("employmentType", "")
    commitment = raw_job.get("commitment", "")
    type_str = (emp_type or commitment).lower()
    if type_str:
        type_map = {
            "fulltime": "full-time", "full-time": "full-time", "permanent": "full-time",
            "parttime": "part-time", "part-time": "part-time",
            "contract": "contract", "contractor": "contract",
            "intern": "internship", "internship": "internship",
            "temporary": "temporary", "temp": "temporary",
            "freelance": "freelance",
        }
        mapped = type_map.get(type_str)
        if mapped:
            merged["job_type"] = mapped

    # --- Equity: Ashby compensation summary ---
    comp_summary = raw_job.get("compensationTierSummary", "")
    if comp_summary and "equity" in comp_summary.lower():
        if isinstance(merged.get("equity"), dict):
            merged["equity"]["offered"] = True
        else:
            merged["equity"] = {"offered": True, "min_pct": None, "max_pct": None}

    if not merged.get("experience_level"):
        structured_experience_level = _derive_structured_experience_level(raw_job)
        if structured_experience_level:
            merged["experience_level"] = structured_experience_level

    if not merged.get("education_level"):
        structured_education_level = _derive_structured_education_level(raw_job)
        if structured_education_level:
            merged["education_level"] = structured_education_level

    # --- Years of experience: description text fallback ---
    if not (merged.get("years_experience") or {}).get("min"):
        desc_years = _extract_years_experience_from_description(raw_job)
        if desc_years:
            merged["years_experience"] = desc_years

    # --- Manager detection: title + description fallback ---
    if "is_manager" not in merged:
        detected = _detect_is_manager(raw_job)
        if detected is not None:
            merged["is_manager"] = detected

    # --- Visa sponsorship: description fallback ---
    if merged.get("visa_sponsorship", "unknown") == "unknown":
        visa = _detect_visa_sponsorship(raw_job)
        if visa:
            merged["visa_sponsorship"] = visa

    # --- Education level: description fallback ---
    if not merged.get("education_level"):
        desc_edu = _extract_education_from_description(raw_job)
        if desc_edu:
            merged["education_level"] = desc_edu

    # --- Industry: Workable industry field mapping ---
    if not merged.get("industry_primary"):
        mapped = _map_workable_industry(raw_job)
        if mapped:
            merged["industry_primary"] = mapped
            _canonicalize_industry_fields(merged)

    # --- Applicant geography for remote roles ---
    ats_requirements, ats_requirements_source = _derive_remote_applicant_location_requirements_with_source(
        raw_job,
        merged.get("office_type"),
    )
    if merged.get("office_type") == "remote":
        merged["applicant_location_requirements"] = _choose_remote_requirement_source(
            llm_requirements,
            ats_requirements,
            ats_requirements_source,
        )

    # --- Work locations ---
    merged["locations"] = _derive_work_locations(
        raw_job,
        merged.get("office_type"),
        merged.get("locations"),
    )

    if merged.get("office_type") == "remote" and not merged.get("applicant_location_requirements"):
        merged["applicant_location_requirements"] = _derive_remote_requirements_from_locations(
            merged.get("locations"),
        )

    _canonicalize_industry_fields(merged)
    return merged


_LINKEDIN_WORKPLACE_TAGS = {
    "#LI-REMOTE": "remote",
    "#LI-HYBRID": "hybrid",
    "#LI-ONSITE": "onsite",
}

_REGION_GROUPS = {
    "EMEA",
    "APAC",
    "LATAM",
    "ANZ",
    "EU",
    "UKI",
}

_SUBNATIONAL_ABBREVIATIONS = {
    # United States
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
    # Canada
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE",
    "QC", "SK", "YT",
    # Australia
    "ACT", "NSW", "NT", "QLD", "SA", "TAS", "VIC", "WA",
}

_COUNTRY_CODE_ALIASES = {
    "US": "US",
    "USA": "US",
    "UNITED STATES": "US",
    "UNITED STATES OF AMERICA": "US",
    "CA": "CA",
    "CANADA": "CA",
    "MX": "MX",
    "MEXICO": "MX",
    "AU": "AU",
    "AUSTRALIA": "AU",
    "NZ": "NZ",
    "NEW ZEALAND": "NZ",
    "GB": "GB",
    "UK": "GB",
    "UNITED KINGDOM": "GB",
    "JAPAN": "JP",
    "INDIA": "IN",
    "SWEDEN": "SE",
    "FRANCE": "FR",
    "GERMANY": "DE",
    "NETHERLANDS": "NL",
    "SPAIN": "ES",
    "ARGENTINA": "AR",
    "COLOMBIA": "CO",
    "PANAMA": "PA",
    "PERU": "PE",
    "PORTUGAL": "PT",
}


def _derive_structured_experience_level(raw_job: dict) -> str | None:
    value = _to_str(raw_job.get("experience"))
    if not value:
        return None

    normalized = value.strip().lower()
    if any(token in normalized for token in ("executive", "vp", "vice president", "chief", "c-suite")):
        return "executive"
    if "principal" in normalized:
        return "principal"
    if "staff" in normalized:
        return "staff"
    if any(token in normalized for token in ("director", "lead", "senior", "mid-senior")):
        return "senior"
    if any(token in normalized for token in ("mid", "intermediate")):
        return "mid"
    if any(token in normalized for token in ("entry", "junior", "associate", "intern")):
        return "entry"
    return None


def _derive_structured_education_level(raw_job: dict) -> str | None:
    value = _to_str(raw_job.get("education"))
    if not value:
        return None

    normalized = value.strip().lower()
    if any(token in normalized for token in ("phd", "doctorate", "doctoral")):
        return "phd"
    if any(token in normalized for token in ("master", "mba", "m.s", "msc", "m.a")):
        return "masters"
    if any(token in normalized for token in ("bachelor", "b.s", "ba ", "b.a", "undergraduate")):
        return "bachelors"
    if "high school" in normalized:
        return "high-school"
    if any(token in normalized for token in ("no degree", "none required", "no education")):
        return "none"
    return None


def _derive_posting_language(raw_job: dict) -> str | None:
    candidates = [
        raw_job.get("postingLanguage"),
        raw_job.get("inLanguage"),
        raw_job.get("language"),
        raw_job.get("lang"),
        raw_job.get("locale"),
    ]
    for candidate in candidates:
        if isinstance(candidate, str):
            normalized = _normalize_language_code(candidate)
            if normalized:
                return normalized
    detector_guess = _detect_posting_language_with_lingua(raw_job)
    if detector_guess:
        return detector_guess
    title = raw_job.get("title")
    content = (
        raw_job.get("content")
        or raw_job.get("description")
        or raw_job.get("descriptionPlain")
        or raw_job.get("descriptionHtml")
        or ""
    )
    text = "\n".join(
        part for part in (
            title if isinstance(title, str) else "",
            remove_html_markup(content, double_unescape=True) if isinstance(content, str) else "",
        )
        if part
    )
    return _guess_posting_language_from_text(text)


def prepare_language_detection_text(raw_job: dict, max_chars: int = 5000) -> str:
    """Prepare text for language identification.

    Unlike prepare_job_text(), this intentionally excludes English ATS metadata
    prefixes like "Location:" and "Department:" because they skew language ID.
    """
    title = raw_job.get("title", "") or ""
    content = (
        raw_job.get("content")
        or raw_job.get("descriptionPlain")
        or raw_job.get("descriptionHtml")
        or raw_job.get("description")
        or ""
    )
    if content:
        content = remove_html_markup(content, double_unescape=True)

    parts = []
    if isinstance(title, str) and title.strip():
        parts.append(title.strip())
    if isinstance(content, str) and content.strip():
        parts.append(content.strip())

    text = "\n\n".join(parts)
    text = re.sub(r"\b[A-Z]{2,}\d+[A-Z0-9]*\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


@lru_cache(maxsize=1)
def _get_lingua_detector():
    from lingua import Language, LanguageDetectorBuilder

    languages = [
        Language.ENGLISH,
        Language.FRENCH,
        Language.GERMAN,
        Language.SPANISH,
        Language.PORTUGUESE,
        Language.ITALIAN,
        Language.DUTCH,
        Language.POLISH,
        Language.SWEDISH,
        Language.DANISH,
        Language.FINNISH,
        Language.NORWEGIAN_BOKMAL,
        Language.CZECH,
        Language.HUNGARIAN,
        Language.ROMANIAN,
        Language.GREEK,
        Language.TURKISH,
        Language.UKRAINIAN,
        Language.RUSSIAN,
        Language.JAPANESE,
        Language.KOREAN,
        Language.CHINESE,
    ]
    return LanguageDetectorBuilder.from_languages(*languages).build()


def _detect_posting_language_with_lingua(raw_job: dict) -> str | None:
    text = prepare_language_detection_text(raw_job)
    if len(text) < 20:
        return None

    try:
        detector = _get_lingua_detector()
    except Exception:
        return None

    try:
        values = detector.compute_language_confidence_values(text)
    except Exception:
        return None
    if not values:
        return None

    top = values[0]
    if getattr(top, "value", 0.0) < 0.6:
        return None

    language = getattr(top, "language", None)
    iso = getattr(language, "iso_code_639_1", None)
    name = getattr(iso, "name", None)
    if not name:
        return None
    return name.lower()


_LANGUAGE_STOPWORDS = {
    "en": {"the", "and", "for", "with", "you", "your", "will", "our", "this", "that", "team", "role"},
    "fr": {"nous", "vous", "notre", "votre", "avec", "dans", "pour", "une", "des", "les", "est", "grâce", "chez", "rejoins", "poste"},
    "de": {"und", "der", "die", "das", "mit", "für", "ein", "eine", "unser", "unsere", "rolle", "team"},
    "es": {"con", "para", "una", "las", "los", "nuestro", "nuestra", "equipo", "puesto", "trabajo"},
    "pt": {"com", "para", "uma", "das", "dos", "nosso", "nossa", "equipe", "vaga", "trabalho"},
    "it": {"con", "per", "una", "della", "delle", "nostro", "nostra", "team", "ruolo", "posizione"},
    "nl": {"met", "voor", "een", "ons", "onze", "team", "rol", "functie", "je", "jij"},
}


def _guess_posting_language_from_text(text: str | None) -> str | None:
    if not text:
        return None

    if re.search(r"[\u3040-\u30ff]", text):
        return "ja"
    if re.search(r"[\uac00-\ud7af]", text):
        return "ko"

    lowered = text.lower()
    tokens = re.findall(r"[a-zà-ÿ]+", lowered)
    if len(tokens) < 8:
        return None

    best_lang = None
    best_score = 0
    for lang, stopwords in _LANGUAGE_STOPWORDS.items():
        score = sum(1 for token in tokens if token in stopwords)
        if score > best_score:
            best_lang = lang
            best_score = score

    if best_score >= 4:
        return best_lang

    return None


def _extract_linkedin_workplace_tag(raw_job: dict) -> str | None:
    fields = [
        raw_job.get("description"),
        raw_job.get("content"),
        raw_job.get("descriptionPlain"),
    ]
    haystack = "\n".join(field for field in fields if isinstance(field, str)).upper()
    for tag, office_type in _LINKEDIN_WORKPLACE_TAGS.items():
        if tag in haystack:
            return office_type
    return None


def _country_code_from_value(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if len(value) == 2 and value.isalpha():
        return value.upper()
    return _COUNTRY_CODE_ALIASES.get(value.upper())


def _make_applicant_requirement(scope: str, name: str, country_code: str | None = None,
                                region: str | None = None) -> dict:
    return {
        "scope": scope,
        "name": name,
        "country_code": country_code,
        "region": region,
    }


def _dedupe_requirements(requirements: list[dict]) -> list[dict]:
    seen = set()
    deduped = []
    for req in requirements:
        scope = req.get("scope")
        if scope == "country" and req.get("country_code"):
            key = ("country", req.get("country_code"))
        elif scope == "region_group":
            key = ("region_group", (req.get("name") or "").upper())
        else:
            key = (
                scope,
                req.get("name"),
                req.get("country_code"),
                req.get("region"),
            )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(req)
    return deduped


_APPLICANT_REQUIREMENT_SPECIFICITY = {
    "region_group": 1,
    "country": 2,
    "state": 3,
    "city": 4,
}


def _requirement_key(requirement: dict) -> tuple:
    scope = requirement.get("scope")
    if scope == "country" and requirement.get("country_code"):
        return ("country", requirement.get("country_code"))
    if scope == "region_group":
        return ("region_group", (requirement.get("name") or "").upper())
    return (
        scope,
        requirement.get("name"),
        requirement.get("country_code"),
        requirement.get("region"),
    )


def _requirements_specificity(requirements: list[dict]) -> int:
    best = 0
    for requirement in requirements or []:
        best = max(best, _APPLICANT_REQUIREMENT_SPECIFICITY.get(requirement.get("scope"), 0))
    return best


def _requirement_keys(requirements: list[dict]) -> set[tuple]:
    return {_requirement_key(requirement) for requirement in requirements or [] if isinstance(requirement, dict)}


def _prefer_requirement_superset(left: list[dict], right: list[dict]) -> list[dict] | None:
    left_keys = _requirement_keys(left)
    right_keys = _requirement_keys(right)
    if not left_keys or not right_keys:
        return None
    if left_keys <= right_keys:
        return right
    if right_keys <= left_keys:
        return left
    return None


def _choose_remote_requirement_source(
    llm_requirements: list[dict],
    ats_requirements: list[dict],
    ats_source_strength: str | None,
) -> list[dict]:
    llm_requirements = _dedupe_requirements(llm_requirements or [])
    ats_requirements = _dedupe_requirements(ats_requirements or [])

    if not ats_requirements:
        return llm_requirements
    if not llm_requirements:
        return ats_requirements

    llm_specificity = _requirements_specificity(llm_requirements)
    ats_specificity = _requirements_specificity(ats_requirements)

    if llm_specificity > ats_specificity:
        return llm_requirements
    if ats_specificity > llm_specificity:
        return ats_requirements

    superset = _prefer_requirement_superset(llm_requirements, ats_requirements)
    if superset is not None:
        return _dedupe_requirements(superset)

    if ats_source_strength == "structured":
        return ats_requirements
    if ats_source_strength == "text":
        return llm_requirements
    return llm_requirements


def _make_location(label: str | None = None, city: str | None = None, state: str | None = None,
                   country_code: str | None = None, lat: float | None = None,
                   lng: float | None = None) -> dict:
    if not label:
        label_parts = [part for part in (city, state, country_code) if part]
        label = ", ".join(label_parts) if label_parts else None
    return {
        "label": label,
        "city": city,
        "state": state,
        "country_code": country_code,
        "lat": lat,
        "lng": lng,
    }


def _dedupe_locations(locations: list[dict]) -> list[dict]:
    seen = set()
    deduped = []
    for loc in locations:
        if loc.get("city") or loc.get("state") or loc.get("country_code"):
            key = (
                "structured",
                loc.get("city"),
                loc.get("state"),
                loc.get("country_code"),
            )
        else:
            key = ("label", (loc.get("label") or "").lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(loc)
    return deduped


def _clean_remote_location_token(token: str) -> str:
    token = token.strip()
    token = re.sub(r"\bremote\b", "", token, flags=re.IGNORECASE)
    token = re.sub(r"\bbased\b", "", token, flags=re.IGNORECASE)
    token = re.sub(r"[()]", "", token)
    token = re.sub(r"\s+", " ", token).strip(" ,-;/")
    return token


def _clean_location_token(token: str) -> str:
    token = token.strip()
    token = re.sub(r"\([^)]*\)", "", token)
    token = re.sub(r"\s+", " ", token).strip(" ,-;/")
    return token


def _looks_like_multi_location_label(label: str | None) -> bool:
    if not label:
        return False
    return bool(re.search(r"[;|\n]", label))


def _split_location_label(label: str | None) -> list[str]:
    if not label:
        return []
    if not _looks_like_multi_location_label(label):
        cleaned = _clean_location_token(label)
        return [cleaned] if cleaned else []
    parts = [
        _clean_location_token(part)
        for part in re.split(r"\s*(?:;|\||\n)+\s*", label)
    ]
    return [part for part in parts if part]


def _region_should_be_treated_as_admin1(region: str | None) -> bool:
    if not region:
        return False
    normalized = region.strip().upper()
    return normalized in _SUBNATIONAL_ABBREVIATIONS


def _normalize_existing_location_fields(label: str | None, city: str | None, state: str | None,
                                        country_code: str | None) -> tuple[str | None, str | None, str | None, str | None]:
    if label and city and not state and country_code and _region_should_be_treated_as_admin1(country_code):
        parsed = _parse_generic_location_label(label)
        if parsed and parsed.get("city") == city and parsed.get("state") and not parsed.get("country_code"):
            return label, city, parsed.get("state"), None
    return label, city, state, country_code


def _parse_generic_location_label(label: str | None) -> dict | None:
    if not label:
        return None
    cleaned = _clean_location_token(label)
    if not cleaned:
        return None
    upper = cleaned.upper()
    if upper in {"REMOTE", "HYBRID", "ONSITE", "ON-SITE", "IN-OFFICE", "IN OFFICE"}:
        return None
    if re.match(r"^\d+\s+LOCATIONS?$", upper):
        return None

    country_code = _country_code_from_value(cleaned)
    if country_code:
        return _make_location(label=cleaned, country_code=country_code)

    if "," in cleaned:
        parts = [part.strip() for part in cleaned.split(",") if part.strip()]
        if len(parts) == 2:
            city, region = parts
            maybe_country = None if _region_should_be_treated_as_admin1(region) else _country_code_from_value(region)
            if maybe_country:
                return _make_location(label=cleaned, city=city, country_code=maybe_country)
            return _make_location(label=cleaned, city=city, state=region)
        if len(parts) >= 3:
            city = parts[0]
            state = parts[1]
            country_code = _country_code_from_value(parts[-1]) or parts[-1]
            return _make_location(label=cleaned, city=city, state=state, country_code=country_code)

    return _make_location(label=cleaned)


def _parse_generic_location_labels(label: str | None) -> list[dict]:
    parsed = []
    for part in _split_location_label(label):
        location = _parse_generic_location_label(part)
        if location:
            parsed.append(location)
    return parsed


def _parse_broad_remoteish_work_location(label: str | None) -> list[dict]:
    if not label or "remote" not in label.lower():
        return []
    cleaned = _clean_remote_location_token(label)
    if not cleaned:
        return []
    location = _parse_generic_location_label(cleaned)
    if location:
        return [location]
    return [_make_location(label=cleaned)]


def _derive_remote_requirements_from_text(text: str) -> list[dict]:
    if not text:
        return []

    reqs = []
    upper_text = text.upper()
    for region_group in _REGION_GROUPS:
        if region_group in upper_text:
            reqs.append(_make_applicant_requirement("region_group", region_group))

    normalized = text.replace(";", "|").replace("/", "|")
    normalized = re.sub(r"\s+&\s+", "|", normalized)
    parts = [_clean_remote_location_token(part) for part in normalized.split("|")]
    parts = [part for part in parts if part]

    if len(parts) == 1:
        part = parts[0]
        if part.lower().startswith("remote,"):
            part = _clean_remote_location_token(part.split(",", 1)[1])
            parts = [part] if part else []
        elif part.upper().endswith("-REMOTE"):
            part = _clean_remote_location_token(part.rsplit("-", 1)[0])
            parts = [part] if part else []

    for part in parts:
        country_code = _country_code_from_value(part)
        if country_code:
            reqs.append(_make_applicant_requirement("country", part, country_code=country_code))
            continue

        parsed_location = _parse_generic_location_label(part)
        if not parsed_location:
            continue

        parsed_country_code = _country_code_from_value(parsed_location.get("country_code"))
        if not parsed_country_code:
            continue

        country_name = None
        if "," in part:
            tail = part.rsplit(",", 1)[-1].strip()
            if _country_code_from_value(tail):
                country_name = tail

        reqs.append(
            _make_applicant_requirement(
                "country",
                country_name or parsed_country_code,
                country_code=parsed_country_code,
            )
        )

    return _dedupe_requirements(reqs)


def _derive_ats_office_type(raw_job: dict) -> str | None:
    workplace = _to_str(raw_job.get("workplaceType"))
    if workplace:
        wp_lower = workplace.lower()
        if wp_lower == "remote":
            return "remote"
        if wp_lower == "hybrid":
            return "hybrid"
        if wp_lower in {"onsite", "on-site", "in-office"}:
            return "onsite"

    if raw_job.get("isRemote") is True:
        return "remote"

    return _extract_linkedin_workplace_tag(raw_job)


def _llm_has_strong_workplace_signal(metadata: dict) -> bool:
    office_type = _to_str(metadata.get("office_type"))
    if office_type == "hybrid":
        hybrid_days = metadata.get("hybrid_days")
        return isinstance(hybrid_days, int) and hybrid_days > 0
    if office_type == "remote":
        return bool(metadata.get("applicant_location_requirements"))
    return False


def _choose_office_type(raw_job: dict, llm_metadata: dict) -> str | None:
    llm_office_type = _to_str(llm_metadata.get("office_type"))
    ats_office_type = _derive_ats_office_type(raw_job)

    if ats_office_type in {"remote", "hybrid"}:
        return ats_office_type
    if ats_office_type == "onsite":
        if llm_office_type in {"remote", "hybrid"} and _llm_has_strong_workplace_signal(llm_metadata):
            return llm_office_type
        return "onsite"
    return llm_office_type


def _derive_remote_applicant_location_requirements_with_source(
    raw_job: dict,
    office_type: str | None,
) -> tuple[list[dict], str | None]:
    if office_type != "remote":
        return [], None

    reqs = []

    # Ashby structured geography is the strongest signal we have.
    location_country = raw_job.get("locationCountry")
    if location_country:
        reqs.append(
            _make_applicant_requirement(
                "country",
                location_country,
                country_code=_country_code_from_value(location_country),
            )
        )

    secondary_locations = raw_job.get("secondaryLocations") or []
    secondary_countries = {
        sl.get("country")
        for sl in secondary_locations
        if isinstance(sl, dict) and sl.get("country")
    }
    if secondary_countries:
        reqs.extend(
            _make_applicant_requirement(
                "country",
                country,
                country_code=_country_code_from_value(country),
            )
            for country in sorted(secondary_countries)
        )

    if reqs:
        return _dedupe_requirements(reqs), "structured"

    # Lever / Greenhouse / fallback location strings.
    location_texts = []
    if isinstance(raw_job.get("allLocations"), list):
        location_texts.extend([loc for loc in raw_job["allLocations"] if isinstance(loc, str)])
    for key in ("location", "locationName"):
        value = raw_job.get(key)
        if isinstance(value, str) and value:
            location_texts.append(value)

    for text in location_texts:
        reqs.extend(_derive_remote_requirements_from_text(text))

    if not reqs:
        title = raw_job.get("title")
        if isinstance(title, str) and title:
            reqs.extend(_derive_remote_requirements_from_text(title))

    return _dedupe_requirements(reqs), ("text" if reqs else None)


def _derive_remote_applicant_location_requirements(raw_job: dict, office_type: str | None) -> list[dict]:
    reqs, _ = _derive_remote_applicant_location_requirements_with_source(raw_job, office_type)
    return reqs


def _derive_remote_requirements_from_locations(locations: list[dict] | None) -> list[dict]:
    reqs = []
    for location in locations or []:
        if not isinstance(location, dict):
            continue
        country_code = _country_code_from_value(_to_str(location.get("country_code"))) or _to_str(location.get("country_code"))
        label = _to_str(location.get("label"))
        if country_code:
            reqs.append(
                _make_applicant_requirement(
                    "country",
                    label or country_code,
                    country_code=country_code,
                )
            )
            continue
        if label:
            reqs.extend(_derive_remote_requirements_from_text(label))
    return _dedupe_requirements(reqs)


def _derive_work_locations(raw_job: dict, office_type: str | None, existing_locations: list[dict] | None) -> list[dict]:
    locations = []

    for loc in existing_locations or []:
        if not isinstance(loc, dict):
            continue
        label = _to_str(loc.get("label"))
        if label and _looks_like_multi_location_label(label):
            locations.extend(_parse_generic_location_labels(label))
            continue
        city = _to_str(loc.get("city"))
        state = _to_str(loc.get("state"))
        country_code = _country_code_from_value(_to_str(loc.get("country_code"))) or _to_str(loc.get("country_code"))
        label, city, state, country_code = _normalize_existing_location_fields(label, city, state, country_code)
        locations.append(_make_location(
            label=label,
            city=city,
            state=state,
            country_code=country_code,
            lat=loc.get("lat"),
            lng=loc.get("lng"),
        ))

    # For fully remote roles, ATS location strings usually describe applicant geography,
    # not a physical work location. Keep only explicit parsed locations.
    if office_type == "remote":
        return _dedupe_locations(locations)

    primary_city = _to_str(raw_job.get("locationCity"))
    primary_state = _to_str(raw_job.get("locationRegion"))
    primary_country = _country_code_from_value(_to_str(raw_job.get("locationCountry"))) or _to_str(raw_job.get("locationCountry"))
    primary_label = _to_str(raw_job.get("locationName")) or _to_str(raw_job.get("location"))
    if primary_city or primary_state or primary_country:
        locations.append(_make_location(
            label=primary_label,
            city=primary_city,
            state=primary_state,
            country_code=primary_country,
        ))
    elif office_type != "remote":
        locations.extend(_parse_broad_remoteish_work_location(primary_label))
    else:
        locations.extend(_parse_generic_location_labels(primary_label))

    for secondary in raw_job.get("secondaryLocations") or []:
        if not isinstance(secondary, dict):
            continue
        sec_city = _to_str(secondary.get("city"))
        sec_state = _to_str(secondary.get("region"))
        sec_country = _country_code_from_value(_to_str(secondary.get("country"))) or _to_str(secondary.get("country"))
        sec_label = _to_str(secondary.get("location"))
        if sec_city or sec_state or sec_country:
            locations.append(_make_location(
                label=sec_label,
                city=sec_city,
                state=sec_state,
                country_code=sec_country,
            ))
        else:
            locations.extend(_parse_generic_location_labels(sec_label))

    for office in raw_job.get("offices") or []:
        if not isinstance(office, dict):
            continue
        office_label = _to_str(office.get("location")) or _to_str(office.get("name"))
        if office_label and "remote" in office_label.lower():
            continue
        locations.extend(_parse_generic_location_labels(office_label))

    if isinstance(raw_job.get("allLocations"), list):
        for label in raw_job["allLocations"]:
            locations.extend(_parse_generic_location_labels(label if isinstance(label, str) else None))

    if not locations and isinstance(raw_job.get("location"), str):
        if office_type != "remote":
            locations.extend(_parse_broad_remoteish_work_location(raw_job.get("location")))
        if not locations:
            locations.extend(_parse_generic_location_labels(raw_job.get("location")))

    return _dedupe_locations(locations)


def load_raw_jobs(path: str, limit: int | None = None) -> list[dict]:
    """Load raw jobs from a JSONL or JSONL.bz2 file."""
    jobs = []
    opener = bz2.open if path.endswith(".bz2") else open
    with opener(path, "rt") as f:
        for i, line in enumerate(f):
            if limit and i >= limit:
                break
            jobs.append(json.loads(line))
    return jobs


PREPARE_JOB_TEXT_MAX_CHARS = 32000


def prepare_job_text(raw_job: dict, max_chars: int = PREPARE_JOB_TEXT_MAX_CHARS) -> str:
    """Clean and prepare job text for extraction, including ATS metadata."""
    title = raw_job.get("title", "") or ""
    content = (
        raw_job.get("content", "")
        or raw_job.get("description", "")
        or raw_job.get("descriptionHtml", "")
        or ""
    )
    if content:
        content = remove_html_markup(content, double_unescape=True)

    # Include ATS metadata as context
    meta_parts = []
    seen_meta_parts = set()

    def add_meta_part(value: str | None):
        if not value:
            return
        normalized = value.strip()
        if not normalized or normalized in seen_meta_parts:
            return
        seen_meta_parts.add(normalized)
        meta_parts.append(normalized)
    # Location from ATS
    loc = raw_job.get("location", {})
    if isinstance(loc, dict) and loc.get("name"):
        add_meta_part(f"Location: {loc['name']}")
    elif isinstance(loc, str) and loc:
        add_meta_part(f"Location: {loc}")
    if raw_job.get("workplaceType"):
        add_meta_part(f"Workplace type: {raw_job['workplaceType']}")
    if raw_job.get("isRemote") is True:
        add_meta_part("Remote flag: true")
    employment_type = raw_job.get("employmentType") or raw_job.get("commitment")
    if employment_type:
        add_meta_part(f"Employment type: {employment_type}")
    if raw_job.get("allLocations"):
        add_meta_part(f"Allowed locations: {', '.join(raw_job['allLocations'])}")
    # Only pass location context to LLM (helps with geocoding)
    # Salary, office_type, job_type come from API structured data — not LLM
    if raw_job.get("departments"):
        add_meta_part(f"Department: {', '.join(raw_job['departments'])}")
    if raw_job.get("offices"):
        office_names = [o.get("location") or o.get("name", "") for o in raw_job["offices"]]
        if office_names:
            add_meta_part(f"Offices: {', '.join(office_names)}")
    if raw_job.get("department"):
        add_meta_part(f"Department: {raw_job['department']}")
    if raw_job.get("allLocations"):
        add_meta_part(f"Locations: {', '.join(raw_job['allLocations'])}")
    elif raw_job.get("categories") and isinstance(raw_job["categories"], dict):
        if raw_job["categories"].get("location"):
            add_meta_part(f"Location: {raw_job['categories']['location']}")
    # Ashby location context (helps LLM with geocoding, other fields come from API)
    if raw_job.get("locationName"):
        add_meta_part(f"Location: {raw_job['locationName']}")
    if raw_job.get("locationCity"):
        loc_detail = raw_job["locationCity"]
        if raw_job.get("locationRegion"):
            loc_detail += f", {raw_job['locationRegion']}"
        if raw_job.get("locationCountry"):
            loc_detail += f", {raw_job['locationCountry']}"
        add_meta_part(f"Location detail: {loc_detail}")
    if raw_job.get("secondaryLocations"):
        locs = [sl.get("location", "") for sl in raw_job["secondaryLocations"] if sl.get("location")]
        if locs:
            add_meta_part(f"Also in: {', '.join(locs)}")
    if raw_job.get("team"):
        add_meta_part(f"Team: {raw_job['team']}")

    meta_str = "\n".join(meta_parts)
    if meta_str:
        text = f"{title}\n\n{meta_str}\n\n{content}".strip()
    else:
        text = f"{title}\n\n{content}".strip()
    return text[:max_chars]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Extract job metadata using LLMs")
    parser.add_argument("input", help="Path to raw JSONL or JSONL.bz2 file")
    parser.add_argument("--output", "-o", help="Output JSONL path (default: stdout)")
    parser.add_argument("--backend", choices=["openai", "local"], default="openai")
    parser.add_argument("--base-url", default="http://10.0.0.158:1234/v1", help="API base URL (openai backend)")
    parser.add_argument("--model", default="qwen/qwen3.5-35b-a3b", help="Model name or path")
    parser.add_argument("--api-key", default="not-needed", help="API key")
    parser.add_argument("--batch-size", type=int, default=4, help="Batch size")
    parser.add_argument("--limit", type=int, default=None, help="Max jobs to process")
    parser.add_argument("--max-tokens", type=int, default=2000, help="Max output tokens per job")
    args = parser.parse_args()

    # Load backend
    if args.backend == "openai":
        backend = OpenAIBackend(args.base_url, args.model, args.api_key)
    else:
        backend = LocalBackend(args.model)

    # Load jobs
    print(f"Loading jobs from {args.input}...", file=sys.stderr)
    raw_jobs = load_raw_jobs(args.input, limit=args.limit)
    print(f"Loaded {len(raw_jobs)} jobs", file=sys.stderr)

    # Prepare texts
    job_texts = [prepare_job_text(j) for j in raw_jobs]

    # Extract
    out = open(args.output, "w") if args.output else sys.stdout

    t0 = time.time()
    successes = 0
    for i in range(0, len(job_texts), args.batch_size):
        batch_texts = job_texts[i : i + args.batch_size]
        batch_raw = raw_jobs[i : i + args.batch_size]

        results = backend.extract_batch(batch_texts, max_tokens=args.max_tokens)

        for raw, result in zip(batch_raw, results):
            if result is not None:
                record = {
                    "id": raw.get("id") or raw.get("absolute_url", ""),
                    "title": raw.get("title", ""),
                    "metadata": result.model_dump(mode="json"),
                }
                out.write(json.dumps(record) + "\n")
                successes += 1

        elapsed = time.time() - t0
        total = i + len(batch_texts)
        rate = total / elapsed if elapsed > 0 else 0
        print(
            f"  Processed {total}/{len(job_texts)} | "
            f"{successes} ok | {rate:.1f} jobs/s | "
            f"{elapsed:.0f}s elapsed",
            file=sys.stderr,
        )

    if args.output:
        out.close()

    elapsed = time.time() - t0
    print(
        f"\nDone: {successes}/{len(job_texts)} extracted in {elapsed:.0f}s",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
