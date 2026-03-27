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
import sys
import time
from enum import Enum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field
from utils.html_utils import remove_html_markup


# ---------------------------------------------------------------------------
# Enums & Literals
# ---------------------------------------------------------------------------

class Industry(str, Enum):
    agriculture = "agriculture"
    aerospace_defense = "aerospace_defense"
    ai_ml = "ai_ml"
    automotive = "automotive"
    biotechnology = "biotechnology"
    construction = "construction"
    consulting = "consulting"
    consumer_goods = "consumer_goods"
    cryptocurrency_web3 = "cryptocurrency_web3"
    cybersecurity = "cybersecurity"
    education = "education"
    energy_utilities = "energy_utilities"
    entertainment_media = "entertainment_media"
    fashion_apparel = "fashion_apparel"
    financial_services = "financial_services"
    food_beverage = "food_beverage"
    gaming = "gaming"
    government = "government"
    healthcare = "healthcare"
    hospitality_tourism = "hospitality_tourism"
    insurance = "insurance"
    legal = "legal"
    logistics_supply_chain = "logistics_supply_chain"
    manufacturing = "manufacturing"
    marketing_advertising = "marketing_advertising"
    nonprofit = "nonprofit"
    pharmaceuticals = "pharmaceuticals"
    real_estate = "real_estate"
    retail_ecommerce = "retail_ecommerce"
    robotics = "robotics"
    saas_software = "saas_software"
    semiconductors = "semiconductors"
    telecommunications = "telecommunications"
    transportation = "transportation"
    other = "other"


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
    city: str | None = None
    state: str | None = None
    country_code: str | None = Field(None, description="ISO 3166-1 alpha-2 country code")
    lat: float | None = Field(None, description="Approximate latitude, best-effort")
    lng: float | None = Field(None, description="Approximate longitude, best-effort")


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
    salary: Salary | None = Field(None, description="Compensation range if mentioned. Set to null if not disclosed.")
    office_type: OfficeType = Field(description="Whether the job is remote, hybrid, or onsite")
    hybrid_days: int | None = Field(None, description="Days per week in office if hybrid. Null otherwise.")
    job_type: JobType = Field(description="Employment type")
    experience_level: ExperienceLevel = Field(description="Seniority level of the role")
    is_manager: bool = Field(description="Is this a people management role?")
    industry: Industry = Field(description="Primary industry of the company")
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

INDUSTRY: Classify by what the company SELLS to end users, not the function of this specific role:
- AI labs, ML platforms, AI safety orgs → ai_ml
- Design/collaboration tools (Figma, Canva) → saas_software
- Gaming platforms → gaming
- Language learning apps → education
- Fintech/expense management → financial_services
- Security/cybersecurity companies → cybersecurity
- Lodging/travel platforms (Airbnb, Booking.com) → hospitality_tourism
- Music/video streaming → entertainment_media
- Do NOT classify by the job function. An accountant at a gaming company is "gaming". A sourcing manager at a travel company is "hospitality_tourism".
- Do NOT use biotechnology for AI companies.

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
- salary_min, salary_max: numbers, 0 if not disclosed. Can be decimal for hourly rates (e.g. 18.50).
- salary_currency: ISO 4217 code. "" if not disclosed.
- salary_period: "hourly"|"weekly"|"monthly"|"annually". Match the period from the posting.
- salary_transparency: "full_range"|"minimum_only"|"not_disclosed"
- office_type: "remote"|"hybrid"|"onsite"
- hybrid_days: number or null (only if hybrid)
- job_type: "full-time"|"part-time"|"contract"|"internship"|"temporary"|"freelance"
- experience_level: "entry"|"mid"|"senior"|"staff"|"principal"|"executive"
- is_manager: boolean
- industry: one of [agriculture, aerospace_defense, ai_ml, automotive, biotechnology, construction, consulting, consumer_goods, cryptocurrency_web3, cybersecurity, education, energy_utilities, entertainment_media, fashion_apparel, financial_services, food_beverage, gaming, government, healthcare, hospitality_tourism, insurance, legal, logistics_supply_chain, manufacturing, marketing_advertising, nonprofit, pharmaceuticals, real_estate, retail_ecommerce, robotics, saas_software, semiconductors, telecommunications, transportation, other]
- hard_skills (array): ALL technical/domain skills mentioned
- soft_skills (array): ALL interpersonal skills mentioned
- cool_factor: "boring"|"standard"|"interesting"|"compelling"|"exceptional". Most jobs are standard/interesting.
- vibe_tags (array): from [mission_driven, high_growth, small_team, cutting_edge_tech, strong_culture, high_autonomy, work_life_balance, well_funded, public_benefit, creative_role, data_intensive, global_team, diverse_inclusive, fast_paced, customer_facing, research_focused]
- visa_sponsorship: "yes"|"no"|"unknown"
- visa_sponsorship_types (array or null): if yes, from [h1b, h1b_transfer, o1, l1, j1, green_card, other]
- equity: {offered: bool, min_pct: number|null, max_pct: number|null}
- company_stage: "pre-seed"|"seed"|"series-a"|"series-b"|"series-c-plus"|"public"|"bootstrapped"|"government"|"nonprofit" or null
- company_size: {min, max} or null (employees)
- team_size: {min, max} or null
- reports_to: string or null
- benefits_categories (array): from [health, dental, vision, life_insurance, disability, 401k, pension, equity_comp, bonus, unlimited_pto, generous_pto, parental_leave, remote_stipend, home_office, relocation, learning_budget, tuition_reimbursement, gym_fitness, wellness, meals, commuter, mental_health, childcare, pet_friendly, sabbatical, stock_purchase]
- benefits_highlights (array, max 3): only UNUSUAL perks, not standard ones
- remote_timezone_range: {earliest, latest} (e.g. "UTC-8", "UTC+1") or null
- years_experience: {min, max} or null
- education_level: "none"|"high-school"|"bachelors"|"masters"|"phd" or null
- certifications (array): required/preferred certifications
- languages (array): ISO 639-1 codes of required spoken languages
- travel_percent: 0-100 or null
- interview_stages: number or null"""


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
        "industry": {"type": "string", "enum": [
            "agriculture", "aerospace_defense", "ai_ml", "automotive", "biotechnology", "construction",
            "consulting", "consumer_goods", "cryptocurrency_web3", "cybersecurity", "education",
            "energy_utilities", "entertainment_media", "fashion_apparel", "financial_services",
            "food_beverage", "gaming", "government", "healthcare", "hospitality_tourism",
            "insurance", "legal", "logistics_supply_chain", "manufacturing", "marketing_advertising",
            "nonprofit", "pharmaceuticals", "real_estate", "retail_ecommerce", "robotics",
            "saas_software", "semiconductors", "telecommunications", "transportation", "other",
        ]},
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
    },
    "required": list({
        "tagline", "location_city", "location_state", "location_country",
        "location_lat", "location_lng", "salary_min", "salary_max",
        "salary_currency", "salary_period", "salary_transparency",
        "office_type", "hybrid_days", "job_type", "experience_level", "is_manager",
        "industry", "hard_skills", "soft_skills", "cool_factor", "vibe_tags",
        "visa_sponsorship", "visa_sponsorship_types", "equity_offered",
        "equity_min_pct", "equity_max_pct", "company_stage",
        "company_size_min", "company_size_max", "team_size_min", "team_size_max",
        "reports_to", "benefits_categories", "benefits_highlights",
        "remote_timezone_earliest", "remote_timezone_latest",
        "years_experience_min", "years_experience_max", "education_level",
        "certifications", "languages", "travel_percent", "interview_stages",
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
    l_country = _to_str(data.pop("location_country", ""))
    l_lat = _to_float(data.pop("location_lat", 0))
    l_lng = _to_float(data.pop("location_lng", 0))
    data["locations"] = [Location(city=l_city, state=l_state, country_code=l_country, lat=l_lat, lng=l_lng)] if (l_city or l_state or l_country) else []

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

    def __init__(self, model: str = "gemini-3.1-flash-lite-preview", api_key: str | None = None):
        import requests
        self._session = requests.Session()
        self._model = model
        self._api_key = api_key or os.environ.get("GEMINI_API_KEY", "")
        self._schema = {
            "type": "OBJECT",
            "properties": {
                "tagline": {"type": "STRING", "description": "One sentence that makes a job seeker stop scrolling"},
                "location_city": {"type": "STRING"}, "location_state": {"type": "STRING"},
                "location_country": {"type": "STRING"}, "location_lat": {"type": "NUMBER"}, "location_lng": {"type": "NUMBER"},
                "salary_min": {"type": "NUMBER"}, "salary_max": {"type": "NUMBER"},
                "salary_currency": {"type": "STRING"},
                "salary_period": {"type": "STRING", "enum": ["hourly", "weekly", "monthly", "annually"]},
                "salary_transparency": {"type": "STRING", "enum": ["full_range", "minimum_only", "not_disclosed"]},
                "office_type": {"type": "STRING", "enum": ["remote", "hybrid", "onsite"]},
                "hybrid_days": {"type": "INTEGER"},
                "job_type": {"type": "STRING", "enum": ["full-time", "part-time", "contract", "internship", "temporary", "freelance"]},
                "experience_level": {"type": "STRING", "enum": ["entry", "mid", "senior", "staff", "principal", "executive"]},
                "is_manager": {"type": "BOOLEAN"},
                "industry": {"type": "STRING", "enum": [e.value for e in Industry]},
                "hard_skills": {"type": "ARRAY", "items": {"type": "STRING"}},
                "soft_skills": {"type": "ARRAY", "items": {"type": "STRING"}},
                "cool_factor": {"type": "STRING", "enum": ["boring", "standard", "interesting", "compelling", "exceptional"]},
                "vibe_tags": {"type": "ARRAY", "items": {"type": "STRING", "enum": [e.value for e in VibeTag]}},
                "visa_sponsorship": {"type": "STRING", "enum": ["yes", "no", "unknown"]},
                "equity_offered": {"type": "BOOLEAN"},
                "company_stage": {"type": "STRING", "enum": [
                    "pre-seed", "seed", "series-a", "series-b", "series-c-plus",
                    "public", "bootstrapped", "government", "nonprofit", "unknown"]},
                "company_size_min": {"type": "INTEGER"}, "company_size_max": {"type": "INTEGER"},
                "team_size_min": {"type": "INTEGER"}, "team_size_max": {"type": "INTEGER"},
                "reports_to": {"type": "STRING"},
                "benefits_categories": {"type": "ARRAY", "items": {"type": "STRING", "enum": [e.value for e in BenefitCategory]}},
                "benefits_highlights": {"type": "ARRAY", "items": {"type": "STRING"}},
                "education_level": {"type": "STRING", "enum": ["none", "high-school", "bachelors", "masters", "phd", "not_specified"]},
                "years_experience_min": {"type": "INTEGER"}, "years_experience_max": {"type": "INTEGER"},
            },
            "required": ["tagline", "office_type", "job_type", "experience_level", "is_manager",
                          "industry", "hard_skills", "soft_skills", "cool_factor", "vibe_tags",
                          "visa_sponsorship", "benefits_categories", "salary_transparency"],
        }

    def extract_batch(self, job_texts: list[str], max_tokens: int = 2000) -> list[JobMetadata | None]:
        results = []
        for text in job_texts:
            try:
                resp = self._session.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{self._model}:generateContent?key={self._api_key}",
                    json={
                        "contents": [{"parts": [{"text": f"{SYSTEM_PROMPT}\n\nExtract metadata:\n\n{text}"}]}],
                        "generationConfig": {
                            "temperature": 0.1,
                            "maxOutputTokens": max_tokens,
                            "responseMimeType": "application/json",
                            "responseSchema": self._schema,
                        },
                    },
                    timeout=60,
                )
                data = resp.json()
                content = data["candidates"][0]["content"]["parts"][0]["text"]
                parsed = _parse_response(content, use_flat=True)
                if parsed is None:
                    parsed = _parse_response(content, use_flat=False)
                if parsed is None:
                    print(f"  Failed to parse Gemini response: {content[:200]}...", file=sys.stderr)
                results.append(parsed)
            except Exception as e:
                print(f"  Gemini error: {e}", file=sys.stderr)
                results.append(None)
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
# Job loading helpers
# ---------------------------------------------------------------------------

def merge_api_data(raw_job: dict, llm_metadata: dict) -> dict:
    """Overlay structured API data onto LLM-extracted metadata.
    API data wins for fields it provides — more reliable and free."""

    merged = dict(llm_metadata)

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

    # --- Office type: Ashby workplaceType, Lever workplaceType ---
    workplace = raw_job.get("workplaceType", "")
    if workplace:
        wp_lower = workplace.lower()
        if wp_lower in ("remote",):
            merged["office_type"] = "remote"
        elif wp_lower in ("hybrid",):
            merged["office_type"] = "hybrid"
        elif wp_lower in ("onsite", "on-site", "in-office"):
            merged["office_type"] = "onsite"

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

    # --- Location: Ashby structured address ---
    if raw_job.get("locationCity") and not merged.get("locations"):
        merged["locations"] = [{
            "city": raw_job.get("locationCity"),
            "state": raw_job.get("locationRegion"),
            "country_code": raw_job.get("locationCountry"),
            "lat": None,
            "lng": None,
        }]

    # --- Equity: Ashby compensation summary ---
    comp_summary = raw_job.get("compensationTierSummary", "")
    if comp_summary and "equity" in comp_summary.lower():
        if isinstance(merged.get("equity"), dict):
            merged["equity"]["offered"] = True
        else:
            merged["equity"] = {"offered": True, "min_pct": None, "max_pct": None}

    return merged


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


def prepare_job_text(raw_job: dict, max_chars: int = 8000) -> str:
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
    # Location from ATS
    loc = raw_job.get("location", {})
    if isinstance(loc, dict) and loc.get("name"):
        meta_parts.append(f"Location: {loc['name']}")
    elif isinstance(loc, str) and loc:
        meta_parts.append(f"Location: {loc}")
    # Only pass location context to LLM (helps with geocoding)
    # Salary, office_type, job_type come from API structured data — not LLM
    if raw_job.get("departments"):
        meta_parts.append(f"Department: {', '.join(raw_job['departments'])}")
    if raw_job.get("offices"):
        office_names = [o.get("location") or o.get("name", "") for o in raw_job["offices"]]
        if office_names:
            meta_parts.append(f"Offices: {', '.join(office_names)}")
    if raw_job.get("department"):
        meta_parts.append(f"Department: {raw_job['department']}")
    if raw_job.get("allLocations"):
        meta_parts.append(f"Locations: {', '.join(raw_job['allLocations'])}")
    elif raw_job.get("categories") and isinstance(raw_job["categories"], dict):
        if raw_job["categories"].get("location"):
            meta_parts.append(f"Location: {raw_job['categories']['location']}")
    # Ashby location context (helps LLM with geocoding, other fields come from API)
    if raw_job.get("locationName"):
        meta_parts.append(f"Location: {raw_job['locationName']}")
    if raw_job.get("locationCity"):
        loc_detail = raw_job["locationCity"]
        if raw_job.get("locationRegion"):
            loc_detail += f", {raw_job['locationRegion']}"
        if raw_job.get("locationCountry"):
            loc_detail += f", {raw_job['locationCountry']}"
        meta_parts.append(f"Location detail: {loc_detail}")
    if raw_job.get("secondaryLocations"):
        locs = [sl.get("location", "") for sl in raw_job["secondaryLocations"] if sl.get("location")]
        if locs:
            meta_parts.append(f"Also in: {', '.join(locs)}")
    if raw_job.get("department"):
        meta_parts.append(f"Department: {raw_job['department']}")
    if raw_job.get("team"):
        meta_parts.append(f"Team: {raw_job['team']}")

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
