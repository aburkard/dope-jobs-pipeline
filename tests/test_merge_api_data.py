"""Tests for merge_api_data — overlaying API structured data onto LLM extraction."""
import pytest
from parse import (
    _detect_is_manager,
    _detect_visa_sponsorship,
    _extract_education_from_description,
    _extract_salary_from_description,
    _extract_years_experience_from_description,
    _flat_to_job_metadata,
    _map_workable_industry,
    merge_api_data,
    prepare_job_text,
    prepare_language_detection_text,
)


class TestGreenhousePayRanges:
    def test_annual_salary(self):
        raw = {"pay_input_ranges": [{
            "min_cents": 15000000, "max_cents": 20000000,
            "currency_type": "USD", "title": "Annual Salary:"
        }]}
        llm = {"salary": None, "salary_transparency": "not_disclosed"}
        result = merge_api_data(raw, llm)
        assert result["salary"]["min"] == 150000
        assert result["salary"]["max"] == 200000
        assert result["salary"]["currency"] == "USD"
        assert result["salary"]["period"] == "annually"
        assert result["salary_transparency"] == "full_range"

    def test_hourly_pay(self):
        raw = {"pay_input_ranges": [{
            "min_cents": 7200, "max_cents": 7200,
            "currency_type": "USD", "title": "Hourly Pay Range"
        }]}
        llm = {"salary": None}
        result = merge_api_data(raw, llm)
        assert result["salary"]["min"] == 72
        assert result["salary"]["period"] == "hourly"

    def test_no_pay_ranges_keeps_llm(self):
        raw = {}
        llm = {"salary": {"min": 100000, "max": 150000, "currency": "USD", "period": "annually"}}
        result = merge_api_data(raw, llm)
        assert result["salary"]["min"] == 100000

    def test_pay_ranges_overrides_llm(self):
        raw = {"pay_input_ranges": [{
            "min_cents": 20000000, "max_cents": 30000000,
            "currency_type": "USD", "title": "Annual Salary"
        }]}
        llm = {"salary": {"min": 100000, "max": 150000, "currency": "USD", "period": "annually"}}
        result = merge_api_data(raw, llm)
        assert result["salary"]["min"] == 200000
        assert result["salary"]["max"] == 300000

    def test_minimum_only(self):
        raw = {"pay_input_ranges": [{
            "min_cents": 10000000, "max_cents": None,
            "currency_type": "USD", "title": "Annual Salary"
        }]}
        llm = {"salary_transparency": "not_disclosed"}
        result = merge_api_data(raw, llm)
        assert result["salary"]["min"] == 100000
        assert result["salary"]["max"] is None
        assert result["salary_transparency"] == "minimum_only"


class TestAshbyCompensation:
    def test_salary_summary_parse(self):
        raw = {"compensationSalarySummary": "$150K - $250K"}
        llm = {"salary": None}
        result = merge_api_data(raw, llm)
        assert result["salary"]["min"] == 150000
        assert result["salary"]["max"] == 250000

    def test_salary_summary_no_override_when_greenhouse_present(self):
        """Greenhouse pay_input_ranges should take priority."""
        raw = {
            "pay_input_ranges": [{"min_cents": 10000000, "max_cents": 20000000, "currency_type": "USD", "title": "Annual Salary"}],
            "compensationSalarySummary": "$150K - $250K",
        }
        llm = {"salary": None}
        result = merge_api_data(raw, llm)
        assert result["salary"]["min"] == 100000  # from pay_input_ranges, not Ashby

    def test_single_amount(self):
        raw = {"compensationSalarySummary": "$295K"}
        llm = {"salary": None}
        result = merge_api_data(raw, llm)
        assert result["salary"]["min"] == 295000
        assert result["salary"]["max"] == 295000
        assert result["salary_transparency"] == "minimum_only"

    def test_handles_none_compensation(self):
        raw = {"compensationSalarySummary": None}
        llm = {"salary": None}
        result = merge_api_data(raw, llm)
        assert result["salary"] is None

    def test_equity_detection(self):
        raw = {"compensationTierSummary": "$150K – $250K • Offers Equity"}
        llm = {"equity": {"offered": False, "min_pct": None, "max_pct": None}}
        result = merge_api_data(raw, llm)
        assert result["equity"]["offered"] is True


class TestWorkplaceType:
    @pytest.mark.parametrize("api_value,expected", [
        ("Remote", "remote"),
        ("remote", "remote"),
        ("Hybrid", "hybrid"),
        ("hybrid", "hybrid"),
        ("OnSite", "onsite"),
        ("on-site", "onsite"),
        ("in-office", "onsite"),
    ])
    def test_mapping(self, api_value, expected):
        raw = {"workplaceType": api_value}
        llm = {"office_type": "onsite"}
        result = merge_api_data(raw, llm)
        assert result["office_type"] == expected

    def test_no_workplace_keeps_llm(self):
        raw = {}
        llm = {"office_type": "remote"}
        result = merge_api_data(raw, llm)
        assert result["office_type"] == "remote"

    def test_unknown_workplace_keeps_llm(self):
        raw = {"workplaceType": ""}
        llm = {"office_type": "hybrid"}
        result = merge_api_data(raw, llm)
        assert result["office_type"] == "hybrid"

    def test_is_remote_flag_sets_remote(self):
        raw = {"isRemote": True}
        llm = {"office_type": "onsite"}
        result = merge_api_data(raw, llm)
        assert result["office_type"] == "remote"

    def test_linkedin_remote_tag_used_when_no_api_workplace(self):
        raw = {"description": "Join us from anywhere. #LI-REMOTE"}
        llm = {"office_type": "onsite"}
        result = merge_api_data(raw, llm)
        assert result["office_type"] == "remote"

    def test_api_workplace_beats_linkedin_tag(self):
        raw = {
            "workplaceType": "Hybrid",
            "description": "This posting is wrapped for LinkedIn. #LI-REMOTE",
        }
        llm = {"office_type": "onsite"}
        result = merge_api_data(raw, llm)
        assert result["office_type"] == "hybrid"

    def test_onsite_workplace_keeps_llm_hybrid_when_model_has_strong_signal(self):
        raw = {"workplaceType": "OnSite"}
        llm = {"office_type": "hybrid", "hybrid_days": 3}
        result = merge_api_data(raw, llm)
        assert result["office_type"] == "hybrid"
        assert result["hybrid_days"] == 3

    def test_onsite_workplace_overrides_llm_hybrid_without_strong_signal(self):
        raw = {"workplaceType": "OnSite"}
        llm = {"office_type": "hybrid", "hybrid_days": None}
        result = merge_api_data(raw, llm)
        assert result["office_type"] == "onsite"
        assert result["hybrid_days"] is None

    def test_onsite_workplace_keeps_llm_remote_when_model_has_strong_signal(self):
        raw = {"workplaceType": "OnSite"}
        llm = {
            "office_type": "remote",
            "hybrid_days": None,
            "applicant_location_requirements": [
                {"scope": "country", "name": "United States", "country_code": "US", "region": None}
            ],
        }
        result = merge_api_data(raw, llm)
        assert result["office_type"] == "remote"
        assert result["hybrid_days"] is None
        assert result["applicant_location_requirements"] == llm["applicant_location_requirements"]

    def test_remote_locations_backfill_applicant_geography_from_existing_locations(self):
        raw = {}
        llm = {
            "office_type": "remote",
            "locations": [
                {
                    "label": "Toronto, Ontario, Canada",
                    "city": "Toronto",
                    "state": "Ontario",
                    "country_code": "CA",
                }
            ],
            "applicant_location_requirements": [],
        }
        result = merge_api_data(raw, llm)
        assert result["applicant_location_requirements"] == [
            {
                "scope": "country",
                "name": "Toronto, Ontario, Canada",
                "country_code": "CA",
                "region": None,
            }
        ]


class TestJobType:
    @pytest.mark.parametrize("api_field,api_value,expected", [
        ("employmentType", "FullTime", "full-time"),
        ("employmentType", "PartTime", "part-time"),
        ("employmentType", "Contract", "contract"),
        ("employmentType", "Intern", "internship"),
        ("employmentType", "Temporary", "temporary"),
        ("commitment", "Permanent", "full-time"),
        ("commitment", "Contract", "contract"),
    ])
    def test_mapping(self, api_field, api_value, expected):
        raw = {api_field: api_value}
        llm = {"job_type": "full-time"}
        result = merge_api_data(raw, llm)
        assert result["job_type"] == expected

    def test_no_type_keeps_llm(self):
        raw = {}
        llm = {"job_type": "contract"}
        result = merge_api_data(raw, llm)
        assert result["job_type"] == "contract"


class TestPostingLanguage:
    def test_api_language_overrides_llm_and_normalizes_locale(self):
        raw = {"inLanguage": "fr-FR"}
        llm = {"posting_language": "en"}
        result = merge_api_data(raw, llm)
        assert result["posting_language"] == "fr"

    def test_keeps_normalized_llm_language_when_api_missing(self):
        raw = {}
        llm = {"posting_language": "de-DE"}
        result = merge_api_data(raw, llm)
        assert result["posting_language"] == "de"

    def test_flat_schema_normalizes_posting_language(self):
        metadata = _flat_to_job_metadata({
            "tagline": "Role",
            "location_city": "",
            "location_state": "",
            "location_country": "",
            "location_lat": 0,
            "location_lng": 0,
            "salary_min": 0,
            "salary_max": 0,
            "salary_currency": "",
            "salary_period": "annually",
            "salary_transparency": "not_disclosed",
            "office_type": "remote",
            "hybrid_days": 0,
            "job_type": "full-time",
            "experience_level": "mid",
            "is_manager": False,
            "industry_primary": "enterprise_software",
            "industry_tags": [],
            "industry_other_hint": "",
            "hard_skills": [],
            "soft_skills": [],
            "cool_factor": "standard",
            "vibe_tags": [],
            "visa_sponsorship": "unknown",
            "visa_sponsorship_types": [],
            "equity_offered": False,
            "equity_min_pct": 0,
            "equity_max_pct": 0,
            "company_stage": "unknown",
            "company_size_min": 0,
            "company_size_max": 0,
            "team_size_min": 0,
            "team_size_max": 0,
            "reports_to": "",
            "benefits_categories": [],
            "benefits_highlights": [],
            "remote_timezone_earliest": "",
            "remote_timezone_latest": "",
            "years_experience_min": 0,
            "years_experience_max": 0,
            "education_level": "not_specified",
            "certifications": [],
            "languages": [],
            "travel_percent": 0,
            "interview_stages": 0,
            "posting_language": "es-MX",
        })
        assert metadata.posting_language == "es"

    def test_heuristic_detects_french_posting_language(self):
        raw = {
            "title": "Delivery Operations Intern",
            "description": (
                "Rejoins notre équipe avec une mission claire. "
                "Nous travaillons avec des partenaires en France et en Belgique. "
                "Le poste est basé à Paris et vous aurez une forte exposition aux opérations."
            ),
        }
        llm = {"posting_language": "en"}
        result = merge_api_data(raw, llm)
        assert result["posting_language"] == "fr"

    def test_heuristic_detects_japanese_posting_language(self):
        raw = {
            "title": "デリバリーソリューションアーキテクト",
            "description": "お客様の成功を支援し、技術的なリーダーとして複雑な課題を解決します。",
        }
        llm = {"posting_language": "en"}
        result = merge_api_data(raw, llm)
        assert result["posting_language"] == "ja"

    def test_language_detection_text_excludes_english_metadata_prefixes(self):
        raw = {
            "title": "Développeur logiciel",
            "description": "Rejoins notre équipe avec une mission claire en France.",
            "location": "Paris, France",
            "department": "Engineering",
        }
        text = prepare_language_detection_text(raw)
        assert "Location:" not in text
        assert "Department:" not in text
        assert "Rejoins notre équipe" in text


class TestIndustryCanonicalization:
    def test_flat_schema_includes_primary_industry_in_tags(self):
        metadata = _flat_to_job_metadata({
            "tagline": "Role",
            "location_city": "",
            "location_state": "",
            "location_country": "",
            "location_lat": 0,
            "location_lng": 0,
            "salary_min": 0,
            "salary_max": 0,
            "salary_currency": "",
            "salary_period": "annually",
            "salary_transparency": "not_disclosed",
            "office_type": "remote",
            "hybrid_days": 0,
            "job_type": "full-time",
            "experience_level": "mid",
            "is_manager": False,
            "industry_primary": "consumer_social",
            "industry_tags": ["advertising_marketing", "consumer_social", "ai_ml", "ai_ml"],
            "industry_other_hint": "",
            "hard_skills": [],
            "soft_skills": [],
            "cool_factor": "standard",
            "vibe_tags": [],
            "visa_sponsorship": "unknown",
            "visa_sponsorship_types": [],
            "equity_offered": False,
            "equity_min_pct": 0,
            "equity_max_pct": 0,
            "company_stage": "unknown",
            "company_size_min": 0,
            "company_size_max": 0,
            "team_size_min": 0,
            "team_size_max": 0,
            "reports_to": "",
            "benefits_categories": [],
            "benefits_highlights": [],
            "remote_timezone_earliest": "",
            "remote_timezone_latest": "",
            "years_experience_min": 0,
            "years_experience_max": 0,
            "education_level": "not_specified",
            "certifications": [],
            "languages": [],
            "travel_percent": 0,
            "interview_stages": 0,
            "posting_language": "en",
            "applicant_location_requirements": [],
        })
        assert metadata.industry_tags == ["consumer_social", "ai_ml", "advertising_marketing"]

    def test_merge_api_data_canonicalizes_industry_tags_and_other_hint(self):
        raw = {}
        llm = {
            "industry_primary": "enterprise_software",
            "industry_tags": ["ai_ml", "enterprise_software", "developer_tools"],
            "industry_other_hint": "business software",
        }
        result = merge_api_data(raw, llm)
        assert result["industry_tags"] == [
            "enterprise_software",
            "ai_ml",
            "developer_tools",
        ]
        assert result["industry_other_hint"] is None


class TestPrepareJobText:
    def test_dedupes_department_and_includes_employment_type(self):
        raw = {
            "title": "Account Manager, Supply",
            "description": "<p>Body</p>",
            "location": "Beijing, China",
            "workplaceType": "hybrid",
            "employmentType": "Full-time",
            "departments": ["Marketplace", "Account Management"],
            "department": "Account Management",
        }

        text = prepare_job_text(raw)

        assert "Workplace type: hybrid" in text
        assert "Employment type: Full-time" in text
        assert text.count("Department: Account Management") == 1
        assert "Department: Marketplace, Account Management" in text


class TestLocationOverlay:
    def test_ashby_location(self):
        raw = {"locationCity": "New York City", "locationRegion": "NY", "locationCountry": "USA"}
        llm = {"locations": []}
        result = merge_api_data(raw, llm)
        assert len(result["locations"]) == 1
        assert result["locations"][0]["city"] == "New York City"
        assert result["locations"][0]["state"] == "NY"
        assert result["locations"][0]["country_code"] == "US"
        assert result["locations"][0]["label"] == "New York City, NY, US"

    def test_no_overlay_when_llm_has_locations(self):
        raw = {"locationCity": "SF", "locationRegion": "CA", "locationCountry": "US"}
        llm = {"locations": [{"label": "New York, NY, US", "city": "New York", "state": "NY", "country_code": "US"}]}
        result = merge_api_data(raw, llm)
        # Existing parsed location preserved, ATS location merged in
        assert result["locations"][0]["city"] == "New York"
        assert len(result["locations"]) == 2

    def test_ashby_secondary_locations_merge_for_non_remote(self):
        raw = {
            "workplaceType": "Hybrid",
            "locationName": "New York",
            "locationCity": "New York",
            "locationRegion": "NY",
            "locationCountry": "US",
            "secondaryLocations": [
                {"location": "San Francisco", "city": "San Francisco", "region": "CA", "country": "US"},
                {"location": "London", "city": "London", "region": "", "country": "GB"},
            ],
        }
        llm = {"locations": []}
        result = merge_api_data(raw, llm)
        assert result["office_type"] == "hybrid"
        assert [loc["label"] for loc in result["locations"]] == [
            "New York",
            "San Francisco",
            "London",
        ]

    def test_greenhouse_offices_merge_for_non_remote(self):
        raw = {
            "offices": [
                {"name": "HQ", "location": "San Francisco, CA"},
                {"name": "NY Office", "location": "New York, NY"},
            ],
        }
        llm = {"office_type": "onsite", "locations": []}
        result = merge_api_data(raw, llm)
        assert [loc["label"] for loc in result["locations"]] == [
            "San Francisco, CA",
            "New York, NY",
        ]

    def test_existing_composite_location_is_split_into_distinct_locations(self):
        raw = {
            "allLocations": ["San Francisco, CA", "Austin, TX", "New York, NY"],
        }
        llm = {
            "office_type": "hybrid",
            "locations": [
                {
                    "label": "San Francisco, CA; Austin, TX; New York, NY",
                    "city": "San Francisco",
                    "state": "CA; Austin",
                    "country_code": "NY",
                }
            ],
        }
        result = merge_api_data(raw, llm)
        assert [loc["label"] for loc in result["locations"]] == [
            "San Francisco, CA",
            "Austin, TX",
            "New York, NY",
        ]

    def test_city_state_abbreviation_is_not_treated_as_country(self):
        raw = {"location": "Mountain View, CA"}
        llm = {"office_type": "onsite", "locations": []}
        result = merge_api_data(raw, llm)
        assert result["locations"] == [
            {
                "label": "Mountain View, CA",
                "city": "Mountain View",
                "state": "CA",
                "country_code": None,
                "lat": None,
                "lng": None,
            }
        ]

    def test_existing_malformed_city_state_location_is_recovered(self):
        raw = {}
        llm = {
            "office_type": "onsite",
            "locations": [
                {
                    "label": "Mountain View, CA",
                    "city": "Mountain View",
                    "state": None,
                    "country_code": "CA",
                }
            ],
        }
        result = merge_api_data(raw, llm)
        assert result["locations"] == [
            {
                "label": "Mountain View, CA",
                "city": "Mountain View",
                "state": "CA",
                "country_code": None,
                "lat": None,
                "lng": None,
            }
        ]

    def test_remote_does_not_promote_candidate_regions_to_work_locations(self):
        raw = {
            "workplaceType": "Remote",
            "location": "France; Germany; Netherlands; Spain; United Kingdom",
        }
        llm = {"locations": []}
        result = merge_api_data(raw, llm)
        assert result["locations"] == []

    def test_hybrid_remote_country_label_becomes_broad_work_location(self):
        raw = {
            "workplaceType": "Hybrid",
            "location": "Remote - United States",
            "offices": [{"name": "AMER", "location": "Remote - AMER"}],
        }
        llm = {"locations": []}
        result = merge_api_data(raw, llm)
        assert result["office_type"] == "hybrid"
        assert result["locations"] == [
            {
                "label": "United States",
                "city": None,
                "state": None,
                "country_code": "US",
                "lat": None,
                "lng": None,
            }
        ]

    def test_remote_requirements_from_ashby_country(self):
        raw = {
            "workplaceType": "Remote",
            "locationCountry": "Canada",
            "secondaryLocations": [{"country": "Canada", "location": "Canada - Remote"}],
        }
        llm = {"office_type": "onsite", "applicant_location_requirements": []}
        result = merge_api_data(raw, llm)
        assert result["office_type"] == "remote"
        assert result["applicant_location_requirements"] == [
            {"scope": "country", "name": "Canada", "country_code": "CA", "region": None}
        ]

    def test_onsite_workplace_clears_weak_llm_remote_requirements(self):
        raw = {"workplaceType": "OnSite"}
        llm = {
            "office_type": "remote",
            "applicant_location_requirements": [],
        }
        result = merge_api_data(raw, llm)
        assert result["office_type"] == "onsite"
        assert result["applicant_location_requirements"] == []

    def test_remote_requirements_from_lever_location_text(self):
        raw = {
            "workplaceType": "remote",
            "allLocations": ["Australia & New Zealand"],
        }
        llm = {"office_type": "onsite", "applicant_location_requirements": []}
        result = merge_api_data(raw, llm)
        assert result["applicant_location_requirements"] == [
            {"scope": "country", "name": "Australia", "country_code": "AU", "region": None},
            {"scope": "country", "name": "New Zealand", "country_code": "NZ", "region": None},
        ]

    def test_remote_requirements_from_greenhouse_location_list(self):
        raw = {
            "title": "Boomi Platform Architect- Remote Setup (EMEA Based)",
            "location": "France; Germany; Netherlands; Spain; United Kingdom",
        }
        llm = {"office_type": "remote", "applicant_location_requirements": []}
        result = merge_api_data(raw, llm)
        assert result["applicant_location_requirements"] == [
            {"scope": "country", "name": "France", "country_code": "FR", "region": None},
            {"scope": "country", "name": "Germany", "country_code": "DE", "region": None},
            {"scope": "country", "name": "Netherlands", "country_code": "NL", "region": None},
            {"scope": "country", "name": "Spain", "country_code": "ES", "region": None},
            {"scope": "country", "name": "United Kingdom", "country_code": "GB", "region": None},
        ]

    def test_remote_requirements_keep_llm_when_ats_has_no_strong_signal(self):
        raw = {
            "workplaceType": "remote",
            "location": "Ankeny, IA",
        }
        llm = {
            "office_type": "remote",
            "applicant_location_requirements": [
                {"scope": "country", "name": "United States", "country_code": "US", "region": None}
            ],
        }
        result = merge_api_data(raw, llm)
        assert result["applicant_location_requirements"] == llm["applicant_location_requirements"]

    def test_remote_requirements_keep_more_specific_llm_states_over_ats_country(self):
        raw = {
            "workplaceType": "Remote",
            "location": "United States",
        }
        llm = {
            "office_type": "remote",
            "applicant_location_requirements": [
                {"scope": "state", "name": "Florida", "country_code": "US", "region": "FL"},
                {"scope": "state", "name": "Texas", "country_code": "US", "region": "TX"},
            ],
        }
        result = merge_api_data(raw, llm)
        assert result["applicant_location_requirements"] == llm["applicant_location_requirements"]

    def test_remote_requirements_prefer_ats_structured_country_over_llm_country(self):
        raw = {
            "workplaceType": "Remote",
            "locationCountry": "Canada",
        }
        llm = {
            "office_type": "remote",
            "applicant_location_requirements": [
                {"scope": "country", "name": "United States", "country_code": "US", "region": None}
            ],
        }
        result = merge_api_data(raw, llm)
        assert result["applicant_location_requirements"] == [
            {"scope": "country", "name": "Canada", "country_code": "CA", "region": None}
        ]

    def test_remote_requirements_keep_llm_when_text_derived_ats_is_same_specificity(self):
        raw = {
            "workplaceType": "Remote",
            "location": "Canada",
        }
        llm = {
            "office_type": "remote",
            "applicant_location_requirements": [
                {"scope": "country", "name": "United States", "country_code": "US", "region": None}
            ],
        }
        result = merge_api_data(raw, llm)
        assert result["applicant_location_requirements"] == llm["applicant_location_requirements"]

    def test_remote_requirements_from_city_country_remote_label(self):
        raw = {
            "workplaceType": "remote",
            "location": "Bogota, Colombia (remote)",
        }
        llm = {"office_type": "remote", "applicant_location_requirements": []}
        result = merge_api_data(raw, llm)
        assert result["applicant_location_requirements"] == [
            {"scope": "country", "name": "Colombia", "country_code": "CO", "region": None}
        ]


class TestDescriptionSalaryExtraction:
    """Tests for _extract_salary_from_description and its integration in merge_api_data."""

    # --- Range patterns (real examples from survey) ---

    def test_standard_range_with_dollar_dash(self):
        raw = {"description": "The salary for this role is $50,000-$80,000 plus sales commission."}
        result = _extract_salary_from_description(raw)
        assert result is not None
        assert result["salary"]["min"] == 50_000
        assert result["salary"]["max"] == 80_000
        assert result["salary"]["currency"] == "USD"
        assert result["salary"]["period"] == "annually"
        assert result["salary_transparency"] == "full_range"

    def test_range_with_to_separator(self):
        raw = {"description": "Salary Range: $140,000 to $220,000 annually plus equity."}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 140_000
        assert result["salary"]["max"] == 220_000
        assert result["salary"]["period"] == "annually"

    def test_range_with_em_dash(self):
        raw = {"description": "Base Pay Range$130,200—$265,300 USD"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 130_200
        assert result["salary"]["max"] == 265_300

    def test_range_with_en_dash(self):
        raw = {"description": "Salary: $130,000–$140,000 (up to $145,000 in select HCOL markets)"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 130_000
        assert result["salary"]["max"] == 140_000

    def test_range_k_suffix(self):
        raw = {"description": "Base salary range of ($90k - $121k)"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 90_000
        assert result["salary"]["max"] == 121_000

    def test_hourly_range(self):
        raw = {"description": "Pay Rate: $31.00-39.00/hour"}
        # Note: second amount doesn't have $ prefix — but the pattern $31.00-$39.00 case:
        raw = {"description": "Pay Rate: $31.00-$39.00/hour"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 31.0
        assert result["salary"]["max"] == 39.0
        assert result["salary"]["period"] == "hourly"

    def test_hourly_range_per_hour(self):
        raw = {"description": "Fellows are paid $23 per hour (overtime when needed)"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 23
        assert result["salary"]["period"] == "hourly"

    def test_hourly_range_industry_leading(self):
        raw = {"description": "Industry-leading compensation ($34.00- $60.00/hr)"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 34.0
        assert result["salary"]["max"] == 60.0
        assert result["salary"]["period"] == "hourly"

    def test_range_decimal_per_year(self):
        raw = {"description": "Pay Range: Lead Software Engineer: $160,000.00 - $225,000.00/per year."}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 160_000.0
        assert result["salary"]["max"] == 225_000.0
        assert result["salary"]["period"] == "annually"

    def test_range_with_equity_suffix(self):
        raw = {"description": "$125,000 - $185,000 + Equity"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 125_000
        assert result["salary"]["max"] == 185_000

    def test_annual_base_pay_multiformat(self):
        raw = {"description": "The annual base pay range for this role is:$120,000—$175,000 USD"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 120_000
        assert result["salary"]["max"] == 175_000
        assert result["salary"]["period"] == "annually"

    def test_expected_salary_range(self):
        raw = {"description": "The expected salary range for this role is $220,000-$270,000 USD."}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 220_000
        assert result["salary"]["max"] == 270_000

    def test_per_visit_detected_as_hourly(self):
        raw = {"description": "Earn $75–$82 per visit"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 75
        assert result["salary"]["max"] == 82
        assert result["salary"]["period"] == "hourly"

    def test_hourly_base_pay(self):
        raw = {"description": "Base Pay Range$50—$58 USD\nHygienists are eligible for monthly bonus."}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 50
        assert result["salary"]["max"] == 58
        assert result["salary"]["period"] == "hourly"

    # --- Single-value patterns ---

    def test_single_base_salary(self):
        raw = {"description": "Base Salary: $128,000"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 128_000
        assert result["salary"]["max"] == 128_000
        assert result["salary_transparency"] == "minimum_only"

    def test_single_hourly_rate(self):
        raw = {"description": "Hourly Rate: $17.75 + Tips"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 17.75
        assert result["salary"]["period"] == "hourly"

    def test_single_starting_pay(self):
        raw = {"description": "Competitive starting pay of $26 per hour!"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 26
        assert result["salary"]["period"] == "hourly"

    def test_single_hourly_with_up_to(self):
        raw = {"description": "Our top-earning Hygienists make up to $73/hour in Atlanta."}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 73
        assert result["salary"]["period"] == "hourly"

    # --- EUR/GBP patterns ---

    def test_gbp_annual_range(self):
        raw = {"description": "Realistic full-time earnings of £40,000–£50,000+ per year"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 40_000
        assert result["salary"]["max"] == 50_000
        assert result["salary"]["currency"] == "GBP"
        assert result["salary"]["period"] == "annually"

    def test_gbp_single_salary(self):
        raw = {"description": "Salary: £65,000 per annum"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 65_000
        assert result["salary"]["currency"] == "GBP"
        assert result["salary"]["period"] == "annually"

    def test_eur_range(self):
        raw = {"description": "The target total compensation for this position is €75,000-€110,000 annually."}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 75_000
        assert result["salary"]["max"] == 110_000
        assert result["salary"]["currency"] == "EUR"

    def test_gbp_base_salary_labeled(self):
        raw = {"description": "Base Salary: £87,500\nBonus at Target Performance: 15%"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 87_500
        assert result["salary"]["currency"] == "GBP"

    # --- Period inference from magnitude ---

    def test_small_amount_inferred_hourly(self):
        raw = {"description": "$24.00 - $26.00 DOE"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["period"] == "hourly"

    def test_large_amount_inferred_annually(self):
        raw = {"description": "$200,000 - $260,000\nCompany Perks:"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["period"] == "annually"

    # --- False positive rejection ---

    def test_rejects_funding_amounts(self):
        raw = {"description": "Leapsome secured $60 million in Series A funding led by Insight Partners."}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_rejects_revenue(self):
        raw = {"description": "With over $8 billion in annual revenue and a blue-chip client base."}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_rejects_arr(self):
        raw = {"description": "It recently surpassed $200M in annual recurring revenue (ARR)."}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_rejects_valuation(self):
        raw = {"description": "backed by a $2.2 billion valuation and $370 million in funding"}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_rejects_series_funding(self):
        raw = {"description": "We're a Series D company with over $325m in funding from a16z."}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_rejects_stipend(self):
        raw = {"description": "$500 work-from-home stipend to be used up to a year of your start date"}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_rejects_tuition_reimbursement(self):
        raw = {"description": "Tuition Reimbursement: up to $5,250 maximum per year subject to manager discretion"}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_rejects_fsa_benefit(self):
        raw = {"description": "Flexible spending account with employer matching up to $1,650/year (medical FSA)"}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_rejects_sign_on_bonus(self):
        raw = {"description": "$5,000 sign-on bonus!"}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_rejects_assets_under_management(self):
        raw = {"description": "With over $30 billion in assets under advisement, 300+ teammates nationwide."}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_rejects_deal_processing(self):
        raw = {"description": "doubled our marketing campaigns from $250M to $500M+ since 2023, making Morgan"}
        result = _extract_salary_from_description(raw)
        assert result is None

    # --- Integration: merge_api_data uses description fallback ---

    def test_merge_api_data_extracts_from_description_when_no_structured_salary(self):
        raw = {"description": "The salary range for this role is $150,000-$200,000 annually."}
        llm = {}
        result = merge_api_data(raw, llm)
        assert result["salary"]["min"] == 150_000
        assert result["salary"]["max"] == 200_000

    def test_merge_api_data_structured_salary_takes_priority(self):
        """Greenhouse pay_input_ranges should beat description extraction."""
        raw = {
            "pay_input_ranges": [{"min_cents": 10000000, "max_cents": 15000000, "currency_type": "USD", "title": "Annual Salary"}],
            "description": "The salary range for this role is $150,000-$200,000 annually.",
        }
        llm = {}
        result = merge_api_data(raw, llm)
        assert result["salary"]["min"] == 100_000  # from pay_input_ranges, not description

    def test_merge_api_data_ashby_comp_takes_priority(self):
        raw = {
            "compensationSalarySummary": "$180K - $220K",
            "description": "The salary range is $150,000-$200,000.",
        }
        llm = {}
        result = merge_api_data(raw, llm)
        assert result["salary"]["min"] == 180_000  # from Ashby, not description

    def test_merge_api_data_no_salary_anywhere(self):
        raw = {"description": "Join our amazing team! We have great culture."}
        llm = {}
        result = merge_api_data(raw, llm)
        assert result.get("salary") is None

    # --- Edge cases ---

    def test_empty_description(self):
        raw = {"description": ""}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_no_description_key(self):
        raw = {}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_html_description_stripped(self):
        raw = {"description": "<p>Salary: <strong>$120,000</strong> - <strong>$150,000</strong></p>"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 120_000
        assert result["salary"]["max"] == 150_000

    def test_prefers_range_over_single(self):
        """When description has both a range and a single amount, pick the range."""
        raw = {"description": "Base Salary: $128,000\nThe salary range is $120,000-$150,000."}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 120_000
        assert result["salary"]["max"] == 150_000
        assert result["salary_transparency"] == "full_range"

    def test_prefers_signaled_range_over_bare_range(self):
        """Range with a salary keyword beats a bare range."""
        raw = {"description": "$24 - $26\nSalary Range: $100,000-$130,000 annually"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 100_000

    def test_content_field_fallback(self):
        raw = {"content": "<p>Pay Range: $80,000 - $100,000 per year</p>"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 80_000
        assert result["salary"]["max"] == 100_000

    def test_rejects_implausible_max_less_than_min(self):
        raw = {"description": "Salary range: $200,000 - $50,000"}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_rejects_implausibly_wide_range(self):
        raw = {"description": "Salary range: $10,000 - $900,000"}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_salary_with_typo_missing_zero(self):
        """Real example: '$180,00 - $243,000' — first value is likely $180,000 but we parse as-is."""
        raw = {"description": "The annual base salary range for this role is $180,00 - $243,000 in Chicago."}
        result = _extract_salary_from_description(raw)
        # $180,00 parses as $18,000 — and $18,000-$243,000 is > 5x ratio, so rejected
        assert result is None

    # --- Additional patterns from validation ---

    def test_range_with_and_separator(self):
        raw = {"description": "The salary for this role is anticipated to be between $130k and $150k."}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 130_000
        assert result["salary"]["max"] == 150_000

    def test_range_missing_dollar_on_second(self):
        """e.g. '$18.50- $21.50/ hour' but also '$18.50- 21.50/ hour'"""
        raw = {"description": "Pay range from $18.50- 21.50/hour in Santa Clara, CA."}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 18.50
        assert result["salary"]["max"] == 21.50
        assert result["salary"]["period"] == "hourly"

    def test_rejects_eur_stipend(self):
        raw = {"description": "Learning and Development: yearly development budget of €2,000"}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_rejects_gbp_remote_setup_budget(self):
        raw = {"description": "Up to £1,200 per year for your remote setup."}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_rejects_small_annual_amount(self):
        """$500 annually is not a salary — it's a perk."""
        raw = {"description": "Compensation: $500 annually for professional development."}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_rejects_401k_match(self):
        raw = {"description": "401(k) with company match up to $5,000 per year"}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_real_multi_zone_picks_first(self):
        """Real: multiple geo-tiered ranges. We should pick one plausible range."""
        raw = {"description": (
            "Zone 1: San Francisco/Bay Area or NYC - $187,000 - $257,000\n"
            "Zone 2: Irvine, LA, Portland - $168,000 - $231,000\n"
            "Zone 3: All other US locations - $158,000 - $218,000"
        )}
        result = _extract_salary_from_description(raw)
        assert result is not None
        assert result["salary"]["min"] == 187_000
        assert result["salary"]["max"] == 257_000

    def test_rejects_retention_incentive(self):
        raw = {"description": "Up to $50,000 in long-term retention incentive"}
        result = _extract_salary_from_description(raw)
        # No salary signal keyword — single amount without "salary", "pay", etc.
        assert result is None

    def test_hourly_range_with_period_between_amounts(self):
        raw = {"description": "Starting wage: $23.00 / hour - $26.50 / hour"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 23.0
        assert result["salary"]["max"] == 26.5
        assert result["salary"]["period"] == "hourly"

    def test_minimum_maximum_range(self):
        raw = {"description": "Competitive compensation package of minimum $20.49 – maximum $22.61"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 20.49
        assert result["salary"]["max"] == 22.61
        assert result["salary"]["period"] == "hourly"

    def test_non_breaking_hyphen_to_separator(self):
        raw = {"description": "We offer a pay range of $25\u2011to\u2011$65 per hour"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 25
        assert result["salary"]["max"] == 65
        assert result["salary"]["period"] == "hourly"

    def test_compensation_label_triggers_signal(self):
        raw = {"description": "Compensation: up to $95k depending upon experience"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 95_000

    def test_earnings_label_triggers_signal(self):
        raw = {"description": "First-year average earnings $70,000 - $120,000"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 70_000
        assert result["salary"]["max"] == 120_000

    # --- Additional currency support ---

    def test_cad_dollar_with_suffix(self):
        raw = {"description": "The salary range for this role is $90,000-110,000 CAD."}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 90_000
        assert result["salary"]["max"] == 110_000
        assert result["salary"]["currency"] == "CAD"

    def test_cad_c_dollar(self):
        raw = {"description": "Salary: C$85,000 - C$110,000 annually"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 85_000
        assert result["salary"]["max"] == 110_000
        assert result["salary"]["currency"] == "CAD"

    def test_eur_code_suffix(self):
        raw = {"description": "From 4300 EUR/month. Salary offers are based on experience."}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 4_300
        assert result["salary"]["currency"] == "EUR"
        assert result["salary"]["period"] == "monthly"

    def test_eur_range_code_suffix(self):
        raw = {"description": "160,000 - 265,000 PLN"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 160_000
        assert result["salary"]["max"] == 265_000
        assert result["salary"]["currency"] == "PLN"

    def test_pln_single_with_signal_and_period(self):
        raw = {"description": "Salary: 9550 PLN gross per month"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 9_550
        assert result["salary"]["currency"] == "PLN"
        assert result["salary"]["period"] == "monthly"

    def test_brl_r_dollar(self):
        raw = {"description": "Salary: R$15,000 - R$25,000 per month"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 15_000
        assert result["salary"]["max"] == 25_000
        assert result["salary"]["currency"] == "BRL"
        assert result["salary"]["period"] == "monthly"

    def test_european_number_format_eur(self):
        raw = {"description": "Salary: 25.500€ gross/year"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 25_500
        assert result["salary"]["currency"] == "EUR"
        assert result["salary"]["period"] == "annually"

    def test_european_number_format_range(self):
        raw = {"description": "Salary: 25.500€ gross/year + up to 4.500€ gross/year in bonus"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 25_500
        assert result["salary"]["currency"] == "EUR"

    def test_czk_kc_suffix(self):
        raw = {"description": "Salary: 40–50 000 Kč monthly"}
        # This is tricky — "40–50 000 Kč" means 40,000-50,000 CZK
        # but our parser would see "50 000" which is hard to parse
        # Just check we don't crash
        result = _extract_salary_from_description(raw)
        # May or may not extract — the format is unusual

    def test_aud_dollar_prefix(self):
        raw = {"description": "Salary: A$85,000 - A$110,000 per year"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 85_000
        assert result["salary"]["max"] == 110_000
        assert result["salary"]["currency"] == "AUD"

    def test_sgd_ote_range(self):
        raw = {"description": "The annual OTE for this role in Singapore is SGD 165,000 - 195,000"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 165_000
        assert result["salary"]["max"] == 195_000
        assert result["salary"]["currency"] == "SGD"

    def test_inr_single(self):
        raw = {"description": "Salary: Up to INR 32,000 per month"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["min"] == 32_000
        assert result["salary"]["currency"] == "INR"
        assert result["salary"]["period"] == "monthly"

    def test_rejects_eur_learning_budget(self):
        """€2,000 learning budget is not salary."""
        raw = {"description": "Learning and Development: Develop your skills with a yearly development budget of €2,000"}
        result = _extract_salary_from_description(raw)
        assert result is None

    def test_usd_still_preferred_over_non_usd(self):
        """When both USD and non-USD present, USD range with signal should win."""
        raw = {"description": "Salary Range: $120,000-$150,000 USD\nAlso available: €100,000-€130,000 EUR"}
        result = _extract_salary_from_description(raw)
        assert result["salary"]["currency"] == "USD"
        assert result["salary"]["min"] == 120_000


class TestDescriptionYearsExperience:
    """Tests for _extract_years_experience_from_description."""

    # --- N+ years patterns ---

    def test_5_plus_years(self):
        raw = {"description": "5+ years of experience in data science or analytics"}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 5, "max": None}

    def test_8_plus_years(self):
        raw = {"description": "8+ years of proven experience in Outsourcing or Category Management"}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 8, "max": None}

    def test_2_plus_years(self):
        raw = {"description": "2+ years minimum work experience within a resource play focused team"}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 2, "max": None}

    # --- N-M years range patterns ---

    def test_3_5_years_dash(self):
        raw = {"description": "3-5 years direct, hands-on experience managing paid search campaigns"}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 3, "max": 5}

    def test_8_10_years_dash(self):
        raw = {"description": "Minimum of 8-10 years of hands-on experience in SAP"}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 8, "max": 10}

    def test_1_3_years_en_dash(self):
        raw = {"description": "1–3 years of experience in an IT-related field"}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 1, "max": 3}

    def test_7_9_years_spaced_dash(self):
        raw = {"description": "7 - 9 years of experience in strategy"}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 7, "max": 9}

    # --- N to M years ---

    def test_3_to_5_years(self):
        raw = {"description": "A minimum of 3 to 5 Years of working experience with Service Now"}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 3, "max": 5}

    def test_0_to_1_years(self):
        raw = {"description": "0 to 1 year of professional experience in software development"}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 0, "max": 1}

    # --- "at least N years" / "minimum N years" ---

    def test_at_least_5_years(self):
        raw = {"description": "At least 5 years of relevant professional experience required"}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 5, "max": None}

    def test_minimum_of_3_years(self):
        raw = {"description": "Minimum of 3 years experience in software engineering"}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 3, "max": None}

    # --- Bare "N years of experience" ---

    def test_5_years_of_experience(self):
        raw = {"description": "5 years of experience in data engineering"}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 5, "max": None}

    def test_2_years_experience(self):
        raw = {"description": "2 years experience as a Mechanic"}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 2, "max": None}

    def test_curly_apostrophe_years(self):
        raw = {"description": "A minimum of 2 years\u2019 experience with Service Now"}
        result = _extract_years_experience_from_description(raw)
        assert result is not None
        assert result["min"] == 2

    # --- False positive rejection ---

    def test_rejects_age_requirement(self):
        raw = {"description": "Minimum Age: 18 years old"}
        result = _extract_years_experience_from_description(raw)
        assert result is None

    def test_rejects_company_history(self):
        raw = {"description": "Planned Parenthood has been a leading provider for more than 85 years, serving women and men."}
        result = _extract_years_experience_from_description(raw)
        assert result is None

    def test_rejects_401k_age(self):
        raw = {"description": "FirstCash 401K program is available to all employees 21 years of age (or older)."}
        result = _extract_years_experience_from_description(raw)
        assert result is None

    def test_rejects_years_in_business(self):
        raw = {"description": "Our company has been operating for over 30 years in the industry."}
        result = _extract_years_experience_from_description(raw)
        assert result is None

    def test_rejects_bare_years_without_experience_context(self):
        """'3-5 years' alone without 'experience' nearby should not match."""
        raw = {"description": "The warranty covers 3-5 years from date of purchase."}
        result = _extract_years_experience_from_description(raw)
        assert result is None

    # --- Picks the best match ---

    def test_picks_highest_from_multiple_lines(self):
        """Multiple experience mentions — pick the most specific/highest."""
        raw = {"description": (
            "1+ years direct customer contact experience required.\n"
            "5+ years of experience in data science or analytics.\n"
            "8+ years of overall professional experience."
        )}
        result = _extract_years_experience_from_description(raw)
        assert result["min"] == 8

    def test_range_beats_single(self):
        """A range pattern is more specific than N+."""
        raw = {"description": (
            "3+ years of experience in software engineering.\n"
            "5-10 years of experience selling complex software."
        )}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 5, "max": 10}

    # --- Integration with merge_api_data ---

    def test_merge_api_data_extracts_experience(self):
        raw = {"description": "5+ years of experience in backend development"}
        result = merge_api_data(raw, {})
        assert result["years_experience"] == {"min": 5, "max": None}

    def test_merge_api_data_llm_takes_priority(self):
        raw = {"description": "5+ years of experience in backend development"}
        llm = {"years_experience": {"min": 7, "max": 10}}
        result = merge_api_data(raw, llm)
        assert result["years_experience"] == {"min": 7, "max": 10}

    def test_merge_api_data_no_experience(self):
        raw = {"description": "Join our amazing team!"}
        result = merge_api_data(raw, {})
        assert result.get("years_experience") is None

    # --- Edge cases ---

    def test_empty_description(self):
        result = _extract_years_experience_from_description({})
        assert result is None

    def test_html_stripped(self):
        raw = {"description": "<p><strong>5+ years</strong> of experience in project management</p>"}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 5, "max": None}

    def test_rejects_implausible_years(self):
        raw = {"description": "25+ years of experience required"}
        result = _extract_years_experience_from_description(raw)
        assert result is None

    def test_content_field_fallback(self):
        raw = {"content": "<p>3-5 years experience in software engineering</p>"}
        result = _extract_years_experience_from_description(raw)
        assert result == {"min": 3, "max": 5}


class TestManagerDetection:
    """Tests for _detect_is_manager and its integration in merge_api_data."""

    # --- Positive title patterns (people management) ---

    @pytest.mark.parametrize("title", [
        "Engineering Manager",
        "Engineering Manager - Employee Spend & Mobile",
        "Senior Manager, AI Engineering",
        "Director, Product Management, Platform",
        "Director of Strategic Accounts",
        "Head of Marketing",
        "VP of Engineering",
        "Vice President of Sales",
        "Engineering Director (Autonomous Vehicles)",
        "Store Manager",
        "Restaurant Manager",
        "Warehouse Manager",
        "District Manager",
        "Assistant Store Manager",
        "Harvest Supervisor",
        "Team Lead, Data Engineering",
        "Data Science Manager",
        "QA Manager",
        "DevOps Manager",
        "Design Director",
        "Managing Director",
        "Superintendent",
    ])
    def test_positive_titles(self, title):
        raw = {"title": title}
        assert _detect_is_manager(raw) is True

    # --- Negative title patterns (IC roles with "Manager" in title) ---

    @pytest.mark.parametrize("title", [
        "Account Manager",
        "Enterprise Account Manager",
        "Customer Success Manager",
        "Project Manager",
        "Program Manager",
        "Product Marketing Manager",
        "Event Marketing Manager",
        "Office Manager",
        "Case Manager",
        "Social Media Manager",
        "Implementation Manager",
        "Compliance Manager",
        "Partnerships Manager",
        "Community Manager",
        "Lead Engineer",
        "Lead Designer",
        "Lead Data Scientist",
        "Technical Lead",
        "Solution Architect Lead",
    ])
    def test_negative_titles(self, title):
        raw = {"title": title}
        result = _detect_is_manager(raw)
        assert result is False or result is None

    # --- Description overrides negative title ---

    def test_account_manager_with_people_signals_is_manager(self):
        raw = {
            "title": "Account Manager",
            "description": (
                "You will manage a team of 5 account executives. "
                "You will have 3-5 direct reports and be responsible for hiring and developing talent."
            ),
        }
        assert _detect_is_manager(raw) is True

    def test_account_manager_without_people_signals_is_not(self):
        raw = {
            "title": "Account Manager",
            "description": "Manage enterprise accounts and drive revenue growth.",
        }
        assert _detect_is_manager(raw) is False

    # --- Description-only detection (no title signal) ---

    def test_description_signals_without_title(self):
        raw = {
            "title": "Senior Software Engineer",
            "description": "Lead a team of 8 engineers. You will have direct reports and mentor junior developers.",
        }
        assert _detect_is_manager(raw) is True

    def test_no_signals_returns_none(self):
        raw = {
            "title": "Software Engineer",
            "description": "Build great software in a collaborative environment.",
        }
        assert _detect_is_manager(raw) is None

    # --- Integration with merge_api_data ---

    def test_merge_api_data_detects_manager(self):
        raw = {"title": "Engineering Manager"}
        result = merge_api_data(raw, {})
        assert result["is_manager"] is True

    def test_merge_api_data_detects_non_manager(self):
        raw = {"title": "Account Manager", "description": "Manage enterprise accounts."}
        result = merge_api_data(raw, {})
        assert result["is_manager"] is False

    def test_merge_api_data_llm_takes_priority(self):
        raw = {"title": "Engineering Manager"}
        llm = {"is_manager": False}
        result = merge_api_data(raw, llm)
        assert result["is_manager"] is False  # LLM says no, trust it

    def test_merge_api_data_ambiguous_leaves_default(self):
        raw = {"title": "Software Engineer", "description": "Write code."}
        result = merge_api_data(raw, {})
        # No signal either way — is_manager not set (defaults to False in Meili doc)
        assert result.get("is_manager") is None


class TestVisaSponsorship:
    """Tests for _detect_visa_sponsorship."""

    # --- Visa YES ---

    def test_sponsorship_available(self):
        raw = {"description": "Visa sponsorship is available for this role."}
        assert _detect_visa_sponsorship(raw) == "yes"

    def test_we_offer_visa(self):
        raw = {"description": "We offer visa sponsorship and can support relocation."}
        assert _detect_visa_sponsorship(raw) == "yes"

    def test_we_sponsor_visas(self):
        raw = {"description": "We sponsor visas in both the UK and US."}
        assert _detect_visa_sponsorship(raw) == "yes"

    def test_we_provide_visa_sponsorship(self):
        raw = {"description": "We provide visa sponsorship & relocation assistance."}
        assert _detect_visa_sponsorship(raw) == "yes"

    # --- Visa NO ---

    def test_no_sponsorship(self):
        raw = {"description": "We are unable to sponsor visas for this position."}
        assert _detect_visa_sponsorship(raw) == "no"

    def test_not_sponsor(self):
        raw = {"description": "We do not sponsor work visas."}
        assert _detect_visa_sponsorship(raw) == "no"

    def test_must_be_authorized(self):
        raw = {"description": "You must be authorized to work in the United States."}
        assert _detect_visa_sponsorship(raw) == "no"

    def test_legally_authorized(self):
        raw = {"description": "Must be legally authorized to work in the U.S. without sponsorship."}
        assert _detect_visa_sponsorship(raw) == "no"

    def test_without_need_for_sponsorship(self):
        raw = {"description": "Candidate must be eligible without the need for sponsorship."}
        assert _detect_visa_sponsorship(raw) == "no"

    def test_work_authorization_required(self):
        raw = {"description": "Work authorization is required for this position."}
        assert _detect_visa_sponsorship(raw) == "no"

    def test_cannot_sponsor(self):
        raw = {"description": "Unfortunately, we cannot sponsor visas at this time."}
        assert _detect_visa_sponsorship(raw) == "no"

    # --- No beats yes when both present ---

    def test_no_overrides_yes(self):
        raw = {"description": "We sponsor visas but cannot sponsor for this role."}
        assert _detect_visa_sponsorship(raw) == "no"

    # --- Unknown ---

    def test_no_mention(self):
        raw = {"description": "Join our amazing engineering team!"}
        assert _detect_visa_sponsorship(raw) is None

    def test_empty(self):
        assert _detect_visa_sponsorship({}) is None

    # --- Integration ---

    def test_merge_api_data_detects_no_visa(self):
        raw = {"description": "Must be authorized to work in the U.S."}
        result = merge_api_data(raw, {})
        assert result["visa_sponsorship"] == "no"

    def test_merge_api_data_llm_takes_priority(self):
        raw = {"description": "Must be authorized to work in the U.S."}
        llm = {"visa_sponsorship": "yes"}
        result = merge_api_data(raw, llm)
        assert result["visa_sponsorship"] == "yes"

    def test_merge_api_data_unknown_gets_overridden(self):
        raw = {"description": "We offer visa sponsorship for this role."}
        llm = {"visa_sponsorship": "unknown"}
        result = merge_api_data(raw, llm)
        assert result["visa_sponsorship"] == "yes"


class TestEducationExtraction:
    """Tests for _extract_education_from_description."""

    def test_bachelors_degree_required(self):
        raw = {"description": "Bachelor's degree in Computer Science or related field required."}
        assert _extract_education_from_description(raw) == "bachelors"

    def test_masters_degree(self):
        raw = {"description": "Master's degree in Engineering preferred."}
        assert _extract_education_from_description(raw) == "masters"

    def test_phd_required(self):
        raw = {"description": "PhD in Machine Learning or related field required."}
        assert _extract_education_from_description(raw) == "phd"

    def test_mba(self):
        raw = {"description": "MBA or equivalent advanced degree preferred."}
        assert _extract_education_from_description(raw) == "masters"

    def test_high_school_diploma(self):
        raw = {"description": "High school diploma or GED required."}
        assert _extract_education_from_description(raw) == "high-school"

    def test_bs_in_field(self):
        raw = {"description": "B.S. in Computer Science or equivalent experience."}
        assert _extract_education_from_description(raw) == "bachelors"

    def test_ms_in_field(self):
        raw = {"description": "M.S. in Data Science or related field."}
        assert _extract_education_from_description(raw) == "masters"

    def test_no_degree_required(self):
        raw = {"description": "No degree required — we value experience over credentials."}
        assert _extract_education_from_description(raw) == "none"

    def test_picks_minimum_required(self):
        """When 'required' and 'preferred' are on separate lines, pick the required level."""
        raw = {"description": "Bachelor's degree required.\nMaster's degree preferred."}
        assert _extract_education_from_description(raw) == "bachelors"

    def test_no_education_mentioned(self):
        raw = {"description": "Join our team and build great software."}
        assert _extract_education_from_description(raw) is None

    def test_empty(self):
        assert _extract_education_from_description({}) is None

    def test_does_not_match_word_fragments(self):
        """Should not match 'ba' in 'based' or 'ma' in 'manager'."""
        raw = {"description": "This is a manager-based role with great benefits."}
        assert _extract_education_from_description(raw) is None

    def test_html_stripped(self):
        raw = {"description": "<li>Bachelor's degree in Engineering</li>"}
        assert _extract_education_from_description(raw) == "bachelors"

    # --- Integration ---

    def test_merge_api_data_extracts_education(self):
        raw = {"description": "Bachelor's degree required."}
        result = merge_api_data(raw, {})
        assert result["education_level"] == "bachelors"

    def test_merge_api_data_llm_takes_priority(self):
        raw = {"description": "Bachelor's degree required."}
        llm = {"education_level": "masters"}
        result = merge_api_data(raw, llm)
        assert result["education_level"] == "masters"

    def test_merge_api_data_structured_ats_takes_priority(self):
        """ATS 'education' field is checked before description fallback."""
        raw = {"education": "PhD", "description": "Bachelor's degree required."}
        result = merge_api_data(raw, {})
        assert result["education_level"] == "phd"


class TestWorkableIndustryMapping:
    """Tests for _map_workable_industry."""

    @pytest.mark.parametrize("industry,expected", [
        ("Information Technology and Services", "enterprise_software"),
        ("Computer Software", "enterprise_software"),
        ("Hospital & Health Care", "healthcare_services"),
        ("Financial Services", "payments_banking"),
        ("Marketing and Advertising", "advertising_marketing"),
        ("Retail", "commerce_marketplaces"),
        ("Construction", "construction_built_environment"),
        ("Defense & Space", "defense_public_safety"),
        ("Education Management", "education_edtech"),
    ])
    def test_known_mappings(self, industry, expected):
        assert _map_workable_industry({"industry": industry}) == expected

    def test_unknown_industry(self):
        assert _map_workable_industry({"industry": "Underwater Basket Weaving"}) is None

    def test_no_industry_field(self):
        assert _map_workable_industry({}) is None

    def test_case_insensitive(self):
        assert _map_workable_industry({"industry": "FINANCIAL SERVICES"}) == "payments_banking"

    # --- Integration ---

    def test_merge_api_data_maps_industry(self):
        raw = {"industry": "Computer Software"}
        result = merge_api_data(raw, {})
        assert result["industry_primary"] == "enterprise_software"

    def test_merge_api_data_llm_takes_priority(self):
        raw = {"industry": "Computer Software"}
        llm = {"industry_primary": "ai_ml"}
        result = merge_api_data(raw, llm)
        assert result["industry_primary"] == "ai_ml"


class TestNoMutation:
    def test_original_dict_not_mutated(self):
        raw = {"workplaceType": "Remote"}
        llm = {"office_type": "onsite", "salary": None}
        original_llm = dict(llm)
        merge_api_data(raw, llm)
        assert llm == original_llm
