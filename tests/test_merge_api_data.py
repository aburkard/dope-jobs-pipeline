"""Tests for merge_api_data — overlaying API structured data onto LLM extraction."""
import pytest
from parse import _flat_to_job_metadata, merge_api_data, prepare_language_detection_text


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

    def test_non_remote_clears_llm_applicant_requirements(self):
        raw = {"workplaceType": "OnSite"}
        llm = {
            "office_type": "remote",
            "applicant_location_requirements": [
                {"scope": "city", "name": "Paris", "country_code": "FR", "region": None}
            ],
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


class TestNoMutation:
    def test_original_dict_not_mutated(self):
        raw = {"workplaceType": "Remote"}
        llm = {"office_type": "onsite", "salary": None}
        original_llm = dict(llm)
        merge_api_data(raw, llm)
        assert llm == original_llm
