"""Tests for merge_api_data — overlaying API structured data onto LLM extraction."""
import pytest
from parse import merge_api_data


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


class TestLocationOverlay:
    def test_ashby_location(self):
        raw = {"locationCity": "New York City", "locationRegion": "NY", "locationCountry": "USA"}
        llm = {"locations": []}
        result = merge_api_data(raw, llm)
        assert len(result["locations"]) == 1
        assert result["locations"][0]["city"] == "New York City"
        assert result["locations"][0]["state"] == "NY"

    def test_no_overlay_when_llm_has_locations(self):
        raw = {"locationCity": "SF", "locationRegion": "CA", "locationCountry": "US"}
        llm = {"locations": [{"city": "New York", "state": "NY", "country_code": "US"}]}
        result = merge_api_data(raw, llm)
        # LLM locations preserved when present
        assert result["locations"][0]["city"] == "New York"


class TestNoMutation:
    def test_original_dict_not_mutated(self):
        raw = {"workplaceType": "Remote"}
        llm = {"office_type": "onsite", "salary": None}
        original_llm = dict(llm)
        merge_api_data(raw, llm)
        assert llm == original_llm
