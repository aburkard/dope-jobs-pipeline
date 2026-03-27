"""Tests for db.py — content hash, job_id, change detection."""
import pytest
from db import content_hash, job_id


class TestContentHash:
    def test_same_content_same_hash(self):
        job1 = {"title": "Engineer", "description": "Build things"}
        job2 = {"title": "Engineer", "description": "Build things"}
        assert content_hash(job1) == content_hash(job2)

    def test_different_content_different_hash(self):
        job1 = {"title": "Engineer", "description": "Build things"}
        job2 = {"title": "Engineer", "description": "Build other things"}
        assert content_hash(job1) != content_hash(job2)

    def test_title_change_changes_hash(self):
        job1 = {"title": "Junior Engineer", "description": "Build things"}
        job2 = {"title": "Senior Engineer", "description": "Build things"}
        assert content_hash(job1) != content_hash(job2)

    def test_handles_missing_fields(self):
        job = {}
        h = content_hash(job)
        assert isinstance(h, str)
        assert len(h) == 64  # SHA256

    def test_html_stripped(self):
        job1 = {"title": "Test", "content": "<p>Hello <b>world</b></p>"}
        job2 = {"title": "Test", "content": "Hello world"}
        # Should produce the same hash since HTML is stripped
        assert content_hash(job1) == content_hash(job2)

    def test_prefers_content_field(self):
        job = {"title": "Test", "content": "From content", "description": "From description"}
        h = content_hash(job)
        # content field takes priority
        job2 = {"title": "Test", "content": "From content"}
        assert content_hash(job) == content_hash(job2)

    def test_falls_back_to_description(self):
        job = {"title": "Test", "description": "Hello"}
        h = content_hash(job)
        assert isinstance(h, str)

    def test_falls_back_to_descriptionHtml(self):
        job = {"title": "Test", "descriptionHtml": "<p>Hello</p>"}
        h = content_hash(job)
        assert isinstance(h, str)


class TestJobId:
    def test_uses_existing_compound_id(self):
        job = {"id": "greenhouse__anthropic__12345", "ats_name": "greenhouse", "board_token": "anthropic"}
        assert job_id(job) == "greenhouse__anthropic__12345"

    def test_builds_id_when_simple(self):
        job = {"id": "12345", "ats_name": "greenhouse", "board_token": "anthropic"}
        assert job_id(job) == "greenhouse__anthropic__12345"

    def test_handles_missing_id(self):
        job = {"ats_name": "greenhouse", "board_token": "anthropic"}
        result = job_id(job)
        assert result == "greenhouse__anthropic__"


class TestScraperNormalization:
    """Test that scrapers produce expected field structures."""

    def test_greenhouse_fields(self):
        from scrapers.greenhouse_scraper import GreenhouseScraper
        scraper = GreenhouseScraper("test")
        raw = {
            "id": 12345,
            "title": "Engineer",
            "content": "<p>Description</p>",
            "location": {"name": "SF, CA"},
            "absolute_url": "https://example.com/job",
            "updated_at": "2026-01-01T00:00:00Z",
            "first_published": "2025-12-01",
            "departments": [{"name": "Engineering", "id": 1}],
            "offices": [{"name": "SF", "location": "San Francisco, CA", "id": 1}],
        }
        normalized = scraper.normalize_job(raw)
        assert normalized["id"] == "greenhouse__test__12345"
        assert normalized["title"] == "Engineer"
        assert normalized["departments"] == ["Engineering"]
        assert normalized["offices"][0]["location"] == "San Francisco, CA"
        assert "content" in normalized

    def test_lever_fields(self):
        from scrapers.lever_scraper import LeverScraper
        scraper = LeverScraper("test")
        raw = {
            "id": "abc-123",
            "text": "Designer",
            "descriptionPlain": "Design things",
            "lists": [],
            "additionalPlain": "Extra info",
            "categories": {"location": "NYC", "department": "Design", "commitment": "Full-time", "team": "Product"},
            "hostedUrl": "https://example.com",
            "applyUrl": "https://example.com/apply",
            "workplaceType": "hybrid",
            "country": "US",
        }
        normalized = scraper.normalize_job(raw)
        assert normalized["id"] == "lever__test__abc-123"
        assert normalized["workplaceType"] == "hybrid"
        assert normalized["department"] == "Design"
        assert normalized["commitment"] == "Full-time"

    def test_ashby_fields(self):
        from scrapers.ashby_scraper import AshbyScraper
        scraper = AshbyScraper("test")
        raw = {
            "id": "uuid-123",
            "title": "PM",
            "descriptionHtml": "<p>Manage products</p>",
            "descriptionPlain": "Manage products",
            "location": "NYC",
            "workplaceType": "Remote",
            "employmentType": "FullTime",
            "isRemote": True,
            "department": "Product",
            "team": "Growth",
            "compensation": {
                "compensationTierSummary": "$150K – $200K",
                "scrapeableCompensationSalarySummary": "$150K - $200K",
                "compensationTiers": [],
            },
            "address": {"postalAddress": {"addressLocality": "New York", "addressRegion": "NY", "addressCountry": "US"}},
            "secondaryLocations": [],
            "jobUrl": "https://jobs.ashbyhq.com/test/uuid-123",
            "applyUrl": "https://jobs.ashbyhq.com/test/uuid-123/apply",
            "publishedAt": "2026-01-01T00:00:00Z",
            "isListed": True,
        }
        normalized = scraper.normalize_job(raw)
        assert normalized["id"] == "ashby__test__uuid-123"
        assert normalized["workplaceType"] == "Remote"
        assert normalized["employmentType"] == "FullTime"
        assert normalized["compensationTierSummary"] == "$150K – $200K"
        assert normalized["locationCity"] == "New York"
        assert normalized["locationCountry"] == "US"
