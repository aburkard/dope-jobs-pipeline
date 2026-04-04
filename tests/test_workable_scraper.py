from scrapers.workable_scraper import WorkableScraper


class DummyResponse:
    def __init__(self, *, text=None, json_data=None, url=None, ok=True):
        self.text = text or ""
        self._json_data = json_data
        self.url = url or "https://apply.workable.com/loopme/"
        self.ok = ok

    def json(self):
        return self._json_data


def test_workable_company_name_comes_from_board_html(monkeypatch):
    widget_payload = {
        "name": "LoopMe",
        "description": None,
        "account": "2b41b749-ad30-43ee-b19f-ee7283d36f7c",
        "jobs": [],
    }

    scraper = WorkableScraper("loopme")
    monkeypatch.setattr(
        scraper.session,
        "get",
        lambda *args, **kwargs: DummyResponse(json_data=widget_payload),
    )

    assert scraper.check_exists() is True
    assert scraper.get_company_name() == "LoopMe"


def test_workable_company_description_comes_from_widget_board(monkeypatch):
    widget_payload = {
        "name": "The Telegraph",
        "account": "5df6f6f6-4739-4b3e-8081-12efb0aab3ab",
        "description": (
            "<p>Telegraph Media Group is an award-winning multimedia news publisher.</p>"
            "<p>We produce Telegraph.co.uk and The Daily Telegraph.</p>"
        ),
        "jobs": [],
    }

    scraper = WorkableScraper("telegraph")

    def fake_get(url, *args, **kwargs):
        if url == "https://apply.workable.com/api/v1/widget/accounts/telegraph?details=true":
            return DummyResponse(json_data=widget_payload, url=url)
        raise AssertionError(f"Unexpected GET {url}")

    monkeypatch.setattr(scraper.session, "get", fake_get)

    assert scraper.get_company_description() == (
        "Telegraph Media Group is an award-winning multimedia news publisher.\n"
        "We produce Telegraph.co.uk and The Daily Telegraph."
    )


def test_workable_fetch_jobs_uses_widget_only(monkeypatch):
    widget_payload = {
        "name": "LoopMe",
        "account": "2b41b749-ad30-43ee-b19f-ee7283d36f7c",
        "description": None,
        "jobs": [
            {
                "title": "Account Manager, Supply",
                "shortcode": "E6CD91E25F",
                "employment_type": "Full-time",
                "telecommuting": False,
                "department": "Marketplace",
                "url": "https://apply.workable.com/j/E6CD91E25F",
                "shortlink": "https://apply.workable.com/j/E6CD91E25F",
                "application_url": "https://apply.workable.com/j/E6CD91E25F/apply",
                "published_on": "2026-03-17",
                "created_at": "2026-03-11",
                "country": "China",
                "city": "Beijing",
                "state": "Beijing",
                "education": "Bachelor's Degree",
                "experience": "Mid-Senior level",
                "function": "Sales",
                "industry": "Marketing and Advertising",
                "locations": [
                    {
                        "country": "China",
                        "countryCode": "CN",
                        "city": "Beijing",
                        "region": "Beijing",
                        "hidden": False,
                    }
                ],
                "description": "<p>Widget description</p>",
            }
        ],
    }

    scraper = WorkableScraper("loopme")

    def fake_get(url, *args, **kwargs):
        if url == "https://apply.workable.com/api/v1/widget/accounts/loopme?details=true":
            return DummyResponse(json_data=widget_payload, url=url)
        raise AssertionError(f"Unexpected GET {url}")

    monkeypatch.setattr(scraper.session, "get", fake_get)

    jobs = list(scraper.fetch_jobs(normalize=False))
    assert len(jobs) == 1
    merged = jobs[0]

    assert merged["telecommuting"] is False
    assert merged["department"] == "Marketplace"
    assert merged["application_url"] == "https://apply.workable.com/j/E6CD91E25F/apply"
    assert merged["education"] == "Bachelor's Degree"

    normalized = scraper.normalize_job(merged)
    assert normalized["id"] == "workable__loopme__E6CD91E25F"
    assert normalized["company"] == "LoopMe"
    assert normalized["workplace"] is None
    assert normalized["workplaceType"] is None
    assert normalized["department"] == "Marketplace"
    assert normalized["departments"] == ["Marketplace"]
    assert normalized["applyUrl"] == "https://apply.workable.com/j/E6CD91E25F/apply"
    assert normalized["employmentType"] == "Full-time"
    assert normalized["createdAt"] == "2026-03-11"
    assert normalized["datePosted"] == "2026-03-17"
    assert normalized["education"] == "Bachelor's Degree"
    assert normalized["requirements"] == ""


def test_workable_fetch_jobs_merges_duplicate_shortcodes_into_multi_location_job(monkeypatch):
    widget_payload = {
        "name": "LoopMe",
        "account": "2b41b749-ad30-43ee-b19f-ee7283d36f7c",
        "description": None,
        "jobs": [
            {
                "title": "AI Platform Senior Software Engineer",
                "shortcode": "525989D896",
                "employment_type": "Full-time",
                "telecommuting": False,
                "department": "Technology",
                "url": "https://apply.workable.com/j/525989D896",
                "application_url": "https://apply.workable.com/j/525989D896/apply",
                "published_on": "2026-03-17",
                "created_at": "2026-03-11",
                "city": "Kraków",
                "state": "Lesser Poland Voivodeship",
                "country": "Poland",
                "locations": [{"country": "Poland", "city": "Kraków", "region": "Lesser Poland Voivodeship"}],
                "description": "<p>Widget description</p>",
            },
            {
                "title": "AI Platform Senior Software Engineer",
                "shortcode": "525989D896",
                "employment_type": "Full-time",
                "telecommuting": False,
                "department": "Technology",
                "url": "https://apply.workable.com/j/525989D896",
                "application_url": "https://apply.workable.com/j/525989D896/apply",
                "published_on": "2026-03-17",
                "created_at": "2026-03-11",
                "city": "Lviv",
                "state": "Lviv Oblast",
                "country": "Ukraine",
                "locations": [{"country": "Ukraine", "city": "Lviv", "region": "Lviv Oblast"}],
                "description": "<p>Widget description</p>",
            },
        ],
    }

    scraper = WorkableScraper("loopme")
    monkeypatch.setattr(
        scraper.session,
        "get",
        lambda *args, **kwargs: DummyResponse(json_data=widget_payload),
    )

    jobs = list(scraper.fetch_jobs(normalize=True))
    assert len(jobs) == 1
    assert jobs[0]["location"] == "Kraków, Lesser Poland Voivodeship, Poland"
    assert jobs[0]["locations"] == [
        {"city": "Kraków", "region": "Lesser Poland Voivodeship", "country": "Poland", "countryCode": None, "hidden": False},
        {"city": "Lviv", "region": "Lviv Oblast", "country": "Ukraine", "countryCode": None, "hidden": False},
    ]
