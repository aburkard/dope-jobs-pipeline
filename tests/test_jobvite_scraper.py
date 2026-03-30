from scrapers.jobvite_scraper import JobviteScraper
from requests import RequestException


def test_jobvite_company_metadata_from_board_page(monkeypatch):
    html = """
    <html>
      <head>
        <title>Sitecore Careers</title>
        <link rel="icon" href="//careers.jobvite.com/sitecore/favicon.ico" />
      </head>
      <body>
        <a href="https://www.sitecore.com">Sitecore Careers</a>
        <a href="https://www.sitecore.com/company/contact-us">Contact</a>
      </body>
    </html>
    """

    class DummyResponse:
        def __init__(self, text):
            self.text = text

    scraper = JobviteScraper("sitecore")

    def fake_get(url, *args, **kwargs):
        return DummyResponse(html)

    monkeypatch.setattr(scraper.session, "get", fake_get)

    assert scraper.get_company_name() == "Sitecore"
    assert scraper.get_company_domain() == "https://www.sitecore.com"
    assert scraper.get_company_logo_url() == "https://careers.jobvite.com/sitecore/favicon.ico"


def test_jobvite_reuses_existing_detail_when_complete(monkeypatch):
    board_html = """
    <html>
      <body>
        <ul class="jv-job-list">
          <li>
            <span class="jv-job-list-name"><a href="/sitecore/job/oz3wzfwr">Senior Software Engineer</a></span>
            <span class="jv-job-list-location">Remote</span>
          </li>
        </ul>
      </body>
    </html>
    """

    class DummyResponse:
        def __init__(self, text):
            self.text = text

    scraper = JobviteScraper("sitecore")

    def fake_get(url, *args, **kwargs):
        return DummyResponse(board_html)

    monkeypatch.setattr(scraper.session, "get", fake_get)
    monkeypatch.setattr(scraper, "fetch_job", lambda job_id: (_ for _ in ()).throw(AssertionError("fetch_job should not be called")))

    jobs = list(scraper.fetch_jobs(
        normalize=False,
        existing_details={
            "oz3wzfwr": {
                "description": "Stored description",
                "descriptionHtml": "<p>Stored description</p>",
                "datePosted": "2026-01-30",
                "validThrough": None,
            }
        },
    ))

    assert jobs[0]["description"] == "Stored description"
    assert jobs[0]["descriptionHtml"] == "<p>Stored description</p>"
    assert jobs[0]["datePosted"] == "2026-01-30"


def test_jobvite_refetches_existing_detail_when_metadata_incomplete(monkeypatch):
    board_html = """
    <html>
      <body>
        <ul class="jv-job-list">
          <li>
            <span class="jv-job-list-name"><a href="/sitecore/job/oz3wzfwr">Senior Software Engineer</a></span>
            <span class="jv-job-list-location">Remote</span>
          </li>
        </ul>
      </body>
    </html>
    """

    class DummyResponse:
        def __init__(self, text):
            self.text = text

    scraper = JobviteScraper("sitecore")

    def fake_get(url, *args, **kwargs):
        return DummyResponse(board_html)

    monkeypatch.setattr(scraper.session, "get", fake_get)
    monkeypatch.setattr(scraper, "fetch_job", lambda job_id: {
        "description": "Fresh description",
        "descriptionHtml": "<p>Fresh description</p>",
        "datePosted": "2026-01-30",
        "validThrough": None,
    })

    jobs = list(scraper.fetch_jobs(
        normalize=False,
        existing_details={
            "oz3wzfwr": {
                "description": "Stored description",
                "descriptionHtml": "",
                "datePosted": None,
                "validThrough": None,
            }
        },
    ))

    assert jobs[0]["description"] == "Fresh description"
    assert jobs[0]["descriptionHtml"] == "<p>Fresh description</p>"
    assert jobs[0]["datePosted"] == "2026-01-30"


def test_jobvite_refetch_flag_still_reuses_complete_existing_detail(monkeypatch):
    board_html = """
    <html>
      <body>
        <ul class="jv-job-list">
          <li>
            <span class="jv-job-list-name"><a href="/sitecore/job/oz3wzfwr">Senior Software Engineer</a></span>
            <span class="jv-job-list-location">Remote</span>
          </li>
        </ul>
      </body>
    </html>
    """

    class DummyResponse:
        def __init__(self, text):
            self.text = text

    scraper = JobviteScraper("sitecore")

    def fake_get(url, *args, **kwargs):
        return DummyResponse(board_html)

    monkeypatch.setattr(scraper.session, "get", fake_get)
    monkeypatch.setattr(
        scraper,
        "fetch_job",
        lambda job_id: (_ for _ in ()).throw(AssertionError("fetch_job should not be called")),
    )

    jobs = list(scraper.fetch_jobs(
        normalize=False,
        existing_details={
            "oz3wzfwr": {
                "description": "Stored description",
                "descriptionHtml": "<p>Stored description</p>",
                "datePosted": "2026-01-30",
                "validThrough": None,
            }
        },
        refetch_existing_detail=True,
    ))

    assert jobs[0]["description"] == "Stored description"
    assert jobs[0]["descriptionHtml"] == "<p>Stored description</p>"
    assert jobs[0]["datePosted"] == "2026-01-30"


def test_jobvite_skips_known_inactive_existing_detail(monkeypatch):
    board_html = """
    <html>
      <body>
        <ul class="jv-job-list">
          <li>
            <span class="jv-job-list-name"><a href="/sitecore/job/oz3wzfwr">Senior Software Engineer</a></span>
            <span class="jv-job-list-location">Remote</span>
          </li>
        </ul>
      </body>
    </html>
    """

    class DummyResponse:
        def __init__(self, text):
            self.text = text

    scraper = JobviteScraper("sitecore")
    monkeypatch.setattr(scraper.session, "get", lambda *args, **kwargs: DummyResponse(board_html))
    monkeypatch.setattr(
        scraper,
        "fetch_job",
        lambda job_id: (_ for _ in ()).throw(AssertionError("fetch_job should not be called")),
    )

    jobs = list(scraper.fetch_jobs(
        normalize=False,
        existing_details={
            "oz3wzfwr": {
                "description": "Stored description",
                "descriptionHtml": "<p>Stored description</p>",
                "datePosted": "2026-01-30",
                "validThrough": None,
                "inactive": True,
            }
        },
    ))

    assert jobs == []


def test_jobvite_fetch_job_marks_inactive_from_page_metadata(monkeypatch):
    html = """
    <html>
      <body>
        <script type="application/ld+json">
          {
            "@context": "https://schema.org",
            "@type": "JobPosting",
            "datePosted": "2026-03-01",
            "industry": "Inactive",
            "description": "<div><p>Build pipelines</p></div>"
          }
        </script>
        <p class="jv-job-detail-meta">Inactive<span class='jv-inline-separator'></span>Remote</p>
      </body>
    </html>
    """

    class Response:
        def __init__(self, text):
            self.text = text

    scraper = JobviteScraper("test")
    monkeypatch.setattr(scraper.session, "get", lambda *args, **kwargs: Response(html))

    fetched = scraper.fetch_job("abc123")

    assert fetched["inactive"] is True
    assert fetched["datePosted"] == "2026-03-01"


def test_jobvite_request_retries_then_succeeds(monkeypatch):
    scraper = JobviteScraper("test")
    calls = {"count": 0}

    class Response:
        text = "<html></html>"
        status_code = 200

    def flaky_get(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] < 3:
            raise RequestException("temporary failure")
        return Response()

    monkeypatch.setattr(scraper.session, "get", flaky_get)
    monkeypatch.setattr("scrapers.jobvite_scraper.time.sleep", lambda *_: None)

    response = scraper._request("get", "https://jobs.jobvite.com/test/search")

    assert isinstance(response, Response)
    assert calls["count"] == 3
