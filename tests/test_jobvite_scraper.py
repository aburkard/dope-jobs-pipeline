from scrapers.jobvite_scraper import JobviteScraper


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
