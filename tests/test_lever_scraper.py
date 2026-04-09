from scrapers.lever_scraper import LeverScraper


def test_lever_company_metadata_ignores_404_support_page(monkeypatch):
    html = """
    <html>
      <head>
        <title>Not found – 404 error</title>
        <meta property="og:image" content="https://files.lever.co/default-logo.png" />
      </head>
      <body>
        <div class="main-footer-text">
          <a href="https://www.lever.co/job-seeker-support/">Support</a>
        </div>
      </body>
    </html>
    """

    class Response:
        def __init__(self, text):
            self.text = text
            self.status_code = 404

    scraper = LeverScraper("veeva")
    monkeypatch.setattr(scraper.session, "get", lambda *args, **kwargs: Response(html))

    assert scraper.get_company_name() is None
    assert scraper.get_company_domain() is None
    assert scraper.get_company_logo_url() is None


def test_lever_company_metadata_uses_valid_board_page(monkeypatch):
    html = """
    <html>
      <head>
        <title>Veeva - Jobs</title>
        <meta property="og:image" content="https://files.lever.co/veeva-logo.png?size=400" />
      </head>
      <body>
        <div class="main-footer-text">
          <a href="https://www.veeva.com">Website</a>
          <a href="https://www.lever.co/job-seeker-support/">Support</a>
        </div>
      </body>
    </html>
    """

    class Response:
        def __init__(self, text):
            self.text = text
            self.status_code = 200

    scraper = LeverScraper("veeva")
    monkeypatch.setattr(scraper.session, "get", lambda *args, **kwargs: Response(html))

    assert scraper.get_company_name() == "Veeva"
    assert scraper.get_company_domain() == "https://www.veeva.com"
    assert scraper.get_company_logo_url() == "https://files.lever.co/veeva-logo.png"
