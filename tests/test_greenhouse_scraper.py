from scrapers.greenhouse_scraper import GreenhouseScraper


def make_scraper(html: str, url: str = "https://boards.greenhouse.io/example"):
    scraper = GreenhouseScraper("example")
    scraper._cached_html = html
    scraper._cached_html_url = url
    return scraper


def test_greenhouse_logo_prefers_json_ld_organization_logo():
    scraper = make_scraper(
        """
        <html>
          <head>
            <script type="application/ld+json">
              {
                "@context": "https://schema.org",
                "@type": "Organization",
                "name": "Airbnb",
                "logo": {
                  "@type": "ImageObject",
                  "url": "https://careers.airbnb.com/wp-content/uploads/sites/7/2019/01/airbnb_vertical_lockup_web-high-res.png"
                }
              }
            </script>
            <meta property="og:image" content="https://careers.airbnb.com/wp-content/uploads/sites/7/2018/12/Tokyo_021.jpg" />
          </head>
        </html>
        """
    )

    assert scraper.get_company_logo_url() == (
        "https://careers.airbnb.com/wp-content/uploads/sites/7/2019/01/airbnb_vertical_lockup_web-high-res.png"
    )


def test_greenhouse_logo_accepts_native_greenhouse_board_logo():
    scraper = make_scraper(
        """
        <html>
          <head>
            <meta property="og:image" content="https://s2-recruiting.cdn.greenhouse.io/external_greenhouse_job_boards/logos/400/254/300/original/Tanuki_white_square.png?1651072661" />
          </head>
        </html>
        """
    )

    assert scraper.get_company_logo_url() == (
        "https://s2-recruiting.cdn.greenhouse.io/external_greenhouse_job_boards/logos/400/254/300/original/Tanuki_white_square.png?1651072661"
    )


def test_greenhouse_logo_falls_back_to_icon_for_non_logo_og_image():
    scraper = make_scraper(
        """
        <html>
          <head>
            <meta property="og:image" content="https://cdn.sanity.io/images/example/social-card.png" />
            <link rel="icon" href="/favicon-32x32.png" />
          </head>
        </html>
        """,
        url="https://boards.greenhouse.io/figma",
    )

    assert scraper.get_company_logo_url() == "https://boards.greenhouse.io/favicon-32x32.png"
