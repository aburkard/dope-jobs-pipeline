import html
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base_scraper import BaseScraper
import utils


class GreenhouseScraper(BaseScraper):

    ats_name = 'greenhouse'

    def __init__(self, board_token):
        super().__init__(board_token)
        self.ats_name = GreenhouseScraper.ats_name
        self.base_url = 'https://boards-api.greenhouse.io/v1/boards'

    def check_exists(self):
        return self.session.head(
            f'{self.base_url}/{self.board_token}').status_code == 200

    def fetch_job_board(self):
        url = f'{self.base_url}/{self.board_token}/'
        response = self.session.get(url, timeout=5)
        job_board = response.json()
        return job_board

    def fetch_jobs(self, content=True, normalize=True):
        url = f"{self.base_url}/{self.board_token}/jobs"
        content = "true" if content else "false"
        response = self.session.get(url,
                                    params={"content": content},
                                    timeout=15)
        jobs = response.json().get("jobs", [])
        if normalize:
            jobs = [self.normalize_job(job) for job in jobs]
        jobs = [self.add_default_fields(job) for job in jobs]
        return jobs

    def fetch_job(self, job_id):
        url = f"{self.base_url}/{self.board_token}/jobs/{job_id}"
        response = self.session.get(url, timeout=5)
        return response.json()

    def fetch_job_pay(self, job_id):
        """Fetch pay transparency data for a single job."""
        url = f"{self.base_url}/{self.board_token}/jobs/{job_id}"
        response = self.session.get(url, params={"pay_transparency": "true"}, timeout=5)
        if response.ok:
            return response.json().get("pay_input_ranges", [])
        return []

    def normalize_job(self, job):
        # Extract structured department/office data
        departments = job.get('departments', [])
        offices = job.get('offices', [])

        return {
            "id": f"greenhouse__{self.board_token}__{job.get('id')}",
            "board_token": self.board_token,
            "company": utils.get_company_name(self.board_token),
            "title": job.get('title'),
            "content": job.get('content', ''),  # raw HTML for LLM
            "description": self.clean_description(job),
            "location": job.get('location', {}).get('name'),
            "url": job.get('absolute_url'),
            "updated_at": job.get('updated_at'),
            "first_published": job.get('first_published'),

            # Structured data from API
            "departments": [d.get('name') for d in departments if d.get('name')],
            "offices": [
                {"name": o.get('name', ''), "location": o.get('location', '')}
                for o in offices
            ],
        }

    # TODO: Compare this with utils.remove_html_markup(double_unescape=True)
    def clean_description(self, job):
        text = job.get('content', '')
        s = html.unescape(text)
        s = BeautifulSoup(s, "lxml").text
        return s.strip()

    def _fetch_html(self, force=False):
        if not hasattr(self, '_cached_html') or force:
            headers = self.session.headers.copy()
            headers['Accept'] = None
            try:
                response = self.session.get(
                    f"https://boards.greenhouse.io/{self.board_token}",
                    timeout=5,
                    headers=headers)
                self._cached_html = response.text
                self._cached_html_url = response.url
            except Exception as e:
                print(e)
                self._cached_html = ""
                self._cached_html_url = f"https://boards.greenhouse.io/{self.board_token}"
        return self._cached_html

    def get_company_name(self):
        if not hasattr(self, '_cached_company_name'):
            self._cached_company_name = self.fetch_job_board().get('name')
        return self._cached_company_name

    def get_company_domain(self):
        raise NotImplementedError

    def _extract_logo_from_json_ld(self, soup):
        def walk(node):
            if isinstance(node, dict):
                node_type = node.get('@type')
                if node_type == 'Organization' or (
                    isinstance(node_type, list) and 'Organization' in node_type
                ):
                    logo = node.get('logo')
                    if isinstance(logo, dict):
                        for key in ('url', 'contentUrl'):
                            value = logo.get(key)
                            if isinstance(value, str) and value.strip():
                                return html.unescape(value.strip())
                    if isinstance(logo, str) and logo.strip():
                        return html.unescape(logo.strip())
                for value in node.values():
                    found = walk(value)
                    if found:
                        return found
            elif isinstance(node, list):
                for value in node:
                    found = walk(value)
                    if found:
                        return found
            return None

        for script in soup.find_all('script', type='application/ld+json'):
            raw = script.string or script.get_text(strip=True)
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            found = walk(data)
            if found:
                return found
        return None

    @staticmethod
    def _is_native_greenhouse_logo(url):
        return 'external_greenhouse_job_boards/logos/' in url.lower()

    def _extract_favicon_url(self, soup):
        base_url = getattr(self, '_cached_html_url', f"https://boards.greenhouse.io/{self.board_token}")
        for link in soup.find_all('link', href=True):
            rels = [rel.lower() for rel in (link.get('rel') or [])]
            if any(rel in {'icon', 'shortcut icon', 'apple-touch-icon', 'mask-icon'} for rel in rels):
                href = link.get('href')
                if href:
                    return urljoin(base_url, html.unescape(href.strip()))
        return None

    def get_company_logo_url(self):
        if not hasattr(self, '_cached_company_logo_url'):
            raw_html = self._fetch_html()
            soup = BeautifulSoup(raw_html, 'html.parser')
            json_ld_logo = self._extract_logo_from_json_ld(soup)
            if json_ld_logo:
                self._cached_company_logo_url = json_ld_logo
                return self._cached_company_logo_url
            og_image = soup.find('meta', property='og:image')
            if og_image and og_image.get('content'):
                candidate = html.unescape(og_image.get('content').strip())
                if self._is_native_greenhouse_logo(candidate):
                    self._cached_company_logo_url = candidate
                    return self._cached_company_logo_url
            self._cached_company_logo_url = self._extract_favicon_url(soup)
        return self._cached_company_logo_url
