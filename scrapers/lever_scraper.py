import html as html_lib
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from .base_scraper import BaseScraper
import utils


class LeverScraper(BaseScraper):

    ats_name = 'lever'

    def __init__(self, board_token, is_eu=False):
        super().__init__(board_token)
        self.ats_name = 'lever_eu' if is_eu else LeverScraper.ats_name
        self.base_url = f"https://api.{'eu.' if is_eu else ''}lever.co/v0"
        self.is_eu = is_eu

    def check_exists(self):
        url = f"https://jobs{'.eu' if self.is_eu else ''}.lever.co/{self.board_token}"
        return self.session.head(url).status_code == 200

    # No equivalent in lever
    def fetch_job_board(self, **kwargs):
        raise NotImplementedError

    def fetch_jobs(self, normalize=True):
        url = f"{self.base_url}/postings/{self.board_token}"
        response = self.session.get(url,
                                    params={
                                        'mode': 'json',
                                        'limit': 5000
                                    },
                                    timeout=10)
        jobs = response.json()

        if not isinstance(jobs, list):
            return []

        if normalize:
            jobs = [self.normalize_job(job) for job in jobs]
        jobs = [self.add_default_fields(job) for job in jobs]
        return jobs

    def fetch_job(self, job_id):
        url = f"{self.base_url}/postings/{self.board_token}/{job_id}"
        response = self.session.get(url, timeout=5)
        return response.json()

    def normalize_job(self, job):
        description = self.clean_description(job)
        description_html = self.clean_description_html(job)
        categories = job.get('categories', {}) or {}

        return {
            "id": f"lever__{self.board_token}__{job.get('id')}",
            "board_token": self.board_token,
            "company": utils.get_company_name(self.board_token),
            "title": job.get('text'),
            "description": description,
            "descriptionHtml": description_html,
            "additionalPlain": job.get('additionalPlain', ''),  # often has compensation
            "location": categories.get('location'),
            "url": job.get('hostedUrl'),
            "applyUrl": job.get('applyUrl'),
            "country": job.get('country', ''),
            "createdAt": job.get('createdAt'),
            "updatedAt": job.get('updatedAt'),

            # Structured data from API
            "workplaceType": job.get('workplaceType', ''),  # onsite, remote, hybrid
            "department": categories.get('department', ''),
            "commitment": categories.get('commitment', ''),  # Full-time, Part-time, etc.
            "team": categories.get('team', ''),
            "allLocations": categories.get('allLocations', []),
        }

    def clean_description(self, job):
        s = job['descriptionPlain']
        for item in job['lists']:
            title = item['text']
            content = BeautifulSoup(item['content'], "lxml").text
            s += f"{title}\n{content}\n"
        s += job['additionalPlain']
        return s.strip()

    def clean_description_html(self, job):
        parts = []

        description_html = job.get("description")
        if description_html:
            parts.append(description_html)
        else:
            description_plain = job.get("descriptionPlain", "").strip()
            if description_plain:
                escaped = html_lib.escape(description_plain).replace("\n", "<br>")
                parts.append(f"<p>{escaped}</p>")

        for item in job.get("lists", []):
            title = (item.get("text") or "").strip()
            content = item.get("content") or ""
            if title:
                parts.append(f"<p>{html_lib.escape(title)}</p>")
            if content:
                parts.append(content)

        additional_html = job.get("additional")
        if additional_html:
            parts.append(additional_html)
        else:
            additional_plain = job.get("additionalPlain", "").strip()
            if additional_plain:
                escaped = html_lib.escape(additional_plain).replace("\n", "<br>")
                parts.append(f"<p>{escaped}</p>")

        return "\n".join(part for part in parts if part).strip()

    def _fetch_html(self, force=False):
        if not hasattr(self, '_cached_html') or force:
            headers = self.session.headers.copy()
            headers['Accept'] = None
            response = self.session.get(
                f"https://jobs.lever.co/{self.board_token}",
                timeout=5,
                headers=headers)
            self._cached_html = response.text
            self._cached_html_status = response.status_code
        return self._cached_html

    def _get_cached_soup(self):
        if not hasattr(self, "_cached_soup"):
            raw_html = self._fetch_html()
            self._cached_soup = BeautifulSoup(raw_html, 'html.parser')
        return self._cached_soup

    def _get_page_title(self):
        soup = self._get_cached_soup()
        title_tag = soup.find("title")
        return title_tag.text.strip() if title_tag and title_tag.text else None

    def _is_invalid_board_page(self):
        status = getattr(self, "_cached_html_status", None)
        if isinstance(status, int) and status >= 400:
            return True

        title = (self._get_page_title() or "").strip().lower()
        return bool(title) and (
            "not found" in title
            or "404 error" in title
            or "job seeker support" in title
        )

    def _company_link_candidates(self):
        soup = self._get_cached_soup()
        for link in soup.find_all("a", href=True):
            href = link["href"].strip()
            if not href.startswith("http"):
                continue
            parsed = urlparse(href)
            host = (parsed.hostname or "").lower()
            if host.startswith("www."):
                host = host[4:]
            if not host:
                continue
            if host.endswith("lever.co"):
                continue
            yield href

    def get_company_name(self):
        if not hasattr(self, '_cached_company_name'):
            if self._is_invalid_board_page():
                self._cached_company_name = None
            else:
                title = self._get_page_title()
                if not title:
                    self._cached_company_name = None
                else:
                    cleaned = title
                    for suffix in [" - Jobs", " | Jobs", " Careers", " - Careers", " | Careers"]:
                        if cleaned.endswith(suffix):
                            cleaned = cleaned[: -len(suffix)].strip()
                            break
                    self._cached_company_name = cleaned or None
        return self._cached_company_name

    def get_company_domain(self):
        if not hasattr(self, '_cached_company_domain'):
            if self._is_invalid_board_page():
                self._cached_company_domain = None
            else:
                self._cached_company_domain = next(self._company_link_candidates(), None)
        return self._cached_company_domain

    def get_company_logo_url(self):
        if not hasattr(self, '_cached_company_logo_url'):
            if self._is_invalid_board_page():
                self._cached_company_logo_url = None
            else:
                soup = self._get_cached_soup()
                og_image = soup.find('meta', property='og:image')
                if og_image and og_image.get('content'):
                    self._cached_company_logo_url = og_image['content'].split('?')[0]
                else:
                    self._cached_company_logo_url = None
        return self._cached_company_logo_url
