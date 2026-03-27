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
        categories = job.get('categories', {}) or {}

        return {
            "id": f"lever__{self.board_token}__{job.get('id')}",
            "board_token": self.board_token,
            "company": utils.get_company_name(self.board_token),
            "title": job.get('text'),
            "description": description,
            "additionalPlain": job.get('additionalPlain', ''),  # often has compensation
            "location": categories.get('location'),
            "url": job.get('hostedUrl'),
            "applyUrl": job.get('applyUrl'),
            "country": job.get('country', ''),

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

    def _fetch_html(self, force=False):
        if not hasattr(self, '_cached_html') or force:
            headers = self.session.headers.copy()
            headers['Accept'] = None
            response = self.session.get(
                f"https://jobs.lever.co/{self.board_token}",
                timeout=5,
                headers=headers)
            self._cached_html = response.text
        return self._cached_html

    def get_company_name(self):
        if not hasattr(self, '_cached_company_name'):
            raw_html = self._fetch_html()
            soup = BeautifulSoup(raw_html, 'html.parser')
            self._cached_company_name = soup.find('title').text.strip()
        return self._cached_company_name

    def get_company_domain(self):
        if not hasattr(self, '_cached_company_domain'):
            raw_html = self._fetch_html()
            soup = BeautifulSoup(raw_html, 'html.parser')
            main_footer_text = soup.find("div", class_="main-footer-text")
            if main_footer_text:
                company_domain_link = main_footer_text.find("a")
                if company_domain_link and company_domain_link.has_attr(
                        'href'):
                    self._cached_company_domain = company_domain_link['href']
                else:
                    self._cached_company_domain = None
            else:
                self._cached_company_domain = None
        return self._cached_company_domain

    def get_company_logo_url(self):
        if not hasattr(self, '_cached_company_logo_url'):
            raw_html = self._fetch_html()
            soup = BeautifulSoup(raw_html, 'html.parser')
            og_image = soup.find('meta', property='og:image')
            if og_image and og_image.get('content'):
                self._cached_company_logo_url = og_image['content'].split('?')[0]
            else:
                self._cached_company_logo_url = None
        return self._cached_company_logo_url
