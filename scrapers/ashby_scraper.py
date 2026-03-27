import datetime
import html2text

from .base_scraper import BaseScraper
import utils


class AshbyScraper(BaseScraper):

    ats_name = 'ashby'

    def __init__(self, board_token):
        super().__init__(board_token)
        self.ats_name = 'ashby'
        self.api_url = f'https://api.ashbyhq.com/posting-api/job-board/{board_token}'
        # Keep GraphQL for org-level data (logo, domain)
        self.graphql_url = 'https://jobs.ashbyhq.com/api/non-user-graphql'

        h = html2text.HTML2Text()
        h.body_width = 0
        h.ignore_links = True
        h.ignore_images = True
        h.ignore_emphasis = True
        self.html2text = h

    def check_exists(self):
        r = self.session.get(self.api_url, timeout=10)
        if r.ok:
            data = r.json()
            return len(data.get('jobs', [])) > 0
        return False

    def fetch_job_board(self, force=False):
        """Fetch org-level data (name, logo, domain) via GraphQL."""
        data = {
            "operationName": "ApiOrganizationFromHostedJobsPageName",
            "variables": {"organizationHostedJobsPageName": self.board_token},
            "query": """query ApiOrganizationFromHostedJobsPageName($organizationHostedJobsPageName: String!) {
                organization: organizationFromHostedJobsPageName(
                    organizationHostedJobsPageName: $organizationHostedJobsPageName
                ) {
                    name
                    publicWebsite
                    theme {
                        logoSquareImageUrl
                    }
                }
            }"""
        }
        if not hasattr(self, '_cached_job_board') or force:
            response = self.session.post(self.graphql_url, json=data, timeout=5)
            self._cached_job_board = response.json()
        return self._cached_job_board

    def fetch_jobs(self, normalize=True):
        """Fetch all jobs via the REST API (single request, includes descriptions)."""
        r = self.session.get(
            self.api_url,
            params={'includeCompensation': 'true'},
            timeout=30,
        )
        if not r.ok:
            return

        data = r.json()
        for job in data.get('jobs', []):
            if normalize:
                job = self.normalize_job(job)
            job = self.add_default_fields(job)
            yield job

    def fetch_job(self, job_id):
        """Fetch a single job — uses REST API and filters."""
        r = self.session.get(
            self.api_url,
            params={'includeCompensation': 'true'},
            timeout=30,
        )
        if not r.ok:
            return None
        for job in r.json().get('jobs', []):
            if job.get('id') == job_id:
                return self.normalize_job(job)
        return None

    def normalize_job(self, job):
        """Normalize Ashby REST API response to standard format.
        Preserves structured fields that save LLM extraction."""
        # Location handling
        location = job.get('location', '')
        address = (job.get('address') or {}).get('postalAddress', {}) or {}
        secondary = job.get('secondaryLocations') or []

        # Compensation
        compensation = job.get('compensation') or {}
        comp_summary = compensation.get('compensationTierSummary', '')
        comp_salary = compensation.get('scrapeableCompensationSalarySummary', '')
        comp_tiers = compensation.get('compensationTiers') or []

        return {
            "id": f"{self.ats_name}__{self.board_token}__{job.get('id', '')}",
            "board_token": self.board_token,
            "company": utils.get_company_name(self.board_token),
            "title": job.get('title', ''),
            "descriptionHtml": job.get('descriptionHtml', ''),
            "descriptionPlain": job.get('descriptionPlain', ''),
            "description": job.get('descriptionPlain', ''),
            "url": job.get('jobUrl', f"https://jobs.ashbyhq.com/{self.board_token}/{job.get('id', '')}"),
            "applyUrl": job.get('applyUrl', ''),

            # Structured location data
            "location": location,
            "locationName": location,
            "locationCity": address.get('addressLocality', ''),
            "locationRegion": address.get('addressRegion', ''),
            "locationCountry": address.get('addressCountry', ''),
            "secondaryLocations": [
                {
                    "location": sl.get('location', ''),
                    "city": ((sl.get('address') or {}).get('postalAddress') or {}).get('addressLocality', ''),
                    "region": ((sl.get('address') or {}).get('postalAddress') or {}).get('addressRegion', ''),
                    "country": ((sl.get('address') or {}).get('postalAddress') or {}).get('addressCountry', ''),
                }
                for sl in secondary
            ],

            # Structured employment data (saves LLM extraction)
            "employmentType": job.get('employmentType', ''),  # FullTime, PartTime, Intern, Contract, Temporary
            "workplaceType": job.get('workplaceType', ''),    # OnSite, Remote, Hybrid
            "isRemote": job.get('isRemote', False),
            "department": job.get('department', ''),
            "team": job.get('team', ''),
            "publishedAt": job.get('publishedAt', ''),
            "isListed": job.get('isListed', True),

            # Structured compensation data (saves LLM extraction)
            "compensationTierSummary": comp_summary,
            "compensationSalarySummary": comp_salary,
            "compensationTiers": comp_tiers,
        }

    def clean_description(self, text):
        return self.html2text.handle(text).strip()

    def get_company_name(self):
        try:
            data = self.fetch_job_board()
            return data['data']['organization']['name']
        except (KeyError, TypeError):
            return None

    def get_company_domain(self):
        try:
            data = self.fetch_job_board()
            return data['data']['organization']['publicWebsite']
        except (KeyError, TypeError):
            return None

    def get_company_logo_url(self):
        try:
            data = self.fetch_job_board()
            return data['data']['organization']['theme']['logoSquareImageUrl']
        except (KeyError, TypeError):
            return None
