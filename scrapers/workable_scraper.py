from __future__ import annotations

from bs4 import BeautifulSoup

from .base_scraper import BaseScraper
import utils


class WorkableScraper(BaseScraper):
    ats_name = "workable"

    def __init__(self, board_token):
        super().__init__(board_token)
        self.ats_name = WorkableScraper.ats_name
        self.base_url = "https://apply.workable.com"

    def check_exists(self):
        try:
            board = self._fetch_widget_board()
            return isinstance(board, dict) and bool(board.get("name")) and "jobs" in board
        except Exception:
            return False

    def fetch_job_board(self, force=False):
        if not hasattr(self, "_cached_job_board") or force:
            widget_data = self._fetch_widget_board()
            self._cached_job_board = {
                "name": widget_data.get("name"),
                "description": widget_data.get("description"),
                "subdomain": self.board_token,
                "account": widget_data.get("account"),
            }
        return self._cached_job_board

    def _fetch_widget_board(self):
        if not hasattr(self, "_cached_widget_board"):
            url = f"{self.base_url}/api/v1/widget/accounts/{self.board_token}?details=true"
            response = self.session.get(url, timeout=20)
            self._cached_widget_board = response.json()
        return self._cached_widget_board

    def _fetch_widget_jobs_by_shortcode(self):
        if not hasattr(self, "_cached_widget_jobs_by_shortcode"):
            jobs = self._fetch_widget_board().get("jobs", []) or []
            self._cached_widget_jobs_by_shortcode = {
                job.get("shortcode"): job
                for job in jobs
                if job.get("shortcode")
            }
        return self._cached_widget_jobs_by_shortcode

    def fetch_jobs(self, normalize=True):
        widget_jobs_by_shortcode = self._fetch_widget_jobs_by_shortcode()
        for shortcode, merged in widget_jobs_by_shortcode.items():
            if normalize:
                merged = self.normalize_job(merged)
            merged = self.add_default_fields(merged)
            yield merged

    def fetch_job(self, job_id):
        return self._fetch_widget_jobs_by_shortcode().get(job_id)

    def _format_location(self, job):
        locations = job.get("locations") or []
        if locations:
            primary = locations[0] or {}
            parts = [primary.get("city"), primary.get("region"), primary.get("country")]
            return ", ".join(part for part in parts if part)

        city = job.get("city")
        state = job.get("state")
        country = job.get("country")
        parts = [city, state, country]
        return ", ".join(part for part in parts if part)

    def normalize_job(self, job):
        shortcode = job.get("shortcode") or job.get("id")
        departments = job.get("department")
        if isinstance(departments, str):
            departments = [departments] if departments else []
        elif not isinstance(departments, list):
            departments = []

        description_html = job.get("description", "") or ""
        return {
            "id": f"{self.ats_name}__{self.board_token}__{shortcode}",
            "board_token": self.board_token,
            "company": self.get_company_name() or utils.get_company_name(self.board_token),
            "title": job.get("title", ""),
            "description": self.clean_description(job),
            "descriptionHtml": description_html,
            "location": self._format_location(job),
            "url": job.get("url") or f"{self.base_url}/j/{shortcode}",
            "applyUrl": job.get("application_url") or f"{self.base_url}/j/{shortcode}/apply",
            "shortcode": shortcode,
            "workplaceType": "remote" if job.get("telecommuting") else None,
            "isRemote": job.get("telecommuting"),
            "employmentType": job.get("employment_type"),
            "workplace": None,
            "remote": job.get("telecommuting"),
            "telecommuting": job.get("telecommuting"),
            "employment_type": job.get("employment_type"),
            "type": None,
            "department": departments[-1] if departments else "",
            "departments": departments,
            "locations": job.get("locations") or [],
            "createdAt": job.get("created_at"),
            "datePosted": job.get("published_on"),
            "publishedAt": job.get("published_on"),
            "created_at": job.get("created_at"),
            "published_on": job.get("published_on"),
            "published": job.get("published_on"),
            "education": job.get("education"),
            "experience": job.get("experience"),
            "function": job.get("function"),
            "industry": job.get("industry"),
            "requirements": "",
            "benefits": "",
        }

    def clean_description(self, job):
        return BeautifulSoup(job.get("description", "") or "", "lxml").get_text("\n").strip()

    def get_company_name(self):
        if not hasattr(self, "_cached_company_name"):
            self._cached_company_name = self.fetch_job_board().get("name")
        return self._cached_company_name

    def get_company_domain(self):
        return None

    def get_company_description(self):
        board = self.fetch_job_board()
        description_html = board.get("description") or ""
        if not description_html:
            return None
        description = BeautifulSoup(description_html, "lxml").get_text("\n").strip()
        return description or None

    def get_company_logo_url(self):
        return None
