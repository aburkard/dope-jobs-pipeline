import html2text
import json
import re
import time
import concurrent.futures
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from requests import RequestException

from .base_scraper import BaseScraper
import utils


# TODO: This whole class
class JobviteScraper(BaseScraper):
    ats_name = 'jobvite'
    request_timeout_seconds = 5
    request_attempts = 4
    request_backoff_seconds = 1.0
    detail_fetch_workers = 8

    def __init__(self, board_token):
        super().__init__(board_token)
        self.ats_name = JobviteScraper.ats_name
        self.base_url = 'https://jobs.jobvite.com'

        h = html2text.HTML2Text()
        h.body_width = 0
        h.ignore_links = True
        h.ignore_images = True
        h.ignore_emphasis = True
        self.html2text = h

    def check_exists(self):
        return self._request('head', f'{self.base_url}/{self.board_token}').status_code == 200

    def _board_search_url(self):
        return f'{self.base_url}/{self.board_token}/search'

    def _get_board_soup(self):
        if not hasattr(self, '_cached_board_soup'):
            response = self._request('get', self._board_search_url())
            self._cached_board_soup = BeautifulSoup(response.text, 'html.parser')
        return self._cached_board_soup

    def _request(self, method, url, **kwargs):
        timeout = kwargs.pop('timeout', self.request_timeout_seconds)
        last_error = None
        for attempt in range(1, self.request_attempts + 1):
            try:
                return getattr(self.session, method)(url, timeout=timeout, **kwargs)
            except RequestException as exc:
                last_error = exc
                if attempt == self.request_attempts:
                    raise
                self.reset_session()
                time.sleep(self.request_backoff_seconds * attempt)
        raise last_error

    def _absolute_url(self, value):
        if not value:
            return None
        value = value.strip()
        if not value:
            return None
        if value.startswith('//'):
            return f'https:{value}'
        if value.startswith('/'):
            return f'{self.base_url}{value}'
        return value

    def get_company_name(self):
        if not hasattr(self, '_cached_company_name'):
            soup = self._get_board_soup()
            title_el = soup.select_one('title')
            company_name = title_el.text.strip() if title_el else None
            if company_name and company_name.endswith(' Careers'):
                company_name = company_name[:-8]
            self._cached_company_name = company_name
        return self._cached_company_name

    def get_company_domain(self):
        if not hasattr(self, '_cached_company_domain'):
            soup = self._get_board_soup()
            company_name = (self.get_company_name() or '').strip().lower()
            domain = None
            for link in soup.select('a[href]'):
                href = self._absolute_url(link.get('href'))
                if not href:
                    continue
                parsed = urlparse(href)
                host = (parsed.netloc or '').lower()
                if not host or 'jobvite.com' in host:
                    continue
                text = ' '.join(link.get_text(' ', strip=True).split()).lower()
                if company_name and company_name in text:
                    domain = href
                    break
            self._cached_company_domain = domain
        return self._cached_company_domain

    def get_company_logo_url(self):
        if not hasattr(self, '_cached_company_logo_url'):
            soup = self._get_board_soup()
            logo_url = None
            for selector in ('link[rel=\"icon\"]', 'link[rel=\"shortcut icon\"]', 'link[rel=\"apple-touch-icon\"]'):
                el = soup.select_one(selector)
                href = self._absolute_url(el.get('href')) if el and el.has_attr('href') else None
                if href:
                    logo_url = href
                    break
            self._cached_company_logo_url = logo_url
        return self._cached_company_logo_url

    def fetch_job_board(self):
        raise NotImplementedError

    @staticmethod
    def _existing_detail_complete(existing_detail):
        return bool(
            existing_detail
            and existing_detail.get("description")
            and existing_detail.get("descriptionHtml")
            and existing_detail.get("datePosted")
        )

    @staticmethod
    def _existing_detail_inactive(existing_detail):
        return bool(existing_detail and existing_detail.get("inactive"))

    def _fetch_jobs(self, page=0, content=True, existing_details=None,
                    refetch_existing_detail=False):
        url = self._board_search_url()
        response = self._request('get', url, params={'p': page})
        soup = BeautifulSoup(response.text, 'html.parser')
        # Job listings have the css class table.jv-job-list tr, ul.jv-job-list li
        job_listings = soup.select(
            'table.jv-job-list tr, ul.jv-job-list li, div.jv-job-list li')
        company_name = self.get_company_name()
        parsed_jobs = []
        for job_listing in job_listings:
            title_node = job_listing.select_one(
                'td.jv-job-list-name, span.jv-job-list-name, div.jv-job-list-name'
            )
            location_node = job_listing.select_one(
                'td.jv-job-list-location, span.jv-job-list-location, div.jv-job-list-location'
            )
            link_node = job_listing.select_one('td.jv-job-list-name a, a')

            # Some Jobvite tables include header or spacer rows that match the broad selector.
            if title_node is None or location_node is None or link_node is None:
                continue

            job = {}
            job['board_token'] = self.board_token
            job['title'] = utils.squish(title_node.text)
            job['location'] = utils.squish(location_node.text)

            href = utils.squish(link_node.get('href', ''))
            if not href:
                continue
            job['url'] = f'{self.base_url}{href}'
            job['id'] = href.split('/')[-1]
            job['company_name'] = company_name

            parsed_jobs.append(job)

        if not content:
            for job in parsed_jobs:
                yield job
            return

        hydrated_jobs = []
        jobs_needing_detail = []
        for job in parsed_jobs:
            existing_detail = existing_details.get(job['id']) if existing_details else None
            if self._existing_detail_inactive(existing_detail):
                continue

            if self._existing_detail_complete(existing_detail):
                hydrated_jobs.append({
                    **job,
                    "description": existing_detail["description"],
                    "descriptionHtml": existing_detail["descriptionHtml"],
                    "datePosted": existing_detail.get("datePosted"),
                    "validThrough": existing_detail.get("validThrough"),
                    "inactive": bool(existing_detail.get("inactive")),
                })
                continue

            hydrated_jobs.append(job)
            jobs_needing_detail.append((len(hydrated_jobs) - 1, job['id']))

        if jobs_needing_detail:
            if len(jobs_needing_detail) == 1:
                index, short_id = jobs_needing_detail[0]
                job_data = self._fetch_job_with_fresh_scraper(short_id)
                hydrated_jobs[index] = None if job_data.get("inactive") else {**hydrated_jobs[index], **job_data}
            else:
                max_workers = min(self.detail_fetch_workers, len(jobs_needing_detail))
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_map = {
                        executor.submit(self._fetch_job_with_fresh_scraper, short_id): (index, short_id)
                        for index, short_id in jobs_needing_detail
                    }
                    for future in concurrent.futures.as_completed(future_map):
                        index, _short_id = future_map[future]
                        job_data = future.result()
                        hydrated_jobs[index] = None if job_data.get("inactive") else {**hydrated_jobs[index], **job_data}

        for job in hydrated_jobs:
            if job is not None:
                yield job

    def _fetch_job_with_fresh_scraper(self, job_id):
        detail_scraper = self.__class__(self.board_token)
        try:
            return detail_scraper.fetch_job(job_id)
        finally:
            detail_scraper.close_session()

    def fetch_jobs(self, normalize=True, content=True, existing_details=None,
                   refetch_existing_detail=False):
        # Keep going until we get an empty page
        for page in range(100):
            found_jobs = False
            for job in self._fetch_jobs(
                    page=page,
                    content=content,
                    existing_details=existing_details,
                    refetch_existing_detail=refetch_existing_detail,
                ):
                if normalize:
                    job = self.normalize_job(job)
                    job = self.add_default_fields(job)
                yield job
                found_jobs = True
            if not found_jobs:
                break

    def fetch_job(self, job_id):
        url = f"{self.base_url}/{self.board_token}/job/{job_id}"
        response = self._request('get', url)
        soup = BeautifulSoup(response.text, 'html.parser')
        job = {}
        job['id'] = job_id
        description_html = soup.select_one('div.jv-job-detail-description')
        metadata = self.extract_job_metadata(soup)
        description_html_string = str(description_html or "").strip()
        json_ld_description = metadata.get("descriptionHtml")
        if not description_html_string and json_ld_description:
            description_html_string = json_ld_description
        job['descriptionHtml'] = description_html_string
        job['description'] = self.html2text.handle(description_html_string).strip()
        if metadata.get("datePosted"):
            job["datePosted"] = metadata["datePosted"]
        if metadata.get("validThrough"):
            job["validThrough"] = metadata["validThrough"]
        job["inactive"] = self.is_inactive_job(soup, metadata)
        return job

    def normalize_job(self, job):
        company_name = job.get('company_name') or utils.get_company_name(
            self.board_token)
        return {
            "id": f"{self.ats_name}__{self.board_token}__{job.get('id')}",
            "board_token": self.board_token,
            "company": company_name,
            "title": job['title'],
            "description": self.clean_description(job),
            "descriptionHtml": job.get("descriptionHtml", ""),
            "location": job['location'],
            "url": job['url'],
            "updated_at": None,
            "datePosted": job.get("datePosted"),
            "validThrough": job.get("validThrough"),
            "inactive": bool(job.get("inactive")),
        }

    def is_inactive_job(self, soup, metadata):
        industry = metadata.get("industry")
        if isinstance(industry, str) and industry.strip().lower() == "inactive":
            return True

        meta_block = soup.select_one("p.jv-job-detail-meta")
        if meta_block:
            meta_text = " ".join(meta_block.stripped_strings).strip().lower()
            if meta_text.startswith("inactive") or " inactive " in f" {meta_text} ":
                return True

        for script in soup.select("script"):
            text = script.string or script.get_text() or ""
            if not text:
                continue
            if re.search(r"jobCategoryName\s*:\s*['\"]Inactive['\"]", text):
                return True

        return False

    def clean_description(self, job):
        # TODO: Do I need to do more here?
        return job.get('description', '')

    def extract_job_metadata(self, soup):
        metadata = {}
        for script in soup.select('script[type="application/ld+json"]'):
            text = script.string or script.get_text() or ""
            text = text.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            for item in self._iter_structured_items(payload):
                if item.get("@type") != "JobPosting":
                    continue
                if item.get("datePosted"):
                    metadata["datePosted"] = item["datePosted"]
                if item.get("validThrough"):
                    metadata["validThrough"] = item["validThrough"]
                if item.get("industry"):
                    metadata["industry"] = item["industry"]
                description_html = item.get("description")
                if isinstance(description_html, str) and description_html.strip():
                    metadata["descriptionHtml"] = description_html.strip()
                return metadata
        return metadata

    def _iter_structured_items(self, payload):
        if isinstance(payload, list):
            for item in payload:
                yield from self._iter_structured_items(item)
            return
        if isinstance(payload, dict):
            yield payload
            graph_items = payload.get("@graph")
            if isinstance(graph_items, list):
                for item in graph_items:
                    yield from self._iter_structured_items(item)
