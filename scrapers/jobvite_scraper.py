import html2text
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper
import utils


# TODO: This whole class
class JobviteScraper(BaseScraper):
    ats_name = 'jobvite'

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
        return self.session.head(
            f'{self.base_url}/{self.board_token}').status_code == 200

    def fetch_job_board(self):
        raise NotImplementedError

    def _fetch_jobs(self, page=0, content=True, existing_descriptions=None,
                    refetch_existing_detail=False):
        url = f'{self.base_url}/{self.board_token}/search'
        response = self.session.get(url, params={'p': page}, timeout=5)
        soup = BeautifulSoup(response.text, 'html.parser')
        # Job listings have the css class table.jv-job-list tr, ul.jv-job-list li
        job_listings = soup.select(
            'table.jv-job-list tr, ul.jv-job-list li, div.jv-job-list li')
        company_name = None
        title_el = soup.select_one('title')
        if title_el:
            company_name = title_el.text
            if company_name.endswith(' Careers'):
                company_name = company_name[:-8]
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

            if content:
                existing_description = None
                if existing_descriptions:
                    existing_description = existing_descriptions.get(job['id'])
                if existing_description and not refetch_existing_detail:
                    job['description'] = existing_description
                else:
                    job_data = self.fetch_job(job['id'])
                    job = {**job, **job_data}

            yield job

    def fetch_jobs(self, normalize=True, content=True, existing_descriptions=None,
                   refetch_existing_detail=False):
        # Keep going until we get an empty page
        for page in range(100):
            found_jobs = False
            for job in self._fetch_jobs(
                page=page,
                content=content,
                existing_descriptions=existing_descriptions,
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
        response = self.session.get(url, timeout=5)
        soup = BeautifulSoup(response.text, 'html.parser')
        job = {}
        job['id'] = job_id
        description_html = soup.select_one('div.jv-job-detail-description')
        job['description'] = self.html2text.handle(
            str(description_html or "")).strip()
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
            "location": job['location'],
            "url": job['url'],
            "updated_at": None,
        }

    def clean_description(self, job):
        # TODO: Do I need to do more here?
        return job.get('description', '')
