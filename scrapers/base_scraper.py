import bz2
import datetime
import hashlib
import json
import os
import requests

from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm
import utils

StealthUserAgent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'


class BaseScraper:

    def __init__(self, board_token):
        self.board_token = board_token

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'User-Agent': StealthUserAgent
        }
        self.session = requests.Session()
        self.session.headers.update(headers)
        retries = Retry(total=3,
                        backoff_factor=0.5,
                        status_forcelist=[502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    # Returns True if a job board exists for the given board_token
    def check_exists(self):
        raise NotImplementedError

    # Get metadata about the job board
    def fetch_job_board(self, **kwargs):
        raise NotImplementedError

    # Get a list of all job posting on the job board
    def fetch_jobs(self, normalize=True, **kwargs):
        raise NotImplementedError

    # Get data for a specific job posting
    def fetch_job(self, job_id, **kwargs):
        raise NotImplementedError

    # TODO: Call super in subclasses and add stuff like hash, first seen at, etc
    # Clean text and map column names to a standard format
    def normalize_job(self, job, **kwargs):
        raise NotImplementedError

    # TODO: This could be better. Probably just a static dict would work
    def clean_company_name(self, **kwargs):
        # Replace dashes and underscores with spaces and title case
        return self.board_token.replace('-', ' ').replace('_', ' ').title()

    # Should typically be overwritten by the subclass
    def clean_description(self, job):
        raise NotImplementedError

    #  This should be unique for each job posting
    def job_id(self, job):
        return f"{self.ats_name}__{self.board_token}__{job.get('id')}"

    # Computes a sha256 hash from the job_id
    def hash_id(self, job):
        return hashlib.sha1(self.job_id(job).encode('utf-8')).hexdigest()

    def get_company_name(self):
        raise NotImplementedError

    def get_company_domain(self):
        raise NotImplementedError

    def get_company_logo_url(self):
        raise NotImplementedError

    def add_default_fields(self, job):
        job['ats_name'] = self.ats_name
        job['board_token'] = self.board_token
        job['hash_id'] = self.hash_id(job)
        job['seen_in_last_fetch'] = True
        job['last_fetched_at'] = datetime.datetime.now(
            datetime.timezone.utc).isoformat()
        return job

    def text_for_processing(self, job):
        text = "\n".join(
            utils.slice(job, ['company', 'title', 'location', 'description'
                              ]).values()).strip()
        return text

    @classmethod
    def write_all_jobs(cls,
                       board_tokens,
                       ats_name=None,
                       path="data/raw",
                       file_name=None,
                       normalize=False,
                       **kwargs):

        ats_name = ats_name or cls.ats_name
        file_name = file_name or f'{ats_name}.jsonl.bz2'
        if path:
            file_path = os.path.join(path, file_name)
        else:
            file_path = file_name

        failed_tokens = []
        with bz2.open(file_path, 'wt') as f:
            for board_token in tqdm(board_tokens):
                try:
                    scraper = cls(board_token, **kwargs)
                    for job in scraper.fetch_jobs(normalize=normalize):
                        f.write(json.dumps(job))
                        f.write('\n')
                except Exception as e:  # TODO: Use logger
                    failed_tokens.append(board_token)
                    print(f'Error in {ats_name} - {board_token}: {e}')
        return failed_tokens
