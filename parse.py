from __future__ import annotations

import time
import requests

from multiprocessing.pool import ThreadPool
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

from logger import init_logger


class DjinniTechnologyStatistics:
    def __init__(self, url_to_scrape: str) -> None:
        self.url_to_scrape = url_to_scrape
        self.main_url = "https://djinni.co"
        self.logger = init_logger(name="DjinniTechnologyStatistics", file_log=True)
        self.session = self.get_session()

    def get_session(self):
        for _ in range(10):
            try:
                session = requests.Session()
                retry = Retry(connect=3, backoff_factor=0.5)
                adapter = HTTPAdapter(max_retries=retry)
                session.mount("https://", adapter)
                self.logger.info("Session successful created")
                return session
            except Exception as e:
                self.logger.warning(f"Session cannot be created ::: {e}")
        exit("failed while create session")

    def get_content_from_page(self, page_url: str, page_num: int = None) -> BeautifulSoup:
        try:
            match page_num:
                case None:
                    response = self.session.get(page_url)
                case _:
                    response = self.session.get(f"{page_url}&page={page_num}")
            self.logger.info(f"Successful get content from {page_url}&page={page_num}")
            return BeautifulSoup(response.content, "html.parser")
        except Exception as e:
            self.logger.warning(e)

    def get_count_of_pages(self, page_soup: BeautifulSoup) -> int:
        try:
            count_of_pages = int(page_soup.select(".page-link")[-2].text)
            self.logger.info(f"Count of pages: {count_of_pages}")
            return count_of_pages
        except Exception as e:
            self.logger.warning(f"Can not get count of pages ::: {e}")

    def get_all_jobs_on_one_page(self, page_soup: BeautifulSoup) -> list:
        try:
            return [
                self.main_url + job_url.select_one(".profile").attrs["href"]
                for job_url in page_soup.select(".list-jobs__item")
            ]
        except Exception as e:
            self.logger.warning(f"Can not get all jobs on one page ::: {e}")

    def get_all_jobs(self, page_url: str) -> list:
        content_from_first_page = self.get_content_from_page(page_url, 1)
        count_of_pages = self.get_count_of_pages(content_from_first_page)
        all_url = [*self.get_all_jobs_on_one_page(content_from_first_page)]
        args = [(page_url, num_page) for num_page in range(2, count_of_pages + 1)]

        with ThreadPool(count_of_pages) as pool:
            a = pool.starmap(self.get_content_from_page, args)

        for content_from_page in a:
            all_url.extend(self.get_all_jobs_on_one_page(content_from_page))
        return all_url

    def get_requirements_for_job(self, page_soup: BeautifulSoup) -> list:
        try:
            job_requirements = page_soup.select("ul:nth-child(1) > li > div > span")
            return [requirement.text for requirement in job_requirements]
        except Exception as e:
            self.logger.warning(f"Can not get requirements for job ::: {e}")

    def get_requirements_for_all_jobs(self):
        all_jobs_url = self.get_all_jobs(self.url_to_scrape)
        requirements_for_all_jobs = []
        with ThreadPool(len(all_jobs_url)) as pool:
            content_from_all_jobs = pool.map(self.get_content_from_page, all_jobs_url)

        for job_content in content_from_all_jobs:
            requirements_for_all_jobs.extend(self.get_requirements_for_job(job_content))
        return requirements_for_all_jobs


if __name__ == "__main__":
    start_time = time.perf_counter()

    djinni = DjinniTechnologyStatistics("https://djinni.co/jobs/?primary_keyword=Python")
    djinni.get_requirements_for_all_jobs()
    end_time = time.perf_counter()
    print("Elapsed:", round(end_time - start_time, 5))
