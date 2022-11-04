import time
import requests

from multiprocessing.pool import ThreadPool
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup


MAIN_PAGE = "https://djinni.co"


session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)


def get_content_from_page(page_url: str, page_num: int = None) -> BeautifulSoup:
    match page_num:
        case None:
            response = session.get(page_url)
        case _:
            response = session.get(f"{page_url}&page={page_num}")

    return BeautifulSoup(response.content, "html.parser")


def get_count_of_pages(page_soup: BeautifulSoup) -> int:
    return int(page_soup.select(".page-link")[-2].text)


def get_all_jobs_on_one_page(page_soup: BeautifulSoup) -> list:
    return [
        MAIN_PAGE + job_url.select_one(".profile").attrs["href"]
        for job_url in page_soup.select(".list-jobs__item")
    ]


def get_all_jobs(page_url: str) -> list:
    content_from_first_page = get_content_from_page(page_url, 1)
    count_of_pages = get_count_of_pages(content_from_first_page)
    all_url = [*get_all_jobs_on_one_page(content_from_first_page)]
    args = [(page_url, num_page) for num_page in range(2, count_of_pages + 1)]

    with ThreadPool(count_of_pages) as pool:
        a = pool.starmap(get_content_from_page, args)

    for content_from_page in a:
        all_url.extend(get_all_jobs_on_one_page(content_from_page))
    return all_url


def get_requirements_for_job(page_soup: BeautifulSoup) -> list:
    job_requirements = page_soup.select("ul:nth-child(1) > li > div > span")
    return [requirement.text for requirement in job_requirements]


def get_requirements_for_all_jobs(page_url: str):
    all_jobs_url = get_all_jobs(page_url)
    result = []

    with ThreadPool(len(all_jobs_url)) as pool:
        content_from_all_jobs = pool.map(get_content_from_page, all_jobs_url)

    for job_content in content_from_all_jobs:
        result.extend(get_requirements_for_job(job_content))
    return result


if __name__ == "__main__":
    start_time = time.perf_counter()

    get_requirements_for_all_jobs("https://djinni.co/jobs/?primary_keyword=Python")

    end_time = time.perf_counter()
    print("Elapsed:", round(end_time - start_time, 5))
