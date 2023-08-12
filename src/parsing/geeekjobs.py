import logging
from urllib.parse import urlparse, parse_qs, unquote

import selenium
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from parsing.converter import ToTelegramMarkdown
from parsing.exceptions import JobArchived
from parsing.interface import Parser

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class GeekJobsParser(Parser):
    S_RESPOND_BUTTON = ".respondbtn"
    S_NAME = ".vacancy h1"  # Unity Developer
    S_COMPANY = ".main .company-name a"  # Octo Games
    S_LOCATION = ".location"  # Амстердам, Нидерланды
    S_JOB_CATEGORY = ".category"  # Джуниор • Миддл
    S_TAGS = ".tags"  # Информационные технологии • Разработка • C# • Gamedev
    S_FORMAT = ".jobformat"  # Удаленная работа Опыт работы от 1 года до 3х лет
    S_SALARY = ".jobinfo .salary"  # от 500 до 1 200 $
    S_DESCRIPTION = ".description"  # everything else
    S_404 = "h1"
    S_ARCHIVED = ".header"

    @staticmethod
    def get_domains():
        return ["gkjb", "geekjob"]

    @staticmethod
    def get_name():
        return "GeekJobs"

    def handle_link(self, href: str) -> str:
        parsed = urlparse(href)
        params = parse_qs(parsed.query)

        # Handle Geekjob redirect
        actual_link = params.get('u')
        if actual_link:
            href = unquote(actual_link[0])

        return href

    def format_for_telegram(self, job_info, add_info_link=False):
        if job_info is None:
            return ""

        if job_info["name"] is None or len(job_info["name"].strip()) == 0:
            raise ValueError(f"Job name is empty, link: {job_info['url']}")

        if job_info["salary"] is None or len(job_info["salary"].strip()) == 0:
            job_info["salary"] = "З/п договорная"

        if job_info["company"] is None or len(job_info["company"].strip()) == 0:
            raise ValueError(f"Job company is empty, link: {job_info['url']}")

        message = f"**{job_info['name']}** ({job_info['salary']})\n"
        message += f"{job_info['company']}\n\n"

        if job_info["category"] is not None and len(job_info["category"].strip()) != 0:
            message += f"• {job_info['category']}\n"
        if job_info["location"] is not None and len(job_info["location"].strip()) != 0:
            message += f"• {job_info['location']}\n"
        if job_info["format"] is not None and len(job_info["format"].strip()) != 0:
            for line in job_info['format'].split("\n"):
                message += f"• {line}\n"

        if job_info["tags"] is not None and len(job_info["tags"].strip()) != 0:
            new_tags = " • ".join([tag.strip() for tag in job_info['tags'].split("•")][:3])
            message += f"• {new_tags}\n\n"

        if job_info["description"] is not None and len(job_info["description"].strip()) != 0:
            html = job_info['description']
            markdown_description = ToTelegramMarkdown(
                bullets="•",
                ignore=["Описание вакансии"],
                handle_link=self.handle_link,
            ).convert(html)
            # markdown_description = markdown_description.replace("\\*", "")
            message += markdown_description
        else:
            raise ValueError(f"Job description is empty, link: {job_info['url']}")

        if add_info_link:
            message += f"\n**[Подробнее]({job_info['url']})**"
        return message

    def check_correct_url(self, url):
        return True

    def parse(self, driver, url):
        try:
            text = driver.find_element(By.CSS_SELECTOR, self.S_ARCHIVED).text
            if text.strip() == "⚠︎ Архивная вакансия":
                raise JobArchived(url, self)
        except selenium.common.exceptions.NoSuchElementException:
            pass

        try:
            WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, self.S_RESPOND_BUTTON)))
        except selenium.common.exceptions.TimeoutException:
            try:
                text = driver.find_element(By.CSS_SELECTOR, self.S_404).text
                if text.strip() == "404":
                    raise JobArchived(url, self)
            except selenium.common.exceptions.NoSuchElementException:
                pass

            return None

        info = {}

        try:
            info["name"] = driver.find_element(By.CSS_SELECTOR, self.S_NAME).text
        except selenium.common.exceptions.NoSuchElementException:
            log.warning(f"Can't process link:{url} err: Can't find vacancy name")
            return None

        try:
            info["company"] = driver.find_element(By.CSS_SELECTOR, self.S_COMPANY).text
        except selenium.common.exceptions.NoSuchElementException:
            log.warning(f"Can't process link:{url} err: Can't find company name")
            return None

        try:
            info["location"] = driver.find_element(By.CSS_SELECTOR, self.S_LOCATION).text
        except selenium.common.exceptions.NoSuchElementException:
            log.warning(f"Can't process link:{url} err: Can't find location")
            return None

        try:
            info["category"] = driver.find_element(By.CSS_SELECTOR, self.S_JOB_CATEGORY).text
        except selenium.common.exceptions.NoSuchElementException:
            log.warning(f"Can't process link:{url} err: Can't find category")
            return None

        try:
            info["tags"] = driver.find_element(By.CSS_SELECTOR, self.S_TAGS).text
        except selenium.common.exceptions.NoSuchElementException:
            log.warning(f"Can't process link:{url} err: Can't find tags")
            return None

        try:
            info["format"] = driver.find_element(By.CSS_SELECTOR, self.S_FORMAT).text
        except selenium.common.exceptions.NoSuchElementException:
            log.warning(f"Can't process link:{url} err: Can't find format")
            return None

        try:
            info["salary"] = driver.find_element(By.CSS_SELECTOR, self.S_SALARY).text
        except selenium.common.exceptions.NoSuchElementException:
            log.warning(f"Can't process link:{url} err: Can't find salary")
            return None

        try:
            elem = driver.find_element(By.CSS_SELECTOR, self.S_DESCRIPTION)
            html = elem.get_attribute('innerHTML')
            info["description"] = html
        except selenium.common.exceptions.NoSuchElementException:
            log.warning(f"Can't process link:{url} err: Can't find description")
            return None

        info["url"] = url
        return info
