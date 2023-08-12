import logging

import selenium
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from common.logging import cls_name
from parsing.converter import ToTelegramMarkdown
from parsing.exceptions import JobArchived, LoginRequired, NotFound, NotSupported
from parsing.interface import Parser

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class HeadHunterParser(Parser):
    XPATH_RESPOND_BUTTON = "//*[@data-qa='vacancy-response-link-top']"
    XPATH_NAME = "//*[@data-qa='vacancy-title']"  # Unity Developer
    XPATH_COMPANY = "//*[@data-qa='vacancy-company-name']"  # Octo Games
    S_TAGS = ".bloko-tag-list"  # Информационные технологии • Разработка • C# • Gamedev
    S_FORMAT = ".vacancy-description-list-item"  # Удаленная работа Опыт работы от 1 года до 3х лет
    S_SALARY = ".vacancy-title .bloko-header-section-2_lite"  # от 500 до 1 200 $
    XPATH_DESCRIPTION = "//*[@data-qa='vacancy-description']"  # everything else
    S_ARCHIVED = ".bloko-header-2"
    S_LOGIN = ".bloko-header-section-2"
    S_VERSION_WITH_PHOTO = ".vacancy-photo-top__shadow"
    S_404 = ".bloko-header-section-1"

    @staticmethod
    def get_domains():
        return ["hh"]

    @staticmethod
    def get_name():
        return "HeadHunter"

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

        points = []
        if job_info["format"] is not None and len(job_info["format"]) != 0:
            for line in job_info['format']:
                points.append(line.strip() + "\n")

        if job_info["tags"] is not None and len(job_info["tags"].strip()) != 0:
            points += [
                          p.strip() + "\n"
                          for tag in job_info['tags'].split("\n")
                          for p in tag.split("•")
                      ][:3]

        message += "• " + "• ".join(points) + "\n"

        if job_info["description"] is not None and len(job_info["description"].strip()) != 0:
            html = job_info['description']
            message += ToTelegramMarkdown(
                bullets="•"
            ).convert(html)
        else:
            raise ValueError(f"Job description is empty, link: {job_info['url']}")

        if add_info_link:
            message += f"\n\n**[Подробнее]({job_info['url']})**"
        return message

    def check_correct_url(self, url):
        return True

    def parse(self, driver, url):
        try:
            WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, self.XPATH_RESPOND_BUTTON)))
        except selenium.common.exceptions.TimeoutException:
            try:
                text = driver.find_element(By.CSS_SELECTOR, self.S_ARCHIVED).text
                if text.lower().strip() == "вакансия в архиве":
                    raise JobArchived(url, self)
            except selenium.common.exceptions.NoSuchElementException:
                pass

            try:
                text = driver.find_element(By.CSS_SELECTOR, self.S_LOGIN).text
                if text.lower().strip() == "войдите на сайт":
                    raise LoginRequired(url, self)
            except selenium.common.exceptions.NoSuchElementException:
                pass

            try:
                text = driver.find_element(By.CSS_SELECTOR, self.S_404).text
                if text.lower().strip() == "такой страницы нет":
                    raise NotFound(url, self)
            except selenium.common.exceptions.NoSuchElementException:
                pass

            log.warning(
                f"{cls_name(self)}: "
                f"Can't process link, "
                f"link:{url} "
                f"err: Can't find respond button"
            )
            return None

        info = {}

        try:
            driver.find_element(By.CSS_SELECTOR, self.S_VERSION_WITH_PHOTO)
            raise NotSupported(url, self)
        except selenium.common.exceptions.NoSuchElementException:
            pass

        try:
            info["name"] = driver.find_element(By.XPATH, self.XPATH_NAME).text
        except selenium.common.exceptions.NoSuchElementException:
            log.warning(
                f"{cls_name(self)}: "
                f"Can't process link, "
                f"link:{url} "
                f"err: Can't find vacancy name"
            )
            return None

        try:
            elem = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, self.XPATH_COMPANY)))
            info["company"] = elem.text
        except selenium.common.exceptions.NoSuchElementException:
            log.warning(
                f"{cls_name(self)}: "
                f"Can't process link, "
                f"link:{url} "
                f"err: Can't find company name"
            )
            return None

        try:
            info["tags"] = driver.find_element(By.CSS_SELECTOR, self.S_TAGS).text
        except selenium.common.exceptions.NoSuchElementException:
            log.debug(
                f"{cls_name(self)}: "
                f"Can't find tags "
                f"link:{url}"
            )
            info["tags"] = None

        try:
            elems = driver.find_elements(By.CSS_SELECTOR, self.S_FORMAT)
            for el in elems:
                info.setdefault("format", []).append(el.text)
            del elems
            del el
        except selenium.common.exceptions.NoSuchElementException:
            log.debug(
                f"{cls_name(self)}: "
                f"Can't find format "
                f"link:{url}"
            )
            info["format"] = []

        try:
            info["salary"] = driver.find_element(By.CSS_SELECTOR, self.S_SALARY).text
        except selenium.common.exceptions.NoSuchElementException:
            log.debug(
                f"{cls_name(self)}: "
                f"Can't find salary "
                f"link:{url}"
            )
            info["salary"] = None

        try:
            elem = driver.find_element(By.XPATH, self.XPATH_DESCRIPTION)
            info["description"] = elem.get_attribute('innerHTML')
            del elem

            search_terms = ["похожие вакансии"]
            description = info["description"].lower()

            if any(term in description for term in search_terms):
                log.warning(
                    f"Something went wrong, description includes wrong info"
                    f"link:{url}"
                )
                return None
        except selenium.common.exceptions.NoSuchElementException:
            log.warning(
                f"Can't process link, "
                f"link:{url} "
                f"err: Can't find description"
            )
            return None

        info["url"] = url
        return info
