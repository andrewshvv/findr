import logging
from urllib.parse import urlparse

import selenium
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from common.logging import cls_name
from common.markdown import remove_excessive_n
from parsing.converter import ToTelegramMarkdown
from parsing.exceptions import JobArchived
from parsing.interface import Parser

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class HabrParser(Parser):
    S_WAIT_FOR = ".button-comp--size-sm span"
    S_NAME = ".page-title__title"  # Unity Developer
    S_COMPANY = ".company_info .company_name"  # Octo Games
    S_TAGS = ".content-section"  # Информационные технологии • Разработка • C# • Gamedev
    S_DESCRIPTION = ".basic-section--appearance-vacancy-description"  # everything else
    S_404 = ".caption"

    @staticmethod
    def get_domains():
        return ["habr", "habrahabr"]

    @staticmethod
    def get_name():
        return "Habr"

    def format_for_telegram(self, job_info, add_info_link=False):
        if job_info is None:
            return ""

        if job_info["name"] is None or len(job_info["name"].strip()) == 0:
            raise ValueError(f"Job name is empty, link: {job_info['url']}")

        if job_info["company"] is None or len(job_info["company"].strip()) == 0:
            raise ValueError(f"Job company is empty, link: {job_info['url']}")

        message = f"**{job_info['name']}**\n"
        message += f"{job_info['company']}\n\n"

        if job_info["tags"] is not None and len(job_info["tags"]) != 0:
            points = []
            for k, tag in enumerate(job_info["tags"]):
                title = tag.split("\n")[0].strip()
                content = tag.split("\n")[1].strip()
                if len(content) == 0:
                    continue

                if k == 0 and title == "Зарплата":
                    points.append(content.strip())
                elif k == 0 and title != "Зарплата":
                    points.append("З/п договорная")

                if title == "Требования":
                    subtags = [subtag.strip() for subtag in content.split("•")]
                    job_level = subtags[0]
                    skills = " • ".join(subtags[1:])
                    points.append(job_level)
                    points.append(skills)
                elif title == "Местоположение и тип занятости":
                    points.append(content.strip())

            new_tags = "• ".join([p.strip() + "\n" for p in points])
            message += f"• {new_tags}" + "\n"

        if job_info["description"] is not None and len(job_info["description"].strip()) != 0:
            html = job_info['description']
            markdown_description = ToTelegramMarkdown(
                bullets="•",
                ignore=[
                    "Описание вакансии",
                    "СВЕРНУТЬ",
                    "Поделиться:",
                    "Добавить в избранное",
                ]
            ).convert(html)

            message += markdown_description.strip()
        else:
            raise ValueError(f"Job description is empty, link: {job_info['url']}")

        if add_info_link:
            message += f"\n\n**[Подробнее]({job_info['url']})**"
        message = remove_excessive_n(message)
        return message.strip()

    def check_correct_url(self, url):
        parsed = urlparse(url)
        if parsed.query == "type=all":
            return False

        if parsed.path[-2] == "all":
            return False

        if "vacancies" not in parsed.path:
            return False

        return True

    def parse(self, driver, url):
        info = {}

        try:
            WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, self.S_WAIT_FOR)))
        except selenium.common.exceptions.TimeoutException:
            try:
                text = driver.find_element(By.CSS_SELECTOR, self.S_404).text
                if "страница уже нашла работу мечты и закрыла свой профиль" in text.strip():
                    raise JobArchived(url, self)
            except selenium.common.exceptions.NoSuchElementException:
                pass

            log.warning(
                f"{cls_name(self)}: "
                f"Can't process link, "
                f"link:{url} "
                f"err: Can't find wait button"
            )

        try:
            info["name"] = driver.find_element(By.CSS_SELECTOR, self.S_NAME).text
        except selenium.common.exceptions.NoSuchElementException:
            log.warning(
                f"{cls_name(self)}: "
                f"Can't process link, "
                f"link:{url} "
                f"err: Can't find vacancy name"
            )
            return None

        try:
            info["company"] = driver.find_element(By.CSS_SELECTOR, self.S_COMPANY).text
        except selenium.common.exceptions.NoSuchElementException:
            log.warning(
                f"{cls_name(self)}: "
                f"Can't process link, "
                f"link:{url} "
                f"err: Can't find company name"
            )
            return None

        try:
            elements = driver.find_elements(By.CSS_SELECTOR, self.S_TAGS)
            info["tags"] = [
                element.text
                for element in elements
            ]
            del elements
        except selenium.common.exceptions.NoSuchElementException:
            log.warning(
                f"{cls_name(self)}: "
                f"Can't process link, "
                f"link:{url} "
                f"err: Can't find tags"
            )
            return None

        try:
            elem = driver.find_element(By.CSS_SELECTOR, self.S_DESCRIPTION)
            info["description"] = elem.get_attribute('innerHTML')
            del elem
        except selenium.common.exceptions.NoSuchElementException:
            log.warning(
                f"{cls_name(self)}: "
                f"Can't process link, "
                f"link:{url} "
                f"err: Can't find description"
            )
            return None

        info["url"] = url
        return info
