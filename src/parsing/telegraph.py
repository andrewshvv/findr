import logging

import selenium
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By

from parsing.converter import ToTelegramMarkdown
from parsing.exceptions import NotFound
from parsing.interface import Parser

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class TelegraphParser(Parser):
    CSS_DESCRIPTION = ".tl_article"  # everything else
    S_404 = ".tl_message"

    # S_LOGIN = ".bloko-header-section-2"

    @staticmethod
    def get_domains():
        return ["telegra"]

    @staticmethod
    def get_name():
        return "Telegraph"

    def format_for_telegram(self, job_info, add_info_link=False):
        message = ""
        if job_info["description"] is not None and len(job_info["description"].strip()) != 0:
            html = job_info['description']
            message += ToTelegramMarkdown(
                bullets="•",
                ignore=[
                    "Edit",
                    "Publish"
                ]
            ).convert(html)
        else:
            raise ValueError(f"Job description is empty, link: {job_info['url']}")

        if add_info_link:
            message += f"\n\n**[Подробнее]({job_info['url']})**"
        return message

    def check_correct_url(self, url):
        return True

    def parse(self, driver, url):
        info = {}
        try:
            elem = driver.find_element(By.CSS_SELECTOR, self.CSS_DESCRIPTION)
            info["description"] = elem.get_attribute('innerHTML')
            del elem
        except selenium.common.exceptions.NoSuchElementException:
            try:
                text = driver.find_element(By.CSS_SELECTOR, self.S_404).text
                if "404" in text.lower().strip():
                    raise NotFound(url, self)
            except selenium.common.exceptions.NoSuchElementException:
                pass

            log.warning(
                f"Can't process link, "
                f"link:{url} "
                f"err: Can't find description"
            )
            return None

        info["url"] = url
        return info
