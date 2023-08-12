# import logging
#
# import selenium
# from markdownify import MarkdownConverter
# from selenium.common.exceptions import TimeoutException
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.support.ui import WebDriverWait
#
# from parsing.exceptions import JobArchived, LoginRequired
# from utils import cls_name
#
# log = logging.getLogger(__name__)
# log.setLevel(logging.INFO)
#
#
# class HeadHunterHTMLConverter(MarkdownConverter):
#     def convert_hr(self, *args, **options):
#         return "\n"
#
#     def convert_p(self, *args, **options):
#         text = super().convert_p(*args, **options)
#         if text.endswith("\n\n"):
#             text = text[:-1]
#
#         return text
#
#
# class LinkedinJobsParser:
#     DOMAINS = ["hhjobs"]
#     XPATH_RESPOND_BUTTON = "//*[@data-qa='vacancy-response-link-top']"
#     XPATH_NAME = "//*[@data-qa='vacancy-title']"  # Unity Developer
#     XPATH_COMPANY = "//*[@data-qa='vacancy-company-name']"  # Octo Games
#     S_TAGS = ".bloko-tag-list"  # Информационные технологии • Разработка • C# • Gamedev
#     S_FORMAT = ".vacancy-description-list-item"  # Удаленная работа Опыт работы от 1 года до 3х лет
#     S_SALARY = ".vacancy-title .bloko-header-section-2_lite"  # от 500 до 1 200 $
#     S_DESCRIPTION = ".vacancy-section"  # everything else
#     S_ARCHIVED = ".bloko-header-2"
#     S_LOGIN = ".bloko-header-section-2"
#
#     def format_for_telegram(self, job_info):
#         if job_info is None:
#             return ""
#
#         if job_info["name"] is None or len(job_info["name"].strip()) == 0:
#             raise ValueError(f"Job name is empty, link: {job_info['url']}")
#
#         if job_info["salary"] is None or len(job_info["salary"].strip()) == 0:
#             job_info["salary"] = "З/п договорная"
#
#         if job_info["company"] is None or len(job_info["company"].strip()) == 0:
#             raise ValueError(f"Job company is empty, link: {job_info['url']}")
#
#         message = f"**{job_info['name']}** ({job_info['salary']})\n"
#         message += f"{job_info['company']}\n\n"
#
#         if job_info["format"] is not None and len(job_info["format"]) != 0:
#             for line in job_info['format']:
#                 message += f" • {line}\n"
#
#         if job_info["tags"] is not None and len(job_info["tags"].strip()) != 0:
#             new_tags = " • ".join([tag.strip() for tag in job_info['tags'].split("\n")][:3])
#             message += f" • {new_tags}\n"
#
#         message += f"\n"
#
#         if job_info["description"] is not None and len(job_info["description"].strip()) != 0:
#             html = job_info['description']
#             markdown_description = HeadHunterHTMLConverter(bullets="•").convert(html)
#             markdown_description = markdown_description.replace("\\*", "")
#             if markdown_description.endswith("\n\n \n\n"):
#                 markdown_description = markdown_description[:5]
#             markdown_description = markdown_description.replace(" •", "•")
#             message += markdown_description
#         else:
#             raise ValueError(f"Job description is empty, link: {job_info['url']}")
#
#         message += f"\n**[Подробнее]({job_info['url']})**"
#         return message
#
#     def parse(self, driver, url):
#         try:
#             WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, self.XPATH_RESPOND_BUTTON)))
#         except selenium.common.exceptions.TimeoutException:
#             try:
#                 text = driver.find_element(By.CSS_SELECTOR, self.S_ARCHIVED).text
#                 if text == "Вакансия в архиве":
#                     raise JobArchived()
#             except selenium.common.exceptions.NoSuchElementException:
#                 pass
#
#             try:
#                 text = driver.find_element(By.CSS_SELECTOR, self.S_LOGIN).text
#                 if text == "Войдите на сайт":
#                     raise LoginRequired()
#             except selenium.common.exceptions.NoSuchElementException:
#                 pass
#
#             log.warning(f"Can't process link:{url} err: Can't find respond button")
#             return None
#
#         info = {}
#
#         try:
#             info["name"] = driver.find_element(By.XPATH, self.XPATH_NAME).text
#         except selenium.common.exceptions.NoSuchElementException:
#             log.warning(f"Can't process link:{url} err: Can't find vacancy name")
#             return None
#
#         try:
#             info["company"] = driver.find_element(By.XPATH, self.XPATH_COMPANY).text
#         except selenium.common.exceptions.NoSuchElementException:
#             log.warning(f"Can't process link:{url} err: Can't find company name")
#             return None
#
#         try:
#             info["tags"] = driver.find_element(By.CSS_SELECTOR, self.S_TAGS).text
#         except selenium.common.exceptions.NoSuchElementException:
#             log.debug(
#                 f"{cls_name(self)}: "
#                 f"Can't find tags "
#                 f"link:{url}"
#             )
#             info["tags"] = None
#
#         try:
#             info["format"] = []
#             elems = driver.find_elements(By.CSS_SELECTOR, self.S_FORMAT)
#             for el in elems:
#                 info["format"].append(el.text)
#             del elems
#             del el
#         except selenium.common.exceptions.NoSuchElementException:
#             log.debug(
#                 f"{cls_name(self)}: "
#                 f"Can't find format "
#                 f"link:{url}"
#             )
#
#         try:
#             info["salary"] = driver.find_element(By.CSS_SELECTOR, self.S_SALARY).text
#         except selenium.common.exceptions.NoSuchElementException:
#             log.debug(
#                 f"{cls_name(self)}: "
#                 f"Can't find salary "
#                 f"link:{url}"
#             )
#             info["salary"] = None
#
#         try:
#             elem = driver.find_element(By.CSS_SELECTOR, self.S_DESCRIPTION)
#             info["description"] = elem.get_attribute('innerHTML')
#             del elem
#         except selenium.common.exceptions.NoSuchElementException:
#             log.warning(f"Can't process link:{url} err: Can't find description")
#             return None
#
#         info["url"] = url
#         return info
