import asyncio
import logging
import queue
import threading
import time
from pprint import pformat
from typing import Optional, Union

import aiomisc
import langid
import selenium
import urllib3
import validators
from selenium.common import WebDriverException
from tldextract import tldextract

from common.logging import cls_name, suppress_logs
from common.markdown import MarkdownPost
from common.telegram import TelegramTextTools
from parsing.driver import setup_driver
from parsing.exceptions import NotFound, NotSupported
from parsing.geeekjobs import GeekJobsParser
from parsing.habrahabr import HabrParser
from parsing.headhunter import HeadHunterParser, JobArchived, LoginRequired
from parsing.interface import Parser
from parsing.telegraph import TelegraphParser
from preprocessing.utils import run_pkill

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
logging.getLogger("urllib3.connectionpool").setLevel(logging.CRITICAL)


class KnownNoneParser(Parser):
    def check_correct_url(self, url):
        raise NotImplementedError()

    def parse(self, driver, url):
        raise NotImplementedError()

    def format_for_telegram(self, job_info, add_info_link):
        raise NotImplementedError()

    @staticmethod
    def get_domains():
        return ["vseti", "linkedin", "careerspace"]

    @staticmethod
    def get_name():
        return "My name is Giovanni Giorgio"


class JobPostingParser(aiomisc.Service):
    queue = queue.Queue()
    _futures = {}

    async def start(self):
        self.context["parser"] = self
        self.start_event.set()

        self.stop_event = threading.Event()
        self.done_event = threading.Event()

        await self.keep_queue(self.done_event, self.stop_event)

    @staticmethod
    def _get_domain(url):
        return tldextract.extract(url).domain

    @staticmethod
    def is_parsable(url):
        parser = JobPostingParser._get_parser(url)
        if isinstance(parser, KnownNoneParser) or not parser:
            return False
        return True

    @staticmethod
    def _get_parser(url) -> Optional[Parser]:
        d = JobPostingParser._get_domain(url)
        parser = None
        if d in HeadHunterParser.get_domains():
            parser = HeadHunterParser()
        elif d in GeekJobsParser.get_domains():
            parser = GeekJobsParser()
        elif d in HabrParser.get_domains():
            parser = HabrParser()
        elif d in TelegraphParser.get_domains():
            parser = TelegraphParser()
        elif d in KnownNoneParser.get_domains():
            parser = KnownNoneParser()

        return parser

    @staticmethod
    def find_known_external_links(text: Union[MarkdownPost, str]):
        external_links = {}
        for url in TelegramTextTools.final_links(text):
            if not validators.url(url): continue

            parser = JobPostingParser._get_parser(url)

            if parser:
                can_process = not isinstance(parser, KnownNoneParser)
                external_links[url] = can_process

        return [(k, v) for k, v in external_links.items()]

    async def wait_for_futures(self, futures):
        gather_future = asyncio.gather(*futures)
        self._futures[gather_future] = None
        res = await gather_future
        del self._futures[gather_future]
        return res

    async def stop(self, *args, **kwargs):
        self.stop_event.set()

        log.debug(
            f"{cls_name(self)}: "
            f"Waiting for keep_queue to finish"
        )

        while not self.done_event.is_set():
            time.sleep(1)

        log.debug(
            f"{cls_name(self)}: "
            f"Stopping, waiting for parsing to finish"
        )

        # for future in self._futures.keys():
        #     await future

        log.debug(
            f"{cls_name(self)}: "
            f"Stopping, quiting drivers"
        )

        initial_size = self.queue.qsize()
        with suppress_logs("urllib3.connectionpool"):
            while not self.queue.empty():
                driver, _ = self.queue.get()
                driver.quit()

        # TODO: Not sure clean up works, will fix later
        run_pkill("geckodriver")
        run_pkill("Firefox")

        log.info(
            f"{cls_name(self)}: "
            f"Stopped, "
            f"drivers spawned: {initial_size} "
            f"queue size: {self.queue.qsize()} "
        )

    @aiomisc.threaded_separate
    def keep_queue(self, done: threading.Event, stopped: threading.Event):
        log.debug(
            f"{cls_name(self)}: "
            f"Started job processing queue"
        )
        while not stopped.is_set():
            if self.queue.qsize() < 1:
                try:
                    driver = setup_driver()
                    self.queue.put((driver, 0))
                    log.debug(
                        f"{cls_name(self)}: "
                        f"Added new driver to queue, "
                        f"size: {self.queue.qsize()}"
                    )
                except (WebDriverException,
                        urllib3.exceptions.ProtocolError) as e:
                    log.warning(
                        f"{cls_name(self)}: "
                        f"Can't setup driver "
                        f"err: {str(e)}"
                    )
                    time.sleep(3)
                    continue

                except Exception as e:
                    log.exception(e)

            elif self.queue.qsize() > 15:
                while self.queue.qsize() > 10:
                    driver, _ = self.queue.get()
                    with suppress_logs("urllib3.connectionpool"):
                        driver.quit()
                    log.debug(
                        f"{cls_name(self)}: "
                        f"Removed driver from queue, "
                        f"size: {self.queue.qsize()}"
                    )
            else:
                time.sleep(2)

        log.debug(
            f"{cls_name(self)}: "
            f"Exiting keep_queue "
            f"size: {self.queue.qsize()}"
        )
        done.set()

    def return_driver(self, driver, counter, close=True):
        if counter >= 10:
            log.debug(
                f"{cls_name(self)}: "
                f"Driver used more than 10 times quiting, "
                f"size:{self.queue.qsize()}"
            )
            with suppress_logs("urllib3.connectionpool"):
                driver.quit()
        else:
            if len(driver.window_handles) > 1 and close:
                with suppress_logs("urllib3.connectionpool"):
                    driver.close()

            self.queue.put((driver, counter))
            log.debug(
                f"{cls_name(self)}: "
                f"Added driver back to queue, "
                f"size:{self.queue.qsize()}"
            )

    @staticmethod
    def check_for_redirect(parser, prev_url, current_url):
        is_redirect = prev_url != current_url

        after_load_domain = JobPostingParser._get_domain(current_url)
        if after_load_domain not in parser.get_domains() or is_redirect:
            parser = JobPostingParser._get_parser(current_url)
            if isinstance(parser, KnownNoneParser) or not parser:
                return None, is_redirect

            return parser, is_redirect
        return parser, is_redirect

    def _parse(self, driver, counter, iteration, url, add_info_link=False, log_info=None):
        li = {} or log_info

        parser = JobPostingParser._get_parser(url)
        if not parser:
            raise NotImplementedError("weird, should been able to find parser")

        try:
            driver.get(url)
            current_url = driver.current_url
            counter += 1
        except selenium.common.exceptions.TimeoutException:
            log.warning(
                f"{cls_name(self)}: "
                f"({li['n']} / {li['ns']}) "
                f"Skipping, page is loading for more than 5 seconds, "
                f"url:{url} "
                f"parser: {','.join(parser.get_domains())} "
                f"iteration:{iteration}"
            )
            return None, None, None
        except selenium.common.exceptions.WebDriverException:
            log.warning(
                f"{cls_name(self)}: "
                f"({li['n']} / {li['ns']}) "
                f"Skipping, page loading unsuccessful (WebDriverException), "
                f"url:{url} "
                f"parser: {','.join(parser.get_domains())} "
                f"iteration:{iteration}"
            )
            return None, None, None

        parser, is_redirected = JobPostingParser.check_for_redirect(
            parser=parser,
            prev_url=url,
            current_url=current_url
        )
        if is_redirected:
            if parser:
                log.debug(
                    f"{cls_name(self)}: "
                    f"({li['n']} / {li['ns']}) "
                    f"Seems like redirect happened, using new parser "
                    f"url:{current_url} "
                    f"parser: {','.join(parser.get_domains())}"
                    f"iteration:{iteration}"
                )
            else:
                log.info(
                    f"{cls_name(self)}: "
                    f"({li['n']} / {li['ns']}) "
                    f"Skipping, seems like redirect happened, no available parser "
                    f"domain:{JobPostingParser._get_domain(current_url)} "
                    f"iteration:{iteration} "
                    f"link: {current_url} "
                )
                return None, None, None

        if not parser.check_correct_url(current_url):
            log.info(
                f"{cls_name(self)}: "
                f"({li['n']} / {li['ns']}) "
                f"Skipping, url is incorrect "
                f"url:{current_url} "
                f"parser:{','.join(parser.get_domains())} "
                f"iteration:{iteration}"
            )
            return None, None, None

        info = parser.parse(driver, url)
        if not info:
            log.warning(
                f"{cls_name(self)}: "
                f"({li['n']} / {li['ns']})  "
                f"Unable to parse the link, "
                f"link:{url} "
                f"parser:{','.join(parser.get_domains())} "
                f"iteration:{iteration}"
            )
            return None, None, None

        content = parser.format_for_telegram(info, add_info_link=add_info_link)
        log.debug(
            f"{cls_name(self)}: "
            f"({li['n']} / {li['ns']})  "
            f"Successfully parsed page, "
            f"link:{url} "
            f"parser:{','.join(parser.get_domains())} "
            f"iteration:{iteration}"
        )
        return content, parser, current_url

    @aiomisc.threaded_separate
    def _iterative_parse(self, url_to_parse, add_info_link=False, log_info=None):
        li = {} or log_info

        if self.stop_event.is_set(): return None
        last_parsed_content = None
        last_parser = None
        iteration = 0
        last_parsed_url = None
        none_result = (last_parsed_content, last_parser, last_parsed_url, None)

        # In case if the post has the content inside, which leads to another post
        # For example:
        # - telegraph might have link on hh or geekjobs and so on.
        # - linkedin posts also have this tendency to point to other service
        # We consider final content to be most accurate
        while True:
            if not JobPostingParser.is_parsable(url_to_parse):
                log.info(
                    f"{cls_name(self)}: "
                    f"({li['n']} / {li['ns']}) Skip, can't find proper parser "
                    f"link:{url_to_parse}"
                    f"iteration:{iteration}"
                )
                break

            iteration += 1
            (driver, counter) = self.queue.get()

            try:
                parsed_content, used_parser, url_after_load = self._parse(driver,
                                                                          counter,
                                                                          iteration,
                                                                          url_to_parse,
                                                                          add_info_link,
                                                                          log_info=li)
            except JobArchived as e:
                log.info(
                    f"{cls_name(self)}: "
                    f"({li['n']} / {li['ns']}) "
                    f"Skipping, job description archived, "
                    f"link:{e.url} "
                    f"parser:{','.join(e.parser.get_domains())} "
                    f"iteration:{iteration}"
                )
                return none_result

            except NotFound as e:
                log.info(
                    f"{cls_name(self)}: "
                    f"({li['n']} / {li['ns']}) "
                    f"Skipping, page not found "
                    f"link:{e.url} "
                    f"parser:{','.join(e.parser.get_domains())} "
                    f"iteration:{iteration}"
                )
                return none_result

            except NotSupported as e:
                log.info(
                    f"{cls_name(self)}: "
                    f"({li['n']} / {li['ns']}) "
                    f"Skipping, not supported version, "
                    f"link:{e.url} "
                    f"parser:{','.join(e.parser.get_domains())} "
                    f"iteration:{iteration}"
                )

                return (
                    last_parsed_content,
                    last_parser,
                    last_parsed_url,
                    JobPostingParser.identify_language(last_parsed_content)
                )

            except LoginRequired as e:
                log.warning(
                    f"{cls_name(self)}: "
                    f"({li['n']} / {li['ns']}) "
                    f"Skipping, login required, "
                    f"link:{e.url} "
                    f"parser:{','.join(e.parser.get_domains())} "
                    f"iteration:{iteration}"
                )

                return (
                    last_parsed_content,
                    last_parser,
                    last_parsed_url,
                    JobPostingParser.identify_language(last_parsed_content)
                )

            finally:
                self.return_driver(driver, counter)

            if not parsed_content:
                break

            last_parsed_url = url_after_load
            last_parsed_content = parsed_content
            last_parser = used_parser

            # Content might have link on another website, which describes the job
            # Lets check for that
            parsable_link = [
                link
                for link, can_process
                in JobPostingParser.find_known_external_links(last_parsed_content)
                if can_process  # we may know about job hosting, by don't have parser yet
            ]

            if not parsable_link:
                break

            if len(parsable_link) > 1:
                raise NotImplementedError(f"More than one parsable link in the recursive content, url:{url_after_load}")

            url_to_parse = parsable_link[0]

            if iteration > 3:
                raise NotImplementedError(f"Seems like a cross-linking between job posts: url:{url_after_load}")

        if last_parsed_content:
            log.info(
                f"{cls_name(self)}: "
                f"({li['n']} / {li['ns']}) "
                f"Successfully parsed link, "
                f"link:{last_parsed_url} "
                f"parser:{','.join(last_parser.get_domains())} "
                f"iteration:{iteration}"
            )

        return (
            last_parsed_content,
            last_parser,
            last_parsed_url,
            JobPostingParser.identify_language(last_parsed_content)
        )

    @staticmethod
    def identify_language(content: MarkdownPost):
        if not content:
            return None

        language, _ = langid.classify(content.plain())
        return language

    async def parse(self, urls, add_info_link=False):
        if type(urls) is not list:
            ValueError("should be list")

        if not urls:
            return []

        futures = []
        log.debug(
            f"{cls_name(self)}: "
            f"Received links for handling \n"
            f"links:\n{pformat(urls)}"
        )
        num_urls = len(urls)
        for n, url in enumerate(urls):
            if not JobPostingParser.is_parsable(url):
                log.info(
                    f"{cls_name(self)}: "
                    f"({n}/{num_urls}) Skip, can't find proper parser "
                    f"link:{url}"
                )
                continue

            log_info = {"n": n + 1, "ns": num_urls}
            future = self._iterative_parse(url, add_info_link=add_info_link, log_info=log_info)
            futures.append(future)

        if futures:
            return await self.wait_for_futures(futures)
        else:
            return []
