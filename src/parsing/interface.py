from abc import ABC, abstractmethod


class Parser(ABC):
    @abstractmethod
    def check_correct_url(self, url):
        raise NotImplementedError()

    @abstractmethod
    def parse(self, driver, url):
        raise NotImplementedError()

    @abstractmethod
    def format_for_telegram(self, job_info, add_info_link):
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def get_domains():
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def get_name():
        raise NotImplementedError()
