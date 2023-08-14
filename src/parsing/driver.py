import os
from urllib.parse import urlencode

from dotenv import load_dotenv
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager

GECKO_DRIVER_PATH = GeckoDriverManager("v0.33.0").install()

load_dotenv()
assert (os.getenv("PROXIES_API_AUTH_KEY") is not None)


def setup_driver():
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")
    # TODO: Need to add proper waiting for each element
    # Can be used only for testing atm
    # options.set_capability("pageLoadStrategy", "eager")

    ua = UserAgent()
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"--user-agent={ua.random}")
    driver = webdriver.Firefox(
        service=Service(
            executable_path=GECKO_DRIVER_PATH,
            log_path=os.path.devnull
        ),
        options=options
    )
    driver.set_page_load_timeout(60)
    return driver


def load_via_proxy(driver, url):
    payload = {'auth_key': os.getenv("PROXIES_API_AUTH_KEY"), 'url': url}
    proxy_url = 'http://api.proxiesapi.com?' + urlencode(payload)
    driver.get(proxy_url)
