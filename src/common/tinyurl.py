import logging
from typing import Dict, Optional

import aiohttp
from aiohttp.client_exceptions import ClientResponseError

from utils import cls_name

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

import string
import random


def generate_random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))


class TinyURL:
    BASE_URL = "https://api.tinyurl.com"

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def create(self, url: str) -> Optional[Dict]:
        endpoint = "/create"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        data = {
            "url": url,
            "domain": "tinyurl.com"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(self.BASE_URL + endpoint, headers=headers, json=data) as resp:
                response = await resp.json()
                try:
                    resp.raise_for_status()
                except ClientResponseError as e:
                    log.warning(
                        f"{cls_name(self)}: "
                        f"Can't create tiny url "
                        f"status:{e.status} "
                        f"err: {response['errors']} "
                        f"data:{data} "
                        f"code:{response['code']} "
                    )
                    return None
                return response["data"]
