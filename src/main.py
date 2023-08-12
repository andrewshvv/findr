import os

import sentry_sdk
from dotenv import load_dotenv

from parsing.parsing import JobPostingParser

load_dotenv()

from bot.bot import TelegramBot
from matching.filtering import JobDescriptionsCheck
from preprocessing.post_manager import TRANSIENT_CHANNEL, PostManager
from preprocessing.post_sourser import iter_channel_messages, SEARCH_CUTOFF_DATE, SetupTelegramSession, Preprocessing
from preprocessing.prompt import PromptTranslate

from common.db import safe_db_execute
from common.telegram import WaitOnFloodTelegramClient, remove_posts_from_channel

from aiomisc import get_context
from db.embedding import EmbeddingDB
from db.sqlite import GET_ACCEPTED_POSTS, SQLLite3Service

import asyncio
import logging
import aiomisc
import aiosqlite
from pyee import AsyncIOEventEmitter

FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig()
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.INFO)
logging.getLogger("chromadb.segment.impl.vector.local_persistent_hnsw").setLevel(logging.ERROR)
logging.getLogger(__name__).setLevel(logging.INFO)


class Utils(aiomisc.Service):
    delete_posts_transient = False
    delete_same_posts = False
    log_posts_for_forwarding = False

    async def get_post_by_id(self, client: WaitOnFloodTelegramClient, post_id):
        post = await client.get_messages(TRANSIENT_CHANNEL, ids=[post_id])
        await client.forward_messages("me", post)

    async def delete_posts(self, client, post_ids):
        async for _ in await remove_posts_from_channel(client, TRANSIENT_CHANNEL, post_ids):
            pass
        log.info(f"Done removing duplicates")

    async def clear_transient_channel(self, client):
        post_ids = [
            post.id
            async for post in iter_channel_messages(client, TRANSIENT_CHANNEL, SEARCH_CUTOFF_DATE)
        ]

        async for _ in await remove_posts_from_channel(client, TRANSIENT_CHANNEL, post_ids):
            pass
        log.info(f"Done removing posts")

    async def start(self):
        context = get_context()
        client = await context['tg_client']

        # pprint(await get_channels_from_folder(client, "Test"))

        if self.delete_posts_transient:
            await self.clear_transient_channel(client)

        if self.delete_same_posts:
            await self.delete_duplicate_posts(client)

        if self.log_posts_for_forwarding:
            # Service is ready
            self.start_event.set()

            while True:
                async with aiosqlite.connect(SQLLite3Service.db_path) as db:
                    async with safe_db_execute(db, GET_ACCEPTED_POSTS) as cursor:
                        async for row in cursor:
                            log.info(f"Need to forward row:{row[0]}")

                await asyncio.sleep(5)


channel_sync_period = 600
if os.getenv("ENV") == "PROD":
    sentry_sdk.init(
        dsn="https://9c48edc997dc41ecaf95250328794ff2@o4505531915763712.ingest.sentry.io/4505531919892480",

        # To set a uniform sample rate
        # Set profiles_sample_rate to 1.0 to profile 100%
        # of sampled transactions.
        # We recommend adjusting this value in production
        profiles_sample_rate=1.0,

        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # We recommend adjusting this value in production.
        traces_sample_rate=1.0
    )
    # posthog = Posthog(project_api_key='phc_CrNaJ2lq45gafn79WuM8oQhOOohSj9LrjL4SUNskjMs', host='https://eu.posthog.com')

elif os.getenv("ENV") == "TEST":
    channel_sync_period = 10

log.info(
    f"Starting main, env: {os.getenv('ENV')}"
)

emitter = AsyncIOEventEmitter()

try:
    with aiomisc.entrypoint(
            SQLLite3Service(
                environment=os.getenv("ENV"),
                # drop_user_posts=True,
                # drop_prompts=True,
                # add_test_posts=True,
                # add_test_prompts=True,
            ),
            EmbeddingDB(
                environment=os.getenv("ENV"),
                # recreate_prompts=True,
            ),
            SetupTelegramSession(),
            TelegramBot(
                api_key=os.getenv("TELEGRAM_BOT_API_KEY"),
                emitter=emitter,
            ),
            PostManager(
                api_key=os.getenv("HELPER_BOT"),
                emitter=emitter,
            ),
            PromptTranslate(emitter=emitter),
            JobDescriptionsCheck(emitter=emitter),
            JobPostingParser(),
            Preprocessing(channel_sync_period=channel_sync_period),
            # Utils(
            #     # delete_posts_transient=False,
            #     # delete_same_posts=True,
            #     # log_posts_for_forwarding=True
            # ),

            log_level="info",
            log_format="color",
            log_buffer_size=10,
    ) as loop:
        log.info("Started services")
        loop.run_forever()
except KeyboardInterrupt as e:
    log.warning(f"Exiting via KeyboardInterrupt, services aren't stopped properly")
finally:
    logging.shutdown()  # jesus fuck, why is that not by default behaviour
