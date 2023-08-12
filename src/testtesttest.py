import asyncio
import json
import logging
import os
from pprint import pprint
import aiomisc
import aiosqlite
import chromadb
import openai
import tiktoken
from aiomisc import get_context
from chromadb import Settings
from pyee import AsyncIOEventEmitter

from dotenv import load_dotenv
from telethon.tl.types import MessageEntityMentionName

from common.markdown import MarkdownPost

load_dotenv()
from common.logging import cls_name, shorten_text
from common.telegram import WaitOnFloodTelegramClient
from common.utils import get_prompt
from db.embedding import EmbeddingDB
from db.sqlite import SQLLite3Service


class SomeService(aiomisc.Service):
    async def start(self):
        parser = await get_context()["parser"]
        parse_info = await parser.parse(
            # add_info_link=True,
            urls=[
                "https://career.habr.com/vacancies/1000128555"
                # "https://telegra.ph/QA-engineer-07-26-2"
                # "https://telegra.ph/Project-manager-07-27-3"
                # "https://vyazma.hh.ru/vacancy/83016176"
                # "https://geekjob.ru/vacancy/64bf866ffa46ed8d7f078ff1",
                # "https://geekjob.ru/vacancy/64bf9a8f68aba0403e030bdd"
                # "https://career.habr.com/vacancies/1000128398",
                # "https://career.habr.com/vacancies/1000128025",
                # "https://u.habr.com/r9jPX",
                # "https://nn.hh.ru/vacancy/83698589",
                # "https://vyazma.hh.ru/vacancy/83138238",
                # "https://hh.ru/vacancy/83176226",
                # "https://nn.hh.ru/vacancy/83696909",
                # "https://hh.ru/vacancy/83176226",
                # "https://gkjb.ru/hbOm",
                # "https://career.habr.com/vacancies/1000128010",
                # "https://u.habr.com/nvkhG",
                # "https://hh.ru/vacancy/82937752",
                # "https://telegra.ph/Rukovoditel-otdela-sistemnogo-analiza-v-Vostok-Zapad-07-10",
                # "https://geekjob.ru/vacancy/64ba3aa525cffdd6ad02d8f5",
                # "https://geekjob.ru/vacancy/64c0c4da822803e3030afa19",
                # "https://hh.ru/vacancy/83608374?from=employer&hhtmFrom=employer",
            ]
        )

        contents = [content for (content, *_) in parse_info if content is not None]
        if not contents:
            return

        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")

        async with WaitOnFloodTelegramClient(session="telegram",
                                             api_id=api_id,
                                             api_hash=api_hash,
                                             system_version="4.16.30-vxCUSTOM") as client:
            for content in contents:
                await client.send_message(
                    entity="me",
                    message=content.plain(),
                    formatting_entities=content.telethon_entities()
                )


# try:
#     with aiomisc.entrypoint(
#             # JobPostingParser(),
#             SomeService(),
#             log_level="info",
#             log_format="color",
#             log_buffer_size=10,
#     ) as loop:
#         loop.run_forever()
# except KeyboardInterrupt as e:
#     pass
# finally:
#     logging.shutdown()

# from pyee import AsyncIOEventEmitter
#
#
# class HandlerService(aiomisc.Service):
#     ee: AsyncIOEventEmitter
#     lock: asyncio.Lock
#
#     async def some_other_thing(self):
#         print("short_event")
#
#     async def poll_db(self, kwargs):
#         async with self.lock:
#             print("long_event: start", kwargs)
#             await asyncio.sleep(10)
#             print("long_event: end", kwargs)
#
#     async def start(self, *args, **kwargs):
#         self.lock = asyncio.Lock()
#
#         self.ee.on("long_event", self.poll_db)
#         self.ee.on("short_event", self.some_other_thing)
#
#         self.ee.emit("long_event", 1)
#         self.ee.emit("short_event")
#         self.ee.emit("short_event")
#         self.ee.emit("short_event")
#         self.ee.emit("long_event", 2)
#         self.ee.emit("long_event", 3)
#         self.ee.emit("long_event", 3)
#
#         print("done")
#         await asyncio.sleep(10)
#         # await wait_for_event_or_execute(
#         #     default_handler=self.poll_db,
#         #     emitter=self.ee,
#         #     event_name="some_event",
#         #     timeout=5,
#         # )
#
#
# class TriggerService(aiomisc.Service):
#     ee: AsyncIOEventEmitter
#
#     async def start(self, *args, **kwargs):
#         await asyncio.sleep(3)
#         # self.ee.emit('some_event', 'trigger data')


# ee = AsyncIOEventEmitter()
#
# try:
#     with aiomisc.entrypoint(
#             HandlerService(ee=ee),
#             TriggerService(ee=ee),
#             log_level="info",
#             log_format="color",
#             log_buffer_size=10,
#     ) as loop:
#         loop.run_forever()
# except KeyboardInterrupt as e:
#     pass
# finally:
#     logging.shutdown()

# emitter = AsyncIOEventEmitter()
# log = logging.getLogger(__name__)
#
#
# class PromptTester(aiomisc.Service):
#     @aiomisc.asyncbackoff(
#         attempt_timeout=60,
#         deadline=60,
#         pause=2,
#         max_tries=10,
#         exceptions=(
#                 openai.error.APIConnectionError,
#                 openai.error.ServiceUnavailableError,
#         )
#     )
#     async def preprocess_prompt(self, prompt):
#         # async with self.rate_limit:
#
#         chat_completions = await openai.ChatCompletion.acreate(
#             model="gpt-4-0613",
#             temperature=0,
#             messages=[
#                 {
#                     "role": "system",
#                     "content": get_prompt("prompt_check.txt"),
#                 },
#                 {
#                     "role": "user",
#                     "content": prompt,
#                 }
#             ]
#         )
#
#         tokens_used = chat_completions['usage']['total_tokens']
#         response = chat_completions['choices'][0]['message']["content"].lower()
#
#         try:
#             json_response = json.loads(response)
#         except json.decoder.JSONDecodeError:
#             log.warning(
#                 f"{cls_name(self)}: "
#                 f"Corrupter GPT response "
#                 f"num:{response}"
#             )
#             return tokens_used, None
#
#         pprint(json_response)
#         # validate(json_response, json_preprocess_schema)
#         return tokens_used, json_response
#
#     async def start(self):
#         self.start_event.set()
#         context = get_context()
#         log.info(f"{cls_name(self)}: Waiting for ChromaDB posts collection")
#         try:
#             try:
#                 self.index_posts
#             except AttributeError:
#                 self.index_posts = await asyncio.wait_for(context['index_posts'], 3)
#         except asyncio.exceptions.TimeoutError:
#             log.warning(
#                 f"{cls_name(self)}: "
#                 f"Exiting: Haven't received ChromaDB posts collection"
#             )
#             return
#
#         log.info(f"{cls_name(self)}: Waiting for SQLite3 to be ready")
#         try:
#             await asyncio.wait_for(context['sqlite_ready'], 3)
#         except asyncio.exceptions.TimeoutError:
#             log.warning(
#                 f"{cls_name(self)}: "
#                 f"Exiting: Haven't received SQLite3 to be ready event"
#             )
#             return
#
#         log.info(f"{cls_name(self)}: Waiting for ChromaDB prompts collection")
#         try:
#             try:
#                 self.index_prompts
#             except AttributeError:
#                 self.index_prompts = await asyncio.wait_for(context['index_prompts'], 3)
#         except asyncio.exceptions.TimeoutError:
#             log.warning(
#                 f"{cls_name(self)}: "
#                 f"Exiting: Haven't received ChromaDB prompts collection"
#             )
#             return
#
#         try:
#             try:
#                 self.create_embedding
#             except AttributeError:
#                 log.info(f"{cls_name(self)}: Waiting for GPT embedding function")
#                 self.create_embedding = await asyncio.wait_for(context['create_embedding'], 3)
#         except asyncio.exceptions.TimeoutError:
#             log.warning(
#                 f"{cls_name(self)}: "
#                 f"Exiting: Haven't received GPT embedding function"
#             )
#             return
#
#         prompt = "project manager bnpl"
#         _, response = await self.preprocess_prompt(prompt)
#         if not response:
#             return
#
#         if not response["is_a_job_search_request"]:
#             log.warning(
#                 f"{cls_name(self)}: "
#                 f"Prompt rejected, not a job search request "
#                 f"prompt:'{shorten_text(prompt)}' "
#             )
#             return
#
#         position_tags_cloud = ",".join(response['position_tags_cloud'])
#         request_tags_cloud = ",".join(response['request_tags_cloud'])
#         print(prompt)
#         # print(position_tags_cloud)
#
#         async def get_embedding(text):
#             _, embeddings = await self.create_embedding([text])
#             return embeddings[0]
#
#         # self.index_prompts.add(
#         #     embeddings=[embeddings[0]],
#         #     ids=[str(1)]
#         # )
#
#         results = self.index_posts.query(
#             query_embeddings=await asyncio.gather(
#                 get_embedding(position_tags_cloud),
#                 # get_embedding(request_tags_cloud),
#                 # get_embedding(prompt),
#             ),
#             n_results=400,
#             include=["distances", "metadatas"]
#         )
#
#         def find_intersection(*arrays):
#             result_set = set(arrays[0])
#             for arr in arrays[1:]:
#                 result_set.intersection_update(arr)
#             return list(result_set)
#
#         post_ids = [[metadata["post_id"] for metadata in metadatas] for metadatas in results["metadatas"]]
#
#         print("\n\n\n" + prompt)
#         async with aiosqlite.connect(SQLLite3Service.db_path) as db:
#             distances = results["distances"][0]
#             metadatas = results["metadatas"][0]
#
#             for n, (distance, metadata) in enumerate(zip(distances, metadatas)):
#                 post_id = metadata["post_id"]
#                 if post_id not in find_intersection(*post_ids): continue
#
#                 if n >= 10: break
#                 cursor = await db.execute("""SELECT description FROM posts WHERE post_id = ?""", [post_id])
#                 (description,) = await cursor.fetchone()
#                 print(n, post_id, distance, shorten_text(description))
#
#
# try:
#     with aiomisc.entrypoint(
#             SQLLite3Service(
#                 environment=os.getenv("ENV"),
#                 # drop_user_posts=True,
#                 # drop_prompts=True,
#                 # add_test_posts=True,
#                 # add_test_prompts=True,
#             ),
#             EmbeddingDB(
#                 environment=os.getenv("ENV"),
#                 recreate_prompts=True,
#             ),
#             PromptTester(),
#             log_level="info",
#             log_format="color",
#             log_buffer_size=10,
#     ) as loop:
#         log.info("Started services")
#         loop.run_forever()
# except KeyboardInterrupt as e:
#     log.warning(f"Exiting via KeyboardInterrupt, services aren't stopped properly")
# finally:
#     logging.shutdown()  # jesus fuck, why is that not by default behaviour
#
# 'проектный менеджер',
# 'web3',
# 'ethereum',
# 'блокчейн',
# 'криптовалюта',
# 'децентрализованные приложения',
# 'смарт-контракты',
# 'управление проектами',
# 'координация',
# 'стратегическое планирование',
# 'управление рисками',
# 'управление стейкхолдерами',
# 'управление ресурсами',
# 'управление бюджетом',
# 'управление командой'
