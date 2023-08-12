import asyncio
import json
import logging
import os
import pprint
from datetime import datetime, timedelta

import aiomisc
import aiosqlite
import chromadb
import jsonschema
import openai
import telethon
import tiktoken
from aiolimiter import AsyncLimiter
from aiomisc import get_context
from aiosqlite import Connection
from chromadb.api.models import Collection
from chromadb.errors import IDAlreadyExistsError
from jsonschema import validate
from pytz import utc
from telethon import events
from telethon.tl.types import Message

from common.db import safe_db_execute
from common.exceptions import IntegrityCheck, TokenLimitExceeded
from common.logging import cls_name, shorten_text, humanize_time
from common.markdown import MarkdownPost, ignore_asterics, remove_excessive_n, remove_weird_ending, \
    fix_brain_cancer
from common.telegram import WaitOnFloodTelegramClient, TelegramTextTools, extract_button_text, get_original_pid_cid, \
    is_negative_sentiment
from common.utils import get_match_percentage, get_prompt, str_utc_time, group_list
from db.embedding import PostsCollection
from db.sqlite import SQLLite3Service, GET_POST_BY_POST_ID, GET_POST_BY_SOURCE, INSERT_INTO_POSTS, POSTS_FOR_CLEAN, \
    CLEAN_POSTS, COUNT_ACCEPTED_POSTS, GET_ALL_ACCEPTED_POSTS
from gpt.schemas.preprocess import schema as json_preprocess_schema
from parsing.parsing import JobPostingParser
from parsing.telegraph import TelegraphParser
from preprocessing.channels import ACTIVE_CHANNELS, get_stop_list

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

CUTFOFF_DAYS = int(os.getenv("CUTOFF_DAYS"))
SEARCH_CUTOFF_DATE = datetime.now(utc) - timedelta(days=CUTFOFF_DAYS)

# Safety:
assert cls_name(TelegraphParser()) == "TelegraphParser"


async def iter_channel_messages(client, channel, *args, wait_time=0.1, cutoff_date=None, reverse=False, **kwargs):
    async for post in client.iter_messages(channel, *args,
                                           wait_time=wait_time,
                                           reverse=reverse, **kwargs):
        if post is None:
            continue

        if type(post) != Message:
            continue

        if len(post.text.strip()) == 0:
            continue

        if cutoff_date is not None:
            if reverse and cutoff_date < post.date:
                break
            if not reverse and cutoff_date > post.date:
                break

        yield post


# noinspection PyTypeChecker
class SetupTelegramSession(aiomisc.Service):
    client: WaitOnFloodTelegramClient = None

    async def start(self):
        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")
        # phone = os.getenv("TELEGRAM_PHONE")

        self.client = WaitOnFloodTelegramClient(
            session="telegram",
            api_id=api_id,
            api_hash=api_hash,
            system_version="4.16.30-vxCUSTOM"
        )

        log.info(
            f"{cls_name(self)}: "
            f"Connecting to telegram"
        )
        await self.client.start()
        log.info(
            f"{cls_name(self)}: "
            f"Connected to telegram"
        )
        self.context['tg_client'] = self.client

    async def stop(self, *args, **kwargs):
        self.client.disconnect()


class Preprocessing(aiomisc.Service):
    client: WaitOnFloodTelegramClient = None
    _post_collection: chromadb.api.models.Collection = None
    create_embedding = None
    preprocessing_rate_limit: AsyncLimiter = None
    parser: JobPostingParser = None
    channel_sync_period = None

    futures = {}
    transient_map_cache = {}

    async def sync_index_and_db(self, db):
        cursor = await safe_db_execute(db, COUNT_ACCEPTED_POSTS)
        (sqlite_accepted_count,) = await cursor.fetchone()
        chroma_count = len(self._post_collection.peek(limit=0)["ids"])
        if chroma_count == sqlite_accepted_count: return

        log.warning(
            f'{cls_name(self)}: '
            f'Seems like mismatch between chroma and sqlite, '
            f"sqlite_count: {sqlite_accepted_count} "
            f"chroma_count: {chroma_count} "
        )

        rows = await safe_db_execute(db, GET_ALL_ACCEPTED_POSTS)
        post_map = {
            str(post_id): {
                "text": text,
                "source": source,
            }
            async for (post_id, source, text,) in rows or []
        }

        index_ids = self._post_collection.get_posts(ids=list(post_map.keys()))["ids"]

        # IDs in db which are not in index
        mismatch_ids = sorted(list(set(post_map.keys()) - set(index_ids)))
        for post_ids in group_list(mismatch_ids, 100):
            texts = [post_map[post_id]["text"] for post_id in post_ids]
            _, embeddings = await self.create_embedding(texts)

            for n, (post_id, embedding) in enumerate(zip(post_ids, embeddings)):
                await self._post_collection.insert_post(
                    post_id=post_id,
                    source=post_map[post_id]["source"],
                    embedding=embedding
                )

                log.warning(
                    f'{cls_name(self)}: '
                    f'{n + 1}: Created missing index for, '
                    f"post_id: {post_id} "
                    f"source: {post_map[str(post_id)]['source']} "
                )

            await asyncio.sleep(1)

        # IDs in index which are not in db
        mismatch_ids = sorted(list(set(index_ids) - set(post_map.keys())))
        await self._post_collection.remove_posts(ids=mismatch_ids)

    async def remove_old_posts(self, db):
        cursor = await safe_db_execute(db, POSTS_FOR_CLEAN.format(days=CUTFOFF_DAYS))
        rows = await cursor.fetchall()
        if not rows:
            return

        index_post_ids = [
            str(post_id)
            for (post_id, status, _) in rows
            if status != "rejected"
        ]

        self._post_collection.remove_posts(ids=index_post_ids)
        await safe_db_execute(db, CLEAN_POSTS.format(days=CUTFOFF_DAYS))
        await db.commit()

        for post_id, status, _date in rows:
            log.info(
                f'{cls_name(self)}: '
                f'Removed old active post, '
                f"post_id: {post_id} "
                f"status: {status} "
                f"time: {humanize_time(_date)} "
            )

    async def db_integrity_check(self, db: Connection, post_id):
        cursor = await db.execute(GET_POST_BY_POST_ID, [post_id])
        row = await cursor.fetchone()
        if not row:
            raise IntegrityCheck(
                f"Can't find post in sqlite db"
                f"source:{post_id} "
            )

        (_, _, source, status) = row
        if status == 'rejected': return

        index_post = self._post_collection.get_posts(ids=[str(post_id)], include=["metadatas"])
        if not index_post["metadatas"]:
            raise IntegrityCheck(
                f"Can't find post in index db"
                f"post_id:{post_id} "
                f"source:{source}"
            )

        if len(index_post["metadatas"]) > 1:
            raise IntegrityCheck(
                f"More than one post was found in index db "
                f"post_id:{post_id} "
                f"source:{source}"
            )

        index_source = index_post["metadatas"][0]["source"]
        if source != index_source:
            raise IntegrityCheck(
                f"Different sources"
                f"post_id:{post_id} "
                f"sqlite_source:{source}"
                f"index_source:{index_source}"
            )

    @aiomisc.asyncbackoff(
        attempt_timeout=60,
        deadline=60,
        pause=2,
        max_tries=3,
        exceptions=(
                openai.error.APIConnectionError,
                openai.error.ServiceUnavailableError,
                openai.error.APIError,
        )
    )
    async def gpt_decompose(self, content):
        async with self.preprocessing_rate_limit:
            enc = tiktoken.get_encoding("cl100k_base")
            num_tokens = len(enc.encode(get_prompt("preprocess.txt") + content))

            if num_tokens > 7500:
                raise TokenLimitExceeded(num_tokens=num_tokens)

            chat_completions = await openai.ChatCompletion.acreate(
                model="gpt-3.5-turbo-0613",
                temperature=0,
                messages=[
                    {
                        "role": "system",
                        "content": get_prompt("preprocess.txt"),
                    },
                    {
                        "role": "user",
                        "content": content
                    }
                ]
            )
            tokens_used = chat_completions['usage']['total_tokens']

            log.debug(
                f"{cls_name(self)}: "
                f"Made GPT request "
                f"expected_tokens:{num_tokens} \n"
                f"used_tokens:{tokens_used} "
                f"text:'{shorten_text(content)}' \n"
                f"prompt:'{shorten_text(get_prompt('preprocess.txt'))}' \n"
            )

            try:
                resp_content = chat_completions['choices'][0]['message']["content"]
                response = json.loads(resp_content)
            except json.decoder.JSONDecodeError:
                log.warning(
                    f"{cls_name(self)}: "
                    f"Bad JSON response "
                    f"content: {resp_content}"
                )
                return 0, None

            # TODO: Job post class?
            invalid_job_post = any([
                not response.get("category"),
                response.get("category") not in ["one_job_description"],
                response.get("closed", False)
            ])
            if not invalid_job_post:
                validate(response, json_preprocess_schema)

            return tokens_used, response

    async def extract_job_postings(self, markdown_text: MarkdownPost, channel_stop_list, original_tg_link,
                                   log_info=None):
        log_info = log_info or {}

        job_infos = []

        # Find all known external links which lead to job descriptions
        known_links = JobPostingParser.find_known_external_links(markdown_text)

        # Get links we can parse
        processable_links = [
            link
            for link, can_process in known_links
            if can_process  # we may know about job hosting, by don't have parser yet
        ]

        # If found some than extract info from them
        if processable_links:
            for (content, parser, final_link, language) in await self.parser.parse(urls=processable_links):
                if not content:
                    yield (
                        content or markdown_text,
                        None,
                        None,
                        f"can't process {final_link}"
                    )
                    continue

                external = content.all_external_links()
                job_infos.append({
                    "language": language,
                    "count": 1,
                    "category": "one_job_description",
                    "is_job": True,
                    "external": external,
                    "origin_link": {
                        "type": "job_platform",
                        "description": f"Ссылка на {parser.get_name()}",
                        "link": final_link
                    },
                    "origin": cls_name(parser),
                    "content": content,
                })

        # If we found some known link, but it is currently unprocessable
        # than skip this post, and it is mostly just a link
        if known_links and not processable_links and (len(markdown_text) < 700 or len(known_links) > 8):
            yield (
                markdown_text,
                None,
                None,
                "known_links more than one, or length is small"
            )
            return

        if not processable_links and len(markdown_text) >= 500 and len(known_links) <= 1:
            external = markdown_text.all_external_links()
            if original_tg_link:
                job_infos.append({
                    "origin": "telegram",
                    "content": markdown_text,
                    "origin_link": {
                        "description": "Ссылка на Telegram пост",
                        "link": original_tg_link
                    } if original_tg_link else None,
                    "external": external
                })

        del known_links
        del processable_links

        temp_job_infos = []
        for job_info in job_infos:
            # TODO: asyncio.gather

            # Process content with GPT if it came from telegram or telegraph
            # it might contain contacts or ads
            if job_info["origin"] not in ["telegram",
                                          "TelegraphParser"]:
                temp_job_infos.append(job_info)
                continue

            try:
                log.info(
                    f'{cls_name(self)}: '
                    f'Checking with gpt, '
                    f"source:{log_info.get('source')} "
                    f"post: {log_info.get('original_tg_link')} "
                    f"channel:{shorten_text(log_info.get('channel_name'))} "
                    f'\ntext:\"{shorten_text(job_info["content"].plain())}\" '
                )

                # job_info["content"] = PostCleaner().clean_channel_ads(
                #     content=job_info["content"],
                #     language="russian",
                #     channel_stop_list=channel_stop_list
                # )

                metadata = pprint.pformat(job_info.get("external", []))
                content_for_gpt = job_info["content"].markdown() + "\n\nExternal metadata:\n\n" + metadata

                _, gpt_job_info = await self.gpt_decompose(content_for_gpt)
                if not gpt_job_info:
                    continue

                job_info.update(gpt_job_info)
                temp_job_infos.append(job_info)
            except jsonschema.exceptions.ValidationError as e:
                log.warning(
                    f"{cls_name(self)}: "
                    f"Skipping, corrupted GPT request or response "
                    f"source:{log_info.get('source')} "
                    f"post: {log_info.get('original_tg_link')} "
                    f"channel:{shorten_text(log_info.get('channel_name'))} "
                    f"\ntext:{shorten_text(job_info['content'].plain())}"
                    f"\nerr:{str(e)}"
                )
                continue
            except TokenLimitExceeded as e:
                log.warning(f"{cls_name(self)}: "
                            f"Skipping, GPT token limit will be exceeded"
                            f"num_tokens:{e.num_tokens} "
                            f"source:{log_info.get('source')} "
                            f"post: {log_info.get('original_tg_link')} "
                            f"channel:{shorten_text(log_info.get('channel_name'))} "
                            f"\ntext:{shorten_text(job_info['content'].plain())}")
                continue

        job_infos = temp_job_infos
        del temp_job_infos

        for k, job_info in enumerate(job_infos):
            if not job_info.get("category") or job_info.get("category") not in ["one_job_description"]:
                log.info(
                    f"{cls_name(self)}: "
                    f"Skip, wrong category"
                    f"category:'{job_info.get('category')}' "
                    f"source:{log_info.get('source')} "
                    f"post: {log_info.get('original_tg_link')} "
                )
                yield (
                    job_info.get("content"),
                    TelegramTextTools.prepare_more_for_tg(job_info.get("external", [])),
                    job_info.get("language").upper() if job_info.get("language") else None,
                    f"wrong category: {job_info.get('category')}"
                )
                continue

            if job_info.get("closed", False):
                log.info(
                    f"{cls_name(self)}: "
                    f"Skip, job posting closed "
                    f"source:{log_info.get('source')} "
                    f"post: {log_info.get('original_tg_link')} "
                )
                yield (
                    job_info.get("content"),
                    TelegramTextTools.prepare_more_for_tg(job_info.get("external", [])),
                    job_info.get("language").upper() if job_info.get("language") else None,
                    "job closed"
                )
                continue

            if not job_info.get("language"):
                log.warning(
                    f"{cls_name(self)}: "
                    f"Skip, unexpected behaviour - language empty "
                    f"source:{log_info.get('source')} "
                    f"post: {log_info.get('original_tg_link')} "
                    f"channel:{shorten_text(log_info.get('channel_name'))} "
                    f"text: {shorten_text(markdown_text.plain())}"
                )
                yield (
                    job_info.get("content"),
                    TelegramTextTools.prepare_more_for_tg(job_info.get("external", [])),
                    job_info.get("language").upper() if job_info.get("language") else None,
                    "language empty"
                )
                continue

            if not job_info.get("origin"):
                log.warning(
                    f"{cls_name(self)}: "
                    f"Skip, unexpected behaviour - origin empty "
                    f"source:{log_info.get('source')} "
                    f"post: {log_info.get('original_tg_link')} "
                    f"channel:{shorten_text(log_info.get('channel_name'))} "
                    f"text: {shorten_text(markdown_text.plain())}"
                )
                yield (
                    job_info.get("content"),
                    TelegramTextTools.prepare_more_for_tg(job_info.get("external", [])),
                    job_info.get("language").upper() if job_info.get("language") else None,
                    "origin empty"
                )
                continue

            if not job_info.get("content"):
                log.warning(
                    f"{cls_name(self)}: "
                    f"Skip, unexpected behaviour - content field missing "
                    f"source:{log_info.get('source')} "
                    f"post: {log_info.get('original_tg_link')} "
                    f"channel:{shorten_text(log_info.get('channel_name'))} "
                    f"text: {shorten_text(markdown_text.plain())}"
                )
                yield (
                    job_info.get("content"),
                    TelegramTextTools.prepare_more_for_tg(job_info.get("external", [])),
                    job_info.get("language").upper() if job_info.get("language") else None,
                    "content missing"
                )
                continue

            if len(job_info.get("content").plain().strip()) == 0:
                log.warning(
                    f"{cls_name(self)}: "
                    f"Skip, unexpected behaviour - content empty "
                    f"source:{log_info.get('source')} "
                    f"post: {log_info.get('original_tg_link')} "
                    f"channel:{shorten_text(log_info.get('channel_name'))} "
                    f"text: {shorten_text(markdown_text.plain())}"
                )
                yield (
                    job_info.get("content"),
                    TelegramTextTools.prepare_more_for_tg(job_info.get("external", [])),
                    job_info.get("language").upper() if job_info.get("language") else None,
                    "content empty"
                )
                continue

            # if not job_info.get("external"):
            #     log.warning(
            #         f"{cls_name(self)}: "
            #         f"Skip, unexpected behaviour - not contacts or an external urls "
            #         f"source:{log_info.get('source')} "
            #         f"post: {log_info.get('original_tg_link')} "
            #         f"channel:{shorten_text(log_info.get('channel_name'))} "
            #         f"text: {shorten_text(markdown_text.plain())}"
            #     )
            #     yield (
            #         job_info.get("content"),
            #         TelegramTextTools.prepare_more_for_tg(job_info.get("external", [])),
            #         job_info.get("language").upper() if job_info.get("language") else None,
            #         "no external"
            #     )
            #     continue

            for i in reversed(range(len(job_info.get("external", [])))):
                link = job_info["external"][i]["link"]
                origin = job_info.get("origin")
                _type = job_info["external"][i].get("type")
                if not _type and origin in ["telegram", "TelegraphParser"]:
                    log.warning(
                        f"{cls_name(self)}: "
                        f"Link without type "
                        f"data:'{link}' "
                        f"origin:'{origin}' "
                    )
                    continue

                if not any([
                    TelegramTextTools.validate_url(link),
                    TelegramTextTools.validate_email(link),
                    TelegramTextTools.validate_phone_number(link),
                ]):
                    del job_info["external"][i]
                    log.warning(
                        f"{cls_name(self)}: "
                        f"Not valid external "
                        f"data:'{link}' "
                        f"text:'{shorten_text(markdown_text.plain())}'"
                    )
                    continue

            at_least_one_info_link = any([
                item.get("type", "unknown") not in [
                    "channel",
                    "unknown"
                ]
                for item in job_info["external"]
            ])

            if not at_least_one_info_link and job_info["origin"] in ["telegram", "TelegraphParser"]:
                log.warning(
                    f"{cls_name(self)}: "
                    f"No external "
                    f"source:{log_info.get('source')} "
                    f"post: {log_info.get('original_tg_link')} "
                    f"channel:{shorten_text(log_info.get('channel_name'))} "
                    f"text: {shorten_text(markdown_text.plain())}"
                )

            # Do additional text processing for telegram and telegraph
            if job_info["origin"] in ["telegram", "TelegraphParser"]:
                job_info["content"] = PostCleaner().clean_channel_ads(
                    content=job_info["content"],
                    language=job_info["language"],
                    channel_stop_list=channel_stop_list
                )

                job_info["content"] = PostCleaner().clean_contact_details(
                    content=job_info["content"],
                    language=job_info["language"],
                    external=job_info["external"],
                    section_headers=job_info["section_headers"]
                )

            # Turn off all links from post
            job_info["content"] = job_info["content"].turn_off_links()

            # Fucking weird, but yeah, another clean up
            # That is has to be done here, because post_clean_up fixing headers
            # If done on stage of parsing, it might break the post_clean_up
            job_info["content"] = job_info["content"].clear()

            more_info = job_info["external"]
            if job_info.get("origin_link"):
                more_info.append(job_info.get("origin_link"))

            yield (
                job_info["content"],
                TelegramTextTools.prepare_more_for_tg(job_info["external"]),
                job_info["language"].upper(),
                None
            )

    async def check_and_save(self, db, post_candidate, channel_name):
        markdown_post = MarkdownPost(post_candidate.raw_text, post_candidate.entities)
        markdown_post += extract_button_text(post_candidate)
        post_candidate_date = str_utc_time(post_candidate.date)

        source, original_tg_link = get_original_pid_cid(post_candidate)
        channel_stop_list = get_stop_list(source)

        log_info = {
            "source": source,
            "original_tg_link": original_tg_link,
            "channel_name": channel_name
        }

        cursor = await db.execute(GET_POST_BY_SOURCE, [source])

        # WARNING:
        # Currently we check whether the telegram post
        # has been processed, it might lead to job
        # postings missing if only half of digest been processed
        if await cursor.fetchone():
            log.debug(
                f"{cls_name(self)}: "
                f"Skip processing post already in db"
                f"source:{source} "
                f"post: {original_tg_link} "
                # f"channel:{shorten_text(channel_name)} "
                # f"text: {shorten_text(post_text_with_button)}"
            )
            return

        if post_candidate.reactions and is_negative_sentiment(post_candidate.reactions.results):
            log.info(
                f"{cls_name(self)}: "
                f"Skipping, negative sentiment "
                f"source:{source} "
                f"post: {original_tg_link} "
            )
            await safe_db_execute(
                db, INSERT_INTO_POSTS, (
                    None,
                    markdown_post.plain(),
                    post_candidate_date,
                    source,
                    'rejected',
                    'negative sentiment',
                    None,
                    None,
                    original_tg_link,
                    markdown_post.json_entities(),
                )
            )
            return

        log.debug(
            f"{cls_name(self)}: "
            f"Start processing candidate"
            f"source:{source} "
            f"post: {original_tg_link} "
            # f"channel:{shorten_text(channel_name)} "
            # f"text: {shorten_text(post_text_with_button)}"
        )

        at_least_one_returned = False
        async for job_posting in self.extract_job_postings(markdown_text=markdown_post,
                                                           log_info=log_info,
                                                           channel_stop_list=channel_stop_list,
                                                           original_tg_link=original_tg_link):

            at_least_one_returned = True

            (content, more_info, language, reject_reason,) = job_posting
            if reject_reason:
                await safe_db_execute(
                    db, INSERT_INTO_POSTS, (
                        None,
                        content.plain() if content else None,
                        post_candidate_date,  # TODO: Get from parser?
                        source,
                        'rejected',
                        reject_reason,
                        more_info or None,
                        language or None,
                        original_tg_link,
                        content.json_entities() if content else None,
                    )
                )
                continue

            log.debug(
                f'{cls_name(self)}: '
                f'Start handling sub-post, '
                f'more_info:{more_info} '
                f'text:{shorten_text(content.plain())} '
            )

            try:
                similar_post, embedding = await self._post_collection.find_same_post(db, content.plain())
            except openai.error.InvalidRequestError as e:
                log.warning(
                    f'{cls_name(self)}: '
                    f'Skip, can\'t find same post, '
                    f'err: {e} '
                    f'text:{shorten_text(content.plain())} '
                )
                continue

            if similar_post:
                log.info(
                    f'{cls_name(self)}: '
                    f'Found same post in our channel, '
                    f'match_ratio:{similar_post["match_ratio"]:.2f} '
                    f'index_distance:{similar_post["index_distance"]:.3f} '
                    f'more_info:{more_info} '
                    f'fc_source:{source} '
                    f"post: {original_tg_link} "
                    f'\ntext:\"{shorten_text(content.plain())}\" '
                    f'\nsame_text:\"{shorten_text(similar_post["text"])}\" '
                )

            # Save post id for father easier SQL query on processing stage
            cursor = await safe_db_execute(
                db, INSERT_INTO_POSTS, (
                    None,
                    content.plain(),
                    post_candidate_date,  # TODO: Get from parser?
                    source,
                    'rejected' if similar_post else 'accepted',
                    f'found similar post - pid:{similar_post["post_id"]} ' if similar_post else None,
                    more_info,
                    language,
                    original_tg_link,
                    content.json_entities(),
                )
            )
            post_id = cursor.lastrowid

            log.info(
                f'{cls_name(self)}: '
                f'Post added to db, '
                f'status:{"rejected" if similar_post else "accepted"} '
                f'source:{source} '
                f"post: {original_tg_link} "
                f'post_id:{post_id} '
            )

            # if no similar found, than we gonna use it, so forward and create the index
            if not similar_post:
                # Add to index store for filtering on processing stage
                success, tokens_used = await self._post_collection.insert_post(text=content,
                                                                               post_id=str(post_id),
                                                                               source=source,
                                                                               embedding=embedding)
                if not success: continue
                log.info(
                    f'{cls_name(self)}: '
                    f'Created index for post, '
                    f'source:{source} '
                    f"post: {original_tg_link} "
                    f'post_id:{post_id} '
                    f'tokens_used:{tokens_used} '
                )

            await self.db_integrity_check(db, post_id)
            await db.commit()

        if not at_least_one_returned:
            await safe_db_execute(
                db, INSERT_INTO_POSTS, (
                    None,
                    post_candidate.raw_text,
                    post_candidate_date,  # TODO: Get from parser?
                    source,
                    'rejected',
                    "not available postings",
                    None,
                    None,
                    original_tg_link,
                    None,
                )
            )
            await db.commit()

    async def iter_and_save(self, db):
        for peer, channel_name, stop_list in ACTIVE_CHANNELS:
            log.info(f"{cls_name(self)}: "
                     f"Start checking channel: {shorten_text(channel_name)}")

            # Try to get from cache first
            channel = self.client.session.get_input_entity(peer)
            if not channel:
                channel = await self.client.get_input_entity(peer)

            async for post_candidate in iter_channel_messages(self.client, channel, cutoff_date=SEARCH_CUTOFF_DATE):
                await self.check_and_save(db, post_candidate, channel_name)
                await db.commit()

    async def handle_on_channel_updates(self, event: events.NewMessage):
        forward_candidate = event.message
        channel_name = event.chat.title
        source, original_tg_link = get_original_pid_cid(forward_candidate)

        log.info(
            f"{cls_name(self)}: "
            f"Subscribe update, new post received "
            f"source:{source} "
            f"post: {original_tg_link} "
        )

        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            await self.check_and_save(db, forward_candidate, channel_name)
            await db.commit()

    async def subscribe_on_channel_update(self):
        for peer, channel_name, _ in ACTIVE_CHANNELS:
            log.info(f"{cls_name(self)}: "
                     f"Subscribe on update from "
                     f"cid:{peer.channel_id} "
                     f"channel:\"{shorten_text(channel_name)}\"")

            self.client.on(events.NewMessage(chats=peer))(self.handle_on_channel_updates)

    async def start(self):
        log.info(f"{cls_name(self)}: Starting service")
        self.start_event.set()

        context = get_context()

        log.info(f"{cls_name(self)}: Waiting for telegram session")
        try:
            self.client = await asyncio.wait_for(context['tg_client'], 3)
        except asyncio.exceptions.TimeoutError:
            log.warning(
                f"{cls_name(self)}: "
                f"Exiting: Haven't received telegram session"
            )
            return

        log.info(f"{cls_name(self)}: Waiting for ChromaDB index")
        try:
            index_posts = await asyncio.wait_for(context['index_posts'], 3)
        except asyncio.exceptions.TimeoutError:
            log.warning(
                f"{cls_name(self)}: "
                f"Exiting: Haven't received ChromaDB index"
            )
            return

        log.info(f"{cls_name(self)}: Waiting for GPT embedding function")
        try:
            self.create_embedding = await asyncio.wait_for(context['create_embedding'], 3)
        except asyncio.exceptions.TimeoutError:
            log.warning(
                f"{cls_name(self)}: "
                f"Exiting: Haven't received GPT embedding function"
            )
            return

        log.info(f"{cls_name(self)}: Waiting for SQLite3")
        try:
            await asyncio.wait_for(context["sqlite_ready"], 3)
        except asyncio.exceptions.TimeoutError:
            log.warning(
                f"{cls_name(self)}: "
                f"Exiting: Haven't received SQLite3 db"
            )
            return

        log.info(f"{cls_name(self)}: Waiting for job parser")
        try:
            self.parser = await asyncio.wait_for(context["parser"], 3)
        except asyncio.exceptions.TimeoutError:
            log.warning(
                f"{cls_name(self)}: "
                f"Exiting: Haven't received  job posts parser"
            )
            return

        self._post_collection = PostsCollection(index_posts, self.create_embedding)

        log.info(f"{cls_name(self)}: Setup GPT rate limiter")
        self.preprocessing_rate_limit = AsyncLimiter(max_rate=20, time_period=60)

        # Set the OpenAI API key
        assert (os.getenv("OPENAI_API_KEY") is not None)
        openai.api_key = os.getenv("OPENAI_API_KEY")

        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            log.info(f"{cls_name(self)}: Removing old posts")
            await self.remove_old_posts(db)

            log.info(f"{cls_name(self)}: Syncing index with db")
            await self.sync_index_and_db(db)

        log.info(f"{cls_name(self)}: Subscribe on new posts")
        await self.subscribe_on_channel_update()

        while True:
            log.info(f"{cls_name(self)}: Enter periodical syncing")
            try:
                async with aiosqlite.connect(SQLLite3Service.db_path) as db:
                    await self.iter_and_save(db)
            except telethon.errors.rpcerrorlist.FloodWaitError as e:
                log.warning(f"{cls_name(self)}: You fucked up bro, flood wait:{str(e)}")
                await asyncio.sleep(e.seconds)
                continue
            except asyncio.exceptions.TimeoutError:
                await asyncio.sleep(5)
                continue
            except Exception as e:
                log.exception(e)

            log.info(
                f"{cls_name(self)} "
                f"Periodical syncing sleep {self.channel_sync_period or 600} seconds"
            )
            await asyncio.sleep(self.channel_sync_period or 600)

    async def stop(self, *args, **kwargs):
        for future in self.futures.keys():
            await future
        log.info(f"{cls_name(self)}: Stopped service")


class PostCleaner:
    STOP_LIST = [
        # "отклик",
        # "t.me",
        # "ваканс",
        # "канал",
        # "контакт",
        # "связь",
        # "подробнее",
        # "публикатор"
        # *TelegraphParser.get_domains(),
        # *HeadHunterParser.get_domains(),
        # *GeekJobsParser.get_domains(),
        # *HabrParser.get_domains(),
        # *KnownNoneParser.get_domains(),
    ]

    def clean_channel_ads(self, content, language, channel_stop_list=None):
        channel_stop_list = channel_stop_list or {}

        for text, remove_type in channel_stop_list.items():
            if remove_type == "chunk":
                if content.plain().count(text) == 1:
                    content = content.replace(text, "")
                else:
                    log.warning(
                        f"{cls_name(self)} "
                        f"Skipping removing stop list chunk "
                        f"chunk: {text} "
                    )
            elif remove_type == "sentence":
                content = TelegramTextTools.remove_substrings(content, [text], language=language)
            else:
                raise NotImplementedError(remove_type)

        return content.strip()

    def clean_contact_details(self, content, language, external, section_headers):
        if not content: raise ValueError()
        if not language: raise ValueError()

        content_for_removal = self.STOP_LIST

        for link_info in external or []:
            if link_info["type"] in ["company", "form", "project"]:
                continue

            # content_for_removal += TelegramTextTools.get_sentences(
            #     original_text=MarkdownPost(link_info["fragment"]).plain(),
            #     language=language
            # )
            content_for_removal.append(TelegramTextTools.extract_info(link_info["link"]))
            content_for_removal.append(TelegramTextTools.extract_info(link_info["link_text"]))

        content = TelegramTextTools.remove_substrings(
            text=content,
            substrings=content_for_removal,
            language=language
        )

        # Remove fucking emojis and make bold
        for header in section_headers or []:
            content = content.fix_header(header)

        return content

    # @aiomisc.asyncbackoff(
    #     attempt_timeout=60,
    #     deadline=60,
    #     pause=2,
    #     max_tries=3,
    #     exceptions=(
    #             openai.error.APIConnectionError,
    #             openai.error.ServiceUnavailableError,
    #     )
    # )
    # async def gpt_check_relevance(self, content) -> (int, bool):
    #     async with self.preprocessing_rate_limit:
    #         enc = tiktoken.get_encoding("cl100k_base")
    #         num_tokens = len(enc.encode(get_prompt("relevant.txt") + content))
    #
    #         if num_tokens > 500:
    #             raise TokenLimitExceeded(num_tokens=num_tokens)
    #
    #         chat_completions = await openai.ChatCompletion.acreate(
    #             model="gpt-3.5-turbo-0613",
    #             temperature=0,
    #             messages=[
    #                 {
    #                     "role": "system",
    #                     "content": get_prompt("relevant.txt"),
    #                 },
    #                 {
    #                     "role": "user",
    #                     "content": content
    #                 }
    #             ]
    #         )
    #
    #         tokens_used = chat_completions['usage']['total_tokens']
    #
    #         log.debug(
    #             f"{cls_name(self)}: "
    #             f"Made GPT spam request "
    #             f"expected_tokens:{num_tokens} \n"
    #             f"used_tokens:{tokens_used} "
    #             f"text:'{shorten_text(content)}' \n"
    #             f"prompt:'{shorten_text(get_prompt('relevant.txt'))}' \n"
    #         )
    #
    #         response = json.loads(chat_completions['choices'][0]['message']["content"].lower())
    #         validate(response, prompts.relevant.schema)
    #         return tokens_used, response["result"]
