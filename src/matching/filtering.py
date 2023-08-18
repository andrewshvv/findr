import asyncio
import json
import logging
import math
import sqlite3
from pprint import pprint

import aiomisc
import aiosqlite
import numpy as np
import openai
from aiolimiter import AsyncLimiter
from aiomisc import get_context
from chromadb.api.models.Collection import Collection
from jsonschema import validate
from pyee import AsyncIOEventEmitter
from telethon.sync import TelegramClient

from common.db import safe_db_execute
from common.logging import cls_name, shorten_text
from common.utils import get_prompt
from db.sqlite import INSERT_OR_IGNORE_USER_POSTS, SQLLite3Service, GET_POSTS_FOR_PROCESSING, GET_POST_BY_PID, \
    GET_PROMPT_BASE_DISTANCE, SET_PROMPT_BASE_DISTANCE, PROMPTS_SET_FIRST_SEARCH_READY
from gpt.schemas.filter import schema as filter_schema
from matching.utils import group_user_data_for_gpt_check, group_user_data_for_index
from preprocessing.post_sourser import CUTFOFF_DAYS

log = logging.getLogger(__name__)


class JobDescriptionsCheck(aiomisc.Service):
    emitter: AsyncIOEventEmitter = None
    rate_limit: AsyncLimiter = None
    client: TelegramClient = None

    index_posts: Collection = None
    index_prompts: Collection = None
    lock: asyncio.Lock = None

    @aiomisc.asyncbackoff(
        attempt_timeout=30,
        deadline=60,
        pause=2,
        max_tries=20,
        exceptions=(
                openai.error.APIConnectionError,
                openai.error.ServiceUnavailableError,
        )
    )
    async def gpt_check(self, text, prompt):
        if not text and len(text.strip()) == 0:
            # safety check
            raise NotImplementedError

        if not prompt and len(prompt.strip()) == 0:
            # safety check
            raise NotImplementedError

        async with self.rate_limit:
            chat_completions = await openai.ChatCompletion.acreate(
                model="gpt-3.5-turbo-0613",
                temperature=0,
                messages=[
                    {
                        "role": "system",
                        "content": get_prompt("filter.txt"),
                    },
                    {
                        "role": "user",
                        "content": f"Job description:\n {text} \n\n User search request:\n {prompt}"
                    }
                ]
            )

            tokens_used = chat_completions['usage']['total_tokens']
            response = json.loads(chat_completions['choices'][0]['message']["content"])
            validate(response, filter_schema)
            return tokens_used, response["query_result"], response.get("reason", "")

    async def do_gpt_check(self, db, rows, post_id, prompt, prompt_id, index_distance, user_id):
        cursor = await safe_db_execute(db, GET_POST_BY_PID, [post_id])
        row = await cursor.fetchone()
        if not row: raise NotImplementedError()
        (post_text,) = row

        _, is_gpt_accepted, reason = await self.gpt_check(post_text, prompt)
        reason = reason.capitalize()

        rows[(post_id, prompt_id)]["process_status"] = "accepted" if is_gpt_accepted else "rejected"
        rows[(post_id, prompt_id)]["gpt_reason"] = reason

        if is_gpt_accepted:
            log.info(
                f"{cls_name(self)} "
                f"GPT check ({'accepted' if is_gpt_accepted else 'rejected'}) "
                f"prid:{prompt_id} "
                f"pid:{post_id} "
                f"uid:{user_id} "
                f"distance: {index_distance} "
                f"prompt:'{shorten_text(prompt)}' "
                f"text:'{shorten_text(post_text)}' "
                f"reason:{reason} "
            )

            await safe_db_execute(
                db, INSERT_OR_IGNORE_USER_POSTS, [
                    user_id,
                    post_id,
                    prompt_id,
                    "accepted",
                    reason,
                    index_distance,
                ]
            )
            await db.commit()
            self.emitter.emit("search_result", **{
                "user_id": user_id,
                "prompt_id": prompt_id,
                "post_id": post_id,
                "source": "gpt"
            })

        return is_gpt_accepted

    async def find_matching_posts(self, db):
        rows = {
            (row[3], row[0]): {
                "prompt_id": row[0],
                "prompt": row[1],
                "prompt_status": row[2],
                "post_id": row[3],
                "user_id": row[4],
                "process_status": row[5],
                "index_distance": row[6],
            } async for row in await safe_db_execute(db, GET_POSTS_FOR_PROCESSING.format(days=CUTFOFF_DAYS))
        }

        if len(rows) == 0:
            log.debug(
                f"{cls_name(self)} "
                f"Skipping, no posts<>prompt pairs for filtering"
            )
            return

        log.info(
            f"{cls_name(self)} "
            f"Processing posts<>prompt pairs "
            f"num:{len(rows)}"
        )

        # Mutates the statuses of rows dictionary data
        await self.apply_index_check(db, rows)

        for (
                user_id,
                prompt_id,
                prompt,
                is_all_rejected,
                is_first_search,
                post_ids,
                post_statuses,
                index_distances,
        ) in group_user_data_for_gpt_check(rows.values()):
            posts = list(enumerate(
                sorted(
                    zip(post_ids, post_statuses, index_distances),
                    key=lambda key: key[2]
                )
            ))

            if is_first_search:
                self.emitter.emit("start_first_search", **{
                    "user_id": user_id,
                    "prompt_id": prompt_id,
                })

            futures = [
                self.do_gpt_check(
                    db=db,
                    rows=rows,
                    post_id=post_id,
                    prompt=prompt,
                    prompt_id=prompt_id,
                    index_distance=index_distance,
                    user_id=user_id
                )
                for (n, (post_id, post_status, index_distance)) in posts
                if post_status == 'index_approved'
            ]

            gpt_results = await asyncio.gather(*futures)
            num_posts = sum(gpt_results)
            is_no_gpt_accepted_posts = num_posts == 0

            # if no posts have been accepted, send at least the closest one
            # one first search request
            # if is_no_gpt_accepted_posts and is_first_search:
            #     for (
            #             n,
            #             (
            #                     post_id,
            #                     post_status,
            #                     index_distance
            #             )
            #     ) in posts:
            #         if post_status != 'index_approved': continue
            #
            #         if n >= 1: break
            #         rows[(post_id, prompt_id)]["process_status"] = 'accepted'
            #         num_posts += 1
            #         # TODO: Add sqlite save
            #         self.emitter.emit("search_result", **{
            #             "user_id": user_id,
            #             "prompt_id": prompt_id,
            #             "post_id": post_id,
            #             "source": "index"
            #         })

            if is_first_search:
                # Send none result for the UI, so that it know the search has ended
                await safe_db_execute(db, PROMPTS_SET_FIRST_SEARCH_READY, [prompt_id])
                self.emitter.emit("search_ended", **{
                    "user_id": user_id,
                    "prompt_id": prompt_id,
                    "num_posts": num_posts,
                    "source": None if is_no_gpt_accepted_posts else "gpt"
                })
                await db.commit()
                log.info(
                    f"{cls_name(self)} "
                    f"Search finished "
                    f"prompt_id:{prompt_id} "
                    f"uid:{user_id} "
                    f"num_posts:{num_posts} "
                )

            for (
                    n,
                    (
                            post_id,
                            _,
                            index_distance
                    )
            ) in posts:
                await safe_db_execute(
                    db, INSERT_OR_IGNORE_USER_POSTS, [
                        user_id,
                        post_id,
                        prompt_id,
                        rows[(post_id, prompt_id)]["process_status"],
                        rows[(post_id, prompt_id)].get("gpt_reason", "").capitalize(),
                        index_distance,
                    ]
                )
            await db.commit()

    @staticmethod
    async def get_percentile_distance(db, prompt_id, distances):
        cursor = await safe_db_execute(db, GET_PROMPT_BASE_DISTANCE, [prompt_id])
        row = await cursor.fetchone()
        if not row:
            return None

        (percentile_distance,) = row

        if not percentile_distance:
            percentile_distance = np.percentile(distances, 2)
            await safe_db_execute(db, SET_PROMPT_BASE_DISTANCE, [percentile_distance, prompt_id])
        else:
            percentile_distance = float(percentile_distance)

        return percentile_distance

    async def apply_index_check(self, db, rows):
        if len(rows) == 0:
            return

        for user_id, prompt_id, post_ids in group_user_data_for_index(rows.values()):
            if len(post_ids) == 1:
                query = {"post_id": {"$eq": str(post_ids[0])}}

            elif len(post_ids) > 1:
                query = {
                    "$or": [
                        {"post_id": {"$eq": str(post_id)}}
                        for post_id in post_ids
                    ]
                }
            else:
                raise ValueError(f"{cls_name(self)}: Unexpected number of post ids: {len(post_ids)}")

            results = self.index_prompts.get(
                ids=[str(prompt_id)],
                include=["embeddings"]
            )
            if len(results["embeddings"]) != 1:
                log.warning(
                    f"{cls_name(self)} "
                    f"Something is wrong, unable to find prompt embedding "
                    f"user_id:{user_id} "
                    f"prompt_id:{prompt_id} "
                )
                continue

            prompt_embedding = results["embeddings"][0]

            try:
                results = self.index_posts.query(
                    query_embeddings=[prompt_embedding],
                    n_results=len(post_ids),
                    include=["distances", "metadatas"],
                    where=query
                )
            except RuntimeError as e:
                log.warning(
                    f"{cls_name(self)} "
                    f"Unable to make query search "
                    f"prompt_id:{prompt_id} "
                    f"err: {str(e)}"
                )
                continue

            distances = results["distances"][0]
            metadatas = results["metadatas"][0]

            if len(distances) == 0:
                log.warning(
                    f"{cls_name(self)} "
                    f"Something is wrong, distances is empty"
                    f"prompt_id:{prompt_id} "
                )
                raise NotImplementedError

            percentile_distance = await JobDescriptionsCheck.get_percentile_distance(db, prompt_id, distances)
            if not percentile_distance:
                continue

            min_num = math.floor(len(distances) * 1 / 100)
            max_num = math.ceil(len(metadatas) * 3 / 100)

            n_allowed = 0
            for distance, metadata in zip(distances, metadatas):
                post_id = int(metadata["post_id"])

                is_allow_by_default = n_allowed < min_num
                is_distance_good = distance < percentile_distance
                is_maximum_reached = n_allowed >= max_num

                # allow top 1% by default, after that use distance
                is_approved = (is_allow_by_default or is_distance_good) and not is_maximum_reached
                if is_approved:
                    rows[(post_id, prompt_id)]["index_distance"] = distance
                    rows[(post_id, prompt_id)]["process_status"] = "index_approved"

                    log.info(
                        f"{cls_name(self)} "
                        f"Approved by index db "
                        f"user_id:{user_id} "
                        f"pid:{post_id} "
                        f"prompt_id:{prompt_id} "
                        f"distance:{distance:.4f} < {percentile_distance:.4f}"
                    )
                    n_allowed += 1
                else:
                    rows[(post_id, prompt_id)]["index_distance"] = distance
                    rows[(post_id, prompt_id)]["process_status"] = "rejected"

                    log.debug(
                        f"{cls_name(self)}: Rejected by index db "
                        f"pid:{post_id} "
                        f"user_id:{user_id} "
                        f"prompt_id:{prompt_id} "
                        f"distance:{distance:.4f} > {percentile_distance:.4f}"
                    )

    async def on_new_approved_prompt(self, *args, **kwargs):
        async with self.lock:
            try:
                async with aiosqlite.connect(SQLLite3Service.db_path) as db:
                    await self.find_matching_posts(db)
            except (openai.error.ServiceUnavailableError, sqlite3.OperationalError) as e:
                log.warning(f"{cls_name(self)}: Sleeping for 20 seconds, reason: {str(e)}")
                await asyncio.sleep(20)
            except Exception as e:
                log.exception(e)

    async def print_event(self, *args, **kwargs):
        pprint(kwargs)

    async def start(self):
        log.info(f"{cls_name(self)}: Entered service")
        self.start_event.set()
        self.lock = asyncio.Lock()

        context = get_context()

        log.info(f"{cls_name(self)}: Waiting for telegram session")
        try:
            if not self.client:
                self.client = await asyncio.wait_for(context['tg_client'], 3)
        except asyncio.exceptions.TimeoutError:
            log.warning(
                f"{cls_name(self)}: "
                f"Exiting: Haven't received telegram session"
            )
            return

        if not self.rate_limit:
            self.rate_limit = AsyncLimiter(max_rate=20, time_period=60)

        log.info(f"{cls_name(self)}: Waiting for ChromaDB posts collection")
        try:
            if not self.index_posts:
                self.index_posts = await asyncio.wait_for(context['index_posts'], 3)
        except asyncio.exceptions.TimeoutError:
            log.warning(
                f"{cls_name(self)}: "
                f"Exiting: Haven't received ChromaDB posts collection"
            )
            return

        log.info(f"{cls_name(self)}: Waiting for SQLite3 to be ready")
        try:
            await asyncio.wait_for(context['sqlite_ready'], 3)
        except asyncio.exceptions.TimeoutError:
            log.warning(
                f"{cls_name(self)}: "
                f"Exiting: Haven't received SQLite3 to be ready event"
            )
            return

        log.info(f"{cls_name(self)}: Waiting for ChromaDB prompts collection")
        try:
            if not self.index_prompts:
                self.index_prompts = await asyncio.wait_for(context['index_prompts'], 3)
        except asyncio.exceptions.TimeoutError:
            log.warning(
                f"{cls_name(self)}: "
                f"Exiting: Haven't received ChromaDB prompts collection"
            )
            return

        self.emitter.on("new_approved_prompt", self.on_new_approved_prompt)
        self.emitter.on("search_ended", self.print_event)
        self.emitter.on("search_result", self.print_event)

        log.info(f"{cls_name(self)}: Start checking / filtering")
        while True:
            await self.on_new_approved_prompt()
            await asyncio.sleep(10)

    async def stop(self, *args, **kwargs):
        # for future in self._futures.keys():
        #     await future
        # log.info(f"{cls_name(self)}: Stopped service")
        pass
