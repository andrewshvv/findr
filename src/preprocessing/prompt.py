import asyncio
import json
import logging
from pprint import pprint

import aiomisc
import aiosqlite
import openai
import tiktoken
from aiolimiter import AsyncLimiter
from aiomisc import get_context
from chromadb.api.models.Collection import Collection
from jsonschema import validate
from pyee import AsyncIOEventEmitter

from common.db import safe_db_execute
from common.exceptions import TokenLimitExceeded
from common.logging import cls_name, shorten_text
from common.utils import get_prompt, print_event, group_list
from db.sqlite import SQLLite3Service, UPDATE_PROMPT, GET_PROMPTS_FOR_PROCESSING, GET_ALL_APPROVED_PROMPTS, \
    COUNT_APPROVED_PROMPTS

from gpt.schemas.prompt_check_short import schema as json_preprocess_schema_short
from gpt.schemas.prompt_check_long import schema as json_preprocess_schema_long

log = logging.getLogger(__name__)


class PromptTranslate(aiomisc.Service):
    delete_posts_transient: bool = False
    log_posts_for_forwarding: bool = False
    emitter: AsyncIOEventEmitter = None

    rate_limit: AsyncLimiter = None
    index_prompts: Collection = None
    create_embedding = None
    lock: asyncio.Lock = None

    @aiomisc.asyncbackoff(
        attempt_timeout=60,
        deadline=60,
        pause=2,
        max_tries=10,
        exceptions=(
                openai.error.APIConnectionError,
                openai.error.ServiceUnavailableError,
        )
    )
    async def preprocess_prompt(self, prompt):
        enc = tiktoken.get_encoding("cl100k_base")
        num_tokens = len(enc.encode(prompt)) + 300  # approximate system message tokens

        if num_tokens <= 400:
            is_long = False
            model = "gpt-4-0613"
            gpt_system_message = "prompt_check_short.txt"
        else:
            model = "gpt-3.5-turbo-0613"
            is_long = True
            gpt_system_message = "prompt_check_long.txt"

        if num_tokens >= 6000:
            raise TokenLimitExceeded(num_tokens)

        async with self.rate_limit:
            chat_completions = await openai.ChatCompletion.acreate(
                model=model,
                temperature=0,
                messages=[
                    {
                        "role": "system",
                        "content": get_prompt(gpt_system_message),
                    },
                    {
                        "role": "user",
                        "content": prompt,
                    }
                ]
            )

        tokens_used = chat_completions['usage']['total_tokens']
        response = chat_completions['choices'][0]['message']["content"]

        try:
            json_response = json.loads(response)
        except json.decoder.JSONDecodeError:
            log.warning(
                f"{cls_name(self)}: "
                f"Corrupter GPT response "
                f"num:{response}"
            )
            return tokens_used, None

        pprint(json_response)
        if is_long:
            validate(json_response, json_preprocess_schema_long)
        else:
            validate(json_response, json_preprocess_schema_short)
        return tokens_used, json_response, is_long

    async def on_new_prompt(self, *args, **kwargs):
        async with self.lock:
            async with aiosqlite.connect(SQLLite3Service.db_path) as db:
                try:
                    await self.process_new_prompts(db)
                except Exception as e:
                    log.exception(e)

    async def process_new_prompts(self, db):
        rows = [
            row
            async for row in await safe_db_execute(db, GET_PROMPTS_FOR_PROCESSING)
        ]

        if len(rows) == 0:
            return

        log.info(
            f"{cls_name(self)}: "
            f"Processing prompts "
            f"num:{len(rows)} "
        )

        for prompt, prompt_id, user_id in rows:
            _, response, is_long = await self.preprocess_prompt(prompt)
            if not response:
                continue

            if not response["is_valid"] or (not is_long and len(response.get('position_tags_cloud', 0)) == 0):
                log.warning(
                    f"{cls_name(self)}: "
                    f"Prompt rejected, not a job search request "
                    f"prid:{prompt_id} "
                    f"prompt:'{shorten_text(prompt)}' "
                )
                await safe_db_execute(db, UPDATE_PROMPT, ['rejected', None, None, prompt_id])
                await db.commit()
                self.emitter.emit("prompt_rejected", **{
                    "prompt_id": prompt_id,
                    "reason": "not_job_search_request"
                })
                continue

            log.info(
                f"{cls_name(self)}: "
                f"Prompt translate "
                f"prid:{prompt_id} "
                f"prompt:'{shorten_text(prompt)}' "
                f"tags:{','.join(response.get('position_tags_cloud', []))} "
            )

            if is_long:
                # Long prompts should have enough context
                tags = prompt
                eli5_user_request = prompt
            else:
                # For short requests create cloud of tags for index,
                # and eli5 explanation from GPT-3.5 from GPT-4
                tags = ",".join(response['position_tags_cloud'])
                eli5_user_request = response['eli5']

            await safe_db_execute(db, UPDATE_PROMPT, ['approved', tags, eli5_user_request, prompt_id])
            _, embeddings = await self.create_embedding([tags])
            self.index_prompts.add(
                embeddings=[embeddings[0]],
                ids=[str(prompt_id)]
            )
            await db.commit()
            self.emitter.emit("new_approved_prompt", **{
                "prompt_id": prompt_id,
                "user_id": user_id
            })

    async def sync_index_and_db(self, db):
        cursor = await safe_db_execute(db, COUNT_APPROVED_PROMPTS)
        (sqlite_active_count,) = await cursor.fetchone()
        chroma_count = len(self.index_prompts.peek(limit=0)["ids"])
        if chroma_count == sqlite_active_count: return

        log.warning(
            f'{cls_name(self)}: '
            f'Seems like mismatch between chroma and sqlite, '
            f"sqlite_count: {sqlite_active_count} "
            f"chroma_count: {chroma_count} "
        )

        rows = await safe_db_execute(db, GET_ALL_APPROVED_PROMPTS)
        prompts_map = {
            str(prompt_id): {
                "tags": tags,
            }
            async for (prompt_id, tags,) in rows or []
        }

        index_ids = self.index_prompts.get(ids=list(prompts_map.keys()))["ids"]
        mismatch_ids = sorted(map(int, list(set(prompts_map.keys()) - set(index_ids))))

        for prompts_ids in group_list(mismatch_ids, 100):
            tags_list = [prompts_map[str(prompt_id)]["tags"] for prompt_id in prompts_ids]
            _, embeddings = await self.create_embedding(tags_list)
            for n, (prompt_id, embedding) in enumerate(zip(prompts_ids, embeddings)):
                self.index_prompts.add(
                    embeddings=[embedding],
                    ids=[str(prompt_id)]
                )

                log.warning(
                    f'{cls_name(self)}: '
                    f'{n + 1}: Created missing prompt index for, '
                    f"prompt_id: {prompt_id} "
                )

            await asyncio.sleep(1)

        # IDs in index which are not in db
        mismatch_ids = sorted(list(set(index_ids) - set(prompts_map.keys())))
        for n, _id in enumerate(mismatch_ids):
            log.warning(
                f'{cls_name(self)}: '
                f'{n + 1}: Remove zombie prompt index, '
                f"prompt_id: {_id} "
            )

        if mismatch_ids:
            self.index_prompts.delete(ids=mismatch_ids)

    async def start(self):
        log.info(f"{cls_name(self)}: Start service")
        self.start_event.set()
        self.lock = asyncio.Lock()

        if not self.rate_limit:
            self.rate_limit = AsyncLimiter(max_rate=20, time_period=60)

        context = get_context()

        try:
            log.info(f"{cls_name(self)}: Waiting for SQLite3 to be ready")
            await asyncio.wait_for(context['sqlite_ready'], 3)
        except asyncio.exceptions.TimeoutError:
            log.warning(
                f"{cls_name(self)}: "
                f"Exiting: Haven't received SQLite3"
            )
            return

        try:
            if not self.index_prompts:
                log.info(f"{cls_name(self)}: Waiting for ChromaDB prompts collection")
                self.index_prompts = await asyncio.wait_for(context['index_prompts'], 3)
        except asyncio.exceptions.TimeoutError:
            log.warning(
                f"{cls_name(self)}: "
                f"Exiting: Haven't received ChromaDB prompts collection"
            )
            return

        try:
            if not self.create_embedding:
                log.info(f"{cls_name(self)}: Waiting for GPT embedding function")
                self.create_embedding = await asyncio.wait_for(context['create_embedding'], 3)
        except asyncio.exceptions.TimeoutError:
            log.warning(
                f"{cls_name(self)}: "
                f"Exiting: Haven't received GPT embedding function"
            )
            return

        log.info(f"{cls_name(self)}: Start processing prompts ")
        self.emitter.on("new_prompt", self.on_new_prompt)
        self.emitter.on("prompt_rejected", print_event)

        log.info(f"{cls_name(self)}: Sync prompt index with db ")
        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            await self.sync_index_and_db(db)

        while True:
            async with self.lock:
                async with aiosqlite.connect(SQLLite3Service.db_path) as db:
                    try:
                        await self.process_new_prompts(db)
                    except Exception as e:
                        log.exception(e)

            await asyncio.sleep(10)
