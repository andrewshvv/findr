import asyncio
import logging

import aiomisc
import aiosqlite
import telegram
import telethon
from aiomisc import get_context
from pyee import AsyncIOEventEmitter
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CallbackQueryHandler, ContextTypes, \
    Application

from common.db import safe_db_execute
from common.logging import cls_name
from common.post import prepare_post
from common.telegram import AsyncRunApplication, WaitOnFloodTelegramClient, remove_posts_from_channel
from db.embedding import PostsCollection
from db.sqlite import SQLLite3Service, GET_POST_BY_TID, FLAG_POST_BY_TID, RESEND_POST_BY_TID, \
    GET_POSTS_NOT_IN_TRANSIENT, ADD_TRANSIENT_ID, CHECK_IS_IN_DB, FLAG_POST_BY_PID, GET_PID_BY_TID, REMOVE_POST_BY_TID
from preprocessing.channels import TRANSIENT_CHANNEL, ALL_CHANNELS
from preprocessing.post_sourser import iter_channel_messages

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


async def get_transient_posts_map(client, channel, cutoff=None):
    posts = {}
    async for transient_post in iter_channel_messages(client, channel, cutoff_date=cutoff):
        posts[transient_post.id] = None

    return posts


class PostManager(aiomisc.Service):
    emitter: AsyncIOEventEmitter
    api_key: str = None
    application: AsyncRunApplication = None
    client: WaitOnFloodTelegramClient = None
    post_collection: PostsCollection = None

    async def start(self):
        log.info(f"{cls_name(self)}: Starting service")
        self.application = AsyncRunApplication(Application.builder().token(self.api_key).build())

        self.application.add_handler(
            CallbackQueryHandler(
                self.handle_post_being_flagged_or_retry,
                pattern="^flag_post$"
            )
        )
        self.application.add_handler(
            CallbackQueryHandler(
                self.handle_more_info,
                pattern="^more_info$"
            )
        )
        self.application.add_handler(
            CallbackQueryHandler(
                self.handle_post_being_flagged_or_retry,
                pattern="^resend_post"
            )
        )

        self.application.add_handler(
            CallbackQueryHandler(
                self.handle_post_being_flagged_or_retry,
                pattern="^retry_post"
            )
        )

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
            if not self.post_collection:
                log.info(f"{cls_name(self)}: Waiting for ChromaDB posts collection")
                self.post_collection = PostsCollection(
                    collection=await asyncio.wait_for(context['index_posts'], 3),
                    create_embedding=None
                )
        except asyncio.exceptions.TimeoutError:
            log.warning(
                f"{cls_name(self)}: "
                f"Exiting: Haven't received ChromaDB prompts collection"
            )
            return

        # Service is ready
        self.start_event.set()
        self.post_map = await get_transient_posts_map(self.client, TRANSIENT_CHANNEL)

        await asyncio.gather(
            self.application.run_polling(allowed_updates=Update.ALL_TYPES, stop_signals=None),
            self.inf_clean_up_posts(),
            self.inf_send_new_posts(),
        )

    async def stop(self, *args, **kwargs):
        try:
            await self.application.stop()
        except RuntimeError:
            # already stopped
            pass

    async def handle_more_info(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        await query.answer()
        if query.data != "more_info": return

        transient_id = query.message.id

        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            cursor = await safe_db_execute(db, GET_POST_BY_TID, [transient_id])
            row = await cursor.fetchone()
            if not row: return
            (
                original_text,
                original_link,
                post_id,
                more_info_text,
                markdown_entities,
                source,
                date
            ) = row

        meta_info = {
            "source": source,
            "date": date,
            "original_link": original_link,
            "post_id": post_id,
            "channels": ALL_CHANNELS
        }

        text, entities = prepare_post(original_text, markdown_entities,
                                      meta_info=meta_info,
                                      more_info_text=more_info_text)

        try:
            await query.edit_message_text(
                text=text,
                entities=entities,
                disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton("Flag", callback_data="flag_post"),
                            InlineKeyboardButton("Resend", callback_data="resend_post"),
                            InlineKeyboardButton("Retry", callback_data="retry_post"),
                            InlineKeyboardButton("Info", callback_data="more_info")
                        ],
                    ]
                )
            )
        except telegram.error.BadRequest as e:
            if not any([
                "Message is not modified" in str(e)
            ]):
                log.warning(
                    f"{cls_name(self)}: "
                    f"Unable to edit "
                    f"err: {str(e)}"
                )
        except telegram.error.RetryAfter as e:
            log.warning(
                f"{cls_name(self)}: "
                f"Too many edit_message's, wait for {e.retry_after} seconds"
            )
            await asyncio.sleep(e.retry_after)

        log.info(
            f"{cls_name(self)}: More info clicked "
            f"tid:{query.message.id} "
        )

    async def handle_post_being_flagged_or_retry(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        await query.answer()

        if query.data == "flag_post":
            db_query = FLAG_POST_BY_TID
        elif query.data == "resend_post":
            db_query = RESEND_POST_BY_TID
        elif query.data == "retry_post":
            db_query = REMOVE_POST_BY_TID
        else:
            return

        transient_id = query.message.id

        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            cursor = await safe_db_execute(db, GET_PID_BY_TID, [transient_id])
            row = await cursor.fetchone()
            if not row:
                log.warning(
                    f"{cls_name(self)}"
                    f"Can't find pid by tid "
                    f"tid:{transient_id} "
                )
                return
            (post_id,) = row

            await safe_db_execute(db, db_query, [transient_id])
            await db.commit()

            if query.data == "flag_post":
                self.post_collection.remove_posts(ids=[str(post_id)])
                log.info(
                    f"{cls_name(self)}: Flagged post, "
                    f"tid:{transient_id} "
                    f"pid:{post_id} "
                )
            elif query.data == "resend_post":
                log.info(
                    f"{cls_name(self)}: Marked for resent, "
                    f"tid:{transient_id} "
                    f"pid:{post_id} "
                )
            elif query.data == "retry_post":
                self.post_collection.remove_posts(ids=[str(post_id)])
                log.info(
                    f"{cls_name(self)}: Retry post, "
                    f"tid:{transient_id} "
                    f"pid:{post_id} "
                )

            async for removed_ids in remove_posts_from_channel(self.client, TRANSIENT_CHANNEL, [transient_id]):
                for tid in removed_ids:
                    del self.post_map[tid]

    async def send_new_posts(self, db):
        cursor = await safe_db_execute(db, GET_POSTS_NOT_IN_TRANSIENT)
        rows = await cursor.fetchall()

        for post_id, original_link, plain_text, markdown_entities, source, date in rows:
            try:
                meta_info = {
                    "source": source,
                    "date": date,
                    "original_link": original_link,
                    "post_id": post_id,
                    "channels": ALL_CHANNELS
                }

                text, entities = prepare_post(plain_text, markdown_entities, meta_info=meta_info)
                transient_id = await self.send_job_post(text, entities)
                self.post_map[transient_id] = None
            except telegram.error.BadRequest as e:
                await safe_db_execute(db, FLAG_POST_BY_PID, [str(e), post_id])
                await db.commit()
                log.warning(
                    f"{cls_name(self)}: Unable to send to transient "
                    f"pid:{post_id} "
                    f"error:{str(e)} "
                )
                continue

            await safe_db_execute(db, ADD_TRANSIENT_ID, [transient_id, post_id])
            await db.commit()

            log.info(
                f"{cls_name(self)}: Sent post to transient "
                f"pid:{post_id} "
                f"tid:{transient_id} "
            )

    async def clean_up_posts(self, db):
        transient_ids = list(self.post_map.keys())
        params = ",".join(["?" for _ in transient_ids])
        in_db_transient_ids = [
            transient_id
            async for (transient_id,) in await db.execute(CHECK_IS_IN_DB.format(params=params), transient_ids)
        ]

        posts_not_in_db = [
            transient_id
            for transient_id in transient_ids
            if transient_id not in in_db_transient_ids
        ]

        if len(posts_not_in_db) == 0:
            return

        async for removed_ids in remove_posts_from_channel(self.client, TRANSIENT_CHANNEL, posts_not_in_db):
            for tid in removed_ids:
                del self.post_map[tid]
                log.info(
                    f"Post not in db, removed from transient "
                    f"tid: {tid}"
                )

    @aiomisc.asyncbackoff(
        attempt_timeout=60,
        deadline=60,
        pause=2,
        max_tries=3,
        exceptions=(telegram.error.TimedOut,)
    )
    async def send_job_post(self, text: str, markdown_entities):

        post = await self.application.bot.send_message(
            chat_id=telethon.utils.get_peer_id(TRANSIENT_CHANNEL, add_mark=True),
            text=text,
            entities=markdown_entities,
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("Flag", callback_data="flag_post"),
                        InlineKeyboardButton("Resend", callback_data="resend_post"),
                        InlineKeyboardButton("Retry", callback_data="retry_post"),
                        InlineKeyboardButton("Info", callback_data="more_info")
                    ],
                ]
            )
        )
        return post.id

    # async def test_post(self):
    #     text = "• alesya.vasilyeva@autoeuro.ru"
    #     text, entities = TelegramTextTools.prepare_description_for_tg(text)
    #
    #     post = await self.application.bot.send_message(
    #         chat_id=TRANSIENT_CHANNEL,
    #         text=text,
    #         entities=entities
    #     )
    #     return post.id

    async def inf_clean_up_posts(self):
        while True:
            try:
                async with aiosqlite.connect(SQLLite3Service.db_path) as db:
                    await self.clean_up_posts(db)
            except Exception as e:
                log.exception(e)

            await asyncio.sleep(5)

    async def inf_send_new_posts(self):
        while True:
            async with aiosqlite.connect(SQLLite3Service.db_path) as db:
                try:
                    await self.send_new_posts(db)
                except telegram.error.RetryAfter as e:
                    log.warning(
                        f"{cls_name(self)}: "
                        f"Too many send_message's, wait for {e.retry_after} seconds"
                    )
                    await asyncio.sleep(e.retry_after)
                except Exception as e:
                    log.exception(e)

            await asyncio.sleep(5)
