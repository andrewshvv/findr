import asyncio
import datetime
import logging
import os

import aiomisc
import aiosqlite
import docx
import requests
import telegram
from PyPDF2 import PdfFileReader
from aiomisc import get_context
from pyee import AsyncIOEventEmitter
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import Forbidden
from telegram.ext import CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes, \
    Application

from bot import bot_texts
from common.db import safe_db_execute
from common.logging import cls_name
from common.post import prepare_post
from db.sqlite import SQLLite3Service

log = logging.getLogger(__name__)


class TelegramBot(aiomisc.Service):
    api_key: str = None
    application: Application = None
    emitter: AsyncIOEventEmitter = None
    lock: asyncio.Lock = None

    async def start(self):
        self.start_event.set()

        self.attach_event_listeners()
        self.lock = asyncio.Lock()
        self.user_states = {}

        self.application = Application.builder().concurrent_updates(10).token(self.api_key).build()


        self.application.add_handler(
            CommandHandler("start", self.handle_bot_start)
        )
        self.application.add_handler(
            CommandHandler("stop_searching", self.handle_stop_searching)
        )

        self.application.add_handler(
            CallbackQueryHandler(self.handle_menu_actions, pattern="^provide_prompt$|^edit_prompt$")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.handle_reveal_contact, pattern="^reveal_contact_")
        )

        self.application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self.store_message)
        )
        self.application.add_handler(
            MessageHandler(filters.ATTACHMENT, self.store_document)
        )

        self.application.add_handler(
            CallbackQueryHandler(self.handle_continue, pattern="^continue_")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.handle_reasoning, pattern="^reasoning_")
        )

        await self.application.bot.set_my_commands([
            ("/provide_prompt", "Создать еще одно описание"),
            ("/active_prompt", "Посмотреть текущий запрос"),
            ("/start", "Запустить бота"),
            ("/stop_searching", "Прекратить поиск")
        ])
        self.application.add_handler(CommandHandler("provide_prompt", self.handle_provide_prompt))
        self.application.add_handler(CommandHandler("active_prompt", self.show_active_prompt))
        await get_context()["sqlite_ready"]

        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()

        await asyncio.gather(
            self.poll_db(),
            self.check_active_prompts()
        )

    async def handle_provide_prompt(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        user_id = update.effective_user.id
        await update.message.reply_text("Введи текст или отправь резюме")
        self.user_states[user_id] = "AWAITING_PROMPT"

    async def show_active_prompt(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        user_id = update.effective_user.id
        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            cursor = await db.cursor()
            await cursor.execute("SELECT original FROM prompts WHERE user_id = ? AND active = 1", (user_id,))
            result = await cursor.fetchone()
            if not result:
                await update.message.reply_text(f"{bot_texts.no_prompts}")
                return
            await update.message.reply_text(f"{bot_texts.active_prompt}: {result[0]}")

    async def stop(self, *args, **kwargs):
        try:
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()
        except RuntimeError:
            # already stopped
            pass

    async def handle_bot_start(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        user_id = update.effective_user.id
        # In case user uses /start instead of /new_prompt
        await self.deactivate_prompts(user_id, banned=False)
        await self.show_menu(update)

    async def show_menu(self, update: Update):
        await update.message.reply_text(
            bot_texts.welcome_message,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(bot_texts.provide_prompt, callback_data="provide_prompt")]
            ])
        )

    async def handle_menu_actions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        await query.answer()

        user_id = update.effective_user.id

        if query.data == "provide_prompt":
            self.user_states[user_id] = "AWAITING_PROMPT"
            await context.bot.send_message(chat_id=user_id, text=bot_texts.first_prompt)
            return

    async def poll_db_forward_messages(self, db):
        cursor = await db.cursor()

        await safe_db_execute(
            cursor,
            """
            SELECT 
                users_posts.user_id, 
                users_posts.post_id, 
                users_posts.prompt_id,
                posts.description,
                posts.markdown_entities
            FROM users_posts 
            LEFT JOIN posts
            ON  users_posts.post_id = posts.post_id
            WHERE users_posts.post_status IS NULL
              AND users_posts.process_status = 'accepted' 
            """
        )
        async for (
                user_id,
                post_id,
                prompt_id,
                description,
                markdown_entities
        ) in cursor:
            try:
                await self.send_post_description(
                    user_id=user_id,
                    prompt_id=prompt_id,
                    post_id=post_id,
                    text=description,
                    markdown_entities=markdown_entities
                )
                await safe_db_execute(
                    cursor,
                    """
                    UPDATE users_posts 
                    SET post_status = 'forwarded' 
                    WHERE post_id = ? 
                      AND prompt_id = ?
                    """,
                    [post_id, prompt_id]
                )
                log.info(
                    f"{cls_name(self)} "
                    f"Poll db, forward message "
                    f"prid:{prompt_id} "
                    f"pid:{post_id} "
                    f"uid:{user_id} "
                )
            except Forbidden:
                await self.deactivate_prompts(user_id)

            except Exception as e:
                log.exception(e)
                await safe_db_execute(
                    cursor,
                    """
                    UPDATE users_posts 
                    SET post_status = 'rejected' 
                    WHERE post_id = ? 
                      AND prompt_id = ?
                    """, [post_id, prompt_id]
                )
            finally:
                await db.commit()

    async def poll_db(self):
        while True:
            # check for new messages
            async with aiosqlite.connect(SQLLite3Service.db_path) as db:
                async with self.lock:
                    await self.poll_db_forward_messages(db)

            await asyncio.sleep(10)  # check every 10 seconds

    async def handle_reveal_contact(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query

        try:
            await query.answer()
        except telegram.error.BadRequest as e:
            if "Query is too old and response" in str(e):
                # TODO: Maybe send something to user, like try again?
                # It happens when bot blocks or something like that
                # and don't respond. Maybe we should think of
                # why is that happening at all
                pass

        parts = query.data.split("_")[-2:]
        if len(parts) != 2:
            raise NotImplementedError

        prompt_id = int(parts[0])
        post_id = int(parts[1])
        user_id = update.effective_user.id

        log.info(
            f"{cls_name(self)}: More info clicked "
            f"tid:{query.message.id} "
            f"user_id: {user_id} "
            f"post_id: {post_id} "
            f"prompt_id: {prompt_id}"
        )

        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            cursor = await db.cursor()
            await safe_db_execute(
                cursor,
                """
                SELECT 
                    description, 
                    post_id, 
                    contact, 
                    markdown_entities
                FROM posts 
                WHERE post_id = ?
                """, [post_id]
            )
            row = await cursor.fetchone()
            if not row: return

        (original_text, post_id, more_info_text, markdown_entities,) = row
        text, entities = prepare_post(
            text=original_text,
            markdown_entities=markdown_entities,
            more_info_text=more_info_text
        )

        try:
            await query.edit_message_text(
                text=text,
                entities=entities,
                disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                text="Почему мы рекомендуем эту вакансию? ",
                                callback_data=f"reasoning_{prompt_id}_{post_id}"
                            ),
                        ]
                    ]
                )
            )
        except telegram.error.BadRequest as e:
            pass
        except telegram.error.RetryAfter as e:
            log.warning(
                f"{cls_name(self)}: "
                f"Too many edit_message's "
                f"user_id: {user_id} "
                f"wait_seconds: {e.retry_after} "
            )
            # TODO: Aren't we blocking this handler for other users by any chance?
            await asyncio.sleep(e.retry_after)

        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            cursor = await db.cursor()
            clicked_at = datetime.datetime.now().isoformat()

            await safe_db_execute(
                cursor,
                """
                    UPDATE prompts 
                    SET date = ?  
                    WHERE user_id = ? 
                      AND prompt_id = ? 
                      AND active = 1
                """, [clicked_at, user_id, prompt_id]
            )
            await safe_db_execute(
                cursor,
                """
                    UPDATE users_posts 
                    SET clicked_more_info_at = ?  
                    WHERE post_id = ? 
                      AND prompt_id = ?
                """, [clicked_at, post_id, prompt_id]
            )
            await db.commit()

    async def send_post_description(self, user_id, prompt_id, post_id, text, markdown_entities):
        text, entities = prepare_post(text, markdown_entities)
        markup = InlineKeyboardMarkup([
            [

                InlineKeyboardButton(
                    text="Подробнее",
                    callback_data=f"reveal_contact_{prompt_id}_{post_id}"
                )
            ]
        ])
        await self.application.bot.send_message(
            chat_id=user_id,
            text=text,
            entities=entities,
            reply_markup=markup,
            disable_web_page_preview=True
        )

    async def store_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_user is None:
            return

        # Get the user id directly from effective_user
        user_id = update.effective_user.id
        if self.user_states.get(user_id) not in ["AWAITING_PROMPT", "AWAITING_PROMPT_EDIT"]:
            await update.message.reply_text(
                'Вот меню бота:\n '
                '/provide_prompt - Создать еще одно описание\n '
                '/active_prompt - Посмотреть текущий запрос\n '
                '/start - Запустить бота'
            )
            return

        if update.message:
            message_text = update.message.text
        elif update.callback_query:
            message_text = update.callback_query.message.text
        else:
            return

        # store message in DB
        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            cursor = await db.cursor()
            date = datetime.datetime.now().isoformat()
            await safe_db_execute(
                cursor,
                """
                    UPDATE prompts 
                    SET active = 0
                    WHERE user_id = ?
                """, [user_id]
            )

            if self.user_states.get(user_id) == "AWAITING_PROMPT":
                await safe_db_execute(
                    cursor,
                    """
                        INSERT INTO prompts(user_id, original, date, active)
                        VALUES (?,?,?,?)
                    """, [user_id, message_text, date, 1]
                )
                prompt_id = cursor.lastrowid
                log.info(
                    f"{cls_name(self)}: "
                    f"Prompt created "
                    f"prompt_id:{prompt_id} "
                )
                await db.commit()
                self.emitter.emit("new_prompt")
            else:
                # TODO: Which prompt entry is updated here?
                await safe_db_execute(
                    cursor,
                    """
                        UPDATE prompts 
                        SET original = ?, date = ?, active = 1 
                        WHERE user_id = ?
                    """, [message_text, date, user_id]
                )
                await db.commit()

        self.user_states[user_id] = None  # reset user state
        if update.message:
            await update.message.reply_text(bot_texts.acknowledgment)
        elif update.callback_query:
            await update.callback_query.message.reply_text(bot_texts.acknowledgment)

        # TODO: All below can be safely removed?
        await asyncio.sleep(60)

        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            cursor = await db.cursor()
            await safe_db_execute(
                cursor,
                """
                    SELECT *
                    FROM USERS_POSTS 
                    WHERE user_id = ? 
                      AND prompt_id = ? 
                      AND process_status != 'rejected'
                    LIMIT 1
                """, [user_id, prompt_id]
            )
            if await cursor.fetchone(): return

            reply_text = "Пока мы не нашли ничего, что бы тебе подошло. \n" \
                         "Попробуй изменить запрос или подожди еще чуть-чуть"
            if update.message:
                await update.message.reply_text(text=reply_text)
            elif update.callback_query:
                await update.callback_query.message.reply_text(text=reply_text)

            log.info(
                f"{cls_name(self)}: No suitable posts has been found "
                f"prompt_id:{prompt_id} "
            )

    async def parse_document(self, file_path: str) -> str:
        content = ""

        if file_path.endswith('.docx'):
            try:
                doc = docx.Document(file_path)
                for paragraph in doc.paragraphs:
                    content += paragraph.text + '\n'
            except Exception as e:
                # TODO: Losing precious stacktrace
                content = "Error parsing .docx file: " + str(e)
        elif file_path.endswith('.pdf'):
            try:
                with open(file_path, 'rb') as file:
                    reader = PdfFileReader(file)
                    for page_num in range(reader.numPages):
                        page = reader.getPage(page_num)
                        content += page.extractText() + '\n'
            except Exception as e:
                # TODO: Losing precious stacktrace
                content = "Error parsing .pdf file: " + str(e)

        else:
            content = "Пока мы не читаем такой формат. Отправь .pdf или .docx"

        return content.strip()

    async def store_document(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:

        file_id = update.message.document.file_id
        file_obj = await self.application.bot.get_file(file_id)
        print(file_obj)

        download_url = f"{file_obj.file_path}"

        response = requests.get(download_url, stream=True)
        filename = os.path.basename(file_obj.file_path)
        file_path = f'./resume.docx'

        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        # Parse the file to get the content
        content = await self.parse_document(file_path)

        if content.startswith("Unsupported"):
            await update.message.reply_text(content)
            return

        await update.message.reply_text("Получили и прочитали твое резюме. Начинаем поиск работы")

        user_id = update.effective_user.id
        date = datetime.datetime.now().isoformat()
        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            cursor = await db.cursor()
            await cursor.execute("UPDATE prompts SET active = 0 WHERE user_id = ?", (user_id,))
            await cursor.execute("INSERT INTO prompts(user_id, original, date, active) VALUES (?,?,?,?)",
                                 (user_id, content, date, 1))
            await db.commit()

        if user_id in self.user_states:
            del self.user_states[user_id]

    async def check_active_prompts(self):
        while True:
            try:

                async with aiosqlite.connect(SQLLite3Service.db_path) as db:

                    cursor = await db.cursor()

                    time_threshold = datetime.datetime.now() - datetime.timedelta(days=2)

                    await cursor.execute("SELECT rowid, user_id FROM prompts WHERE active = 1 AND date < ?",
                                         (time_threshold.isoformat(),))
                    prompts = await cursor.fetchall()

                    for prompt in prompts:
                        rowid, user_id = prompt
                        # Update prompt
                        date = datetime.datetime.now().isoformat()
                        await cursor.execute("UPDATE prompts SET date = ?, active = 0 WHERE rowid = ?",
                                             (date, rowid))
                        await db.commit()

                        # Send continue message to user
                        await self.application.bot.send_message(chat_id=user_id,
                                                                text=bot_texts.going_on,
                                                                reply_markup=InlineKeyboardMarkup(
                                                                    [[InlineKeyboardButton(bot_texts.go_on,
                                                                                           callback_data=f"continue_1_{rowid}")]]))

            except Exception as e:
                log.exception(e)

            await asyncio.sleep(10)  # check every 10 seconds

    async def handle_continue(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        await query.answer()

        rowid = int(query.data.split("_")[-1])

        # Update prompt
        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            cursor = await db.cursor()
            date = datetime.datetime.now().isoformat()
            await cursor.execute("UPDATE prompts SET date = ?, active = 1 WHERE rowid = ?",
                                 (date, rowid))
            await db.commit()
        await query.edit_message_reply_markup(reply_markup=None)

    async def handle_reasoning(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        await query.answer()

        parts = query.data.split("_")[-2:]
        if len(parts) != 2:
            raise NotImplementedError

        prompt_id = int(parts[0])
        post_id = int(parts[1])
        user_id = update.effective_user.id

        log.info(
            f"{cls_name(self)}: Get reasoning - clicked "
            f"tid:{query.message.id} "
            f"user_id: {user_id} "
            f"post_id: {post_id} "
            f"prompt_id: {prompt_id} "
        )

        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            cursor = await db.cursor()
            await safe_db_execute(
                cursor,
                """
                    SELECT gpt_reason 
                    FROM users_posts 
                    WHERE post_id = ? 
                      AND prompt_id = ? 
                    LIMIT 1
                """,
                [post_id, prompt_id]
            )
            row = await cursor.fetchone()
            if not row: return
            (gpt_reason_text,) = row

        try:
            query = update.callback_query
            reply_id = query.message.id
            await self.application.bot.send_message(
                chat_id=user_id,
                text=gpt_reason_text,
                reply_to_message_id=reply_id
            )
        except Forbidden:
            await self.deactivate_prompts(user_id)

        except Exception as e:
            log.exception(e)

    async def handle_stop_searching(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        user_id = update.effective_user.id
        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            cursor = await db.cursor()
            # Get the latest active prompt for the user
            # TODO: Maybe better UPDATE OR IGNORE and check whether the result was successful?
            #   in this case just one sql query
            await safe_db_execute(
                cursor,
                """
                    SELECT prompt_id 
                    FROM prompts 
                    WHERE user_id = ? 
                      AND active = 1 
                    ORDER BY date DESC 
                    LIMIT 1
                """, [user_id]
            )
            row = await cursor.fetchone()
            if not row:
                await update.message.reply_text("У вас нет активных запросов.")
                return

            # Deactivate the prompt
            (prompt_id,) = row
            await safe_db_execute(
                cursor,
                """
                    UPDATE prompts 
                    SET active = 0 
                    WHERE prompt_id = ?
                """, [prompt_id]
            )
            await db.commit()
            await update.message.reply_text("Мы больше не будем присылать вам уведомления")

    def attach_event_listeners(self):
        self.emitter.on("search_result", self.handle_search_result_event)
        self.emitter.on("search_ended", self.handle_search_ended_event)

    @staticmethod
    async def is_prompt_active(cursor, prompt_id):
        await safe_db_execute(
            cursor,
            """
                SELECT prompt_id
                FROM prompts
                WHERE prompt_id = ? 
                  AND active = 1
                LIMIT 1
            """, [prompt_id]
        )

        return await cursor.fetchone() is not None

    @staticmethod
    async def is_post_forwarded(cursor, post_id, prompt_id):
        await safe_db_execute(
            cursor,
            """
                SELECT post_id
                FROM users_posts
                WHERE prompt_id = ? 
                  AND post_id = ?
                  AND post_status = 'forwarded'
                LIMIT 1
            """, [prompt_id, post_id]
        )

        return await cursor.fetchone() is not None

    async def handle_search_result_event(self, user_id, prompt_id, post_id, *args, **kwargs):
        # Check if prompt is active
        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            async with self.lock:
                cursor = await db.cursor()
                if not await TelegramBot.is_prompt_active(cursor, prompt_id):
                    log.warning(
                        f"{cls_name(self)}: "
                        f"Received update for non-active prompt, "
                        f"prompt_id: {prompt_id} "
                        f"user_id: {user_id} "
                        f"post_id: {post_id} "
                    )
                    return

                if await TelegramBot.is_post_forwarded(cursor, post_id, prompt_id):
                    # TODO: debug, instead of info
                    log.info(
                        f"{cls_name(self)}: "
                        f"Received event for already forwarded post, "
                        f"prompt_id: {prompt_id} "
                        f"user_id: {user_id} "
                        f"post_id: {post_id} "
                    )
                    return

                await safe_db_execute(
                    cursor,
                    """
                        SELECT description, markdown_entities 
                        FROM posts 
                        WHERE post_id = ?
                    """, [post_id]
                )
                rows = await cursor.fetchone()
                # Make sure we found the post with that ID
                if not rows:
                    log.error(
                        f"{cls_name(self)}: "
                        f"No post found with "
                        f"prompt_id: {prompt_id} "
                        f"user_id: {user_id} "
                        f"post_id: {post_id} "
                    )
                    return

                (description, markdown_entities,) = rows
                try:
                    await self.send_post_description(
                        user_id=user_id,
                        prompt_id=prompt_id,
                        post_id=post_id,
                        text=description,
                        markdown_entities=markdown_entities
                    )
                    await safe_db_execute(
                        cursor,
                        """
                            UPDATE users_posts 
                            SET post_status = 'forwarded' 
                            WHERE post_id = ? AND prompt_id = ? 
                        """, [post_id, prompt_id]
                    )
                    log.info(
                        f"{cls_name(self)}: "
                        f"Search event received, forward message "
                        f"prid:{prompt_id} "
                        f"pid:{post_id} "
                        f"uid:{user_id}"
                    )
                except Forbidden:
                    await self.deactivate_prompts(user_id)

                except Exception as e:
                    await safe_db_execute(
                        cursor,
                        """
                            UPDATE users_posts 
                            SET post_status = 'rejected' 
                            WHERE post_id = ? AND prompt_id = ?
                        """, [post_id, prompt_id]
                    )
                    log.exception(e)

                finally:
                    await db.commit()

    async def handle_search_ended_event(self, user_id, prompt_id, num_posts, *args, **kwargs):
        try:
            async with aiosqlite.connect(SQLLite3Service.db_path) as db:
                cursor = await db.cursor()
                if not await TelegramBot.is_prompt_active(cursor, prompt_id):
                    log.warning(
                        f"{cls_name(self)}: "
                        f"Received search ended for non-active prompt, "
                        f"prompt_id: {prompt_id} "
                        f"user_id: {user_id} "
                    )
                    return

            if num_posts <= 0:
                await self.application.bot.send_message(
                    chat_id=user_id,
                    text='Пока мы не нашли для тебя ничего подходящего',
                )
            else:
                await asyncio.sleep(3)
                await self.application.bot.send_message(
                    chat_id=user_id,
                    text='Пока это все, что мы нашли для тебя. Мы напишем позже',
                )
        except Forbidden:
            await self.deactivate_prompts(user_id)
        except Exception as e:
            log.exception(e)

    async def deactivate_prompts(self, user_id, banned=True):
        if banned:
            logging.info(
                f"{cls_name(self)}: "
                f"Bot was blocked by user, deactivating all prompts "
                f"user_id: {user_id} "
            )

        # Update the database to set active = 0 for the user's last prompt
        # TODO: Potential error? Double db connect?
        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            cursor = await db.cursor()
            await safe_db_execute(
                cursor,
                """
                    UPDATE prompts 
                    SET active = 0 
                    WHERE user_id = ? AND active = 1
                """, [int(user_id)]
            )
            await db.commit()
