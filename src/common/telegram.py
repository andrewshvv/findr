import asyncio
import os
import platform
import re
import signal
import time
from typing import Optional, List, Sequence, Coroutine, Union
from urllib.parse import urlparse

import nltk
import phonenumbers
import validators
from telegram._utils.defaultvalue import DEFAULT_NONE, DefaultValue
from telegram._utils.types import ODVInput
from telegram._utils.warnings import warn
from telegram.error import TelegramError
from telethon import utils, errors, functions, TelegramClient
from telethon.client.users import _NOT_A_REQUEST, _fmt_flood
from telethon.errors import RPCError, MultiError
from telethon.helpers import retry_range
from telethon.tl import TLRequest
from telethon.tl.functions.channels import GetFullChannelRequest, DeleteMessagesRequest
from telethon.tl.functions.messages import GetDialogFiltersRequest
from telethon.tl.types import InputChannel

from common.markdown import MarkdownPost, fix_brain_cancer
from common.utils import remove_emojis


class WaitOnFloodTelegramClient(TelegramClient):
    async def _call(self: 'TelegramClient', sender, request, ordered=False, flood_sleep_threshold=None):
        if flood_sleep_threshold is None:
            flood_sleep_threshold = self.flood_sleep_threshold
        requests = (request if utils.is_list_like(request) else (request,))
        for r in requests:
            if not isinstance(r, TLRequest):
                raise _NOT_A_REQUEST()
            await r.resolve(self, utils)

            # Avoid making the request if it's already in a flood wait
            if r.CONSTRUCTOR_ID in self._flood_waited_requests:
                due = self._flood_waited_requests[r.CONSTRUCTOR_ID]
                diff = max(round(due - time.time()), 1)
                if diff <= flood_sleep_threshold:
                    self._log[__name__].info(*_fmt_flood(diff, r, early=True))
                    await asyncio.sleep(diff)
                    self._flood_waited_requests.pop(r.CONSTRUCTOR_ID, None)
                else:
                    raise errors.FloodWaitError(request=r, capture=diff)

            if self._no_updates:
                r = functions.InvokeWithoutUpdatesRequest(r)

        request_index = 0
        last_error = None
        self._last_request = time.time()

        for attempt in retry_range(self._request_retries):
            try:
                future = sender.send(request, ordered=ordered)
                if isinstance(future, list):
                    results = []
                    exceptions = []
                    for f in future:
                        try:
                            result = await f
                        except RPCError as e:
                            exceptions.append(e)
                            results.append(None)
                            continue
                        self.session.process_entities(result)
                        exceptions.append(None)
                        results.append(result)
                        request_index += 1
                    if any(x is not None for x in exceptions):
                        raise MultiError(exceptions, results, requests)
                    else:
                        return results
                else:
                    result = await future
                    self.session.process_entities(result)
                    return result
            except (errors.ServerError, errors.RpcCallFailError,
                    errors.RpcMcgetFailError, errors.InterdcCallErrorError,
                    errors.InterdcCallRichErrorError) as e:
                last_error = e
                self._log[__name__].warning(
                    'Telegram is having internal issues %s: %s',
                    e.__class__.__name__, e)

                await asyncio.sleep(2)
            except (errors.FloodWaitError, errors.SlowModeWaitError, errors.FloodTestPhoneWaitError) as e:
                last_error = e
                if utils.is_list_like(request):
                    request = request[request_index]

                # SLOW_MODE_WAIT is chat-specific, not request-specific
                if not isinstance(e, errors.SlowModeWaitError):
                    self._flood_waited_requests \
                        [request.CONSTRUCTOR_ID] = time.time() + e.seconds

                # In test servers, FLOOD_WAIT_0 has been observed, and sleeping for
                # such a short amount will cause retries very fast leading to issues.
                if e.seconds == 0:
                    e.seconds = 1

                if e.seconds <= self.flood_sleep_threshold:
                    self._log[__name__].info(*_fmt_flood(e.seconds, request))
                    await asyncio.sleep(e.seconds)
                else:
                    raise
            except (errors.PhoneMigrateError, errors.NetworkMigrateError,
                    errors.UserMigrateError) as e:
                last_error = e
                self._log[__name__].info('Phone migrated to %d', e.new_dc)
                should_raise = isinstance(e, (
                    errors.PhoneMigrateError, errors.NetworkMigrateError
                ))
                if should_raise and await self.is_user_authorized():
                    raise
                await self._switch_dc(e.new_dc)

        if self._raise_last_call_error and last_error is not None:
            raise last_error
        raise ValueError('Request was unsuccessful {} time(s)'.format(attempt))


# By default load nltk
nltk.download('punkt')


class TelegramTextTools:
    PREFERRED_LEN = 3000
    MAX_LEN = 4096

    @staticmethod
    def _replace_newlines(string):
        string = string.replace('?\n', ' . ')
        string = string.replace('!\n', ' . ')
        string = string.replace(')\n', ' . ')
        string = string.replace(';\n', ' . ')
        string = string.replace('.\n', ' . ')
        for k in range(10, 0, -1):
            string = string.replace('\n' * k, '. ')

        return string

    @staticmethod
    def _proper_language(language="russian"):
        language = language.lower()
        if language == "ru":
            language = "russian"
        elif language == "en":
            language = "english"
        elif language == "russian":
            pass
        elif language == "english":
            pass
        else:
            raise NotImplementedError

        return language

    @staticmethod
    def remove_substrings(text: MarkdownPost, substrings: List[str], language="RU") -> MarkdownPost:
        if not isinstance(substrings, list):
            raise NotImplementedError

        if not substrings:
            return text

        language = TelegramTextTools._proper_language(language)

        for substring in substrings:
            for sentence in TelegramTextTools._find_sentences_by_substr(text.plain(), substring, language):
                text = text.replace(sentence, "")

        return text

    @staticmethod
    def _approximate_markdown_cut(original_text: MarkdownPost, length=PREFERRED_LEN, language="RU") -> MarkdownPost:
        sentences = TelegramTextTools.get_sentences(original_text.plain(), language)
        sentences_for_removal = []

        plain_original_text = original_text.plain()

        current_len = 0
        for sentence in sentences:
            current_len += len(sentence)
            if current_len > length:
                # TODO: Remove, safety
                if plain_original_text.count(sentence) > 1:
                    continue

                sentences_for_removal.append(sentence)

        cutted_text = TelegramTextTools.remove_substrings(original_text, sentences_for_removal, language)
        return cutted_text

    @staticmethod
    def get_sentences(original_text, language="RU") -> List[str]:
        language = TelegramTextTools._proper_language(language)

        changed_text = TelegramTextTools._replace_newlines(original_text)
        temp_sentences = nltk.tokenize.sent_tokenize(changed_text, language=language)
        temp_sentences = [sentence for sentence in temp_sentences if sentence.strip() != "."]

        reverted_sentences = [
            TelegramTextTools._revert_sentence(sentence, original_text)
            for sentence in temp_sentences
        ]

        return reverted_sentences

    @staticmethod
    def _revert_sentence(sentence, original_text) -> str:
        num_changed = 0
        while sentence not in original_text:
            sentence = sentence[:-1]
            num_changed += 1

        start_index = original_text.find(sentence)
        end_index = start_index + len(sentence) + num_changed
        original_sentence = original_text[start_index:end_index]

        while original_text[end_index:end_index + 1] == "\n":
            original_sentence += "\n"
            end_index += 1

        return original_sentence

    @staticmethod
    def _find_sentences_by_substr(original_text: str, sentence_substring, language="russian"):
        sentences = TelegramTextTools.get_sentences(original_text, language)
        sentence_substring = sentence_substring.strip()
        if not sentence_substring:
            return []

        return [
            sentence
            for sentence in sentences
            if sentence_substring.lower() in sentence.lower()
        ]

    @staticmethod
    def extract_info(link):
        parsed = urlparse(link)

        # take just nickname
        if parsed.netloc == "t.me":
            return parsed.path.split('/')[-1]

        # take email or phone
        if parsed.scheme in ['mailto', 'tel']:
            return parsed.path

        # for links do nothing
        return link

    @staticmethod
    def prepare_description_for_tg(original_text: MarkdownPost, more_info_text=None):
        assert os.getenv("UI_BOT_NICKNAME") is not None
        assert os.getenv("UI_BOT_NAME") is not None

        bot_link = f"[{os.getenv('UI_BOT_NAME')}](https://t.me/{os.getenv('UI_BOT_NICKNAME')})"
        promo = f"\n\nВакансия найдена через {bot_link}"

        max_len = TelegramTextTools.PREFERRED_LEN
        more_info_title = f"\n\n**Подробнее:** \nНажми - Подробнее 👇"
        if more_info_text:
            prev_len = len(more_info_title)
            more_info_title = f"\n\n**Подробнее:** \n{more_info_text.strip()}"
            after_len = len(more_info_title)

            # Ensure that the length of main text is not
            # going to be affected by increase
            # of contact section
            max_len += (after_len - prev_len)
            if max_len > TelegramTextTools.MAX_LEN: raise NotImplementedError()

        full_text = original_text + more_info_title + promo
        if len(full_text) > max_len:
            dots = "\n\nУпс, кажется текст обрезался 🥲"
            cut_text = TelegramTextTools._approximate_markdown_cut(
                original_text=original_text,
                length=max_len - (len(dots) + len(more_info_title) + len(promo))
            ).strip()

            full_text = cut_text + dots + more_info_title + promo

        return full_text

    @staticmethod
    def validate_url(item):
        item = item.replace(" ", "").strip()
        return any([
            validators.url(item),
            validators.url("https://" + item),
            item.startswith("tg://")
        ])

    @staticmethod
    def validate_email(item):
        item = item.replace(" ", "").strip()
        if item.startswith('mailto:'):
            item = item.split('mailto:')[1]

        return validators.email(item)

    @staticmethod
    def validate_phone_number(number):
        number = number.replace(" ", "").strip()
        if number.startswith('tel:'):
            number = number.split('tel:')[1]

        if number.startswith('8'):
            number = "+7" + number[1:]

        try:
            z = phonenumbers.parse(number, None)
            return phonenumbers.is_valid_number(z)
        except phonenumbers.phonenumberutil.NumberParseException:
            return False

    @staticmethod
    def prepare_more_for_tg(external_link, dont_include=None):
        points = []

        dont_include = dont_include or [
            # "company",
            "channel",
            "unknown",
        ]

        for info in external_link:
            if info.get("type", "unknown") in dont_include:
                continue

            parsed = urlparse(info["link"])
            content = None

            description = info["description"]

            if any([
                TelegramTextTools.validate_phone_number(info["link"]),
                TelegramTextTools.validate_email(info["link"])
            ]):
                short_name = description.replace(' ', '').strip().lower()
                number_or_email = parsed.path.replace(' ', '').strip().lower()
                if number_or_email != short_name:
                    points.append(f"{description} - {number_or_email}")
                    continue
                else:
                    points.append(f"{number_or_email}")
                    continue

            elif TelegramTextTools.validate_url(info["link"]):
                if parsed.scheme == "tg":
                    # TODO: https://t.me/BotTalk/157765
                    # content = f"[{info['short'].strip()}]({info['link']})"
                    pass

                elif parsed.netloc == "t.me":
                    # nickname = parsed.path.split("/")[-1].strip()
                    description = description.capitalize()
                    name = description.strip()
                    # if nickname.lower() == name.lower() and not nickname.startswith("@"):
                    #     nickname = "@" + nickname
                    points.append(f"[{name}]({info['link']})")
                    continue
                else:
                    description = description.capitalize()
                    points.append(f"[{description}]({info['link']})")
                    continue

        return "• " + "\n• ".join(list(set(points)))

    @staticmethod
    def final_links(text: Union[MarkdownPost, str]):
        if isinstance(text, MarkdownPost):
            return text.urls()
        else:
            rgx = r"\b((?:https?://)?(?:(?:www\.)?(?:[\da-z\.-]+)\.(?:[a-z]{2,6})|(?" \
                  r":(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4]" \
                  r"[0-9]|[01]?[0-9][0-9]?)|(?:(?:[0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|" \
                  r"(?:[0-9a-fA-F]{1,4}:){1,7}:|(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|" \
                  r"(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}|(?:[0-9a-fA-F]{1,4}:)" \
                  r"{1,4}(?::[0-9a-fA-F]{1,4}){1,3}|(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{" \
                  r"1,4}){1,4}|(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1" \
                  r",4}:(?:(?::[0-9a-fA-F]{1,4}){1,6})|:(?:(?::[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(?::" \
                  r"[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(?:ffff(?::0{1,4}){0,1}:){0,1}(?:(?:2" \
                  r"5[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9" \
                  r"]){0,1}[0-9])|(?:[0-9a-fA-F]{1,4}:){1,4}:(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0," \
                  r"1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])))(?::[0-9]{1,4}|[1-" \
                  r"5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])?(?:/[\w\.-]*)*/?)\b"
            return re.findall(rgx, text)


EMOTICONS = {
    "\U0001fae1": "negative",
    "🤡": "negative",
    "👍": "positive",
    "🤣": "negative",
    "👏": "positive",
    "🤯": "negative",
    "🤮": "negative",
    "🥴": "negative",
    "👎": "negative",
    "❤": "positive",
    "💯": "positive",
}


def is_negative_sentiment(results):
    if len(results) == 0:
        return False

    positives = 0
    negatives = 0
    for result in results:
        sentiment = EMOTICONS.get(result.reaction.emoticon, None)
        if sentiment == "negative":
            negatives += result.count
        elif sentiment == "positive":
            positives += result.count

    if positives + negatives >= 5 and negatives / (positives + negatives) > 0.5:
        return True

    return False


def extract_button_text(post):
    if not hasattr(post, "reply_markup"):
        return ""

    if not post.reply_markup:
        return ""

    button_text = "\n"
    for row in post.reply_markup.rows:
        for button in row.buttons:
            button_text += f"[{remove_emojis(button.text).strip()}]({button.url})\n"

    return button_text


async def remove_posts_from_channel(client, channel, ids):
    while len(ids) > 0:
        res = await client(DeleteMessagesRequest(channel, ids))
        if res.pts_count == 0:
            break

        yield ids[:int(res.pts_count)]
        ids = ids[int(res.pts_count):]


async def get_channel_name(client: TelegramClient, channel_peer: InputChannel):
    full_channel = await client(GetFullChannelRequest(channel_peer))
    return full_channel.chats[0].title


async def get_channels_from_folder(client: TelegramClient, folder_name):
    request = await client(GetDialogFiltersRequest())
    for folder in request:
        if not hasattr(folder, "title"): continue

        channels = []
        if folder.title == folder_name:
            for peer in folder.include_peers:
                channel_peer = InputChannel(peer.channel_id, peer.access_hash)
                channels.append({
                    "name": await get_channel_name(client, channel_peer),
                    "peer": channel_peer.to_dict()
                })
            return channels

    return []


def is_forwarded_from_channel(post):
    return hasattr(post, "fwd_from") and post.fwd_from is not None and post.fwd_from.from_id is not None


def get_original_pid_cid(post) -> (str, str):
    original_link = None
    if post.chat.username:
        # TODO: Should be original channel! Not the one where you found it
        original_link = f"https://t.me/{post.chat.username}/{post.id}"

    # if post was forwarded from somewhere else
    if is_forwarded_from_channel(post):
        return f"{post.fwd_from.channel_post}:{post.fwd_from.from_id.channel_id}", original_link

    else:
        # if post originated in the channel
        return f"{post.id}:{post.peer_id.channel_id}", original_link


class AsyncRunApplication:
    def __init__(self, application):
        self.application = application

    def __getattr__(self, name):
        # if a method/attribute isn't found in this class,
        # look for it in the application object
        return getattr(self.application, name)

    async def run_polling(
            self,
            poll_interval: float = 0.0,
            timeout: int = 10,
            bootstrap_retries: int = -1,
            read_timeout: float = 2,
            write_timeout: ODVInput[float] = DEFAULT_NONE,
            connect_timeout: ODVInput[float] = DEFAULT_NONE,
            pool_timeout: ODVInput[float] = DEFAULT_NONE,
            allowed_updates: Optional[List[str]] = None,
            drop_pending_updates: Optional[bool] = None,
            close_loop: bool = True,
            stop_signals: ODVInput[Sequence[int]] = DEFAULT_NONE,
    ) -> None:
        """Convenience method that takes care of initializing and starting the app,
        polling updates from Telegram using :meth:`telegram.ext.Updater.start_polling` and
        a graceful shutdown of the app on exit.

        The app will shut down when :exc:`KeyboardInterrupt` or :exc:`SystemExit` is raised.
        On unix, the app will also shut down on receiving the signals specified by
        :paramref:`stop_signals`.

        The order of execution by `run_polling` is roughly as follows:

        - :meth:`initialize`
        - :meth:`post_init`
        - :meth:`telegram.ext.Updater.start_polling`
        - :meth:`start`
        - Run the application until the users stops it
        - :meth:`telegram.ext.Updater.stop`
        - :meth:`stop`
        - :meth:`post_stop`
        - :meth:`shutdown`
        - :meth:`post_shutdown`

        .. include:: inclusions/application_run_tip.rst

        .. seealso::
            :meth:`initialize`, :meth:`start`, :meth:`stop`, :meth:`shutdown`
            :meth:`telegram.ext.Updater.start_polling`, :meth:`telegram.ext.Updater.stop`,
            :meth:`run_webhook`

        Args:
            poll_interval (:obj:`float`, optional): Time to wait between polling updates from
                Telegram in seconds. Default is ``0.0``.
            timeout (:obj:`int`, optional): Passed to
                :paramref:`telegram.Bot.get_updates.timeout`. Default is ``10`` seconds.
            bootstrap_retries (:obj:`int`, optional): Whether the bootstrapping phase of the
                :class:`telegram.ext.Updater` will retry on failures on the Telegram server.

                * < 0 - retry indefinitely (default)
                *   0 - no retries
                * > 0 - retry up to X times

            read_timeout (:obj:`float`, optional): Value to pass to
                :paramref:`telegram.Bot.get_updates.read_timeout`. Defaults to ``2``.
            write_timeout (:obj:`float` | :obj:`None`, optional): Value to pass to
                :paramref:`telegram.Bot.get_updates.write_timeout`. Defaults to
                :attr:`~telegram.request.BaseRequest.DEFAULT_NONE`.
            connect_timeout (:obj:`float` | :obj:`None`, optional): Value to pass to
                :paramref:`telegram.Bot.get_updates.connect_timeout`. Defaults to
                :attr:`~telegram.request.BaseRequest.DEFAULT_NONE`.
            pool_timeout (:obj:`float` | :obj:`None`, optional): Value to pass to
                :paramref:`telegram.Bot.get_updates.pool_timeout`. Defaults to
                :attr:`~telegram.request.BaseRequest.DEFAULT_NONE`.
            drop_pending_updates (:obj:`bool`, optional): Whether to clean any pending updates on
                Telegram servers before actually starting to poll. Default is :obj:`False`.
            allowed_updates (List[:obj:`str`], optional): Passed to
                :meth:`telegram.Bot.get_updates`.
            close_loop (:obj:`bool`, optional): If :obj:`True`, the current event loop will be
                closed upon shutdown. Defaults to :obj:`True`.

                .. seealso::
                    :meth:`asyncio.loop.close`
            stop_signals (Sequence[:obj:`int`] | :obj:`None`, optional): Signals that will shut
                down the app. Pass :obj:`None` to not use stop signals.
                Defaults to :data:`signal.SIGINT`, :data:`signal.SIGTERM` and
                :data:`signal.SIGABRT` on non Windows platforms.

                Caution:
                    Not every :class:`asyncio.AbstractEventLoop` implements
                    :meth:`asyncio.loop.add_signal_handler`. Most notably, the standard event loop
                    on Windows, :class:`asyncio.ProactorEventLoop`, does not implement this method.
                    If this method is not available, stop signals can not be set.

        Raises:
            :exc:`RuntimeError`: If the Application does not have an :class:`telegram.ext.Updater`.
        """
        if not self.updater:
            raise RuntimeError(
                "Application.run_polling is only available if the application has an Updater."
            )

        def error_callback(exc: TelegramError) -> None:
            self.create_task(self.process_error(error=exc, update=None))

        return await self.__run(
            updater_coroutine=self.updater.start_polling(
                poll_interval=poll_interval,
                timeout=timeout,
                bootstrap_retries=bootstrap_retries,
                read_timeout=read_timeout,
                write_timeout=write_timeout,
                connect_timeout=connect_timeout,
                pool_timeout=pool_timeout,
                allowed_updates=allowed_updates,
                drop_pending_updates=drop_pending_updates,
                error_callback=error_callback,  # if there is an error in fetching updates
            ),
            close_loop=close_loop,
            stop_signals=stop_signals,
        )

    async def __run(
            self,
            updater_coroutine: Coroutine,
            stop_signals: ODVInput[Sequence[int]],
            close_loop: bool = True,
    ) -> None:
        # Calling get_event_loop() should still be okay even in py3.10+ as long as there is a
        # _running event loop or we are in the main thread, which are the intended use cases.
        # See the docs of get_event_loop() and get_running_loop() for more info
        loop = asyncio.get_running_loop()

        if stop_signals is DEFAULT_NONE and platform.system() != "Windows":
            stop_signals = (signal.SIGINT, signal.SIGTERM, signal.SIGABRT)

        try:
            if not isinstance(stop_signals, DefaultValue):
                for sig in stop_signals or []:
                    loop.add_signal_handler(sig, self._raise_system_exit)
        except NotImplementedError as exc:
            warn(
                f"Could not add signal handlers for the stop signals {stop_signals} due to "
                f"exception `{exc!r}`. If your event loop does not implement `add_signal_handler`,"
                f" please pass `stop_signals=None`.",
                stacklevel=3,
            )

        try:
            await self.initialize()
            if self.post_init:
                await self.post_init(self)
            await updater_coroutine  # one of updater.start_webhook/polling
            await self.start()
            while True:
                await asyncio.sleep(10)
        except (KeyboardInterrupt, SystemExit):
            pass
        except Exception as exc:
            # In case the coroutine wasn't awaited, we don't need to bother the user with a warning
            updater_coroutine.close()
            raise exc
        finally:
            # We arrive here either by catching the exceptions above or if the loop gets stopped
            try:
                # Mypy doesn't know that we already check if updater is None
                if self.updater._running:  # type: ignore[union-attr]
                    await self.updater.stop()  # type: ignore[union-attr]
                if self.running:
                    await self.stop()
                if self.post_stop:
                    await self.post_stop(self)
                await self.shutdown()
                if self.post_shutdown:
                    await self.post_shutdown(self)
            finally:
                if close_loop:
                    loop.close()
