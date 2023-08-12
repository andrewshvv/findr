"""
Simple markdown parser which does not support nesting. Intended primarily
for use within the library, which attempts to handle emojies correctly,
since they seem to count as two characters and it's a bit strange.
"""

import copy
import json
import re

from telegram import MessageEntity
from telegram.constants import MessageEntityType

from telethon.helpers import add_surrogate, del_surrogate, within_surrogate, strip_text
from telethon.tl import TLObject
from telethon.tl.types import (
    MessageEntityBold, MessageEntityItalic, MessageEntityCode,
    MessageEntityPre, MessageEntityTextUrl, MessageEntityMentionName,
    MessageEntityStrike, MessageEntityMention, MessageEntityHashtag, MessageEntityCashtag, MessageEntityPhone,
    MessageEntityBotCommand, MessageEntityUrl, MessageEntityEmail, MessageEntityUnderline, MessageEntityCustomEmoji
)

from common.logging import cls_name
from common.utils import remove_emojis, is_emoji_or_space

DEFAULT_DELIMITERS = {
    '**': MessageEntityBold,
    '__': MessageEntityItalic,
    '~~': MessageEntityStrike,
    '`': MessageEntityCode,
    '```': MessageEntityPre
}

DEFAULT_URL_RE = re.compile(r'\[([^\]]+)\]\(([^)]+)\)')
DEFAULT_URL_FORMAT = '[{0}]({1})'
SHOULD_NOT_BE_INSIDE_MARKDOWN = [' ', '\n', '\t', '\r', '\\', '\'', '\"', '\a', '\b', '\v', '\f']


def remove_excessive_space(text):
    while '  ' in text:
        text = text.replace('  ', ' ')

    return text


def remove_excessive_n(text):
    char_dict = {
        '\u200E': '',  # LRM
        '\u200F': '',  # RLM
        '\u200B': '',  # ZWSP
        '\u202A': '',  # LRE
        '\u202B': '',  # RLE
        '\xa0': ' ',  # Non-Breaking Space replaced by normal space
        '\u00AD': '',  # Soft Hyphen
        '\u200C': '',  # ZWNJ
        '\u200D': '',  # ZWJ
        '\uFEFF': ''  # BOM
    }

    for char, replacement in char_dict.items():
        text = text.replace(char, replacement)

    for k in range(10, 0, -1):
        text = text.replace("\n" + " " * k + "\n", "\n\n")

    for k in range(6, 2, -1):
        text = text.replace("\n" * k, "\n\n")

    return text


def fix_brain_cancer(text):
    replace_table = [
        ("\n✔️", "\n• "),
        ("\n✔️", "\n• "),
        ("\n▪️", "\n• "),
        ("\n● ", "\n• "),
        ("\n◼ ", "\n• "),
        ("\n○ ", "\n• "),
        ("\n· ", "\n• "),
        ("\n— ", "\n• "),
        ("\n🟣 ", "\n• "),
        ("\n⚠️ ", "\n• "),
        ("\n✅ ", "• "),
        ("\n✅", "• "),
        ("\n✳️", "\n• "),
        ("\n🔹️", "\n• "),
        ("\n🔹 ️", "\n• "),
        ("\n💡️", "\n• "),
        ("\n💡 ️", "\n• "),
        ("\n📍️", "\n• "),
        ("\n📌  ", "\n• "),
        ("\n📌 ", "\n• "),
        ("\n📌", "\n• "),
        ("\n🔵", "\n• ️"),
        ("\n🧿", ""),
        ("\n❗️", "\n• "),
        ("\n🔥️", "\n• "),
        ("\n⭕️ ", "\n• "),
        ("\n🔸 ", "\n• "),
        ("\n◼️ ", "\n• "),
        ("\n◼️", "\n• "),
        ("\n◾️ ", "\n• "),
        ("\n◾️", "\n• "),
        ("\n⚠️", "\n• "),

        ("\no •", "\n• "),
        ("\no ", "\n• "),
        ("\n• -", "\n• "),
        ("\n- ", "\n• "),
        ("\n-", "\n• "),

        # List has to be close to each other
        ("\n\n• ", "\n• "),
        ("\n\n  •", "\n• "),
    ]

    for _old_, _new in replace_table:
        text = text.replace(_old_, _new)

    return text


def remove_weird_ending(text):
    return text.replace(".;", ".").replace(";.", ".")


def ignore_asterics(text):
    return text.replace("\*", "")


class MarkdownPost:
    def __init__(self, text, entities=None):
        if isinstance(text, MarkdownPost):
            raise NotImplementedError

        if not entities:
            plain_text, entities = MarkdownPost._impl_telethon_parse(text)
            self._entities = entities
            self._surrogate_text = add_surrogate(plain_text)

        elif isinstance(entities, list):
            entities = MarkdownPost._copy_entities(entities)
            self._entities = entities
            self._surrogate_text = add_surrogate(text)

        elif isinstance(entities, str):
            entities = MarkdownPost._json_load_entities(entities)
            self._entities = entities
            self._surrogate_text = add_surrogate(text)

        MarkdownPost._fix_entities(self._surrogate_text, self._entities)
        MarkdownPost._del_empty(self._surrogate_text, self._entities)

    def strip(self):
        """
        Strips whitespace from the given surrogated text modifying the provided
        entities, also removing any empty (0-length) entities.

        This assumes that the length of entities is greater or equal to 0, and
        that no entity is out of bounds.
        """

        _copy = copy.copy(self)
        len_ori = len(_copy._surrogate_text)
        text = _copy._surrogate_text.lstrip()
        left_offset = len_ori - len(text)
        text = text.rstrip()
        len_final = len(text)
        _copy._surrogate_text = text

        for i in reversed(range(len(_copy._entities))):
            e = _copy._entities[i]
            if e.length == 0:
                del _copy._entities[i]
                continue

            if e.offset + e.length > left_offset:
                if e.offset >= left_offset:
                    #  0 1|2 3 4 5       |       0 1|2 3 4 5
                    #     ^     ^        |          ^
                    #   lo(2)  o(5)      |      o(2)/lo(2)
                    e.offset -= left_offset
                    #     |0 1 2 3       |          |0 1 2 3
                    #           ^        |          ^
                    #     o=o-lo(3=5-2)  |    o=o-lo(0=2-2)
                else:
                    # e.offset < left_offset and e.offset + e.length > left_offset
                    #  0 1 2 3|4 5 6 7 8 9 10
                    #   ^     ^           ^
                    #  o(1) lo(4)      o+l(1+9)
                    e.length = e.offset + e.length - left_offset
                    e.offset = 0
                    #         |0 1 2 3 4 5 6
                    #         ^           ^
                    #        o(0)  o+l=0+o+l-lo(6=0+6=0+1+9-4)
            else:
                # e.offset + e.length <= left_offset
                #   0 1 2 3|4 5
                #  ^       ^
                # o(0)   o+l(4)
                #        lo(4)
                del _copy._entities[i]
                continue

            if e.offset + e.length <= len_final:
                # |0 1 2 3 4 5 6 7 8 9
                #   ^                 ^
                #  o(1)       o+l(1+9)/lf(10)
                continue
            if e.offset >= len_final:
                # |0 1 2 3 4
                #           ^
                #       o(5)/lf(5)
                del _copy._entities[i]
            else:
                # e.offset < len_final and e.offset + e.length > len_final
                # |0 1 2 3 4 5 (6) (7) (8) (9)
                #   ^         ^           ^
                #  o(1)     lf(6)      o+l(1+8)
                e.length = len_final - e.offset
                # |0 1 2 3 4 5
                #   ^         ^
                #  o(1) o+l=o+lf-o=lf(6=1+5=1+6-1)

        return _copy

    def __len__(self):
        return len(self.markdown())

    @staticmethod
    def _json_load_entities(json_string: str):
        data = json.loads(json_string)

        entity_types = {
            cls_name(MessageEntityMention): MessageEntityMention,
            cls_name(MessageEntityHashtag): MessageEntityHashtag,
            cls_name(MessageEntityCashtag): MessageEntityCashtag,
            cls_name(MessageEntityPhone): MessageEntityPhone,
            cls_name(MessageEntityBotCommand): MessageEntityBotCommand,
            cls_name(MessageEntityUrl): MessageEntityUrl,
            cls_name(MessageEntityEmail): MessageEntityEmail,
            cls_name(MessageEntityBold): MessageEntityBold,
            cls_name(MessageEntityItalic): MessageEntityItalic,
            cls_name(MessageEntityCode): MessageEntityCode,
            cls_name(MessageEntityPre): MessageEntityPre,
            cls_name(MessageEntityTextUrl): MessageEntityTextUrl,
            cls_name(MessageEntityMentionName): MessageEntityMentionName,
            cls_name(MessageEntityUnderline): MessageEntityUnderline,
            cls_name(MessageEntityStrike): MessageEntityStrike,
            cls_name(MessageEntityCustomEmoji): MessageEntityCustomEmoji,
        }

        entities = []
        for entity_data in data:
            _cls_name = entity_data["_"]
            cls = entity_types.get(_cls_name)
            if not cls:
                raise NotImplementedError()

            entity_data.pop("_")
            entities.append(cls(**entity_data))

        return entities

    def __copy__(self):
        return MarkdownPost(
            text=del_surrogate(self._surrogate_text),
            entities=MarkdownPost._copy_entities(self._entities)
        )

    def turn_off_links(self):
        _copy = copy.copy(self)
        for i in reversed(range(len(_copy._entities))):
            e = _copy._entities[i]

            if isinstance(e, MessageEntityTextUrl):
                del _copy._entities[i]
            elif isinstance(e, MessageEntityUrl):
                del _copy._entities[i]

        return _copy

    def __contains__(self, item):
        if not isinstance(item, str):
            raise NotImplementedError
        return add_surrogate(item) in self._surrogate_text

    def clear(self):
        _copy = remove_excessive_space(self)
        _copy = remove_excessive_n(_copy)
        _copy = remove_weird_ending(_copy)
        _copy = fix_brain_cancer(_copy)
        return _copy.strip()

    def urls(self):
        links = []
        for e in self._entities:
            content = self._surrogate_text[e.offset: e.offset + e.length]

            if isinstance(e, MessageEntityTextUrl):
                links.append(e.url)
            elif isinstance(e, MessageEntityUrl):
                links.append(content)

        return links

    def all_external_links(self):
        external = []
        for e in self._entities:
            content = self._surrogate_text[e.offset: e.offset + e.length]

            if isinstance(e, MessageEntityEmail):
                external.append({
                    "type": "contact_email",
                    "link_text": content,
                    "link": f"mailto:{content}"
                })
            elif isinstance(e, MessageEntityMention):
                if content.startswith("@"):
                    content = content[1:]

                external.append({
                    # "type": "contact_telegram",
                    "link_text": content,
                    "link": f"https://t.me/{content}"
                })
            elif isinstance(e, MessageEntityPhone):
                external.append({
                    "type": "contact_phone",
                    "link_text": content,
                    "link": f"tel:{content}"
                })
            elif isinstance(e, MessageEntityTextUrl):
                external.append({
                    "link_text": content,
                    "link": f"{e.url}"
                })
            elif isinstance(e, MessageEntityUrl):
                external.append({
                    "link_text": content,
                    "link": f"{content}"
                })
            elif isinstance(e, MessageEntityMentionName):
                external.append({
                    "link_text": content,
                    "link": f"tg://user?id={e.user_id}"
                })

        return external

    def __iter__(self):
        return iter(self.plain())

    def __radd__(self, other):
        if not isinstance(other, str):
            raise NotImplementedError

        return MarkdownPost(other) + self

    def __add__(self, other):
        if isinstance(other, str):
            other = MarkdownPost(other)
        elif isinstance(other, MarkdownPost):
            pass
        else:
            raise ValueError(f"Wrong type {type(other)}")

        _self = copy.copy(self)
        _other = copy.copy(other)

        for e in _other._entities:
            e.offset += len(_self._surrogate_text)

        _self._surrogate_text += _other._surrogate_text
        _self._entities += _other._entities
        return _self

    @staticmethod
    def _fix_entities(surrogate_text, entities):
        for e in entities:
            content = surrogate_text[e.offset: e.offset + e.length]

            for char in content:
                if char not in SHOULD_NOT_BE_INSIDE_MARKDOWN:
                    break

                e.offset += 1
                e.length -= 1

            for char in reversed(content):
                if char not in SHOULD_NOT_BE_INSIDE_MARKDOWN:
                    break

                e.length -= 1

    @staticmethod
    def _impl_telethon_parse(message, delimiters=None, url_re=None, should_strip=False):
        """
        Parses the given markdown message and returns its stripped representation
        plus a list of the MessageEntity's that were found.

        :param message: the message with markdown-like syntax to be parsed.
        :param delimiters: the delimiters to be used, {delimiter: type}.
        :param url_re: the URL bytes regex to be used. Must have two groups.
        :return: a tuple consisting of (clean message, [message entities]).
        """
        if not message:
            return message, []

        if url_re is None:
            url_re = DEFAULT_URL_RE
        elif isinstance(url_re, str):
            url_re = re.compile(url_re)

        if not delimiters:
            if delimiters is not None:
                return message, []
            delimiters = DEFAULT_DELIMITERS

        # Build a regex to efficiently test all delimiters at once.
        # Note that the largest delimiter should go first, we don't
        # want ``` to be interpreted as a single back-tick in a code block.
        delim_re = re.compile('|'.join([
            '({})'.format(re.escape(k))
            for k in sorted(delimiters, key=len, reverse=True)
        ]))

        # Cannot use a for loop because we need to skip some indices
        i = 0
        result = []

        # Work on byte level with the utf-16le encoding to get the offsets right.
        # The offset will just be half the index we're at.
        message = add_surrogate(message)
        while i < len(message):
            m = delim_re.match(message, pos=i)

            # Did we find some delimiter here at `i`?
            if m:
                delim = next(filter(None, m.groups()))

                # +1 to avoid matching right after (e.g. "****")
                end = message.find(delim, i + len(delim) + 1)

                # Did we find the earliest closing tag?
                if end != -1:

                    # Remove the delimiter from the string
                    message = ''.join((
                        message[:i],
                        message[i + len(delim):end],
                        message[end + len(delim):]
                    ))

                    # Check other affected entities
                    for ent in result:
                        # If the end is after our start, it is affected
                        if ent.offset + ent.length > i:
                            # If the old start is also before ours, it is fully enclosed
                            if ent.offset <= i:
                                ent.length -= len(delim) * 2
                            else:
                                ent.length -= len(delim)

                    # Append the found entity
                    ent = delimiters[delim]
                    if ent == MessageEntityPre:
                        result.append(ent(i, end - i - len(delim), ''))  # has 'lang'
                    else:
                        result.append(ent(i, end - i - len(delim)))

                    # No nested entities inside code blocks
                    if ent in (MessageEntityCode, MessageEntityPre):
                        i = end - len(delim)

                    continue

            elif url_re:
                m = url_re.match(message, pos=i)
                if m:
                    # Replace the whole match with only the inline URL text.
                    message = ''.join((
                        message[:m.start()],
                        m.group(1),
                        message[m.end():]
                    ))

                    delim_size = m.end() - m.start() - len(m.group())
                    for ent in result:
                        # If the end is after our start, it is affected
                        if ent.offset + ent.length > m.start():
                            ent.length -= delim_size

                    result.append(MessageEntityTextUrl(
                        offset=m.start(), length=len(m.group(1)),
                        url=del_surrogate(m.group(2))
                    ))
                    i += len(m.group(1))
                    continue

            i += 1

        if should_strip:
            message = strip_text(message, result)

        return del_surrogate(message), result

    @staticmethod
    def _impl_telethon_unparse(text, entities, delimiters=None):
        """
        Performs the reverse operation to .parse(), effectively returning
        markdown-like syntax given a normal text and its MessageEntity's.

        :param text: the text to be reconverted into markdown.
        :param entities: the MessageEntity's applied to the text.
        :return: a markdown-like text representing the combination of both inputs.
        """
        if not text or not entities:
            return text

        if not delimiters:
            if delimiters is not None:
                return text
            delimiters = DEFAULT_DELIMITERS

        if isinstance(entities, TLObject):
            entities = (entities,)

        text = add_surrogate(text)
        delimiters = {v: k for k, v in delimiters.items()}
        insert_at = []
        for entity in entities:
            s = entity.offset
            e = entity.offset + entity.length
            delimiter = delimiters.get(type(entity), None)
            if delimiter:
                insert_at.append((s, delimiter))
                insert_at.append((e, delimiter))
            else:
                url = None
                if isinstance(entity, MessageEntityTextUrl):
                    url = entity.url
                elif isinstance(entity, MessageEntityMentionName):
                    url = 'tg://user?id={}'.format(entity.user_id)
                if url:
                    insert_at.append((s, '['))
                    insert_at.append((e, ']({})'.format(url)))

        insert_at.sort(key=lambda t: t[0])
        while insert_at:
            at, what = insert_at.pop()

            # If we are in the middle of a surrogate nudge the position by -1.
            # Otherwise we would end up with malformed text and fail to encode.
            # For example of bad input: "Hi \ud83d\ude1c"
            # https://en.wikipedia.org/wiki/UTF-16#U+010000_to_U+10FFFF
            while within_surrogate(text, at):
                at += 1

            text = text[:at] + what + text[at:]

        return del_surrogate(text)

    def get_entities_content(self):
        content = ""
        for e in self._entities:
            text = self._surrogate_text[e.offset:e.offset + e.length]
            content += f"- {cls_name(e)}: {repr(text)} offset:{e.offset} len:{e.length}\n"
        return content

    def fix_header(self, header):
        if len(header.strip()) == 0:
            return self

        header = add_surrogate(header)

        # safety check, heading should be unique
        if self._surrogate_text.count(header) != 1:
            return self

        # safety check, ignore small or long headings
        if 5 > len(header) or len(header) > 50:
            return self

        index = self._surrogate_text.find(header) - 1
        while is_emoji_or_space(self._surrogate_text[index]) and index >= 0:
            header = self._surrogate_text[index] + header
            index -= 1

        full_heading = header.strip()
        header = fix_brain_cancer(header)
        header = header.replace("•", "")
        header = remove_emojis(header)
        cleared_heading = header.strip()

        _copy = self.replace(full_heading, cleared_heading)
        _copy._entities.append(MessageEntityBold(
            offset=_copy._surrogate_text.find(cleared_heading),
            length=len(cleared_heading)
        ))

        _copy._del_empty(_copy._surrogate_text, _copy._entities)
        _copy._fix_entities(_copy._surrogate_text, _copy._entities)
        return _copy

    def plain(self):
        return del_surrogate(self._surrogate_text)

    def telethon_entities(self):
        return MarkdownPost._copy_entities(self._entities)

    @staticmethod
    def _copy_entities(entities):
        return [
            copy.copy(e)
            for e in entities
        ]

    @staticmethod
    def _entity_from_telethon_to_ptb(entity) -> MessageEntityType:
        return {
            MessageEntityMention: MessageEntityType.MENTION,
            MessageEntityHashtag: MessageEntityType.HASHTAG,
            MessageEntityCashtag: MessageEntityType.CASHTAG,
            MessageEntityPhone: MessageEntityType.PHONE_NUMBER,
            MessageEntityBotCommand: MessageEntityType.BOT_COMMAND,
            MessageEntityUrl: MessageEntityType.URL,
            MessageEntityEmail: MessageEntityType.EMAIL,
            MessageEntityBold: MessageEntityType.BOLD,
            MessageEntityItalic: MessageEntityType.ITALIC,
            MessageEntityCode: MessageEntityType.CODE,
            MessageEntityPre: MessageEntityType.PRE,
            MessageEntityTextUrl: MessageEntityType.TEXT_LINK,
            MessageEntityMentionName: MessageEntityType.TEXT_MENTION,
            MessageEntityUnderline: MessageEntityType.UNDERLINE,
            MessageEntityStrike: MessageEntityType.STRIKETHROUGH
        }.get(type(entity))

    @staticmethod
    def _convert_entities(entities):
        ne = []
        for e in entities:
            ptb_type = MarkdownPost._entity_from_telethon_to_ptb(e)
            if not ptb_type:
                continue

            data = e.to_dict()
            ptb_entity = MessageEntity(
                type=ptb_type,
                offset=e.offset,
                length=e.length,
                url=data.get("url"),
                user=data.get("user_id"),
                language=data.get("language"),
                custom_emoji_id=data.get("custom_emoji_id"),
            )

            ne.append(ptb_entity)

        return ne

    def ptb_entities(self):
        return MarkdownPost._convert_entities(self._entities)

    def json_entities(self):
        return json.dumps([
            e.to_dict()
            for e in self._entities
        ])

    def markdown(self):
        return MarkdownPost._impl_telethon_unparse(
            text=del_surrogate(self._surrogate_text),
            entities=self._entities
        )

    # @staticmethod
    # def _impl_madeline_parse(markdown, resolve_user_fn=None):
    #     markdown = add_surrogate(markdown)
    #
    #     entities = []
    #     message = ''
    #
    #     markdown = markdown.replace('\r\n', '\n')
    #     offset = 0
    #     stack = []
    #     while offset < len(markdown):
    #         match = re.search(r'[*_~@`[\]|!\\]', markdown[offset:])
    #         if match:
    #             len_ = match.start()
    #         else:
    #             len_ = len(markdown) - offset
    #         piece = markdown[offset:offset + len_]
    #         offset += len_
    #         if offset == len(markdown):
    #             message += piece
    #             break
    #         char = markdown[offset]
    #         prev_ = markdown[offset - 1] if offset - 1 > 0 else ''
    #         next_ = markdown[offset + 1] if offset + 1 < len(markdown) else ''
    #
    #         offset += 1
    #
    #         if char == '\\':
    #             message += piece + next_
    #             offset += 1
    #             continue
    #
    #         if char == '@' and (prev_ == " " or prev_ == ""):
    #             match = re.search(r"[.\n\t\W*_]$", markdown[offset:])
    #             if not match:
    #                 pos_close = offset + len(markdown)
    #             else:
    #                 pos_close = offset + match.start()
    #
    #             is_standalone_at = pos_close + 1 == offset
    #             if is_standalone_at:
    #                 piece += char
    #                 offset += len(char)
    #             else:
    #                 nickname = markdown[offset - 1:pos_close]
    #                 piece += nickname
    #
    #                 if resolve_user_fn:
    #                     entities.append(MessageEntityMentionName(
    #                         offset=len(message),
    #                         length=len(nickname),
    #                         user_id=resolve_user_fn(nickname),
    #                     ))
    #
    #                 offset += len(nickname)
    #
    #         elif char == '_' and next_ == '_':
    #             offset += 1
    #             char = '__'
    #         elif char == '*' and next_ == '*':
    #             offset += 1
    #             char = '**'
    #         elif char == '|':
    #             if next_ == '|':
    #                 offset += 1
    #                 char = '||'
    #             else:
    #                 message += piece + char
    #                 continue
    #         elif char == '!':
    #             if next_ == '[':
    #                 offset += 1
    #                 char = ']('
    #             else:
    #                 message += piece + char
    #                 continue
    #         elif char == '[':
    #             char = ']('
    #         elif char == ']':
    #             if not stack or stack[-1][0] != '](':
    #                 message += piece + char
    #                 continue
    #             if next_ != '(':
    #                 stack.pop()
    #                 message += '[' + piece + char
    #                 continue
    #             offset += 1
    #             char = "]("
    #         elif char == '`' and next_ == '`' and markdown[offset + 1] == '`':
    #             message += piece
    #             offset += 2
    #             lang_len = re.search(r'\n ', markdown[offset:]).start()
    #             language = markdown[offset:offset + lang_len]
    #             offset += lang_len
    #             if markdown[offset] == '\n':
    #                 offset += 1
    #             pos_close = offset
    #             while True:
    #                 pos_close = markdown.find('```', pos_close)
    #                 if pos_close == -1 or markdown[pos_close - 1] != '\\':
    #                     break
    #                 pos_close += 1
    #
    #             if pos_close == -1:
    #                 raise AssertionError(f"Unclosed ``` opened @ pos {offset}!")
    #             start = len(message)
    #             piece = markdown[offset:pos_close]
    #             message += piece
    #             piece_len = len(piece.rstrip(' \r\n'))
    #             offset = pos_close + 3
    #             if piece_len > 0:
    #                 entities.append(MessageEntityPre(start, piece_len, language=language))
    #             continue
    #
    #         if stack and stack[-1][0] == char:
    #             _, start = stack.pop()
    #             if char == '](':
    #                 pos_close = offset
    #                 while True:
    #                     pos_close = markdown.find(')', pos_close)
    #                     if pos_close == -1 or markdown[pos_close - 1] != '\\':
    #                         break
    #                     pos_close += 1
    #                 if pos_close == -1:
    #                     raise AssertionError(f"Unclosed ) opened at pos {offset}!")
    #
    #                 href = markdown[offset:pos_close]
    #                 entity = Markdown.handle_link(href)
    #                 offset = pos_close + 1
    #
    #             else:
    #                 entity = {
    #                     '**': MessageEntityBold,
    #                     '_': MessageEntityItalic,
    #                     '__': MessageEntityUnderline,
    #                     '`': MessageEntityCode,
    #                     '~': MessageEntityStrike,
    #                     '||': MessageEntitySpoiler,
    #                 }.get(char)(offset=0, length=0)
    #
    #             message += piece
    #             length_real = len(message) - start
    #             length_real -= len(message.rstrip(' \r\n')) - len(message)
    #
    #             if length_real > 0:
    #                 entity.offset = start
    #                 entity.length = length_real
    #                 entities.append(entity)
    #         else:
    #             message += piece
    #             if char not in ["@"]:
    #                 stack.append((char, len(message)))
    #
    #     if stack:
    #         for elem in stack:
    #             message, entities = Markdown._insert_at(
    #                 surrogate_text=message,
    #                 substring=elem[0],
    #                 pos=elem[1],
    #                 entities=entities
    #             )
    #
    #     return del_surrogate(message), entities

    @staticmethod
    def handle_link(href):
        match = re.search('^mention:(.+)', href) or re.search('^tg://user\\?id=(.+)', href)
        if match:
            return MessageEntityMentionName(offset=0, length=0, user_id=int(match.group(1)))

        match = re.search('^emoji:(\\d+)$', href) or re.search('^tg://emoji\\?id=(.+)', href)
        if match:
            return MessageEntityCustomEmoji(offset=0, length=0, document_id=int(match.group(1)))

        return MessageEntityTextUrl(offset=0, length=0, url=href)

    def replace(self, _old, _new):
        _copy = copy.copy(self)
        _old = add_surrogate(_old)
        _new = add_surrogate(_new)
        _copy._surrogate_text, _copy._entities = MarkdownPost._replace(
            _old=_old,
            _new=_new,
            surrogate_text=_copy._surrogate_text,
            entities=_copy._entities
        )
        return _copy

    @staticmethod
    def _insert_at(surrogate_text, pos, substring, entities):
        start = pos
        end = start + len(substring)
        MarkdownPost._add_range(entities, [start, end])
        surrogate_text = surrogate_text[:pos] + substring + surrogate_text[pos:]
        return surrogate_text, entities

    @staticmethod
    def _replace(_old, _new, surrogate_text, entities):

        if _old == "":
            return surrogate_text, entities

        start = 0
        while True:
            start = surrogate_text.find(_old, start)
            end = start + len(_old)
            if start == -1: break

            if len(_new) != len(_old):
                MarkdownPost._delete_range(entities, [start, start + len(_old)])
                MarkdownPost._add_range(entities, [start, start + len(_new)])
                MarkdownPost._del_empty(surrogate_text, entities)

            surrogate_text = surrogate_text[:start] + _new + surrogate_text[end:]
            start += len(_new)

        return surrogate_text, entities

    # is range a left from range b
    @staticmethod
    def _is_range_a_left_from_range_b(a, b):
        a_start, a_end = a
        b_start, b_end = b
        return a_end <= b_start

    # is range a touching the range b from left side
    @staticmethod
    def _is_range_a_touching_range_b_from_left(a, b):
        a_start, a_end = a
        b_start, b_end = b
        return a_end == b_start

    # is range a overlapping the range b from left side
    @staticmethod
    def _is_range_a_overlapping_range_b_from_left(a, b):
        a_start, a_end = a
        b_start, b_end = b
        return a_end > b_start > a_start

    # is range a inside of range b
    @staticmethod
    def _is_range_a_inside_range_b(a, b):
        a_start, a_end = a
        b_start, b_end = b
        return a_start >= b_start and a_end <= b_end

    @staticmethod
    def _add_range(entities, add_range):
        add_start = add_range[0]
        add_end = add_range[1]  # non-inclusive
        add_len = add_end - add_start

        r_add = [add_start, add_end]
        for i in reversed(range(len(entities))):
            e = entities[i]
            e_end = e.offset + e.length
            r_entity = [e.offset, e.offset + e.length]

            if MarkdownPost._is_range_a_touching_range_b_from_left(r_entity, r_add):
                e.length += add_len
                continue

            if MarkdownPost._is_range_a_overlapping_range_b_from_left(r_entity, r_add):
                overlap = e_end - add_start
                e.length += add_len - overlap

            if MarkdownPost._is_range_a_inside_range_b(r_entity, r_add):
                e.length += add_len
                continue

            if MarkdownPost._is_range_a_inside_range_b(r_add, r_entity):
                e.length += add_len
                continue

            if MarkdownPost._is_range_a_overlapping_range_b_from_left(r_add, r_entity):
                e.offset += add_len
                continue

            if MarkdownPost._is_range_a_left_from_range_b(r_add, r_entity):
                e.offset += add_len
                continue

    @staticmethod
    def _del_empty(surrogate_text, entities):
        for i in reversed(range(len(entities))):
            e = entities[i]
            if e.length <= 0:
                del entities[i]
                continue

            content = surrogate_text[e.offset: e.offset + e.length].strip()
            if len(content) == 0:
                del entities[i]
                continue

    @staticmethod
    def _delete_range(entities, delete_range):
        delete_start = delete_range[0]
        delete_end = delete_range[1]  # non-inclusive
        delete_len = delete_range[1] - delete_range[0]

        r_delete = [delete_start, delete_end]
        for i in reversed(range(len(entities))):
            e = entities[i]

            e_start = e.offset
            e_end = e.offset + e.length
            r_entity = [e.offset, e.offset + e.length]

            if MarkdownPost._is_range_a_overlapping_range_b_from_left(r_entity, r_delete):
                overlap = e_end - delete_start
                e.length -= overlap
                continue

            if MarkdownPost._is_range_a_inside_range_b(r_entity, r_delete):
                e.length = 0
                continue

            if MarkdownPost._is_range_a_inside_range_b(r_delete, r_entity):
                e.length -= delete_len
                continue

            if MarkdownPost._is_range_a_overlapping_range_b_from_left(r_delete, r_entity):
                overlap = delete_start - e_start
                e.offset += overlap
                e.length += overlap
                continue

            if MarkdownPost._is_range_a_left_from_range_b(r_delete, r_entity):
                e.offset -= delete_len
                continue


def test_markdown():
    # is range a left from range b
    assert MarkdownPost._is_range_a_left_from_range_b((1, 3), (5, 7)) == True
    assert MarkdownPost._is_range_a_left_from_range_b((4, 6), (3, 9)) == False

    # is range a touching the range b from left side
    assert MarkdownPost._is_range_a_touching_range_b_from_left((1, 3), (3, 6)) == True
    assert MarkdownPost._is_range_a_touching_range_b_from_left((3, 6), (1, 3)) == False

    # is range a overlapping the range b from left side
    assert MarkdownPost._is_range_a_overlapping_range_b_from_left((3, 6), (3, 6)) == False
    assert MarkdownPost._is_range_a_overlapping_range_b_from_left((3, 6), (4, 7)) == True
    assert MarkdownPost._is_range_a_overlapping_range_b_from_left((3, 6), (5, 7)) == True
    assert MarkdownPost._is_range_a_overlapping_range_b_from_left((5, 7), (3, 6)) == False

    # is range a inside of range b
    assert MarkdownPost._is_range_a_inside_range_b((2, 4), (1, 5)) == True
    assert MarkdownPost._is_range_a_inside_range_b((2, 4), (2, 4)) == True
    assert MarkdownPost._is_range_a_inside_range_b((2, 4), (2, 3)) == False
    assert MarkdownPost._is_range_a_inside_range_b((1, 5), (2, 4)) == False

    assert MarkdownPost._is_range_a_left_from_range_b((1, 3), (3, 6)) == True  # edge case where ranges touch

    # is range a touching the range b from left side
    assert MarkdownPost._is_range_a_touching_range_b_from_left((1, 3), (3, 6)) == True
    assert MarkdownPost._is_range_a_touching_range_b_from_left((1, 4), (4, 6)) == True

    # is range a overlapping the range b from left side
    assert MarkdownPost._is_range_a_overlapping_range_b_from_left((1, 3), (3, 6)) == False  # just touching
    assert MarkdownPost._is_range_a_overlapping_range_b_from_left((1, 2),
                                                                  (2, 4)) == False  # end of one range is start of other

    # is range a inside of range b
    assert MarkdownPost._is_range_a_inside_range_b((1, 5), (1, 5)) == True  # both ranges are same

    test_cases = [
        {
            "input": "remove [this](github.com) link **pls**",
            "old": "this",
            "new": "",
            "output": "remove  link **pls**",
        },
        {
            "input": "**hello** **hello**",
            "old": "hello",
            "new": "",
            "output": " ",
        },
        {
            "input": "**one** **two**",
            "old": "one",
            "new": "",
            "output": " **two**",
        },
        {
            "input": "**one** **two**",
            "old": "two",
            "new": "",
            "output": "**one** ",
        },
        {
            "input": "**onetwo**",
            "old": "two",
            "new": "",
            "output": "**one**",
        },
        {
            "input": "**onetwo**",
            "old": "one",
            "new": "",
            "output": "**two**",
        },
        {
            "input": "**aaa              ccccc**",
            "old": "aaa",
            "new": "bbbbb",
            "output": "**bbbbb              ccccc**",
        },
        {
            "input": "**aaabbbb**",
            "old": "aaa",
            "new": "ccccccccccccccc",
            "output": "**cccccccccccccccbbbb**",
        },
        {
            "input": "**one**",
            "old": "one",
            "new": "three",
            "output": "**three**",
        },
        {
            "input": "replace [this](github.com) link **pls**",
            "old": "this",
            "new": "thisee",
            "output": "replace [thisee](github.com) link **pls**",
        },
        {
            "input": "remove [this link](github.com) **pls**",
            "old": "this",
            "new": "",
            "output": "remove [ link](github.com) **pls**",
        },
        {
            "input": "remove [this](github.com) link **pls**",
            "old": "this",
            "new": "",
            "output": "remove  link **pls**",
        },
        {
            "input": "replace [this\U0001fae1](github.com) link **pls**",
            "old": "this",
            "new": "thisee",
            "output": "replace [thisee\U0001fae1](github.com) link **pls**",
        },
        {
            "input": "remove __**hello__",
            "old": "hello",
            "new": "",
            "output": "remove __**__",
        },
        {
            "input": "remove __**hello**__",
            "old": "hello",
            "new": "",
            "output": "remove ",
        },
        {
            "input": "    **remove**",
            "old": "",
            "new": "",
            "output": "    **remove**",
        },
        {
            "input": "**" + "".join(SHOULD_NOT_BE_INSIDE_MARKDOWN) + "__content__" + "".join(
                SHOULD_NOT_BE_INSIDE_MARKDOWN) + "**",
            "old": "",
            "new": "",
            "output": "".join(SHOULD_NOT_BE_INSIDE_MARKDOWN) + "**__" + "content" + "**__" + "".join(
                SHOULD_NOT_BE_INSIDE_MARKDOWN),
        },
        {
            "input": "**\n**",
            "old": "",
            "new": "",
            "output": "\n",
        },
        {
            "input": "****",
            "old": "",
            "new": "",
            "output": "****",
        },
    ]

    for case in test_cases:
        mt = MarkdownPost(case["input"])
        mt = mt.replace(case["old"], case["new"])
        if mt.markdown() != case["output"]:
            raise AssertionError(f"Input:{repr(case['input'])} Result:{repr(mt.markdown())} != {repr(case['output'])}")
