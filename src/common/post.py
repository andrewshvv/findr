from datetime import datetime

import pytz

from common.logging import humanize_time
from common.markdown import MarkdownPost
from common.telegram import TelegramTextTools


def prepare_post(text: str, markdown_entities: str, more_info_text=None, meta_info=None):
    text = MarkdownPost(text=text, entities=markdown_entities)
    if meta_info:
        text = add_meta(text, meta_info)

    text = TelegramTextTools.prepare_description_for_tg(text, more_info_text)
    return text.plain(), text.ptb_entities()


def add_meta(text: MarkdownPost, meta_info) -> MarkdownPost:
    meta_text = ""
    date = humanize_time(meta_info["date"])

    if meta_info["original_link"]:
        meta_text = f"original_link: [link]({meta_info['original_link']})\n"

    if meta_info["post_id"]:
        meta_text += f"pid: {meta_info['post_id']}\n"
        meta_text += f"source: {meta_info['source']}\n"

    meta_text += f"posted at: {date}\n"

    channel_id = int(meta_info["source"].split(":")[1])
    channel_name = None
    for channel in meta_info["channels"]:
        if channel["channel_id"] == channel_id:
            channel_name = channel["name"]
            break
    if channel_name:
        meta_text += f"channel: {channel_name}\n"

    meta_text += "\n"
    return MarkdownPost(meta_text) + text
