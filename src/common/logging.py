import logging
from datetime import datetime, timedelta

import pytz
from pytz import utc
from telethon.utils import sanitize_parse_mode


def strip_markdown(text):
    parser = sanitize_parse_mode("md")
    text, _ = parser.parse(text)
    return text


def shorten_text(text):
    return strip_markdown(text).replace("\n", " ")[:50]


def humanize_time(dt):
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
        dt = dt.replace(tzinfo=pytz.UTC)

    diff = datetime.now(utc) - dt
    if diff < timedelta(minutes=1):
        return 'just now'
    elif diff < timedelta(hours=1):
        return f'{diff.seconds // 60} minutes ago'
    elif diff < timedelta(days=1):
        return f'{diff.seconds // 3600} hours ago'
    elif diff < timedelta(days=7):
        return f'{diff.days} days ago'
    else:
        return f'{diff.days // 7} weeks ago'


def cls_name(obj):
    if isinstance(obj, type):
        return obj.__name__
    else:
        return type(obj).__name__


class suppress_logs:
    def __init__(self, logger_name):
        self.logger = logging.getLogger(logger_name)

    def __enter__(self):
        self.old_level = self.logger.level
        self.logger.setLevel(logging.CRITICAL)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.setLevel(self.old_level)


def flush_logs():
    current_loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    for logger in current_loggers:
        # iterate over each logger and get their handlers
        handlers = logger.handlers
        for handler in handlers:
            # flush handler logs
            handler.flush()
