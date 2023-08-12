# -*- coding: UTF-8 -*-
import asyncio
import os
import re
from collections import defaultdict
from difflib import SequenceMatcher

from pyee import AsyncIOEventEmitter
from pytz import utc
from telethon.helpers import add_surrogate, del_surrogate


def str_utc_time(date):
    return date.astimezone(utc).strftime("%Y-%m-%d %H:%M:%S")


def get_prompt(name):
    with open(os.path.join(os.getcwd(), "src", "gpt", "prompts", name), "r") as f:
        return f.read()


def print_event(**kwargs):
    print(kwargs)


# def how_similar_chromadb(a, b):
#     client = chromadb.Client(Settings(
#         anonymized_telemetry=False,
#         chroma_db_impl="duckdb+parquet",
#         persist_directory="chroma"
#     ))
#
#     try:
#         client.delete_collection(name="test")
#     except IndexError:
#         pass
#
#     collection = client.get_or_create_collection(
#         "test",
#         metadata={"hnsw:space": "cosine"}
#     )
#
#     collection.add(
#         embeddings=[a, b],
#         ids=["1", "2"]
#     )
#
#     results = collection.query(
#         query_embeddings=[a],
#         n_results=2,
#         include=["distances"],
#     )
#
#     return results["distances"][0][1]
#
#
# def how_similar_hnswlib(a, b):
#     a = np.array(a)
#     b = np.array(b)
#
#     dim = len(a)
#     num_elements = 2
#
#     # Declaring _index
#     p = hnswlib.Index(space='cosine', dim=dim)  # possible options are l2, cosine or ip
#
#     # Initializing _index - the maximum number of elements should be known beforehand
#     p.init_index(max_elements=num_elements, ef_construction=200, M=16)
#
#     # Element insertion (can be called several times):
#     p.add_items([a, b], [1, 2])
#
#     # Controlling the recall by setting ef:
#     # p.set_ef(50)  # ef should always be > k
#
#     # Query dataset, k - number of the closest elements (returns 2 numpy arrays)
#     labels, distances = p.knn_query(a, k=2)
#     return distances[0][1]


def get_match_percentage(text1, text2):
    return SequenceMatcher(None, text1, text2).ratio()


async def perform(ag):
    async for _ in ag:
        pass


def get_by_index(array: list, index: int, default=None):
    try:
        return array[index]
    except IndexError:
        return default


EMOJ = re.compile(
    "["
    u"\U0001F600-\U0001F64F"  # emoticons
    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
    u"\U0001F680-\U0001F6FF"  # transport & map symbols
    u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
    u"\U00002500-\U00002BEF"  # chinese char
    u"\U00002702-\U000027B0"
    u"\U00002702-\U000027B0"
    u"\U000024C2-\U0001F251"
    u"\U0001f926-\U0001f937"
    u"\U00010000-\U0010ffff"
    u"\u2640-\u2642"
    u"\u2600-\u2B55"
    u"\u200d"
    u"\u23cf"
    u"\u23e9"
    u"\u231a"
    u"\ufe0f"  # dingbats
    u"\u3030"
    "]+", re.UNICODE)


def is_emoji_or_space(char):
    if re.match(EMOJ, char) or char == ' ':
        return True
    else:
        return False


def remove_emojis(data):
    return del_surrogate(re.sub(EMOJ, '', add_surrogate(data)))


def is_event_received(emitter: AsyncIOEventEmitter, name):
    is_fired = asyncio.get_running_loop().create_future()

    # register your event
    async def wrapper(*args, **kwargs):
        is_fired.set_result(None)

    emitter.once(name, wrapper)
    return is_fired


async def wait_for_event_or_execute(
        default_handler,
        emitter: AsyncIOEventEmitter,
        event_name,
        timeout=5):
    try:
        await asyncio.wait_for(is_event_received(
            emitter=emitter,
            name=event_name,
        ), timeout=timeout)
    except asyncio.TimeoutError:
        await default_handler()


def group_list(input_list, length):
    # Using list comprehension
    return [input_list[i:i + length] for i in range(0, len(input_list), length)]
