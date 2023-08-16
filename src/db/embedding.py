import logging
import os

import aiomisc
import chromadb
import openai
from aiolimiter import AsyncLimiter
from chromadb import API
from chromadb.api.models.Collection import Collection
from chromadb.api.types import ID
from chromadb.errors import IDAlreadyExistsError

from common.logging import cls_name, shorten_text
from common.utils import get_match_percentage
from db.sqlite import GET_POST_BY_POST_ID

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class EmbeddingDB(aiomisc.Service):
    environment = "PROD"
    client: API = None
    recreate_prompts = False
    recreate_posts = False
    embedding_rate_limit = AsyncLimiter(max_rate=60, time_period=60)

    def prepare_text_for_index(self, text):
        # TODO: Clear text for index
        # - Hypothesis: Better index performance, better similarity search
        # - remove markdown
        # - replace \n to .
        # - split on nltk
        # - filter sentences
        # - re-join in text
        # TODO: Possible store cleared text in the index?
        pass

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
    async def create_embedding(self, contents: list) -> (int, list):
        if type(contents) is not list:
            raise ValueError("input should be list")

        async with self.embedding_rate_limit:
            response = await openai.Embedding.acreate(
                model="text-embedding-ada-002",
                input=contents
            )

        tokens_used = response['usage']['total_tokens']

        # enc = tiktoken.get_encoding("cl100k_base")
        # num_tokens = len(enc.encode(get_prompt("preprocess.txt") + content))

        log.debug(
            f"{cls_name(self)}: "
            f"Created embedding  "
            f"tokens_used:{tokens_used} "
            f"text:'{shorten_text(contents[0])}'"
        )

        if len(response["data"]) > 1:
            return tokens_used, [elem["embedding"] for elem in response["data"]]
        else:
            return tokens_used, [response["data"][0]["embedding"]]

    def is_test(self):
        return self.environment == "TEST"

    def is_dev(self):
        return self.environment == "DEV"

    async def start(self):
        # Change class field according to instance field
        EmbeddingDB.environment = self.environment or "PROD"

        if self.is_dev():
            db_path = os.path.join(os.getcwd(), "dev_chroma")
        elif self.is_test():
            db_path = os.path.join(os.getcwd(), "test_chroma")
            try:
                from send2trash import send2trash
                send2trash(db_path)
            except OSError:
                pass
        else:
            db_path = os.path.join(os.getcwd(), "chroma")

        self.client = chromadb.PersistentClient(path=db_path)

        assert (os.getenv("OPENAI_API_KEY") is not None)
        openai.api_key = os.getenv("OPENAI_API_KEY")

        if self.recreate_posts:
            try:
                self.client.delete_collection(name="posts")
            except ValueError:  # in case there is no collection
                pass

        if self.recreate_prompts:
            try:
                self.client.delete_collection(name="prompts")
            except ValueError:  # in case there is no collection
                pass

        log.info(
            f"{cls_name(self)}: "
            f"Getting the post collection"
        )
        index_posts = self.client.get_or_create_collection(
            name="posts",
            metadata={
                "hnsw:space": "cosine",
                "hnsw:M": 16,
                "hnsw:construction_ef": 200,
            }
        )

        log.info(
            f"{cls_name(self)}: "
            f"Getting the search_requests collection"
        )
        index_prompts = self.client.get_or_create_collection(
            name="prompts",
            metadata={
                "hnsw:space": "cosine",
                "hnsw:M": 16,
                "hnsw:construction_ef": 200,
            }
        )

        self.context['index_prompts'] = index_prompts
        self.context['index_posts'] = index_posts
        self.context['create_embedding'] = self.create_embedding

    async def stop(self, *args, **kwargs):
        log.info(
            f"{cls_name(self)}: "
            f"Stopping index client"
        )
        self.client.stop()


class PostsCollection:
    _collection: Collection

    def __init__(self, collection: Collection, create_embedding):
        self._collection = collection
        self._create_embedding = create_embedding

    def peek(self, *args, **kwargs):
        return self._collection.peek(*args, **kwargs)

    async def find_same_post(self, db, post_text: str):
        if len(post_text.strip()) == 0:
            log.warning(
                f"{cls_name(self)} "
                f"Empty content "
            )
            return 0, []

        _, embeddings = await self._create_embedding([post_text])
        try:
            results = self.query_post(
                query_embeddings=embeddings,
                n_results=30,
                include=["distances", "metadatas"]
            )
        except RuntimeError as e:
            log.exception(e)
            return 0, []

        for distance, metadata in zip(results["distances"][0], results["metadatas"][0]):
            if distance > 0.1:
                break

            cursor = await db.execute(GET_POST_BY_POST_ID, [metadata["post_id"]])
            row = await cursor.fetchone()
            if not row:
                log.error(
                    f"Requested by ChromaDB post id doesn't exist in database "
                    f"index_id:{metadata['post_id']}"
                )
                continue

            (similar_text, _, _, post_status) = row
            if post_status == "rejected":
                # TODO: Just remove the index
                log.warning(
                    f"{cls_name(self)} "
                    f"Seems like index contains rejected posts "
                    f"pid: {metadata['post_id']}"
                )
                continue

            match_ratio = get_match_percentage(post_text, similar_text)
            if match_ratio < 0.8:
                break

            return {
                       "source": metadata["source"],
                       "post_id": metadata["post_id"],
                       "match_ratio": match_ratio,
                       "index_distance": distance,
                       "text": similar_text,
                   }, embeddings[0]

        return None, embeddings[0]

    # @with_lock(locks["chromadb.get"])
    # @aiomisc.threaded  # for some reason it fails with segmentation fault if added
    async def insert_post(self, post_id: ID, source, text=None, embedding=None):
        tokens_used = 0
        if not text and not embedding:
            raise NotImplementedError

        if text and not embedding:
            tokens_used, embeddings = await self._create_embedding([text])
            embedding = embeddings[0]

        try:
            self._collection.add(
                embeddings=[embedding],
                ids=[post_id],
                metadatas=[{"source": source, "post_id": post_id}]  # for search
            )
            return True, tokens_used
        except IDAlreadyExistsError:
            log.warning(f"{cls_name(self)}: "
                        f"IDAlreadyExistsError: "
                        f"source:'{source}' "
                        f"post_id:'{post_id}' "
                        f"text:{shorten_text(text)}")
            return False, tokens_used

    def remove_posts(self, *args, **kwargs):
        if kwargs.get("ids") is not None and len(kwargs["ids"]) == 0: return
        self._collection.delete(*args, **kwargs)

    def get_posts(self, *args, **kwargs):
        return self._collection.get(*args, **kwargs)

    def query_post(self, *args, **kwargs):
        results = self._collection.query(*args, **kwargs)

        if "documents" in results and results["documents"] is not None:
            if len(results["documents"][0]) != len(results["ids"][0]):
                raise ValueError("seems like _index corrupted")

        if "embeddings" in results and results["embeddings"] is not None:
            if len(results["embeddings"][0]) != len(results["ids"][0]):
                raise ValueError("seems like _index corrupted")

        if "distances" in results and results["distances"] is not None:
            if len(results["distances"][0]) != len(results["ids"][0]):
                raise ValueError("seems like _index corrupted")

        return results

    # @with_lock(locks["chromadb.get"])
    # @aiomisc.threaded  # Process finished with exit code 139 (interrupted by signal 11: SIGSEGV)
    def get_post_by_source(self, source, **kwargs):
        results = self._collection.get(where={"source": source}, **kwargs, limit=1)
        if len(results["ids"]) > 0:
            return results

        return None
