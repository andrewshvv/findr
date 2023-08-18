import logging
import os
import sqlite3

import aiomisc
import aiosqlite

from common.logging import cls_name

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

MY_TEST_USER = "1"

with open(os.path.join(os.path.dirname(__file__), "resume.txt")) as f:
    MY_TEST_PROMPT = f.read()

GET_ACCEPTED_POSTS = f"""
    SELECT post_id 
    FROM users_posts
    WHERE users_posts.user_id = {MY_TEST_USER} AND
          users_posts.process_status = 'accepted' AND
          users_posts.post_status = 'new'
    ORDER BY users_posts.post_id
"""

UPDATE_PROMPT = f"""
    UPDATE prompts 
    SET status = ?, tags = ?, eli5 = ?
    WHERE prompt_id = ?
"""

GET_PROMPT_BASE_DISTANCE = """
    SELECT distance 
    FROM prompts
    WHERE prompt_id = ?
"""

SET_PROMPT_BASE_DISTANCE = """
    UPDATE prompts 
    SET distance = ?
    WHERE prompt_id = ?
"""

GET_POST_FOR_INTEGRITY_CHECK = """
SELECT status
FROM posts 
WHERE source = ?
"""

GET_POST_BY_POST_ID = """
    SELECT description, date, source, status
    FROM posts
    WHERE post_id = ?
"""

GET_POST_BY_SOURCE = """
    SELECT description, date, source, status
    FROM posts
    WHERE source = ?
"""

GET_PROMPTS_FOR_PROCESSING = f"""
    SELECT original as prompt, prompt_id, user_id
    FROM prompts
    WHERE status IS NULL 
        AND active = 1
"""

GET_POSTS_FOR_PROCESSING = """
    WITH
    numbered_active_prompts AS (
        SELECT
            prompt_id,
            user_id,
            eli5 as prompt,
            status,
            ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY prompts.date DESC) AS row_num
        FROM prompts
        WHERE active = 1 
            AND status != 'rejected' 
            AND status IS NOT NULL 
    ),

    active_prompts AS (
        SELECT
               prompt_id,
               user_id,
               prompt,
               status
        FROM numbered_active_prompts
        WHERE row_num = 1
    ),

    recent_posts AS (
        SELECT post_id
        FROM posts
        WHERE posts.date > date('now','-{days} day') AND
              posts.status = 'accepted'
    )

    SELECT
        active_prompts.prompt_id,
        active_prompts.prompt,
        active_prompts.status as prompt_status,
        recent_posts.post_id,
        active_prompts.user_id,
        users_posts.process_status,
        users_posts.index_distance
    FROM recent_posts, active_prompts
    LEFT JOIN users_posts
        ON  recent_posts.post_id = users_posts.post_id AND
            active_prompts.prompt_id = users_posts.prompt_id
        WHERE
            (users_posts.prompt_id IS NULL AND 
            users_posts.post_id IS NULL AND 
            TRIM(COALESCE(active_prompts.prompt, '')) <> '')
        -- OR
        --     users_posts.process_status = 'index_approved'
        ORDER BY recent_posts.post_id, active_prompts.user_id
"""

GET_POST_BY_PID = """
SELECT description
FROM posts
WHERE post_id = ?
"""

CHECK_IS_IN_DB = """
SELECT transient_id
FROM posts
WHERE transient_id IN ({params})
"""

INSERT_INTO_POSTS = """
    INSERT 
    INTO posts(transient_id, description, date, source, status, reason, contact, language, original_link, markdown_entities) 
    VALUES(?,?,?,?,?,?,?,?,?,?)
"""

ADD_TRANSIENT_ID = """
UPDATE posts 
SET transient_id = ? 
WHERE post_id = ?
"""

REMOVE_POST_BY_TID = """
DELETE
FROM posts
WHERE transient_id = ?
"""

RESEND_POST_BY_TID = """
UPDATE posts
SET transient_id = NULL
WHERE transient_id = ? AND status <> 'rejected'
"""

GET_PID_BY_TID = """
SELECT post_id
FROM posts 
WHERE transient_id = ?
LIMIT 1
"""

GET_ALL_ACCEPTED_POSTS = """
SELECT post_id, source, description
FROM posts
WHERE status <> 'rejected'
"""

GET_ALL_APPROVED_PROMPTS = """
SELECT prompt_id, tags
FROM prompts
WHERE status != 'rejected' AND status IS NOT NULL
"""

COUNT_ACCEPTED_POSTS = """
SELECT COUNT(*)
FROM posts
WHERE status <> 'rejected' 
"""

COUNT_APPROVED_PROMPTS = """
SELECT COUNT(*)
FROM prompts
WHERE status != 'rejected' AND status IS NOT NULL
"""

POSTS_FOR_CLEAN = """
SELECT post_id, status, date
FROM posts 
WHERE posts.date <= date('now','-{days} day')
"""

CLEAN_POSTS = """
DELETE
FROM posts 
WHERE posts.date <= date('now','-{days} day')
"""

GET_POST_BY_TID = """
SELECT description, original_link, post_id, contact, markdown_entities, source, date
FROM posts 
WHERE transient_id = ?
LIMIT 1
"""

FLAG_POST_BY_TID = """
UPDATE posts 
SET transient_id = NULL, 
    status = 'rejected',
    reason = 'flagged' 
WHERE transient_id = ? AND status <> 'rejected'
"""

FLAG_POST_BY_PID = """
UPDATE posts 
SET transient_id = NULL, 
    status = 'rejected',
    reason = ? 
WHERE post_id = ? AND status <> 'rejected'
"""

GET_POSTS_NOT_IN_TRANSIENT = """
SELECT post_id, original_link, description, markdown_entities, source, date
FROM posts
WHERE posts.status = 'accepted' 
    AND posts.transient_id is NULL
"""

PROMPTS_SET_FIRST_SEARCH_READY = """
    UPDATE prompts 
    SET status = 'first_search_done'
    WHERE prompt_id = ?
"""

INSERT_OR_IGNORE_USER_POSTS = """
    INSERT OR IGNORE
    INTO users_posts(user_id, post_id, prompt_id, process_status, gpt_reason, index_distance)
    VALUES(?,?,?,?,?,?)
"""


class SQLLite3Service(aiomisc.Service):
    db_path = None
    environment = "PROD"
    name = "db.sqlite"
    add_test_prompts: bool = False
    add_test_posts: bool = False
    add_test_user_posts: bool = False

    drop_user_posts: bool = False
    drop_prompts: bool = False
    drop_posts: bool = False

    DROP_PROMPTS = """
        DROP TABLE IF EXISTS prompts;
    """

    DROP_USERS_POSTS = """
        DROP TABLE IF EXISTS users_posts;
    """

    DROP_POSTS = """
        DROP TABLE IF EXISTS posts;
    """

    CREATE_USERS_POST_TABLE = f"""
        CREATE TABLE IF NOT EXISTS users_posts( 
            user_id INTEGER, 
            prompt_id INTEGER,
            post_id INTEGER, 
            post_status TEXT, /* new, forwarded */ 
            process_status TEXT, /* accepted, rejected */
            clicked_more_info_at TEXT,
            gpt_reason TEXT,
            index_distance TEXT,
            PRIMARY KEY (post_id, prompt_id)   
        );
        """

    CREATE_PROMPTS_TABLE = f"""
        CREATE TABLE IF NOT EXISTS prompts(
            prompt_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, 
            tags TEXT,
            original TEXT,
            date TEXT,
            active UINT,
            status TEXT,
            distance TEXT,
            eli5 TEXT
        );
    """

    CREATE_POSTS_TABLE = """
        CREATE TABLE IF NOT EXISTS posts(
            post_id INTEGER PRIMARY KEY AUTOINCREMENT,
            transient_id INTEGER,
            description TEXT,
            date TEXT,
            source TEXT,
            status TEXT,
            reason TEXT,
            contact TEXT,
            language TEXT,
            original_link TEXT,
            markdown_entities TEXT
        );        
        """

    #             CREATE TABLE IF NOT EXISTS jobs(
    #                 job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    #                 plain_text TEXT, /* text without markdown */
    #                 markdown_entities TEXT  /* json markdown objects */
    #                 more_info TEXT, /* additional info, like links, email, phones */
    #                 job_created_at TEXT, /* date of job posting created */
    #                 status TEXT, /* accepted, rejected */
    #                 reject_reason TEXT /* why job post was rejected */,
    #                 language TEXT, /* language of the job posting */
    #                 meta_post_manager_id INTEGER, /* id in post manager channel */
    #                 meta_from_pid_cid TEXT, /* unique id of telegram post from which we source the jobs */
    #                 meta_from_link TEXT, /* link on telegram post for public channels */
    #
    #
    #                 name TEXT, /* for image */
    #                 category TEXT, /* for image */
    #                 salary_min INTEGER, /* for image */
    #                 salary_max INTEGER, /* for image */
    #                 currency TEXT, /* for image */
    #                 company TEXT, /* for image */

    # ADD_PROMPT = """
    #     INSERT INTO prompts(user_id,message,date,active) VALUES(?,?,?,?);
    # """
    #
    # ADD_TEST_PROMPTS = f"""
    #     /* Create few records in this table */
    #     INSERT INTO prompts(user_id,message,date,active) VALUES('2', 'Not active prompt', '2023-07-08', 0);
    #     INSERT INTO prompts(user_id,message,date,active) VALUES('2', 'First prompt', '2023-07-09', 1);
    #     INSERT INTO prompts(user_id,message,date,active) VALUES('2', 'Second prompt', '2023-07-08', 1);
    # """
    #
    # ADD_TEST_USERS_POSTS = f"""
    #     /* user_posts table needs 4 values as per the table structure but there are only 2 entries */
    #     INSERT INTO users_posts VALUES(1, 1, 1,'new', 'accepted');
    #     INSERT INTO users_posts VALUES(1, 2, 1, 'new', 'rejected');
    # """
    #
    # ADD_TEST_POSTS = f"""   /* Avoiding duplicate Ids */
    #     INSERT INTO posts VALUES('1','{datetime.now(utc)}');
    #     INSERT INTO posts VALUES('2','{datetime.now(utc)}');
    #     INSERT INTO posts VALUES('3','{datetime.now(utc)}');
    #     INSERT INTO posts VALUES('4','{datetime.now(utc)}');
    #     INSERT INTO posts VALUES('5','{datetime.now(utc)}');
    #     INSERT INTO posts VALUES('6','{datetime.now(utc)}');
    #
    #     INSERT INTO posts VALUES('7','2023-07-09 08:01:03+00:00');
    #     INSERT INTO posts VALUES('8','2023-06-09');
    #     INSERT INTO posts VALUES('9','2023-06-09');
    #     INSERT INTO posts VALUES('10','2023-06-09');
    #     INSERT INTO posts VALUES('11','2023-06-09');
    # """

    def is_test(self):
        return self.environment == "TEST"

    def is_dev(self):
        return self.environment == "DEV"

    async def start(self):
        SQLLite3Service.environment = self.environment

        if not self.db_path:
            if self.is_dev():
                SQLLite3Service.db_path = os.path.join(os.getcwd(), "dev.sqlite")
            elif self.is_test():
                SQLLite3Service.db_path = os.path.join(os.getcwd(), "test.sqlite")
                try:
                    from send2trash import send2trash
                    send2trash(SQLLite3Service.db_path)
                except OSError:
                    pass
            else:
                SQLLite3Service.db_path = os.path.join(os.getcwd(), "db.sqlite")

        async with aiosqlite.connect(SQLLite3Service.db_path) as db:
            if self.drop_prompts or self.drop_user_posts:
                await db.executescript(self.DROP_USERS_POSTS)

            if self.drop_prompts:
                await db.executescript(self.DROP_PROMPTS)

            if self.drop_posts:
                await db.executescript(self.DROP_POSTS)

            await db.executescript(self.CREATE_POSTS_TABLE)
            await db.executescript(self.CREATE_USERS_POST_TABLE)
            await db.executescript(self.CREATE_PROMPTS_TABLE)

            if self.add_test_prompts:
                try:
                    await db.execute(self.ADD_PROMPT, (MY_TEST_USER, MY_TEST_PROMPT, '2023-07-08', 1))
                    await db.executescript(self.ADD_TEST_PROMPTS)
                except sqlite3.IntegrityError:
                    log.warning(f"{cls_name(self)}: add test prompts sqlite3.IntegrityError")

            if self.add_test_posts:
                await db.executescript(self.ADD_TEST_POSTS)

            if self.add_test_user_posts:
                await db.executescript(self.ADD_TEST_USERS_POSTS)

            await db.commit()

        self.context["sqlite_ready"] = True

    async def stop(self, *args, **kwargs):
        pass
