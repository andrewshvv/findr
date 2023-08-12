import sqlite3
from typing import Union

import aiomisc
from aiosqlite import Connection, Cursor


@aiomisc.asyncbackoff(attempt_timeout=10,
                      deadline=10,
                      pause=0.1,
                      max_tries=10,
                      exceptions=(sqlite3.OperationalError,))
async def safe_db_execute(db: Union[Connection, Cursor], *args, **kwargs) -> Cursor:
    return await db.execute(*args, **kwargs)
