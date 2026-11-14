"""日志库与源库的连接辅助。"""
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

from .config import Settings


@contextmanager
def log_conn(settings: Settings):
    conn = psycopg2.connect(settings.log_db_dsn)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def source_conn(settings: Settings):
    conn = psycopg2.connect(settings.source_db_dsn)
    try:
        yield conn
    finally:
        conn.close()


def log_query(settings: Settings, sql: str, params=None):
    with log_conn(settings) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def source_query(settings: Settings, sql: str, params=None):
    with source_conn(settings) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]
