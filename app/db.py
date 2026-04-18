from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import psycopg
from psycopg.rows import dict_row


ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = ROOT / "schema.sql"


def database_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://truepb:truepb@localhost:5432/truepb_live",
    )


@contextmanager
def get_conn():
    conn = psycopg.connect(database_url(), row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()


def ensure_schema() -> None:
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with psycopg.connect(database_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
