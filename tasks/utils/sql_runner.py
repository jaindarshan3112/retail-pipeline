"""
SQL runner — executes .sql files against Snowflake.

Used by the Phase 2 driver and (later) by Airflow tasks. Handles:
    * Reading a .sql file from disk
    * Splitting on semicolons for multi-statement files
    * Logging which statement is running
    * Capturing row counts for SELECTs and DDL/DML results
"""

from __future__ import annotations

from pathlib import Path

import snowflake.connector

from tasks.utils.config import SF
from tasks.utils.logger import get_logger

log = get_logger(__name__)


def _split_statements(sql_text: str) -> list[str]:
    """
    Naive splitter: splits on ; that ends a statement.
    Skips empty statements and pure-comment chunks.
    Good enough for our hand-written files (no semicolons inside strings).
    """
    raw = [s.strip() for s in sql_text.split(";")]
    cleaned = []
    for s in raw:
        # Drop empty or comment-only chunks
        if not s:
            continue
        # Check if any non-comment, non-blank line exists
        has_code = any(
            line.strip() and not line.strip().startswith("--")
            for line in s.splitlines()
        )
        if has_code:
            cleaned.append(s)
    return cleaned


def run_sql_file(file_path: Path,
                 conn: snowflake.connector.SnowflakeConnection | None = None) -> None:
    """
    Execute every statement in `file_path` against Snowflake.

    If `conn` is None, opens its own connection (one-shot use).
    Otherwise reuses the provided connection (when running many files).
    """
    sql_text = file_path.read_text()
    statements = _split_statements(sql_text)
    log.info(f"  ▶ {file_path.relative_to(file_path.parents[2])} "
             f"({len(statements)} statement{'s' if len(statements) != 1 else ''})")

    close_after = False
    if conn is None:
        conn = snowflake.connector.connect(**SF.conn_kwargs())
        close_after = True

    try:
        cur = conn.cursor()
        for i, stmt in enumerate(statements, start=1):
            cur.execute(stmt)
            # For CREATE TABLE AS, Snowflake returns "Table X successfully created"
            result = cur.fetchall()
            if result and len(result) == 1 and len(result[0]) == 1:
                log.info(f"      [{i}] {result[0][0]}")
    finally:
        if close_after:
            conn.close()


def run_sql_directory(dir_path: Path,
                      conn: snowflake.connector.SnowflakeConnection | None = None) -> None:
    """
    Execute every .sql file in a directory, alphabetically sorted.

    Naming convention: prefix files with order if needed (e.g., 01_, 02_).
    Otherwise files run in lexicographic order.
    """
    sql_files = sorted(dir_path.glob("*.sql"))
    if not sql_files:
        log.warning(f"  (no .sql files in {dir_path})")
        return

    close_after = False
    if conn is None:
        conn = snowflake.connector.connect(**SF.conn_kwargs())
        close_after = True

    try:
        for f in sql_files:
            run_sql_file(f, conn)
    finally:
        if close_after:
            conn.close()
