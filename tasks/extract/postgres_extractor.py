"""
Postgres extractor for the NorthWind retail pipeline.

Extracts three tables (customers, orders, order_items) from the source
Postgres database and writes each as a Parquet file to EXTRACT_DIR.

Phase 2 = FULL load. Every run pulls everything.
Phase 4 will add incremental logic using a watermark column.

Run standalone:
    python -m tasks.extract.postgres_extractor

Or import from another module:
    from tasks.extract.postgres_extractor import extract_all
    manifest = extract_all()
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg

from tasks.utils.config import PG, PostgresConfig, EXTRACT_DIR
from tasks.utils.logger import get_logger

log = get_logger(__name__)


# Tables to extract, in dependency-safe order
TABLES = ["customers", "orders", "order_items"]


def _get_pg_config(local: bool) -> PostgresConfig:
    """Pick the right Postgres host based on execution context."""
    return PostgresConfig.for_local_use() if local else PG


def _read_table_to_df(conn: psycopg.Connection, table: str) -> pd.DataFrame:
    """Read a Postgres table into a pandas DataFrame."""
    log.info(f"  reading from postgres: {table}")
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table}")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _write_parquet(df: pd.DataFrame, table: str, run_id: str) -> Path:
    """Write DataFrame to a partitioned Parquet path: EXTRACT_DIR/run_id/table.parquet"""
    out_dir = EXTRACT_DIR / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{table}.parquet"
    df.to_parquet(out_path, index=False, engine="pyarrow")
    log.info(f"  wrote {len(df):>8,} rows → {out_path}")
    return out_path


def extract_all(run_id: str | None = None, local: bool = False) -> dict[str, dict]:
    """
    Extract all Postgres tables.

    Args:
        run_id: Optional run identifier. Defaults to current UTC timestamp.
        local:  If True, use POSTGRES_HOST_LOCAL (for laptop scripts).
                If False, use POSTGRES_HOST (for Airflow container).

    Returns a manifest dict:
        {
          "customers":   {"path": Path, "row_count": int},
          "orders":      {"path": Path, "row_count": int},
          "order_items": {"path": Path, "row_count": int},
        }
    """
    run_id = run_id or datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    cfg = _get_pg_config(local)
    log.info(f"=== Postgres extraction (run_id={run_id}, host={cfg.host}) ===")

    manifest: dict[str, dict] = {}
    with psycopg.connect(**cfg.conn_kwargs()) as conn:
        for table in TABLES:
            df   = _read_table_to_df(conn, table)
            path = _write_parquet(df, table, run_id)
            manifest[table] = {"path": path, "row_count": len(df)}

    total = sum(m["row_count"] for m in manifest.values())
    log.info(f"✓ Postgres extract complete. {total:,} rows across {len(manifest)} tables.")
    return manifest


if __name__ == "__main__":
    # Standalone runs are by definition on the laptop
    extract_all(local=True)
