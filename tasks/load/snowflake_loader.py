"""
Snowflake loader for the NorthWind retail pipeline.

Takes a manifest from the extractors and:
    1. PUTs each file to the RAW.ETL_STAGE internal stage
    2. TRUNCATEs the target RAW table (Phase 2 = full load = clean slate)
    3. Runs COPY INTO with the right file format
    4. Reports rows loaded

DESIGN NOTES:

  * Phase 2 uses TRUNCATE + COPY INTO. This is idempotent for full loads
    (rerunning produces identical result) but throws away history.
    Phase 4 will switch to MERGE for incrementals.

  * For Postgres data: Parquet → typed COPY INTO with MATCH_BY_COLUMN_NAME.
    For API data: JSON → COPY INTO a single VARIANT column.

  * Every loaded row gets _SOURCE_FILE populated automatically via
    METADATA$FILENAME so we can trace data lineage.

Run standalone (requires Phase 2 extracts to exist on disk):
    python -m tasks.load.snowflake_loader
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import snowflake.connector

from tasks.utils.config import SF, SNOWFLAKE_STAGE, TABLE_MAPPING
from tasks.utils.logger import get_logger

log = get_logger(__name__)


# ---------- Connection helper ------------------------------------------

def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Open a Snowflake connection using shared config."""
    return snowflake.connector.connect(**SF.conn_kwargs())


# ---------- PUT (upload file to stage) ---------------------------------

def put_file(cur, file_path: Path, stage_subpath: str) -> None:
    """Upload a single local file to an internal stage path."""
    # AUTO_COMPRESS=FALSE because Parquet is already compressed; for JSON
    # we let Snowflake gzip it on upload (smaller transfer).
    auto_compress = "FALSE" if file_path.suffix == ".parquet" else "TRUE"
    overwrite = "TRUE"  # idempotent: replace any existing file with same name

    sql = (
        f"PUT 'file://{file_path.as_posix()}' "
        f"@{SNOWFLAKE_STAGE}/{stage_subpath}/ "
        f"AUTO_COMPRESS={auto_compress} OVERWRITE={overwrite}"
    )
    log.info(f"  PUT {file_path.name} → @{SNOWFLAKE_STAGE}/{stage_subpath}/")
    cur.execute(sql)


# ---------- COPY INTO --------------------------------------------------

def copy_parquet_into_table(cur, table: str, stage_subpath: str, file_name: str) -> int:
    """
    COPY a Parquet file from stage into a typed RAW table.

    Uses MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE so column order in the
    Parquet file doesn't have to match the table — names do the matching.
    """
    sql = f"""
        COPY INTO {table} (
            -- list business columns only; metadata cols have DEFAULTs
            {_business_columns_for(table)}
            , _source_file
        )
        FROM (
            SELECT
                {_select_columns_for(table)}
                , METADATA$FILENAME
            FROM @{SNOWFLAKE_STAGE}/{stage_subpath}/{file_name}
        )
        FILE_FORMAT = (FORMAT_NAME = RAW.FF_PARQUET)
        ON_ERROR = 'ABORT_STATEMENT'
    """
    log.info(f"  COPY INTO {table} FROM @.../{stage_subpath}/{file_name}")
    cur.execute(sql)
    rows = _fetch_loaded_rowcount(cur)
    log.info(f"    loaded {rows:,} rows")
    return rows


def copy_json_into_table(cur, table: str, stage_subpath: str, file_name: str) -> int:
    """
    COPY a JSON file into a VARIANT-style RAW table (RAW_DATA, _SOURCE_FILE).
    The JSON file is an array; STRIP_OUTER_ARRAY makes each element a row.
    """
    sql = f"""
        COPY INTO {table} (raw_data, _source_file)
        FROM (
            SELECT $1, METADATA$FILENAME
            FROM @{SNOWFLAKE_STAGE}/{stage_subpath}/{file_name}
        )
        FILE_FORMAT = (FORMAT_NAME = RAW.FF_JSON)
        ON_ERROR = 'ABORT_STATEMENT'
    """
    log.info(f"  COPY INTO {table} FROM @.../{stage_subpath}/{file_name}")
    cur.execute(sql)
    rows = _fetch_loaded_rowcount(cur)
    log.info(f"    loaded {rows:,} rows")
    return rows


def _fetch_loaded_rowcount(cur) -> int:
    """COPY INTO returns one row per file with rows_loaded column."""
    rows = cur.fetchall()
    return sum(r[2] for r in rows) if rows else 0


# ---------- Column maps -----------------------------------------------
# Each column gets a SELECT expression rather than just a cast type. This
# matters for timestamps: Parquet stores them as integers, but pandas /
# pyarrow / Snowflake can disagree on the precision (s vs ms vs us vs ns)
# encoded by those integers.
#
# Our extractor writes microsecond-precision timestamps via pandas
# `datetime64[us]`. To remove any ambiguity, we do the conversion
# explicitly in SQL:  TO_TIMESTAMP_NTZ(value_us / 1_000_000)
# (Snowflake's TO_TIMESTAMP_NTZ(seconds) accepts a fractional seconds
# value and resolves microseconds correctly.)
#
# Each entry is (column_name, sql_expression). The expression uses
# `$1:column_name` to reference the column from the staged Parquet file.

_COL_MAPS: dict[str, list[tuple[str, str]]] = {
    "RAW.CUSTOMERS": [
        ("customer_id",      "$1:customer_id::NUMBER"),
        ("email",            "$1:email::VARCHAR"),
        ("first_name",       "$1:first_name::VARCHAR"),
        ("last_name",        "$1:last_name::VARCHAR"),
        ("customer_segment", "$1:customer_segment::VARCHAR"),
        ("country",          "$1:country::VARCHAR"),
        ("signup_date",      "$1:signup_date::DATE"),
        ("created_at",       "TO_TIMESTAMP_NTZ($1:created_at::NUMBER / 1000000)"),
        ("modified_at",      "TO_TIMESTAMP_NTZ($1:modified_at::NUMBER / 1000000)"),
    ],
    "RAW.ORDERS": [
        ("order_id",         "$1:order_id::NUMBER"),
        ("customer_id",      "$1:customer_id::NUMBER"),
        ("order_date",       "$1:order_date::DATE"),
        ("status",           "$1:status::VARCHAR"),
        ("shipping_country", "$1:shipping_country::VARCHAR"),
        ("currency_code",    "$1:currency_code::VARCHAR"),
        ("total_amount",     "$1:total_amount::NUMBER(12,2)"),
        ("created_at",       "TO_TIMESTAMP_NTZ($1:created_at::NUMBER / 1000000)"),
        ("modified_at",      "TO_TIMESTAMP_NTZ($1:modified_at::NUMBER / 1000000)"),
    ],
    "RAW.ORDER_ITEMS": [
        ("order_item_id",    "$1:order_item_id::NUMBER"),
        ("order_id",          "$1:order_id::NUMBER"),
        ("product_id",       "$1:product_id::NUMBER"),
        ("quantity",         "$1:quantity::NUMBER"),
        ("unit_price",       "$1:unit_price::NUMBER(12,2)"),
        ("created_at",       "TO_TIMESTAMP_NTZ($1:created_at::NUMBER / 1000000)"),
    ],
}


def _business_columns_for(table: str) -> str:
    return ", ".join(col_name for col_name, _ in _COL_MAPS[table])


def _select_columns_for(table: str) -> str:
    # Each column has its full SELECT expression already
    return ", ".join(expr for _, expr in _COL_MAPS[table])


# ---------- TRUNCATE before load (Phase 2 only) -----------------------

def truncate_table(cur, table: str) -> None:
    """Phase 2 full-load pattern: clean slate before each load."""
    log.info(f"  TRUNCATE {table}")
    cur.execute(f"TRUNCATE TABLE {table}")


# ---------- Driver -----------------------------------------------------

def load_all(extract_manifest: dict[str, dict[str, Any]],
             run_id: str | None = None) -> dict[str, int]:
    """
    Load every extracted dataset into its RAW table.

    extract_manifest format (combined from Postgres + API extractors):
        {
          "customers":   {"path": Path, "row_count": int},
          "orders":      {"path": Path, "row_count": int},
          "order_items": {"path": Path, "row_count": int},
          "products":    {"path": Path, "row_count": int},
          "fx_rates":    {"path": Path, "row_count": int},
        }

    Returns: { dataset_name: rows_loaded_into_snowflake }
    """
    log.info("=== Snowflake load (RAW layer) ===")
    results: dict[str, int] = {}

    with get_connection() as conn:
        cur = conn.cursor()
        for dataset, info in extract_manifest.items():
            path: Path = info["path"]
            target_table = TABLE_MAPPING[dataset]
            stage_subpath = run_id or "current"

            # 1. Upload file to stage
            put_file(cur, path, stage_subpath)

            # 2. Truncate (Phase 2 full-load)
            truncate_table(cur, target_table)

            # 3. COPY INTO, dispatching by file type
            if path.suffix == ".parquet":
                rows = copy_parquet_into_table(cur, target_table,
                                               stage_subpath, path.name)
            else:                                     # .json
                rows = copy_json_into_table(cur, target_table,
                                            stage_subpath, path.name)

            results[dataset] = rows

    total = sum(results.values())
    log.info(f"✓ Snowflake load complete. {total:,} rows across {len(results)} tables.")
    return results


# ---------- Standalone test --------------------------------------------

if __name__ == "__main__":
    # Looks for the most recent extract directory and loads it.
    from tasks.utils.config import EXTRACT_DIR

    if not EXTRACT_DIR.exists():
        raise SystemExit(f"No extracts found at {EXTRACT_DIR}. Run extractors first.")
    latest_run = sorted(EXTRACT_DIR.iterdir())[-1]
    log.info(f"Loading from latest run: {latest_run.name}")

    # Build a fake manifest from files on disk
    manifest = {}
    for f in latest_run.iterdir():
        dataset = f.stem
        if dataset in TABLE_MAPPING:
            manifest[dataset] = {"path": f, "row_count": -1}

    load_all(manifest, run_id=latest_run.name)
