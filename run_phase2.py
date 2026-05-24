"""
Phase 2 master driver — runs the COMPLETE end-to-end pipeline manually.

Sequence:
    1. Extract from Postgres        → local Parquet files
    2. Extract from APIs            → local JSON files
    3. Load all files into Snowflake RAW
    4. Build STAGING tables (typed, deduped)
    5. Build CURATED dims and facts
    6. Build MARTS aggregates

This is what an Airflow DAG will orchestrate in Phase 3. Running it
manually first proves each layer works in isolation.

Usage:
    # Activate your venv
    source .venv/bin/activate

    # Make sure env vars are exported (or use python-dotenv to load .env)
    export $(grep -v '^#' .env | xargs)

    # Run!
    python run_phase2.py
"""

from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure project root is on path (so `tasks.*` imports work)
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from tasks.extract import postgres_extractor, api_extractor
from tasks.load import snowflake_loader
from tasks.utils.config import SF, SQL_DIR
from tasks.utils.logger import get_logger
from tasks.utils.sql_runner import run_sql_directory

import snowflake.connector

log = get_logger("phase2")


def banner(text: str) -> None:
    bar = "=" * 70
    log.info("")
    log.info(bar)
    log.info(f"  {text}")
    log.info(bar)


def main() -> int:
    t0 = time.time()
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    log.info(f"Run ID: {run_id}")

    # ---------- 1. Extract ---------------------------------------------
    banner("STEP 1/5  EXTRACT FROM POSTGRES")
    pg_manifest = postgres_extractor.extract_all(run_id=run_id)

    banner("STEP 2/5  EXTRACT FROM APIs")
    api_manifest = api_extractor.extract_all(run_id=run_id)

    combined_manifest = {**pg_manifest, **api_manifest}

    # ---------- 2. Load to RAW -----------------------------------------
    banner("STEP 3/5  LOAD RAW INTO SNOWFLAKE")
    load_results = snowflake_loader.load_all(combined_manifest, run_id=run_id)

    # ---------- 3. STAGING ---------------------------------------------
    banner("STEP 4/5  BUILD STAGING LAYER")
    with snowflake.connector.connect(**SF.conn_kwargs()) as conn:
        run_sql_directory(SQL_DIR / "staging", conn)

    # ---------- 4. CURATED + MARTS -------------------------------------
        banner("STEP 5/5  BUILD CURATED + MARTS")
        run_sql_directory(SQL_DIR / "curated", conn)
        run_sql_directory(SQL_DIR / "marts",   conn)

    # ---------- Summary -------------------------------------------------
    elapsed = time.time() - t0
    banner(f"✓ PHASE 2 COMPLETE in {elapsed:.1f}s")
    log.info("Rows loaded to RAW:")
    for ds, n in load_results.items():
        log.info(f"  {ds:<15} {n:>10,}")
    log.info("")
    log.info("Next: open Snowflake → run queries in sql/validation_queries.sql")
    return 0


if __name__ == "__main__":
    sys.exit(main())
