"""
hello_world_check
=================

A diagnostic DAG that verifies Phase 1 environment is wired correctly.
It runs three independent checks in parallel and a final summary task:

    1. check_postgres_connection : counts rows in `orders` table
    2. check_snowflake_connection: confirms RETAIL_DB schemas exist
    3. check_apis                : hits fakestoreapi and exchangerate.host
    4. summary                   : prints success banner

If all four tasks turn green, Phase 1 is complete.

Run from Airflow UI: Trigger DAG → watch the graph view.
"""

from __future__ import annotations

from datetime import datetime

import requests
from airflow.models import Variable

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# --------- Configuration ----------------------------------------------

POSTGRES_CONN_ID  = "postgres_source"
SNOWFLAKE_CONN_ID = "snowflake_default"

PRODUCTS_API = "https://fakestoreapi.com/products?limit=3"
FX_API       = "https://api.exchangerate.host/latest?base=USD"


# --------- DAG --------------------------------------------------------

with DAG(
    dag_id="hello_world_check",
    description="Phase 1 environment verification",
    start_date=datetime(2026, 1, 1),
    schedule=None,                   # manual trigger only
    catchup=False,
    tags=["phase1", "diagnostic"],
):

    @task
    def check_postgres_connection() -> int:
        """Connect to source Postgres and count orders."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        count = hook.get_first("SELECT COUNT(*) FROM orders")[0]
        print(f"✓ Postgres OK. orders row count = {count:,}")
        if count == 0:
            raise ValueError(
                "Postgres reachable but `orders` is empty — "
                "did you run infra/seed_postgres.py?"
            )
        return count

    @task
    def check_snowflake_connection() -> list[str]:
        """Connect to Snowflake and verify all schemas exist."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        rows = hook.get_records(
            "SELECT schema_name "
            "FROM RETAIL_DB.INFORMATION_SCHEMA.SCHEMATA "
            "WHERE schema_name IN "
            "('RAW','STAGING','CURATED','MARTS','METADATA') "
            "ORDER BY schema_name"
        )
        schemas = [r[0] for r in rows]
        print(f"✓ Snowflake OK. found schemas: {schemas}")
        expected = {"RAW", "STAGING", "CURATED", "MARTS", "METADATA"}
        missing = expected - set(schemas)
        if missing:
            raise ValueError(
                f"Snowflake reachable but missing schemas: {missing}. "
                f"Did you run infra/snowflake_setup.sql?"
            )
        return schemas

    @task
    def check_apis() -> dict:
        """Hit both public APIs and verify they respond."""
        results = {}

        # Product catalog API
        r = requests.get(PRODUCTS_API, timeout=10)
        r.raise_for_status()
        products = r.json()
        if not isinstance(products, list) or len(products) == 0:
            raise ValueError(f"fakestoreapi returned unexpected payload: {products!r}")
        print(f"✓ fakestoreapi OK. fetched {len(products)} product(s)")
        print(f"  sample: {products[0].get('title')!r}")
        results["products_sample_count"] = len(products)

        # FX rates API
        api_key = Variable.get("exchangerate_api_key")
        fx_url = f"https://api.exchangerate.host/live?base=USD&access_key={api_key}"
        r = requests.get(fx_url, timeout=10)
        r.raise_for_status()
        payload = r.json()
        quotes = payload.get("quotes", {})
        if "USDEUR" not in quotes:
            raise ValueError(f"exchangerate.host returned unexpected payload: {payload!r}")
        print(f"✓ exchangerate.host OK. source={payload.get('source')}  rates_count={len(quotes)}")
        print(f"  USD→EUR={quotes.get('USDEUR')}  USD→INR={quotes.get('USDINR')}")
        results["fx_base"] = payload.get("source")
        results["fx_rates_count"] = len(quotes)

        return results

    @task
    def summary(pg_count: int, sf_schemas: list[str], api_results: dict) -> None:
        print("=" * 60)
        print("  PHASE 1 ENVIRONMENT CHECK — ALL GREEN ✓")
        print("=" * 60)
        print(f"  Postgres  : {pg_count:,} orders found")
        print(f"  Snowflake : schemas = {sf_schemas}")
        print(f"  APIs      : {api_results}")
        print("=" * 60)
        print("  Ready to proceed to Phase 2: Initial Full Load")
        print("=" * 60)

    pg  = check_postgres_connection()
    sf  = check_snowflake_connection()
    api = check_apis()
    summary(pg, sf, api)
