"""
Central configuration for the retail pipeline.

All extractors, loaders, and runners read connection details from here.
Values come from environment variables, which are loaded from a `.env`
file at the project root using `python-dotenv`.

This means:
  - You never have to `export` anything in your shell
  - It works identically on Windows, Mac, and Linux
  - The script is self-contained and reproducible
  - Changing a value in `.env` is picked up on next run

Import order matters: `load_dotenv()` runs at module import time, before
any os.getenv() calls. So as long as any module imports `tasks.utils.config`
early, env vars are populated for the rest of the program.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


# --- Project paths -----------------------------------------------------

# Resolve to the repo root regardless of where you run scripts from
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR      = PROJECT_ROOT / "sql"

# Load .env from project root BEFORE reading any env vars below.
# `override=False` means existing shell env vars take precedence over
# .env values (good for Airflow, which sets connection vars differently).
load_dotenv(PROJECT_ROOT / ".env", override=False)

# EXTRACT_DIR can be overridden via env var; default lives under project
EXTRACT_DIR = Path(os.getenv("EXTRACT_DIR", PROJECT_ROOT / "local_data" / "extracts"))


# --- Postgres (source OLTP) --------------------------------------------

@dataclass(frozen=True)
class PostgresConfig:
    """
    Postgres connection config.

    By default, uses POSTGRES_HOST from .env (set to 'host.docker.internal'
    so Airflow container can reach laptop Postgres).

    For scripts running on the laptop directly (run_phase2.py, seed_postgres.py),
    call `PostgresConfig.for_local_use()` to get a config that uses
    POSTGRES_HOST_LOCAL instead (typically 'localhost').

    This avoids the need to switch .env values between contexts.
    """
    host:     str = os.getenv("POSTGRES_HOST",     "localhost")
    port:     int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB",       "northwind_oltp")
    user:     str = os.getenv("POSTGRES_USER",     "northwind_user")
    password: str = os.getenv("POSTGRES_PASSWORD", "northwind_pass")

    @classmethod
    def for_local_use(cls) -> "PostgresConfig":
        """Return a config that uses POSTGRES_HOST_LOCAL (defaults to 'localhost')."""
        return cls(
            host=os.getenv("POSTGRES_HOST_LOCAL", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB",       "northwind_oltp"),
            user=os.getenv("POSTGRES_USER",     "northwind_user"),
            password=os.getenv("POSTGRES_PASSWORD", "northwind_pass"),
        )

    def conn_kwargs(self) -> dict:
        return {
            "host":     self.host,
            "port":     self.port,
            "dbname":   self.database,
            "user":     self.user,
            "password": self.password,
        }


# --- Snowflake (warehouse) ---------------------------------------------

@dataclass(frozen=True)
class SnowflakeConfig:
    account:   str = os.getenv("SNOWFLAKE_ACCOUNT",   "")
    user:      str = os.getenv("SNOWFLAKE_USER",      "ETL_USER")
    password:  str = os.getenv("SNOWFLAKE_PASSWORD",  "")
    role:      str = os.getenv("SNOWFLAKE_ROLE",      "ETL_ROLE")
    warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "ETL_WH")
    database:  str = os.getenv("SNOWFLAKE_DATABASE",  "RETAIL_DB")

    def conn_kwargs(self) -> dict:
        if not self.account or not self.password:
            raise ValueError(
                "SNOWFLAKE_ACCOUNT and SNOWFLAKE_PASSWORD must be set "
                "in your .env file at the project root."
            )
        return {
            "account":   self.account,
            "user":      self.user,
            "password":  self.password,
            "role":      self.role,
            "warehouse": self.warehouse,
            "database":  self.database,
        }


# --- API endpoints -----------------------------------------------------

@dataclass(frozen=True)
class APIConfig:
    products_url: str = "https://fakestoreapi.com/products"
    fx_url:       str = "https://api.exchangerate.host/live?base=USD&access_key=e4d6b6547b301457a8122a446fbf58f9"
    timeout_sec:  int = 30


# --- Pipeline-wide constants -------------------------------------------

SNOWFLAKE_STAGE = "RAW.ETL_STAGE"

TABLE_MAPPING = {
    "customers":   "RAW.CUSTOMERS",
    "orders":      "RAW.ORDERS",
    "order_items": "RAW.ORDER_ITEMS",
    "products":    "RAW.PRODUCTS",
    "fx_rates":    "RAW.FX_RATES",
}


# --- Singletons for easy import ----------------------------------------

PG = PostgresConfig()
SF = SnowflakeConfig()
API = APIConfig()
