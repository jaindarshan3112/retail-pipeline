"""
Central configuration for the retail pipeline.

All extractors, loaders, and runners read connection details from here.
Values come from environment variables (or a .env file), with sensible
defaults that match PHASE1_SETUP.md.

This means you can change a database name, schema, or warehouse in ONE
place and the entire pipeline picks it up.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


# --- Project paths -----------------------------------------------------

# Resolve to the repo root regardless of where you run scripts from
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR      = PROJECT_ROOT / "sql"
EXTRACT_DIR  = Path(os.getenv("EXTRACT_DIR", "/tmp/retail_extract"))


# --- Postgres (source OLTP) --------------------------------------------

@dataclass(frozen=True)
class PostgresConfig:
    host:     str = os.getenv("POSTGRES_HOST",     "localhost")
    port:     int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB",       "northwind_oltp")
    user:     str = os.getenv("POSTGRES_USER",     "northwind_user")
    password: str = os.getenv("POSTGRES_PASSWORD", "northwind_pass")

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
                "in your .env file."
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
    fx_url:       str = "https://api.exchangerate.host/latest?base=USD"
    timeout_sec:  int = 30


# --- Pipeline-wide constants -------------------------------------------

# Snowflake internal stage we'll use for landing files.
# Lives in RAW schema; we create it in infra/snowflake_tables.sql
SNOWFLAKE_STAGE = "RAW.ETL_STAGE"

# Source-table → RAW-table mapping. Single source of truth.
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
