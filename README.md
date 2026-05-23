# NorthWind Retail Pipeline

An end-to-end data engineering project that builds a weekly ETL pipeline from two heterogeneous sources (Postgres OLTP + public APIs) into a layered Snowflake warehouse, orchestrated by Airflow.

## Architecture

```
PostgreSQL (orders, items, customers)  ──┐
                                          ├──► Airflow (Docker) ──► Snowflake
Public APIs (products, FX rates)  ───────┘                            RAW → STAGING → CURATED → MARTS
```

## Project Phases

| Phase | Goal | Status |
|-------|------|--------|
| 1 | Environment setup (Postgres + Snowflake + Airflow + Hello World DAG) | ← you are here |
| 2 | Initial full load (one-time) | |
| 3 | Airflow DAG for full load | |
| 4 | Incremental load with watermarking | |
| 5 | Data quality, alerting, idempotency | |
| 6 | (Optional) Convert SQL to dbt models | |

## Tech Stack

- Source DB: PostgreSQL 16 (local)
- Source APIs: fakestoreapi.com, exchangerate.host
- Orchestrator: Apache Airflow 2.9 (Docker Compose)
- Warehouse: Snowflake (Enterprise free trial)
- Language: Python 3.11

See PHASE1_SETUP.md for step-by-step setup instructions.
