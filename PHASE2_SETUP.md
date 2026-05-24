# Phase 2: Initial Full Load — Step by Step

You've got Phase 1 working (Postgres + Snowflake + Airflow all green). Now we build the actual pipeline — extractors, loader, transformations — and run it manually from your terminal. **No Airflow involvement yet.** Phase 3 will wrap this in a DAG.

**Estimated time:** 30-45 minutes (mostly setup; the pipeline itself runs in ~60-90 seconds).

---

## What you'll end up with

```
RAW (5 tables)  →  STAGING (5 tables)  →  CURATED (5 tables)  →  MARTS (3 tables)
```

Live data from Postgres + APIs flowing through your layered Snowflake warehouse, with a star schema and pre-aggregated business marts.

---

## Step 1: Add the new files to your project

If you're continuing from Phase 1, unzip `retail-pipeline-phase2.zip` over your existing project — it only adds new files, doesn't modify existing ones except `requirements.txt`. The new pieces:

```
+ infra/snowflake_tables.sql         # RAW table DDL
+ tasks/utils/config.py              # shared config
+ tasks/utils/logger.py
+ tasks/utils/sql_runner.py
+ tasks/extract/postgres_extractor.py
+ tasks/extract/api_extractor.py
+ tasks/load/snowflake_loader.py
+ sql/staging/*.sql                  # 5 staging files
+ sql/curated/*.sql                  # 5 curated files
+ sql/marts/*.sql                    # 3 mart files
+ sql/validation_queries.sql
+ run_phase2.py                      # master driver
```

Commit and push to GitHub.

---

## Step 2: Install Phase 2 Python dependencies in your venv

The extractors run on your laptop (not in the Airflow container), so your local venv needs the deps:

```bash
source .venv/bin/activate

pip install -r requirements-seed.txt          # already installed if Phase 1 done
pip install \
  snowflake-connector-python[pandas] \
  requests \
  pandas \
  pyarrow
```

> **Note on versions:** I'm not pinning exact versions here because you mentioned you tweaked them. Use whatever versions you have — the code uses stable APIs that work across the last several major versions of each library.

---

## Step 3: Create the RAW tables in Snowflake

Open the Snowflake UI → Worksheets → paste the contents of `infra/snowflake_tables.sql` → **Run All**.

You should see:
- 5 tables created (`CUSTOMERS`, `ORDERS`, `ORDER_ITEMS`, `PRODUCTS`, `FX_RATES`)
- 1 internal stage (`ETL_STAGE`)
- 2 file formats (`FF_PARQUET`, `FF_JSON`)

The `SHOW` statements at the end confirm everything exists.

---

## Step 4: Make sure your .env is loaded

The Phase 2 driver reads connection params from environment variables. The easiest way to load them from `.env`:

```bash
# Mac/Linux
set -a; source .env; set +a

# Or one-line equivalent
export $(grep -v '^#' .env | xargs)
```

Verify:
```bash
echo $SNOWFLAKE_ACCOUNT   # should print your account, not be empty
echo $POSTGRES_HOST       # should print 'host.docker.internal' or 'localhost'
```

> **Important:** for running scripts on your laptop (outside Docker), `POSTGRES_HOST` should be `localhost`, not `host.docker.internal`. The latter is only for the Airflow container. Either:
> - Edit `.env` to use `localhost` for running scripts directly, then change it back when running Airflow
> - Or override on the command line: `POSTGRES_HOST=localhost python run_phase2.py`

---

## Step 5: Run the full pipeline

```bash
python run_phase2.py
```

You should see ~90 seconds of output ending with something like:

```
======================================================================
  ✓ PHASE 2 COMPLETE in 73.4s
======================================================================
Rows loaded to RAW:
  customers           10,000
  orders             100,000
  order_items        302,847
  products                20
  fx_rates               170

Next: open Snowflake → run queries in sql/validation_queries.sql
```

---

## Step 6: Validate in Snowflake

Open the Snowflake UI → Worksheets → paste the contents of `sql/validation_queries.sql` → run section by section.

The first query gives you a row-count summary across all layers. Expected approximate counts:

| Table | Count |
|---|---|
| `RAW.CUSTOMERS` | 10,000 |
| `RAW.ORDERS` | 100,000 |
| `RAW.ORDER_ITEMS` | ~300,000 (varies, 1-5 per order) |
| `RAW.PRODUCTS` | 20 |
| `RAW.FX_RATES` | ~170 |
| `STAGING.STG_ORDERS` | ~95,000 (CANCELLED filtered out) |
| `CURATED.FCT_ORDERS` | ~95,000 |
| `CURATED.FCT_ORDER_ITEMS` | ~285,000 |
| `MARTS.REVENUE_BY_CATEGORY` | dozens |

The other queries verify business logic (no NULL FX rates, no orphan items, sensible revenue ranges).

---

## What you've actually built

```
Postgres (laptop)              ┐
  customers, orders, items     │
                               │      run_phase2.py
APIs (internet)                ├─────────────────────► Snowflake
  products, fx_rates           │                         RAW (5 tables)
                               ┘                          ↓
                                                        STAGING (5 tables, deduped/typed)
                                                          ↓
                                                        CURATED (3 dims + 2 facts)
                                                          ↓
                                                        MARTS (3 aggregates)
```

Every layer is **idempotent** — rerun `run_phase2.py` and you get identical results (because we TRUNCATE + reload in RAW, and CREATE OR REPLACE in STAGING/CURATED/MARTS).

---

## Common issues

**`SNOWFLAKE_ACCOUNT must be set`**
Your `.env` wasn't loaded into the current shell. Re-run the export command in Step 4.

**`connection to server failed: Connection refused`**
The Postgres host in your `.env` is `host.docker.internal` (for Airflow). For local scripts, override:
```bash
POSTGRES_HOST=localhost python run_phase2.py
```

**`Object 'RAW.ETL_STAGE' does not exist`**
You haven't run `infra/snowflake_tables.sql` yet. Step 3.

**`Numeric value 'X' is not recognized` during COPY INTO**
Your Parquet file has a column type that doesn't match the RAW table. Check that the Postgres seeder ran successfully and produced the expected schema (`\d orders` in psql).

**Pipeline ran but `MARTS.REVENUE_BY_CATEGORY` is empty**
The `FCT_ORDER_ITEMS` references products by ID (1-20 from fakestoreapi), and your seeded order_items use `product_id` in range 1-20 too. If you changed `PRODUCT_ID_RANGE` in `seed_postgres.py`, they won't join. Either reseed with the default range, or adjust.

---

## What's next

Once your validation queries look healthy:

```bash
git add .
git commit -m "Phase 2 complete: end-to-end pipeline working manually"
git push
```

Then we move to **Phase 3: Airflow DAG**. The amazing thing you'll notice: Phase 3 reuses 100% of the Python and SQL you just built. Airflow just orchestrates these existing pieces — that's the power of clean separation of concerns.

Phase 4 then converts from full-load to **incremental load** with watermarking, which is where the production-grade patterns really kick in.
