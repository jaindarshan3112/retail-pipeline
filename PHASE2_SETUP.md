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

Unzip `retail-pipeline-phase2.zip` over your existing Phase 1 folder. It adds new files plus updates `.env.example` and `requirements.txt`.

Commit and push.

---

## Step 2: Update your .env file

Two important changes:

**A) Add the new `POSTGRES_HOST_LOCAL` variable**

Open your `.env` and add this line below `POSTGRES_HOST`:

```bash
POSTGRES_HOST=host.docker.internal     # already there — for Airflow container
POSTGRES_HOST_LOCAL=localhost          # NEW — for scripts running on your laptop
```

The pipeline auto-picks the right one based on where it's running. No more swapping values.

**B) That's it.** Everything else stays the same.

---

## Step 3: Install Phase 2 Python dependencies

The extractors run on your laptop (not in the Airflow container), so your local venv needs the deps. There's now a dedicated `requirements-local.txt`:

**Windows (PowerShell or CMD):**
```powershell
.venv\Scripts\activate
pip install -r requirements-local.txt
```

**Mac/Linux:**
```bash
source .venv/bin/activate
pip install -r requirements-local.txt
```

This includes `python-dotenv` which auto-loads your `.env` — no shell exports needed, works identically on Windows/Mac/Linux.

---

## Step 4: Create the RAW tables in Snowflake

Open the Snowflake UI → Worksheets → paste the contents of `infra/snowflake_tables.sql` → **Run All**.

You should see:
- 5 tables created (`CUSTOMERS`, `ORDERS`, `ORDER_ITEMS`, `PRODUCTS`, `FX_RATES`)
- 1 internal stage (`ETL_STAGE`)
- 2 file formats (`FF_PARQUET`, `FF_JSON`)

The `SHOW` statements at the end confirm everything exists.

---

## Step 5: Run the full pipeline

That's it — no env-var dance, no shell tricks:

```bash
python run_phase2.py
```

The script auto-loads `.env` via `python-dotenv` at startup. You should see ~90 seconds of output ending with:

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

## How `.env` loading works (worth understanding)

1. You run `python run_phase2.py`
2. Python imports `run_phase2.py` → which imports `tasks.utils.config`
3. At the top of `config.py`, this line runs:
   ```python
   load_dotenv(PROJECT_ROOT / ".env", override=False)
   ```
4. `python-dotenv` reads `.env` and sets each `KEY=value` as an environment variable for the current process
5. Then `os.getenv("SNOWFLAKE_ACCOUNT")` etc. just work

`override=False` means: if a variable is already set in the shell (e.g. by Airflow), don't override it. This is the safe default — production environments inject vars through other means.

**This works identically on Windows, Mac, and Linux. No `export`, no `set`, no shell-specific syntax.**

---

## Common issues

**`SNOWFLAKE_ACCOUNT and SNOWFLAKE_PASSWORD must be set in your .env file`**
The `.env` file isn't being found, or those values are empty. Check:
- File is at the project root (same level as `run_phase2.py`)
- File is literally named `.env` (not `.env.txt` — Windows likes to add hidden extensions)
- The values aren't still set to `replace_with_...`

**`connection to server failed: Connection refused`**
Postgres host issue. Make sure you added `POSTGRES_HOST_LOCAL=localhost` to your `.env`. Verify Postgres is running on your laptop:
- Windows: `Get-Service postgresql*` in PowerShell
- Mac: `brew services list`
- Linux: `sudo systemctl status postgresql`

**`Object 'RAW.ETL_STAGE' does not exist`**
You haven't run `infra/snowflake_tables.sql` yet. Step 4.

**`Numeric value 'X' is not recognized` during COPY INTO**
Your Parquet file has a column type mismatch. Re-run the seed script to regenerate clean data.

**Pipeline ran but `MARTS.REVENUE_BY_CATEGORY` is empty**
The `FCT_ORDER_ITEMS` references products by ID (1-20 from fakestoreapi). If you changed `PRODUCT_ID_RANGE` in `seed_postgres.py`, IDs won't join.

---

## What's next

```bash
git add .
git commit -m "Phase 2 complete: end-to-end pipeline working manually"
git push
```

Then we move to **Phase 3: Airflow DAG**. The amazing thing: Phase 3 reuses 100% of the Python and SQL you just built. Airflow just orchestrates these existing pieces.

Phase 4 then converts from full-load to **incremental load** with watermarking, which is where the production-grade patterns really kick in.
