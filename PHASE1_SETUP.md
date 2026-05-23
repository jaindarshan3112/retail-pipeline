# Phase 1: Environment Setup — Step by Step

This guide walks you from "nothing installed" to "Hello World DAG runs successfully and connects to both Postgres and Snowflake."

**Estimated time:** 60-90 minutes (most of it is downloads and Snowflake account creation)

---

## Prerequisites — install on your laptop first

1. **Docker Desktop** — https://www.docker.com/products/docker-desktop/
   - Verify: `docker --version && docker compose version`
2. **PostgreSQL 16** — https://www.postgresql.org/download/
   - During install, set a password for the `postgres` superuser (remember it!)
   - Verify: `psql --version` --Mowgli@31
3. **Python 3.11+** — https://www.python.org/downloads/
   - Verify: `python --version`
4. **Git** — you already have GitHub, so likely already installed
   - Verify: `git --version`

---

## Step 1: Create the project on GitHub

```bash
# On your laptop
mkdir retail-pipeline
cd retail-pipeline
git init
git branch -M main
```

Create a new **empty** repo on GitHub called `retail-pipeline` (no README, no .gitignore — we'll add them). Then:

```bash
git remote add origin https://github.com/<your-username>/retail-pipeline.git
```

Copy all the files I've generated into this folder, then:

```bash
git add .
git commit -m "Phase 1: initial project structure"
git push -u origin main
```

---

## Step 2: Create your Snowflake account

1. Go to https://signup.snowflake.com/
2. Choose **Enterprise** edition (you said you wanted Enterprise)
3. Choose **AWS** as cloud provider
4. Choose **Asia Pacific (Mumbai)** as region (closest to Pune for lowest latency)
5. Verify email, set password
6. **Save these values somewhere safe:**
   - Account identifier (looks like `abc12345.ap-south-1.aws` — visible in the URL after login)
   - Username
   - Password

After login, click **Worksheets** (left sidebar) → **+ Worksheet** to open a SQL editor.

---

## Step 3: Run the Snowflake setup script

Open `infra/snowflake_setup.sql` from the project. Copy its contents into the Snowflake worksheet and run it (Cmd/Ctrl + Enter executes the current statement; or use "Run All").

This script creates:
- A dedicated `ETL_ROLE` (least-privilege principle)
- A dedicated `ETL_USER` for pipeline use
- A warehouse `ETL_WH` (X-SMALL, auto-suspend 60s — keeps costs near zero)
- The `RETAIL_DB` database with all five schemas: `RAW`, `STAGING`, `CURATED`, `MARTS`, `METADATA`
- Proper grants

**Important:** When the script asks you to set a password for `ETL_USER`, pick a strong one and save it. You'll need it for Airflow's Snowflake connection.

---

## Step 4: Set up Postgres locally

Open a terminal and connect to your local Postgres as the superuser:

```bash
psql -U postgres
```

Run these commands to create the source database and a dedicated user:

```sql
CREATE USER northwind_user WITH PASSWORD 'northwind_pass';
CREATE DATABASE northwind_oltp OWNER northwind_user;
GRANT ALL PRIVILEGES ON DATABASE northwind_oltp TO northwind_user;
\q
```

Now seed it with synthetic data:

```bash
# In the project root
python -m venv .venv
source .venv/bin/activate          # Mac/Linux
# .venv\Scripts\activate           # Windows

pip install -r requirements-seed.txt
python infra/seed_postgres.py
```

You should see output like:
```
Connecting to northwind_oltp...
Creating tables...
Generating 10,000 customers... done
Generating 100,000 orders... done
Generating 300,000 order_items... done
✓ Seed complete.
```

Verify:
```bash
psql -U northwind_user -d northwind_oltp -c "SELECT COUNT(*) FROM orders;"
# Should return 100000
```

---

## Step 5: Configure environment variables

Copy the `.env.example` to `.env`:

```bash
cp .env.example .env
```

Edit `.env` and fill in the real values:

```
# Postgres (source)
POSTGRES_HOST=host.docker.internal    # so Airflow container can reach your laptop's Postgres
POSTGRES_PORT=5432
POSTGRES_DB=northwind_oltp
POSTGRES_USER=northwind_user
POSTGRES_PASSWORD=northwind_pass

# Snowflake
SNOWFLAKE_ACCOUNT=abc12345.ap-south-1.aws    # your real account identifier
SNOWFLAKE_USER=ETL_USER
SNOWFLAKE_PASSWORD=<the password you set in step 3>
SNOWFLAKE_ROLE=ETL_ROLE
SNOWFLAKE_WAREHOUSE=ETL_WH
SNOWFLAKE_DATABASE=RETAIL_DB

# Airflow internals (leave as-is unless you know what you're doing)
AIRFLOW_UID=50000
AIRFLOW_FERNET_KEY=<we'll generate this in next step>
```

Generate the Fernet key for Airflow:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Paste the output into `AIRFLOW_FERNET_KEY=` in `.env`.

**Important:** `.env` is in `.gitignore` — never commit it.

---

## Step 6: Allow Postgres to accept connections from Docker

By default, your local Postgres only accepts connections from localhost. The Airflow container needs to reach it.

Edit `pg_hba.conf` (location depends on your install):
- Mac (Homebrew): `/usr/local/var/postgres/pg_hba.conf` or `/opt/homebrew/var/postgresql@16/pg_hba.conf`
- Linux: `/etc/postgresql/16/main/pg_hba.conf`
- Windows: `C:\Program Files\PostgreSQL\16\data\pg_hba.conf`

Add this line:
```
host    northwind_oltp    northwind_user    0.0.0.0/0    md5
```

Edit `postgresql.conf` (same directory) and ensure:
```
listen_addresses = '*'
```

Restart Postgres:
- Mac: `brew services restart postgresql@16`
- Linux: `sudo systemctl restart postgresql`
- Windows: restart the PostgreSQL service from Services panel

---

## Step 7: Start Airflow

```bash
# Initialize Airflow (first time only)
docker compose up airflow-init

# When it finishes ("exited with code 0"), start the stack
docker compose up -d
```

Wait ~60 seconds for everything to start. Check status:

```bash
docker compose ps
```

All services should show `healthy`. Open Airflow UI:

**http://localhost:8080**

Login: `airflow` / `airflow`

---

## Step 8: Configure Airflow Connections

In the Airflow UI, go to **Admin → Connections → +** and create two connections:

**Connection 1: `postgres_source`**
- Connection Id: `postgres_source`
- Connection Type: `Postgres`
- Host: `host.docker.internal`
- Schema: `northwind_oltp`
- Login: `northwind_user`
- Password: `northwind_pass`
- Port: `5432`

**Connection 2: `snowflake_default`**
- Connection Id: `snowflake_default`
- Connection Type: `Snowflake`
- Login: `ETL_USER`
- Password: `<the password you set in step 3>`
- Schema: `RAW`
- Extra:
  ```json
  {
    "account": "abc12345.ap-south-1.aws",
    "warehouse": "ETL_WH",
    "database": "RETAIL_DB",
    "role": "ETL_ROLE",
    "region": "ap-south-1"
  }
  ```

---

## Step 9: Run the Hello World DAG

In the Airflow UI:
1. Find the DAG named `hello_world_check`
2. Toggle it **ON** (the switch on the left)
3. Click the DAG name → click **▶ Trigger DAG**

Watch the graph view. All three tasks should turn green:
- `check_postgres_connection` — connects to Postgres, counts orders
- `check_snowflake_connection` — connects to Snowflake, checks DB exists
- `check_apis` — hits both public APIs

If all green → **Phase 1 complete.** 🎉

If any red → click the failed task → **Log** tab. Common issues:
- Postgres connection refused → check Step 6 (`pg_hba.conf` and restart)
- Snowflake account format wrong → use the full identifier including region
- Docker can't reach Postgres → ensure `host.docker.internal` is used (not `localhost`)

---

## What's Next

Once your Hello World runs green, commit to GitHub:

```bash
git add .
git commit -m "Phase 1 complete: environment verified"
git push
```

Then we move to **Phase 2: Initial Full Load** — actually extracting data, landing it in Snowflake, and building the staging/curated layers.
