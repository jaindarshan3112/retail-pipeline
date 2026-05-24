"""
Postgres source database seeder for the NorthWind Retail Pipeline.

Creates three tables (customers, orders, order_items) and populates them
with realistic synthetic data using Faker. Includes a `modified_at`
column on each table to support incremental extraction in later phases.

Usage:
    python infra/seed_postgres.py

Connection defaults match what's in PHASE1_SETUP.md step 4:
    db=northwind_oltp  user=northwind_user  password=northwind_pass
"""

import os
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from faker import Faker

# Load .env from project root so seeding uses the same credentials
# as the rest of the pipeline.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env", override=False)

# ---------- Config -----------------------------------------------------
# Note: When running this script on your laptop, POSTGRES_HOST should be
# 'localhost'. The 'host.docker.internal' value in .env is only for the
# Airflow container — but here we always want to talk to local Postgres,
# so we force 'localhost' as the default.

PG_CONN = {
    "host":     os.getenv("POSTGRES_HOST_LOCAL", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",   "northwind_oltp"),
    "user":     os.getenv("POSTGRES_USER", "northwind_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "northwind_pass"),
}

N_CUSTOMERS   = 10_000
N_ORDERS      = 100_000
ITEMS_PER_ORDER_RANGE = (1, 5)

CURRENCIES   = ["USD", "EUR", "GBP", "INR", "JPY"]
SEGMENTS     = ["CONSUMER", "SMB", "ENTERPRISE"]
ORDER_STATUS = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]
COUNTRIES    = ["US", "GB", "DE", "FR", "IN", "JP", "CA", "AU", "BR", "SG"]

# fakestoreapi has product IDs 1..20 — we reference them in order_items
PRODUCT_ID_RANGE = (1, 20)

# Reproducibility
random.seed(42)
Faker.seed(42)
fake = Faker()


# ---------- DDL --------------------------------------------------------

DDL = """
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders      CASCADE;
DROP TABLE IF EXISTS customers   CASCADE;

CREATE TABLE customers (
    customer_id      BIGSERIAL PRIMARY KEY,
    email            TEXT      NOT NULL UNIQUE,
    first_name       TEXT      NOT NULL,
    last_name        TEXT      NOT NULL,
    customer_segment TEXT      NOT NULL,
    country          TEXT      NOT NULL,
    signup_date      DATE      NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX ix_customers_modified_at ON customers(modified_at);

CREATE TABLE orders (
    order_id          BIGSERIAL PRIMARY KEY,
    customer_id       BIGINT      NOT NULL REFERENCES customers(customer_id),
    order_date        DATE        NOT NULL,
    status            TEXT        NOT NULL,
    shipping_country  TEXT        NOT NULL,
    currency_code     TEXT        NOT NULL,
    total_amount      NUMERIC(12,2) NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX ix_orders_modified_at ON orders(modified_at);
CREATE INDEX ix_orders_customer    ON orders(customer_id);

CREATE TABLE order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id      BIGINT       NOT NULL REFERENCES orders(order_id),
    product_id    INT          NOT NULL,
    quantity      INT          NOT NULL,
    unit_price    NUMERIC(12,2) NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX ix_order_items_order ON order_items(order_id);
"""


# ---------- Generators -------------------------------------------------

def random_ts_between(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def gen_customers(n: int):
    """Yield tuples for COPY into customers."""
    start = datetime(2022, 1, 1, tzinfo=timezone.utc)
    end   = datetime(2026, 5, 1, tzinfo=timezone.utc)
    seen_emails = set()
    while len(seen_emails) < n:
        email = fake.unique.email()
        if email in seen_emails:
            continue
        seen_emails.add(email)
        signup_ts = random_ts_between(start, end)
        yield (
            email,
            fake.first_name(),
            fake.last_name(),
            random.choice(SEGMENTS),
            random.choice(COUNTRIES),
            signup_ts.date(),
            signup_ts,
            signup_ts,
        )


def gen_orders(n: int, n_customers: int):
    """Yield tuples for COPY into orders."""
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end   = datetime(2026, 5, 20, tzinfo=timezone.utc)
    for _ in range(n):
        ts = random_ts_between(start, end)
        yield (
            random.randint(1, n_customers),               # customer_id
            ts.date(),                                    # order_date
            random.choices(
                ORDER_STATUS,
                weights=[10, 20, 65, 5],                  # most are DELIVERED
                k=1
            )[0],
            random.choice(COUNTRIES),
            random.choice(CURRENCIES),
            Decimal(str(round(random.uniform(10, 5000), 2))),
            ts,
            ts,
        )


def gen_order_items(order_id: int):
    """Yield 1..5 line items for a given order."""
    n_items = random.randint(*ITEMS_PER_ORDER_RANGE)
    for _ in range(n_items):
        yield (
            order_id,
            random.randint(*PRODUCT_ID_RANGE),
            random.randint(1, 5),
            Decimal(str(round(random.uniform(5, 500), 2))),
        )


# ---------- Main -------------------------------------------------------

def main() -> None:
    print(f"Connecting to {PG_CONN['dbname']} at {PG_CONN['host']}:{PG_CONN['port']}...")
    with psycopg.connect(**PG_CONN, autocommit=False) as conn:
        with conn.cursor() as cur:
            print("Creating tables...")
            cur.execute(DDL)
            conn.commit()

            # --- customers ---
            print(f"Generating {N_CUSTOMERS:,} customers...", end=" ", flush=True)
            with cur.copy(
                "COPY customers (email, first_name, last_name, customer_segment, "
                "country, signup_date, created_at, modified_at) FROM STDIN"
            ) as copy:
                for row in gen_customers(N_CUSTOMERS):
                    copy.write_row(row)
            conn.commit()
            print("done")

            # --- orders ---
            print(f"Generating {N_ORDERS:,} orders...", end=" ", flush=True)
            with cur.copy(
                "COPY orders (customer_id, order_date, status, shipping_country, "
                "currency_code, total_amount, created_at, modified_at) FROM STDIN"
            ) as copy:
                for row in gen_orders(N_ORDERS, N_CUSTOMERS):
                    copy.write_row(row)
            conn.commit()
            print("done")

            # --- order_items ---
            print("Generating order_items...", end=" ", flush=True)
            cur.execute("SELECT order_id FROM orders")
            order_ids = [r[0] for r in cur.fetchall()]
            total_items = 0
            with cur.copy(
                "COPY order_items (order_id, product_id, quantity, unit_price) "
                "FROM STDIN"
            ) as copy:
                for oid in order_ids:
                    for row in gen_order_items(oid):
                        copy.write_row(row)
                        total_items += 1
            conn.commit()
            print(f"done ({total_items:,} items)")

            # --- summary ---
            cur.execute("SELECT COUNT(*) FROM customers");   c = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM orders");      o = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM order_items"); i = cur.fetchone()[0]
            print(f"\n✓ Seed complete.  customers={c:,}  orders={o:,}  order_items={i:,}")


if __name__ == "__main__":
    main()
