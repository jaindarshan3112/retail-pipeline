"""
API extractor for the NorthWind retail pipeline.

Pulls two datasets:
    products  → https://fakestoreapi.com/products       (list of product objects)
    fx_rates  → https://api.exchangerate.host/latest    (USD-base FX rates)

Both are written as JSON files because Snowflake's VARIANT column ingests
JSON directly — this gives us schema flexibility (if the API adds a new
field, our pipeline doesn't break).

The FX rates response is reshaped slightly: the API returns
    { "base": "USD", "date": "...", "rates": { "EUR": 0.92, ... } }
We flatten this into one row per currency for easier loading:
    [{"base": "USD", "date": "...", "currency": "EUR", "rate": 0.92}, ...]

Run standalone:
    python -m tasks.extract.api_extractor
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import requests

from tasks.utils.config import API, EXTRACT_DIR
from tasks.utils.logger import get_logger

log = get_logger(__name__)


# ---------- helpers ----------------------------------------------------

def _write_json(records: list[dict], name: str, run_id: str) -> Path:
    """Write a list of records as a JSON array file."""
    out_dir = EXTRACT_DIR / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{name}.json"
    with out_path.open("w") as f:
        json.dump(records, f)
    log.info(f"  wrote {len(records):>6,} records → {out_path}")
    return out_path


# ---------- extractors -------------------------------------------------

def extract_products(run_id: str) -> dict:
    """Fetch the full product catalog from fakestoreapi."""
    log.info(f"  GET {API.products_url}")
    resp = requests.get(API.products_url, timeout=API.timeout_sec)
    resp.raise_for_status()
    products = resp.json()
    if not isinstance(products, list):
        raise ValueError(f"Expected list from products API, got {type(products)}")
    path = _write_json(products, "products", run_id)
    return {"path": path, "row_count": len(products)}


def extract_fx_rates(run_id: str) -> dict:
    """Fetch the latest USD-base FX rates and flatten to one row per currency."""
    log.info(f"  GET {API.fx_url}")
    resp = requests.get(API.fx_url, timeout=API.timeout_sec)
    resp.raise_for_status()
    payload = resp.json()
    base = payload.get("base", "USD")
    rate_date = payload.get("date")
    rates = payload.get("rates", {})
    if not rates:
        raise ValueError(f"FX API returned no rates: {payload!r}")

    # Flatten { "EUR": 0.92, ... } → [{base, date, currency, rate}, ...]
    flattened = [
        {"base": base, "rate_date": rate_date, "currency": ccy, "rate": rate}
        for ccy, rate in rates.items()
    ]
    path = _write_json(flattened, "fx_rates", run_id)
    return {"path": path, "row_count": len(flattened)}


# ---------- driver -----------------------------------------------------

def extract_all(run_id: str | None = None) -> dict[str, dict]:
    """Extract all API sources. Returns manifest like postgres_extractor."""
    run_id = run_id or datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    log.info(f"=== API extraction (run_id={run_id}) ===")

    manifest = {
        "products": extract_products(run_id),
        "fx_rates": extract_fx_rates(run_id),
    }

    total = sum(m["row_count"] for m in manifest.values())
    log.info(f"✓ API extract complete. {total:,} records across {len(manifest)} sources.")
    return manifest


if __name__ == "__main__":
    extract_all()
