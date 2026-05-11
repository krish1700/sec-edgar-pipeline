"""
DAG: ingest_company_facts
Schedule: Daily at 06:00 UTC

What this DAG does:
    Calls the SEC EDGAR companyfacts endpoint for every company in companies.csv
    and saves the raw JSON to data/raw/facts/{cik}.json.

    The companyfacts feed is the most important data source in the pipeline.
    It contains every financial metric (Revenue, Net Income, EPS, Assets, etc.)
    across all historical filings for a company, in one response.

    File sizes: 5–20 MB per company. Total for 54 companies: ~500 MB on disk.

Staleness check:
    If a file was written less than 6 hours ago, the fetch is skipped.
    This means Airflow retries and manual re-triggers don't re-download
    gigabytes of data that haven't changed.

XCom:
    Pushes a summary dict with fetched/skipped/failed counts. The downstream
    run_dbt_transformations DAG reads this to confirm ingestion succeeded.
"""

from __future__ import annotations

import csv
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Container paths ───────────────────────────────────────────────────────────
COMPANIES_CSV  = Path("/opt/airflow/ingestion/companies.csv")
RAW_FACTS_DIR  = Path("/opt/airflow/data/raw/facts")

sys.path.insert(0, "/opt/airflow/plugins")

log = logging.getLogger(__name__)

STALE_AFTER_HOURS = 6

default_args = {
    "owner":                     "data-engineering",
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_fresh(path: Path) -> bool:
    """Return True if the file exists and was modified within STALE_AFTER_HOURS."""
    if not path.exists():
        return False
    age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
    return age < timedelta(hours=STALE_AFTER_HOURS)


# ── Task functions ────────────────────────────────────────────────────────────

def fetch_all_company_facts(**context) -> dict:
    """
    PythonOperator callable: fetch XBRL facts for all companies.

    For each company:
    1. Check if the JSON file is fresh (< 6 hours old) — skip if so
    2. Call SEC API via SecApiHook
    3. Validate the response has a 'facts' key
    4. Write raw JSON to disk

    Pushes summary to XCom.
    """
    from sec_api_hook import SecApiHook

    hook = SecApiHook()
    RAW_FACTS_DIR.mkdir(parents=True, exist_ok=True)

    with open(COMPANIES_CSV, encoding="utf-8") as f:
        companies = list(csv.DictReader(f))

    log.info("Starting company facts ingestion for %d companies", len(companies))

    results = {"fetched": 0, "skipped": 0, "failed": 0, "failures": []}

    for i, company in enumerate(companies, start=1):
        cik    = str(company["cik"]).zfill(10)
        ticker = company["ticker"]
        name   = company["company_name"]
        output_path = RAW_FACTS_DIR / f"{cik}.json"

        log.info("[%d/%d] %s (%s)", i, len(companies), name, ticker)

        # Staleness check — skip fresh files
        if _is_fresh(output_path):
            log.info("  Skipped — file is fresh (< %d h old)", STALE_AFTER_HOURS)
            results["skipped"] += 1
            continue

        try:
            data = hook.get_company_facts(cik)

            # Sanity check before writing — a response without 'facts' is useless
            if "facts" not in data:
                raise ValueError(
                    f"Response missing 'facts' key. Keys: {list(data.keys())}"
                )

            output_path.write_text(json.dumps(data), encoding="utf-8")

            file_mb      = output_path.stat().st_size / (1024 * 1024)
            gaap_concepts = len(data.get("facts", {}).get("us-gaap", {}))
            log.info("  Saved %.1f MB  %d GAAP concepts", file_mb, gaap_concepts)

            results["fetched"] += 1

        except Exception as e:
            log.error("  FAILED %s (%s): %s", ticker, cik, e)
            results["failed"] += 1
            results["failures"].append({"cik": cik, "ticker": ticker, "error": str(e)})

    log.info(
        "Facts ingestion complete — fetched: %d  skipped: %d  failed: %d",
        results["fetched"], results["skipped"], results["failed"],
    )

    # Push to XCom — run_dbt_transformations will read this
    context["ti"].xcom_push(key="ingestion_summary", value=results)

    # Fail the task if ANY company failed — Airflow will retry the whole task
    # and the staleness check means fresh files won't be re-downloaded
    if results["failed"] > 0:
        raise RuntimeError(
            f"{results['failed']} companies failed to ingest. "
            f"Check task logs for details. Failures: {results['failures']}"
        )

    return results


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="ingest_company_facts",
    description="Daily fetch of SEC EDGAR XBRL company facts for all tracked companies",
    schedule_interval="0 6 * * *",   # Every day at 06:00 UTC
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["sec", "ingestion", "daily"],
) as dag:

    fetch_facts_task = PythonOperator(
        task_id="fetch_all_company_facts",
        python_callable=fetch_all_company_facts,
    )
