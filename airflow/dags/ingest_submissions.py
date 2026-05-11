"""
DAG: ingest_submissions
Schedule: Weekly — every Sunday at 04:00 UTC

What this DAG does:
    Calls the SEC EDGAR submissions endpoint for every company in companies.csv
    and saves the raw JSON to data/raw/submissions/{cik}.json.

    The submissions feed contains filing metadata — form type, filing date,
    accession number, document URLs — for every filing a company has ever made.
    This data changes slowly (new filings appear once per quarter), so weekly
    refresh is sufficient.

Why it runs before the facts DAG:
    Submissions data tells us *which* filings exist. The company facts data
    tells us *what numbers* those filings contain. Running submissions weekly
    keeps the filing index fresh without hitting the API daily.
"""

from __future__ import annotations

import csv
import json
import logging
import sys
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Paths inside the Airflow container ───────────────────────────────────────
# These paths match the volume mounts in docker-compose.yml
COMPANIES_CSV       = Path("/opt/airflow/ingestion/companies.csv")
RAW_SUBMISSIONS_DIR = Path("/opt/airflow/data/raw/submissions")

# Add the ingestion folder to sys.path so we can import the hook
sys.path.insert(0, "/opt/airflow/plugins")

log = logging.getLogger(__name__)

# ── Default task arguments ────────────────────────────────────────────────────
# These apply to every task in the DAG unless overridden at the task level.
default_args = {
    "owner":                    "data-engineering",
    "retries":                  3,
    "retry_delay":              timedelta(minutes=5),
    "retry_exponential_backoff": True,   # 5m → 10m → 20m between retries
}


# ── Task functions ────────────────────────────────────────────────────────────

def fetch_all_submissions(**context) -> dict:
    """
    PythonOperator callable: fetch submissions for all companies.

    Uses SecApiHook so the HTTP session is managed by Airflow.
    Pushes a summary dict to XCom so downstream tasks or alerts can
    check how many companies were successfully fetched.

    Returns:
        Summary dict: {"fetched": int, "failed": int, "failures": [...]}
    """
    from sec_api_hook import SecApiHook

    hook = SecApiHook()
    RAW_SUBMISSIONS_DIR.mkdir(parents=True, exist_ok=True)

    with open(COMPANIES_CSV, encoding="utf-8") as f:
        companies = list(csv.DictReader(f))

    log.info("Fetching submissions for %d companies", len(companies))

    results = {"fetched": 0, "failed": 0, "failures": []}

    for i, company in enumerate(companies, start=1):
        cik    = str(company["cik"]).zfill(10)
        ticker = company["ticker"]
        name   = company["company_name"]
        output_path = RAW_SUBMISSIONS_DIR / f"{cik}.json"

        log.info("[%d/%d] %s (%s)", i, len(companies), name, ticker)

        try:
            data = hook.get_submissions(cik)
            output_path.write_text(json.dumps(data), encoding="utf-8")
            results["fetched"] += 1
            log.info("  Saved %s", output_path.name)

        except Exception as e:
            log.error("  FAILED %s (%s): %s", ticker, cik, e)
            results["failed"] += 1
            results["failures"].append({"cik": cik, "ticker": ticker, "error": str(e)})

    log.info("Submissions fetch complete — fetched: %d  failed: %d",
             results["fetched"], results["failed"])

    # Push to XCom so the summary is visible in the Airflow UI
    context["ti"].xcom_push(key="submissions_summary", value=results)
    return results


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="ingest_submissions",
    description="Weekly refresh of SEC EDGAR filing submission metadata",
    schedule_interval="0 4 * * 0",   # Every Sunday at 04:00 UTC
    start_date=days_ago(1),
    catchup=False,                   # Don't backfill missed runs on first deploy
    default_args=default_args,
    tags=["sec", "ingestion", "weekly"],
) as dag:

    fetch_submissions_task = PythonOperator(
        task_id="fetch_all_submissions",
        python_callable=fetch_all_submissions,
    )
