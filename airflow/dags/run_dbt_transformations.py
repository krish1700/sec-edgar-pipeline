"""
DAG: run_dbt_transformations
Schedule: Daily at 07:00 UTC (1 hour after ingest_company_facts at 06:00)

What this DAG does:
    1. WAITS for ingest_company_facts to finish successfully (ExternalTaskSensor)
    2. Runs dbt in order: seed → run → test → source freshness

    This DAG never runs dbt on stale data. The ExternalTaskSensor blocks
    execution until the ingestion DAG confirms fresh JSON is on disk.

Why the 1-hour offset?
    The ingestion DAG starts at 06:00 and takes ~10–20 minutes for 54 companies.
    Starting this DAG at 07:00 gives ingestion a comfortable buffer.
    The ExternalTaskSensor provides the hard guarantee — even if ingestion runs
    longer than expected, dbt will wait rather than proceeding.

dbt run order:
    seed      → loads gaap_tag_synonyms.csv into DuckDB
    run       → Bronze → Silver → Gold models in dependency order
    test      → runs all generic + singular tests; fails the task if any test fails
    freshness → checks source data age; alerts if raw files are > 25 hours old
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

# Path to the dbt project inside the container (matches docker-compose.yml volume)
DBT_DIR = "/opt/airflow/dbt"

default_args = {
    "owner":        "data-engineering",
    "retries":      2,
    "retry_delay":  timedelta(minutes=10),
}

with DAG(
    dag_id="run_dbt_transformations",
    description="Run dbt Bronze → Silver → Gold after daily ingestion completes",
    schedule_interval="0 7 * * *",   # Every day at 07:00 UTC
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "transformation", "daily"],
) as dag:

    # ── Step 1: Wait for ingestion ────────────────────────────────────────────
    # ExternalTaskSensor polls Airflow's metadata database every 60 seconds.
    # It checks whether the fetch_all_company_facts task in the
    # ingest_company_facts DAG completed successfully for today's run.
    # If ingestion fails or hasn't finished, this sensor keeps waiting
    # (up to timeout=3600s = 1 hour) before raising a timeout error.
    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingest_company_facts",
        external_dag_id="ingest_company_facts",
        external_task_id="fetch_all_company_facts",
        allowed_states=["success"],          # only proceed if the task succeeded
        failed_states=["failed", "skipped"], # fail this sensor if ingestion failed
        timeout=3600,                        # wait up to 1 hour
        poke_interval=60,                    # check every 60 seconds
        mode="reschedule",                   # release the worker slot while waiting
    )

    # ── Step 2: Load seed data ────────────────────────────────────────────────
    # Loads gaap_tag_synonyms.csv into DuckDB as a reference table.
    # Must run before dbt run because stg_financials.sql joins against this seed.
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_DIR} && dbt seed --profiles-dir . --no-partial-parse",
    )

    # ── Step 3: Run all models ────────────────────────────────────────────────
    # dbt resolves model dependencies automatically and runs them in order:
    #   Bronze (raw_company_facts, raw_submissions)
    #     → Silver (stg_financials, stg_companies, stg_filing_periods)
    #       → Gold (fact_financials, dim_*, rpt_anomaly_flags)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir . --no-partial-parse",
    )

    # ── Step 4: Run all tests ─────────────────────────────────────────────────
    # Runs generic tests (not_null, unique, relationships) and singular tests
    # (assert_no_duplicate_periods, assert_revenue_nonnegative).
    # If any test fails, this task fails and Airflow will alert — dbt prints
    # exactly which rows caused the failure.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir . --no-partial-parse",
    )

    # ── Step 5: Check source freshness ───────────────────────────────────────
    # Verifies that the raw JSON files in data/raw/ were updated within the
    # last 25 hours. If the ingestion DAG silently produced empty files or
    # the files are older than expected, this raises a warning.
    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && dbt source freshness --profiles-dir .",
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    # Each step must complete before the next starts.
    # Arrow reads: "must complete before"
    wait_for_ingestion >> dbt_seed >> dbt_run >> dbt_test >> dbt_freshness
