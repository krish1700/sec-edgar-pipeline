"""
Fetch company filing submission metadata from SEC EDGAR.

What this script does:
    For each company in companies.csv, calls the SEC submissions endpoint:
        https://data.sec.gov/submissions/CIK{cik}.json

    The response contains every filing the company has ever made — 10-K, 10-Q,
    8-K, etc. — along with metadata: accession numbers, filing dates, form types,
    and document URLs.

    The raw JSON is written to disk as-is. No parsing, no transformation.
    The Bronze dbt layer (Phase 3) is responsible for loading it into DuckDB.

Output:
    data/raw/submissions/{cik}.json   (one file per company)

Usage:
    python fetch_submissions.py --all              # fetch all companies
    python fetch_submissions.py --cik 0000320193   # fetch one company (Apple)
    python fetch_submissions.py --ticker AAPL      # fetch by ticker symbol
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

from utils import build_session, rate_limited_get

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
# Both paths are relative to this file's location so the script works
# regardless of which directory it is run from.
_HERE             = Path(__file__).parent
COMPANIES_CSV     = _HERE / "companies.csv"
RAW_SUBMISSIONS   = _HERE.parent / "data" / "raw" / "submissions"

# ── SEC API ───────────────────────────────────────────────────────────────────
BASE_URL = "https://data.sec.gov"


# ── Core fetch function ───────────────────────────────────────────────────────

def fetch_one(cik: str, session, output_dir: Path) -> Path:
    """
    Fetch and save the submissions JSON for a single company.

    The SEC endpoint requires a 10-digit zero-padded CIK in the URL.
    Example: CIK 320193 (Apple) → CIK0000320193.json

    Args:
        cik:        Company CIK — any format, zero-padding applied here.
        session:    Configured requests.Session from build_session().
        output_dir: Directory to write the JSON file into.

    Returns:
        Path of the written file.
    """
    # Always zero-pad to 10 digits — SEC URL format requires it
    cik = str(cik).zfill(10)
    url = f"{BASE_URL}/submissions/CIK{cik}.json"
    output_path = output_dir / f"{cik}.json"

    data = rate_limited_get(session, url)

    # Write raw JSON — no transformation, no filtering
    output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    file_kb = output_path.stat().st_size / 1024
    log.info("  Saved %-14s  %.1f KB  →  %s", cik, file_kb, output_path.name)

    return output_path


def fetch_all(companies_csv: Path = COMPANIES_CSV,
              output_dir: Path = RAW_SUBMISSIONS) -> dict:
    """
    Fetch submissions for every company in companies.csv.

    Returns a summary dict with counts of successful and failed fetches.
    This return value is also used by the Airflow DAG to push metrics to XCom.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    session = build_session()

    # Load company list
    with open(companies_csv, encoding="utf-8") as f:
        companies = list(csv.DictReader(f))

    log.info("Starting submissions fetch for %d companies", len(companies))
    log.info("Output directory: %s", output_dir)

    results = {"fetched": 0, "failed": 0, "failures": []}

    for i, company in enumerate(companies, start=1):
        cik    = company["cik"]
        ticker = company["ticker"]
        name   = company["company_name"]

        log.info("[%d/%d] %s (%s)", i, len(companies), name, ticker)

        try:
            fetch_one(cik, session, output_dir)
            results["fetched"] += 1

        except Exception as e:
            # Log the failure but continue — one bad company shouldn't stop the run
            log.error("  FAILED %s (%s): %s", ticker, cik, e)
            results["failed"] += 1
            results["failures"].append({"cik": cik, "ticker": ticker, "error": str(e)})

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("─" * 60)
    log.info("Submissions fetch complete")
    log.info("  Fetched : %d", results["fetched"])
    log.info("  Failed  : %d", results["failed"])

    if results["failures"]:
        log.warning("Failed companies:")
        for f in results["failures"]:
            log.warning("  %s (%s) — %s", f["ticker"], f["cik"], f["error"])

    return results


# ── CLI entry point ───────────────────────────────────────────────────────────

def _parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch SEC EDGAR submission metadata for tracked companies."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--all",
        action="store_true",
        help="Fetch all companies listed in companies.csv",
    )
    group.add_argument(
        "--cik",
        metavar="CIK",
        help="Fetch a single company by CIK (e.g. 0000320193)",
    )
    group.add_argument(
        "--ticker",
        metavar="TICKER",
        help="Fetch a single company by ticker symbol (e.g. AAPL)",
    )
    return parser.parse_args()


def _lookup_cik_by_ticker(ticker: str, companies_csv: Path) -> str:
    """Return the CIK for a given ticker from companies.csv."""
    ticker = ticker.upper()
    with open(companies_csv, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["ticker"].upper() == ticker:
                return row["cik"]
    raise ValueError(f"Ticker '{ticker}' not found in {companies_csv}")


if __name__ == "__main__":
    args = _parse_args()

    if args.all:
        summary = fetch_all()
        # Exit with error code if any companies failed — useful for CI / Airflow
        sys.exit(1 if summary["failed"] > 0 else 0)

    elif args.cik:
        RAW_SUBMISSIONS.mkdir(parents=True, exist_ok=True)
        session = build_session()
        fetch_one(args.cik, session, RAW_SUBMISSIONS)

    elif args.ticker:
        cik = _lookup_cik_by_ticker(args.ticker, COMPANIES_CSV)
        RAW_SUBMISSIONS.mkdir(parents=True, exist_ok=True)
        session = build_session()
        fetch_one(cik, session, RAW_SUBMISSIONS)
