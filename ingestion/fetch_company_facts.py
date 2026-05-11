"""
Fetch XBRL company facts from SEC EDGAR.

What this script does:
    For each company in companies.csv, calls the SEC companyfacts endpoint:
        https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json

    This is the most important endpoint in the project. It returns every
    financial metric the company has ever reported across all filings,
    structured as nested JSON:

        facts > us-gaap > {concept} > units > USD > [array of values]

    Example path to Apple's revenue figures:
        facts.us-gaap.Revenues.units.USD → list of {val, start, end, form, filed, accn}

    Files are 5–20 MB each. The raw JSON is written to disk unchanged.
    The Bronze → Silver dbt layers (Phase 3) handle unnesting and normalisation.

Output:
    data/raw/facts/{cik}.json   (one file per company, 5–20 MB each)

Staleness check:
    If a file already exists and was modified within the last 6 hours,
    the fetch is skipped. This prevents redundant API calls when the
    Airflow DAG re-triggers or a partial run is retried.

Usage:
    python fetch_company_facts.py --all               # fetch all companies
    python fetch_company_facts.py --cik 0000320193    # fetch one company (Apple)
    python fetch_company_facts.py --ticker AAPL       # fetch by ticker
    python fetch_company_facts.py --all --force       # ignore staleness check
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Make utils.py importable when the script is run from any working directory
# e.g. `python ingestion/fetch_company_facts.py` from the project root
sys.path.insert(0, str(Path(__file__).parent))

from utils import build_session, rate_limited_get

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
_HERE          = Path(__file__).parent
COMPANIES_CSV  = _HERE / "companies.csv"
RAW_FACTS_DIR  = _HERE.parent / "data" / "raw" / "facts"

# ── Constants ─────────────────────────────────────────────────────────────────
BASE_URL = "https://data.sec.gov"

# Skip re-fetching if the file was written less than this many hours ago.
# The Airflow daily DAG uses this so a manual re-trigger doesn't hammer the API.
STALE_AFTER_HOURS = 6


# ── Staleness check ───────────────────────────────────────────────────────────

def _is_fresh(path: Path, max_age_hours: int = STALE_AFTER_HOURS) -> bool:
    """
    Return True if the file exists and was modified within max_age_hours.

    Why this exists:
        The companyfacts files are 5–20 MB each and the SEC data changes
        at most once per quarter. Re-downloading 54 × 20 MB = 1 GB on every
        Airflow re-run is wasteful. We skip the fetch if the file is fresh enough.
    """
    if not path.exists():
        return False
    age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
    return age < timedelta(hours=max_age_hours)


# ── Core fetch function ───────────────────────────────────────────────────────

def fetch_one(cik: str, session, output_dir: Path, force: bool = False) -> tuple[Path, str]:
    """
    Fetch and save the companyfacts JSON for a single company.

    Args:
        cik:        Company CIK — any format, zero-padding applied here.
        session:    Configured requests.Session from build_session().
        output_dir: Directory to write the JSON file into.
        force:      If True, skip the staleness check and always re-fetch.

    Returns:
        Tuple of (output_path, status) where status is 'fetched' or 'skipped'.
    """
    cik = str(cik).zfill(10)
    url = f"{BASE_URL}/api/xbrl/companyfacts/CIK{cik}.json"
    output_path = output_dir / f"{cik}.json"

    # Staleness check — skip if the file is recent enough
    if not force and _is_fresh(output_path):
        age_hours = (datetime.now() - datetime.fromtimestamp(output_path.stat().st_mtime)).seconds / 3600
        log.info("  Skipped %-14s  (file is %.1f h old, threshold %d h)",
                 cik, age_hours, STALE_AFTER_HOURS)
        return output_path, "skipped"

    # Fetch from SEC API — rate_limited_get handles retry + rate limiting
    data = rate_limited_get(session, url)

    # Sanity check: the response must have a 'facts' key
    # If it doesn't, we got a malformed response — don't write a bad file
    if "facts" not in data:
        raise ValueError(
            f"Response for CIK {cik} is missing 'facts' key. "
            f"Keys present: {list(data.keys())}"
        )

    # Count how many GAAP concepts came back — useful for monitoring
    gaap_concepts = len(data.get("facts", {}).get("us-gaap", {}))

    # Write raw JSON to disk — no transformation
    output_path.write_text(json.dumps(data), encoding="utf-8")

    file_mb = output_path.stat().st_size / (1024 * 1024)
    log.info("  Saved %-14s  %.1f MB  %d concepts  →  %s",
             cik, file_mb, gaap_concepts, output_path.name)

    return output_path, "fetched"


def fetch_all(companies_csv: Path = COMPANIES_CSV,
              output_dir: Path = RAW_FACTS_DIR,
              force: bool = False) -> dict:
    """
    Fetch companyfacts for every company in companies.csv.

    Args:
        companies_csv: Path to the seed CSV file.
        output_dir:    Where to write the raw JSON files.
        force:         If True, re-fetch even fresh files.

    Returns:
        Summary dict with counts — also pushed to Airflow XCom by the DAG.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    session = build_session()

    with open(companies_csv, encoding="utf-8") as f:
        companies = list(csv.DictReader(f))

    log.info("Starting company facts fetch for %d companies", len(companies))
    log.info("Output directory : %s", output_dir)
    log.info("Force re-fetch   : %s", force)
    log.info("Stale threshold  : %d hours", STALE_AFTER_HOURS)

    results = {
        "fetched":  0,
        "skipped":  0,
        "failed":   0,
        "failures": [],
    }

    for i, company in enumerate(companies, start=1):
        cik    = company["cik"]
        ticker = company["ticker"]
        name   = company["company_name"]

        log.info("[%d/%d] %s (%s)", i, len(companies), name, ticker)

        try:
            _, status = fetch_one(cik, session, output_dir, force=force)
            results[status] += 1

        except Exception as e:
            log.error("  FAILED %s (%s): %s", ticker, cik, e)
            results["failed"] += 1
            results["failures"].append({
                "cik":    cik,
                "ticker": ticker,
                "error":  str(e),
            })

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("─" * 60)
    log.info("Company facts fetch complete")
    log.info("  Fetched : %d", results["fetched"])
    log.info("  Skipped : %d  (files were fresh)", results["skipped"])
    log.info("  Failed  : %d", results["failed"])

    if results["failures"]:
        log.warning("Failed companies:")
        for failure in results["failures"]:
            log.warning("  %s (%s) — %s", failure["ticker"], failure["cik"], failure["error"])

    return results


# ── CLI entry point ───────────────────────────────────────────────────────────

def _parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch SEC EDGAR XBRL company facts for tracked companies."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--all",
        action="store_true",
        help="Fetch all companies in companies.csv",
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
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore the staleness check and always re-fetch",
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
        summary = fetch_all(force=args.force)
        sys.exit(1 if summary["failed"] > 0 else 0)

    elif args.cik:
        RAW_FACTS_DIR.mkdir(parents=True, exist_ok=True)
        session = build_session()
        fetch_one(args.cik, session, RAW_FACTS_DIR, force=args.force)

    elif args.ticker:
        cik = _lookup_cik_by_ticker(args.ticker, COMPANIES_CSV)
        RAW_FACTS_DIR.mkdir(parents=True, exist_ok=True)
        session = build_session()
        fetch_one(cik, session, RAW_FACTS_DIR, force=args.force)
