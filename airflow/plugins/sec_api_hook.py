"""
Airflow Hook for the SEC EDGAR public API.

Why a Hook instead of importing the ingestion scripts directly?
    Airflow tasks run in isolated processes. A Hook wraps the external service
    connection in a class that Airflow manages — it is instantiated fresh for
    each task run, which avoids shared state between tasks.

    The logic inside is the same as ingestion/utils.py. The Hook is the
    Airflow-native way to expose it to DAGs.

Usage in a DAG:
    from sec_api_hook import SecApiHook

    hook = SecApiHook()
    data = hook.get_company_facts("0000320193")   # returns parsed dict
"""

import os
import sys
import time
import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from airflow.hooks.base import BaseHook  # type: ignore[import]
except ImportError:
    # Airflow is not installed locally — it runs in Docker only.
    # This stub keeps the file importable and lintable outside the container.
    class BaseHook:  # type: ignore[no-redef]
        log: logging.Logger = logging.getLogger(__name__)
        def __init__(self): pass

# The ingestion folder is mounted into the container at /opt/airflow/ingestion
# (see docker-compose.yml volumes). Add it to the path so we can reuse utils.py.
_INGESTION_PATH = "/opt/airflow/ingestion"
if _INGESTION_PATH not in sys.path:
    sys.path.insert(0, _INGESTION_PATH)


class SecApiHook(BaseHook):  # type: ignore[misc]
    """
    Hook for the SEC EDGAR public REST API (data.sec.gov).

    Handles:
        - User-Agent header injection (required by SEC fair-access policy)
        - urllib3 retry with exponential backoff on 429 / 5xx responses
        - 0.1s inter-request delay to respect SEC rate guidance
        - Structured logging via Airflow's task logger

    Instantiate once per task; do not share across tasks.
    """

    conn_name_attr = "sec_api_conn_id"
    default_conn_name = "sec_api_default"
    conn_type = "http"
    hook_name = "SEC EDGAR API"

    BASE_URL = "https://data.sec.gov"
    REQUEST_DELAY = 0.1       # seconds between requests
    REQUEST_TIMEOUT = 30      # seconds before raising Timeout

    _RETRY_CONFIG = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    def __init__(self, delay_seconds: float = REQUEST_DELAY):
        super().__init__()
        self.delay_seconds = delay_seconds
        self._session: requests.Session | None = None

    # ── Session management ────────────────────────────────────────────────────

    def get_conn(self) -> requests.Session:
        """
        Build and return the configured requests.Session.

        Called get_conn() to follow the Airflow Hook convention — all Hooks
        expose their underlying connection through this method name.
        """
        if self._session is not None:
            return self._session

        user_agent = os.environ.get("SEC_USER_AGENT", "")
        if not user_agent:
            raise EnvironmentError(
                "SEC_USER_AGENT environment variable is not set. "
                "Add it to your .env file: SEC_USER_AGENT=\"First Last email@example.com\""
            )

        adapter = HTTPAdapter(max_retries=self._RETRY_CONFIG)
        session = requests.Session()
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": user_agent,
            "Accept":     "application/json",
        })

        self._session = session
        return session

    # ── Request helpers ───────────────────────────────────────────────────────

    def _get(self, path: str) -> dict:
        """
        Internal GET — builds the full URL, sends the request, enforces delay.

        Args:
            path: API path starting with '/'. Example: '/submissions/CIK0000320193.json'

        Returns:
            Parsed JSON response as a Python dict.
        """
        session = self.get_conn()
        url = f"{self.BASE_URL}{path}"
        self.log.info("GET %s", url)

        response = session.get(url, timeout=self.REQUEST_TIMEOUT)
        response.raise_for_status()
        time.sleep(self.delay_seconds)

        return response.json()

    # ── Public API methods ────────────────────────────────────────────────────

    def get_company_facts(self, cik: str) -> dict:
        """
        Fetch all XBRL financial facts for a company across all filings.

        This is the core endpoint for the pipeline. The response contains
        every reported financial metric structured as:
            facts > us-gaap > {concept} > units > USD > [array of values]

        File sizes: 5–20 MB per company.

        Args:
            cik: Company CIK — any format, zero-padded to 10 digits here.

        Returns:
            Full companyfacts response as a dict.
        """
        cik = str(cik).zfill(10)
        return self._get(f"/api/xbrl/companyfacts/CIK{cik}.json")

    def get_submissions(self, cik: str) -> dict:
        """
        Fetch all filing submission metadata for a company.

        Returns filing history: form types, dates, accession numbers, URLs.
        Used to discover which 10-K and 10-Q filings exist for each company.

        Args:
            cik: Company CIK — any format, zero-padded to 10 digits here.

        Returns:
            Full submissions response as a dict.
        """
        cik = str(cik).zfill(10)
        return self._get(f"/submissions/CIK{cik}.json")
