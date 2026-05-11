"""
Shared HTTP utilities for SEC EDGAR ingestion scripts.

Two public functions:
    build_session()            -> requests.Session
    rate_limited_get(session, url) -> dict
"""

import os
import time
import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

# SEC fair-access policy: every automated client must send name + email.
# Loaded from .env so it is never hard-coded in source.
_USER_AGENT: str = os.environ.get("SEC_USER_AGENT", "")

# SEC asks for no more than 10 requests per second.
# 0.1 s sleep after every response keeps us comfortably under that limit.
_REQUEST_DELAY_SECONDS: float = 0.1

# urllib3-level retry: handles network failures and bad HTTP status codes
# *before* the response body even reaches our code.
_URLLIB3_RETRY = Retry(
    total=5,                              # maximum number of retry attempts
    backoff_factor=2,                     # sleep 2s, 4s, 8s, 16s, 32s between attempts
    status_forcelist=[429, 500, 502, 503, 504],  # retry on these HTTP codes
    allowed_methods=["GET"],              # only retry idempotent requests
    raise_on_status=False,               # let our code call raise_for_status()
)


# ── Public API ────────────────────────────────────────────────────────────────

def build_session() -> requests.Session:
    """
    Create and return a requests.Session configured for the SEC EDGAR API.

    The session:
    - Injects the required User-Agent header on every request
    - Mounts a retry adapter that automatically retries on network errors
      and 429/5xx responses, with exponential backoff

    Call this once per script run and reuse the session for all requests.
    Connection pooling means subsequent calls reuse the same TCP connection.

    Raises:
        EnvironmentError: if SEC_USER_AGENT is not set in the environment.
    """
    if not _USER_AGENT:
        raise EnvironmentError(
            "SEC_USER_AGENT is not set. "
            "Add it to your .env file: SEC_USER_AGENT=\"First Last email@example.com\""
        )

    adapter = HTTPAdapter(max_retries=_URLLIB3_RETRY)

    session = requests.Session()
    session.mount("https://", adapter)   # applies retry logic to all HTTPS calls
    session.headers.update({
        "User-Agent": _USER_AGENT,
        "Accept":     "application/json",
    })

    log.debug("Session created with User-Agent: %s", _USER_AGENT)
    return session


@retry(
    # tenacity retry: application-level safety net on top of urllib3
    # triggered when raise_for_status() throws an HTTPError
    retry=retry_if_exception_type(requests.HTTPError),
    wait=wait_exponential(multiplier=1, min=2, max=60),  # 2s → 4s → 8s … up to 60s
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(log, logging.WARNING),  # log each retry attempt
    reraise=True,                                          # re-raise if all attempts fail
)
def rate_limited_get(session: requests.Session, url: str, timeout: int = 30) -> dict:
    """
    Make a single GET request, enforce the inter-request delay, and return
    the parsed JSON response body.

    Two layers of retry protection:
    1. urllib3 (in the session adapter)  — handles connection-level failures
       and retries on 429/5xx before the response body is read.
    2. tenacity (this decorator)         — handles application-level failures
       such as raise_for_status() throwing after a bad response slips through.

    Args:
        session:  A session created by build_session().
        url:      Full URL to GET.
        timeout:  Seconds to wait for a response before raising Timeout.

    Returns:
        Parsed JSON response as a Python dict.

    Raises:
        requests.HTTPError:  if the server returns a non-2xx status after all retries.
        requests.Timeout:    if the server does not respond within `timeout` seconds.
    """
    log.info("GET %s", url)

    response = session.get(url, timeout=timeout)

    # Raise immediately on 4xx/5xx — tenacity will catch HTTPError and retry
    response.raise_for_status()

    # Enforce rate limit AFTER a successful response so we don't delay retries
    time.sleep(_REQUEST_DELAY_SECONDS)

    return response.json()
