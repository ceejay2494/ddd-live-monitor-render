"""OddAlerts read-only API client. Keep ODDALERTS_TOKEN in Render environment.

API documentation: https://data.oddalerts.com/llms.txt
This module is isolated from the existing API-Football scanner.
"""
import os
from urllib.parse import urlparse
import requests

BASE = "https://data.oddalerts.com/api"
TIMEOUT = 25


class OddAlertsError(RuntimeError):
    pass


def configured():
    return bool(os.environ.get("ODDALERTS_TOKEN", "").strip())


def fetch_fixtures(feed="upcoming", from_unix=None, to_unix=None,
                   max_pages=20, include="probability,odds",
                   bookmakers="pinnacle,bet365"):
    """Fetch complete pages of fixtures; return data plus audit metadata.

    feed: upcoming, live, between. 'between' requires UNIX-second bounds.
    """
    token = os.environ.get("ODDALERTS_TOKEN", "").strip()
    if not token:
        raise OddAlertsError("ODDALERTS_TOKEN is not configured")
    if feed not in ("upcoming", "live", "between"):
        raise ValueError("Unsupported feed")
    if feed == "between" and (from_unix is None or to_unix is None):
        raise ValueError("between requires UNIX-second from/to")
    max_pages = max(1, min(int(max_pages), 100))
    params = {"api_token": token, "per_page": 250, "include": include}
    if "odds" in include:
        params["bookmakers"] = bookmakers
    if from_unix is not None:
        params["from"] = int(from_unix)
    if to_unix is not None:
        params["to"] = int(to_unix)
    url = BASE + "/fixtures/" + feed
    data = []
    page_count = 0
    seen_urls = set()
    while url and page_count < max_pages:
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.netloc != "data.oddalerts.com" or not parsed.path.startswith("/api/"):
            raise OddAlertsError("Unsafe pagination URL")
        if url in seen_urls:
            raise OddAlertsError("Pagination loop")
        seen_urls.add(url)
        try:
            response = requests.get(url, params=params, timeout=TIMEOUT,
                                    allow_redirects=False)
        except requests.RequestException as exc:
            raise OddAlertsError("OddAlerts network error") from exc
        if response.status_code in (301, 302, 303, 307, 308, 401, 403):
            raise OddAlertsError("OddAlerts token or API subscription rejected")
        if response.status_code == 429:
            raise OddAlertsError("OddAlerts rate limit exceeded")
        if response.status_code != 200:
            raise OddAlertsError("OddAlerts HTTP " + str(response.status_code))
        try:
            payload = response.json()
        except ValueError as exc:
            raise OddAlertsError("OddAlerts returned invalid JSON") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            raise OddAlertsError("Unexpected OddAlerts response")
        data.extend(payload["data"])
        page_count += 1
        info = payload.get("info") or {}
        url = info.get("next_page_url") or None
        # Next-page URLs already include the original query string.
        params = None
    return {"provider": "OddAlerts", "feed": feed, "pages_fetched": page_count,
            "count": len(data), "truncated": bool(url), "data": data}
