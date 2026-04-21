"""
Extensiv Integration Manager (IM) / CartRover API client.

Minimal, dependency-free Python client for pulling orders from Nok's
"nok IM (Order Source to OM)" integration.

Supports two access levels:

- `cart`     -> /v1/cart/...     (one specific cart, e.g. a dev/test source)
- `merchant` -> /v1/merchant/... (ALL carts under a merchant — what you want
                                  for the full live production feed)

Usage:
    from client import ExtensivIM

    # Cart-level (reads IM_USER / IM_KEY from environment)
    im_cart = ExtensivIM.from_env(level="cart")

    # Merchant-level (reads IM_MERCHANT_USER / IM_MERCHANT_KEY)
    im = ExtensivIM.from_env(level="merchant")

    # Or pass creds directly
    im = ExtensivIM(api_user="...", api_key="...", level="merchant")

    orders = im.list_orders(status="any", from_date="2026-03-18T00:00:00+00:00")
    all_orders = im.list_all_orders(status="any", from_date="2026-03-18T00:00:00+00:00")
    order = im.get_order("NOK-DEV-XYZ")
    status = im.get_order_status("NOK-DEV-XYZ")

The two levels share the same auth (HTTP Basic), the same response envelope,
and the same order schema — only the URL path prefix differs.
"""

import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterator, List, Optional

BASE_URL = "https://api.cartrover.com"

VALID_STATUSES = {
    "new",
    "at_wms",
    "shipped",
    "confirmed",
    "error",
    "canceled",
    "new_or_at_wms",
    "shipped_or_confirmed",
    "any",
}

VALID_LEVELS = {"cart", "merchant"}

# Env var names per level. Kept separate so you can keep both key pairs
# loaded simultaneously and switch with the `level` parameter.
_ENV_VARS = {
    "cart":     ("IM_USER",          "IM_KEY"),
    "merchant": ("IM_MERCHANT_USER", "IM_MERCHANT_KEY"),
}


class ExtensivIMError(Exception):
    """Raised for any non-transient API error."""


class RateLimitError(ExtensivIMError):
    """Raised when the API responds with RateLimit — usually worth a backoff + retry."""


class ExtensivIM:
    """
    Thin client around the CartRover Cart API.

    Auth is HTTP Basic — `api_user:api_key` base64-encoded. We reuse a single
    Authorization header across calls.

    The `level` parameter determines which API level we're hitting:
    - "cart"     -> /v1/cart/orders/...
    - "merchant" -> /v1/merchant/orders/...
    """

    def __init__(
        self,
        api_user: str,
        api_key: str,
        level: str = "cart",
        base_url: str = BASE_URL,
        timeout: int = 30,
        max_retries: int = 3,
    ):
        if not api_user or not api_key:
            raise ValueError("api_user and api_key are required")
        if level not in VALID_LEVELS:
            raise ValueError(f"level must be one of {sorted(VALID_LEVELS)}, got {level!r}")
        self.api_user = api_user
        self.api_key = api_key
        self.level = level
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries

        token = base64.b64encode(f"{api_user}:{api_key}".encode()).decode()
        self._auth_header = f"Basic {token}"

        # Track the most recent process ID — handy for support tickets.
        self.last_process_id: Optional[str] = None
        self.last_rate_remaining: Optional[int] = None

    @classmethod
    def from_env(
        cls,
        level: str = "cart",
        user_var: Optional[str] = None,
        key_var: Optional[str] = None,
    ) -> "ExtensivIM":
        """
        Build a client from environment variables.

        By default, uses the standard env-var pair for the requested level:
          cart     -> IM_USER / IM_KEY
          merchant -> IM_MERCHANT_USER / IM_MERCHANT_KEY

        Pass `user_var` / `key_var` explicitly to override. We prefer env vars
        over hardcoded values so keys don't leak into files under mnt/outputs/
        or into committed code.
        """
        if level not in VALID_LEVELS:
            raise ValueError(f"level must be one of {sorted(VALID_LEVELS)}, got {level!r}")
        default_user, default_key = _ENV_VARS[level]
        user_var = user_var or default_user
        key_var = key_var or default_key
        try:
            api_user = os.environ[user_var]
            api_key = os.environ[key_var]
        except KeyError as e:
            raise ExtensivIMError(
                f"Missing environment variable {e.args[0]}. "
                f"Set both {user_var} and {key_var} before running, "
                f"or recall them from AutoMem."
            ) from e
        return cls(api_user=api_user, api_key=api_key, level=level)

    # ------------------------------------------------------------------
    # Path helper
    # ------------------------------------------------------------------

    def _path(self, suffix: str) -> str:
        """Build a versioned URL path for the current level, e.g. '/v1/cart/orders/list/any'."""
        return f"/v1/{self.level}/orders{suffix}"

    # ------------------------------------------------------------------
    # Core HTTP plumbing
    # ------------------------------------------------------------------

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                 body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Issue an HTTP request, handle auth, percent-encoding, rate-limit retries.

        Returns the decoded JSON body. Raises ExtensivIMError on non-rate-limit
        failures and RateLimitError if we exhaust retries against the bucket.
        """
        url = f"{self.base_url}{path}"
        if params:
            # urlencode handles the `+` → `%2B` problem in timestamps for us.
            url = f"{url}?{urllib.parse.urlencode(params)}"

        data = None
        headers = {
            "Authorization": self._auth_header,
            "Accept": "application/json",
        }
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    raw = resp.read().decode("utf-8")
                    self.last_process_id = resp.headers.get("X-CartRover-Process-ID")
                    hits = resp.headers.get("X-CartRover-Api-Minute-Hits-Remaining")
                    if hits is not None:
                        try:
                            self.last_rate_remaining = int(hits)
                        except ValueError:
                            pass
                    payload = json.loads(raw) if raw else {}
            except urllib.error.HTTPError as e:
                # HTTPError is itself a file-like object — read the body for detail.
                body_text = e.read().decode("utf-8", errors="replace")
                last_err = ExtensivIMError(
                    f"HTTP {e.code} {e.reason} on {method} {path}: {body_text}"
                )
                # 5xx is worth retrying; 4xx usually isn't.
                if 500 <= e.code < 600 and attempt < self.max_retries:
                    time.sleep(1.0 * (attempt + 1))
                    continue
                raise last_err
            except urllib.error.URLError as e:
                last_err = ExtensivIMError(f"Network error on {method} {path}: {e}")
                if attempt < self.max_retries:
                    time.sleep(1.0 * (attempt + 1))
                    continue
                raise last_err

            # CartRover returns 200 + error_code=RateLimit when you've been throttled.
            if isinstance(payload, dict) and payload.get("error_code") == "RateLimit":
                if attempt < self.max_retries:
                    # 0.6s is the refill rate; sleep a little longer to be polite.
                    time.sleep(1.0 * (attempt + 1))
                    continue
                raise RateLimitError(
                    f"Rate limited after {self.max_retries + 1} attempts on {path}"
                )

            return payload

        # We shouldn't get here, but mypy wants a return.
        if last_err:
            raise last_err
        raise ExtensivIMError(f"Unknown failure on {method} {path}")

    # ------------------------------------------------------------------
    # Order endpoints (path prefix depends on self.level)
    # ------------------------------------------------------------------

    def list_orders(
        self,
        status: str = "any",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        limit: int = 100,
        page: int = 1,
    ) -> List[Dict[str, Any]]:
        """
        GET /v1/{level}/orders/list/{status}

        Returns a list of order dicts (never more than `limit`, max 100).
        Dates should be ISO 8601 with timezone, e.g. "2026-04-17T00:00:00+00:00".
        The meaning of from_date/to_date depends on the status — see
        references/api-reference.md.

        Important: for `status="any"` at the merchant level, always supply
        `from_date` (and ideally `to_date`) to keep response time reasonable.
        An unbounded 'any' query can time out.
        """
        if status not in VALID_STATUSES:
            raise ValueError(
                f"Invalid status {status!r}. Must be one of {sorted(VALID_STATUSES)}."
            )
        if limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100 (API hard cap)")
        params: Dict[str, Any] = {"limit": limit, "page": page}
        if from_date:
            params["from_date"] = from_date
        if to_date:
            params["to_date"] = to_date

        payload = self._request("GET", self._path(f"/list/{status}"), params=params)
        if isinstance(payload, dict):
            return payload.get("response") or payload.get("orders") or []
        return payload if isinstance(payload, list) else []

    def list_all_orders(
        self,
        status: str = "any",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
        polite_delay: float = 0.7,
    ) -> List[Dict[str, Any]]:
        """
        Convenience wrapper that pages through `list_orders` until exhausted.

        Yields one combined list. For very large windows, prefer `iter_all_orders`
        or narrow your date range — this keeps everything in memory at once.
        """
        all_orders: List[Dict[str, Any]] = []
        for chunk in self.iter_all_orders(
            status=status, from_date=from_date, to_date=to_date,
            page_size=page_size, max_pages=max_pages, polite_delay=polite_delay,
        ):
            all_orders.extend(chunk)
        return all_orders

    def iter_all_orders(
        self,
        status: str = "any",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
        polite_delay: float = 0.7,
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Generator version of list_all_orders — yields one page at a time so you can
        stream to disk without blowing memory. Sleeps `polite_delay` seconds
        between pages to stay well under the 100/minute rate limit.
        """
        page = 1
        while True:
            chunk = self.list_orders(
                status=status, from_date=from_date, to_date=to_date,
                limit=page_size, page=page,
            )
            if not chunk:
                return
            yield chunk
            if len(chunk) < page_size:
                return
            page += 1
            if max_pages and page > max_pages:
                return
            if polite_delay:
                time.sleep(polite_delay)

    def get_order(self, cust_ref: str) -> Dict[str, Any]:
        """
        GET /v1/{level}/orders/{cust_ref}

        Fetch one full order by its CartRover-unique cust_ref.
        """
        if not cust_ref:
            raise ValueError("cust_ref is required")
        payload = self._request(
            "GET",
            self._path(f"/{urllib.parse.quote(cust_ref, safe='')}"),
        )
        if isinstance(payload, dict) and "response" in payload:
            return payload["response"]
        return payload

    def get_order_status(self, cust_ref: str) -> Dict[str, Any]:
        """
        GET /v1/{level}/orders/status/{cust_ref}

        Returns status + shipment/tracking data. Lighter-weight than `get_order`.
        """
        if not cust_ref:
            raise ValueError("cust_ref is required")
        return self._request(
            "GET",
            self._path(f"/status/{urllib.parse.quote(cust_ref, safe='')}"),
        )

    def push_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v1/cart/orders/cartrover

        Push a brand-new order into IM. Only available at the Cart level.

        Nok's IM receives orders from connected marketplaces automatically, so
        this is rarely the right endpoint for us. Kept here for completeness
        and for edge cases like backfilling a failed ingestion.
        """
        if self.level != "cart":
            raise ExtensivIMError(
                "push_order is only available at the Cart level. "
                "Re-instantiate with level='cart'."
            )
        return self._request("POST", "/v1/cart/orders/cartrover", body=order)
