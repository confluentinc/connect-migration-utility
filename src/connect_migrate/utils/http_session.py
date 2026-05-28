"""Factory for a configured :class:`requests.Session`.

Bundles the per-call kwargs (``verify``, ``auth``) that nearly every HTTP
call in this codebase passes individually, plus an optional retry adapter
with exponential backoff for transient failures.

``requests`` has no session-level timeout, so callers still pass ``timeout=``
on each request — use :data:`DEFAULT_HTTP_TIMEOUT` so the value is the same
everywhere. The timeout is a ``(connect, read)`` tuple in seconds.

By default, retries are limited to the urllib3-default safe methods
(``GET``/``HEAD``/``OPTIONS``). Callers can widen ``retry_on_methods`` when
their target endpoint is known to be idempotent (e.g. CC's ``/translate``
and ``/config/validate`` PUTs, which are read-shaped server-side).
"""

from typing import Any, Iterable, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_HTTP_TIMEOUT = (5, 30)  # (connect, read) seconds


def make_http_session(
    *,
    disable_ssl_verify: bool = False,
    auth: Optional[Any] = None,
    retries: int = 2,
    backoff_factor: float = 0.5,
    status_forcelist: Iterable[int] = (500, 502, 503, 504),
    retry_on_methods: Optional[Iterable[str]] = None,
) -> requests.Session:
    """Build a configured :class:`requests.Session`.

    Args:
        disable_ssl_verify: If True, set ``session.verify = False``.
        auth: Stored on the session and applied to every request.
        retries: Total retry attempts for connection errors and ``status_forcelist`` responses.
        backoff_factor: Exponential backoff seed (urllib3 ``Retry`` semantics).
        status_forcelist: HTTP statuses that trigger a retry.
        retry_on_methods: HTTP methods to retry. ``None`` uses urllib3's safe-method default.
    """
    session = requests.Session()
    session.verify = not disable_ssl_verify
    if auth is not None:
        session.auth = auth

    retry_kwargs: dict = {
        "total": retries,
        "connect": retries,
        "read": retries,
        "backoff_factor": backoff_factor,
        "status_forcelist": tuple(status_forcelist),
        "raise_on_status": False,
    }
    if retry_on_methods is not None:
        retry_kwargs["allowed_methods"] = frozenset(m.upper() for m in retry_on_methods)
    retry = Retry(**retry_kwargs)

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session