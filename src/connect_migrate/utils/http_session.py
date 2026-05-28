"""Factory for a configured :class:`requests.Session`.

Bundles the per-call kwargs (``verify``, ``auth``) that nearly every HTTP
call in this codebase passes individually. Callers can use the session
directly with the standard requests verbs and skip those kwargs.

No retry adapter is installed — adding retries to PUTs against the
Confluent Cloud APIs would be a behavior change for some non-idempotent
endpoints. Retries can be wired in later behind an explicit opt-in.
"""

from typing import Any, Optional

import requests


def make_http_session(
    disable_ssl_verify: bool = False,
    auth: Optional[Any] = None,
) -> requests.Session:
    session = requests.Session()
    session.verify = not disable_ssl_verify
    if auth is not None:
        session.auth = auth
    return session
