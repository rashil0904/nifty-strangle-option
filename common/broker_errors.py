"""
broker_errors.py
==================

Error types for jainam_client.py's failures, so leg_lifecycle.py and the
delta models can catch "a broker call failed" / "a broker call failed due
to auth" via one consistent vocabulary rather than reaching into requests'
or XTS's own exception/response shapes directly.

jainam_client.py wraps its own transport-level errors into these before
they reach shared code, so retry_call()'s isinstance(exc.__cause__, ...)
auth-check and the various `except BrokerError` clauses across the
codebase treat every failure the same way: BrokerAuthError is never
retried (the credentials are broken, retrying can't help), everything else
(BrokerError) is retried with backoff.
"""

from __future__ import annotations


class BrokerError(Exception):
    """A broker API call failed (network, rejected request, bad response shape)."""


class BrokerAuthError(BrokerError):
    """A broker API call failed due to invalid/expired credentials -- not retryable."""
