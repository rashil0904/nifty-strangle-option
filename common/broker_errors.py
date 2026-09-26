"""
broker_errors.py
==================

Broker-agnostic error types, so leg_lifecycle.py and the delta models can
catch "a broker call failed" / "a broker call failed due to auth" without
hardcoding kiteconnect's exception classes -- those are Kite-specific and
don't cover a second broker's failure modes.

Every broker adapter (Kite is used directly; Jainam via jainam_client.py)
wraps its own transport-level errors into these before they reach shared
code, so retry_call()'s isinstance(exc.__cause__, ...) auth-check and the
various `except (..., KiteException, ...)` clauses across the codebase can
treat any broker's failures the same way: BrokerAuthError is never retried
(the credentials are broken, retrying can't help), everything else
(BrokerError) is retried with backoff like NetworkException/KiteException
already are.
"""

from __future__ import annotations


class BrokerError(Exception):
    """A broker API call failed (network, rejected request, bad response shape)."""


class BrokerAuthError(BrokerError):
    """A broker API call failed due to invalid/expired credentials -- not retryable."""
