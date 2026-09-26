"""
broker_client.py
==================

Broker selection dispatcher. Every strategy script and delta model already
builds its Kite client via its own get_kite_client() (unchanged, still
Kite-specific -- these already-verified daily-session-cache functions stay
where they are rather than being refactored solely for this feature). This
module just adds the second option: `--broker jainam` builds a JainamClient
instead, giving every caller a single decision point instead of a
per-broker copy of each script.

get_broker_client() takes get_kite_client_fn as a parameter (rather than
importing nifty_delta_model.py or sensex_delta_model.py directly) so this
module has no dependency on either index's delta model -- it stays exactly
as broker-agnostic as leg_lifecycle.py already is index-agnostic.
"""

from __future__ import annotations

from typing import Callable

from jainam_client import JainamClient

SUPPORTED_BROKERS = ("kite", "jainam")


def get_broker_client(broker: str, get_kite_client_fn: Callable):
    """Return a Kite-shaped broker client for `broker` ("kite" or "jainam").

    get_kite_client_fn is the caller's own get_kite_client (env vars first,
    falling back to the daily session cache written by
    common/kite_login_helper.py). Jainam needs no equivalent cache -- its
    login is a direct, non-interactive appKey/secretKey POST, so
    JainamClient.from_env() logs in fresh every call.
    """
    if broker == "kite":
        return get_kite_client_fn()
    if broker == "jainam":
        return JainamClient.from_env()
    raise ValueError(f"Unknown broker {broker!r} -- expected one of {SUPPORTED_BROKERS}.")
