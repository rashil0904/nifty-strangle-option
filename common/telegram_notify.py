"""
telegram_notify.py
====================

Best-effort trade notifications via Telegram's Bot API. Used by every
strategy script (through leg_lifecycle.py for the shared stop-loss/re-entry
events, and directly for strategy-specific events like contract selection,
tracked_low freezing, and entry/exit) to post a human-readable message for
every state-changing trade event -- never for every poll tick.

Never raises and never blocks trading on failure: a missing/invalid token,
a network error, or any other failure just logs a warning (if a logger is
given) and returns. Notifications are a convenience layer on top of the
existing file logging, not a dependency live trading can fail on.

Configuration: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment
variables. If either is unset, send() silently no-ops (after one warning
per process, not one per call, so a missing config doesn't spam the log).
"""

from __future__ import annotations

import logging
import os

import requests

_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
_REQUEST_TIMEOUT_SECONDS = 10

_warned_missing_config = False


def send(message: str, logger: logging.Logger | None = None) -> None:
    """Post `message` to the configured Telegram chat. Best-effort: any
    failure (missing config, network error, bad response) is swallowed and
    only logged as a warning, never raised."""
    global _warned_missing_config

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        if not _warned_missing_config:
            _warned_missing_config = True
            if logger:
                logger.warning(
                    "TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set -- Telegram notifications disabled for this process."
                )
        return

    try:
        response = requests.post(
            _TELEGRAM_API.format(token=token),
            json={"chat_id": chat_id, "text": message},
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code != 200:
            if logger:
                logger.warning("Telegram notification failed (HTTP %d): %s", response.status_code, response.text[:300])
    except requests.RequestException as exc:
        if logger:
            logger.warning("Telegram notification failed: %s", exc)
