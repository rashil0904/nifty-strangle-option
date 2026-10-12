"""
leg_lifecycle.py
==================

Shared mechanics for short-options strategies via Jainam's XTS Connect API
(common/jainam_client.py's JainamClient): market-hours/scheduling helpers,
retryable client calls, order execution (paper/live), and the stop-loss ->
re-entry state transition.

This module is exchange/index-agnostic on purpose -- it takes `exchange`
as an explicit parameter wherever an instrument key or order needs one,
rather than importing a hardcoded exchange constant from an index-specific
delta model. That's what lets this exact same file be shared, unmodified,
by every index's strategy folder without the underlying mechanics ever
silently diverging between markets.

Used by both delta_strangle_strategy.py and delta_breakdown_strategy.py
(within each index folder) so these mechanics also cannot silently diverge
between the two strategies. Anything strategy-specific (entry trigger
timing, strike-selection rule, stop-loss multiplier, breakout detection)
stays in each strategy's own script and is passed in as a parameter
(quantity, live, multiplier, exchange, etc.) rather than living here.

Every function that logs takes an explicit `logger` argument rather than
using a module-level logger, since each calling script has its own
logging.getLogger(name) writing to its own log file.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from datetime import time as dtime
from typing import Optional

from jainam_client import JainamClient
from broker_errors import BrokerAuthError, BrokerError

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9 fallback
    from backports.zoneinfo import ZoneInfo  # type: ignore

IST = ZoneInfo("Asia/Kolkata")

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

MARKET_OPEN_TIME = dtime(9, 15)
MARKET_CLOSE_TIME = dtime(15, 30)

RETRYABLE_MAX_ATTEMPTS = 3
RETRYABLE_BACKOFF_SECONDS = 2

ORDER_FILL_POLL_INTERVAL_SECONDS = 2
ORDER_FILL_POLL_MAX_ATTEMPTS = 30  # ~60s max wait for a fill confirmation

SLEEP_CHUNK_SECONDS = 60  # cap on any single time.sleep() call while waiting for a future timestamp


# --------------------------------------------------------------------------
# State persistence (path-parameterized so each strategy uses its own file)
# --------------------------------------------------------------------------


def save_state(state: dict, path: str) -> None:
    """Atomically write state to `path` (write to a temp file, then rename)."""
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp_path, path)


def load_state(path: str, logger: logging.Logger) -> Optional[dict]:
    """Load `path` if it exists and is valid JSON; None if no cycle is in progress."""
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Could not read %s (%s) -- treating as no active cycle.", path, exc)
        return None


def clear_state(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)


# --------------------------------------------------------------------------
# Market-hours / scheduling helpers
# --------------------------------------------------------------------------


def is_trading_day(d: date) -> bool:
    """Weekday check only -- no NSE holiday calendar."""
    return d.weekday() < 5


def next_trading_day(d: date) -> date:
    d = d + timedelta(days=1)
    while not is_trading_day(d):
        d += timedelta(days=1)
    return d


def is_market_hours(now: datetime, market_open: dtime = MARKET_OPEN_TIME, market_close: dtime = MARKET_CLOSE_TIME) -> bool:
    if not is_trading_day(now.date()):
        return False
    return market_open <= now.time() <= market_close


def next_market_open(now: datetime, market_open: dtime = MARKET_OPEN_TIME) -> datetime:
    d = now.date()
    if is_trading_day(d) and now.time() < market_open:
        return datetime.combine(d, market_open, tzinfo=IST)
    d = next_trading_day(d)
    return datetime.combine(d, market_open, tzinfo=IST)


def deadline_next_trading_day(from_date: date, deadline_time: dtime) -> datetime:
    """deadline_time IST on the next trading day after from_date."""
    d = next_trading_day(from_date)
    return datetime.combine(d, deadline_time, tzinfo=IST)


def sleep_until(target_dt: datetime, chunk_seconds: int = SLEEP_CHUNK_SECONDS) -> None:
    """Sleep in bounded chunks until target_dt, so a long wait isn't one giant blocking call."""
    while True:
        remaining = (target_dt - datetime.now(IST)).total_seconds()
        if remaining <= 0:
            return
        time.sleep(min(chunk_seconds, remaining))


# --------------------------------------------------------------------------
# Retry wrapper for read-only broker calls (instrument/quote fetches)
# --------------------------------------------------------------------------


def retry_call(
    logger: logging.Logger,
    func,
    *args,
    description: str = "call",
    max_attempts: int = RETRYABLE_MAX_ATTEMPTS,
    backoff_seconds: int = RETRYABLE_BACKOFF_SECONDS,
    **kwargs,
):
    """Retry a delta-model fetch function a few times on transient failure.

    Those functions raise RuntimeError(...) from <original BrokerError> on
    Jainam errors, so the original exception is inspected via __cause__: a
    BrokerAuthError is not retried (auth is broken, retrying can't help),
    but other BrokerError failures are retried with backoff.
    """
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except RuntimeError as exc:
            if isinstance(exc.__cause__, BrokerAuthError):
                raise
            last_exc = exc
            logger.warning("%s failed (attempt %d/%d): %s", description, attempt, max_attempts, exc)
            if attempt < max_attempts:
                time.sleep(backoff_seconds)
    raise RuntimeError(f"{description} failed after {max_attempts} attempts: {last_exc}")


def fetch_ltp(
    client: JainamClient,
    tradingsymbol: str,
    logger: logging.Logger,
    exchange: str,
    max_attempts: int = RETRYABLE_MAX_ATTEMPTS,
    backoff_seconds: int = RETRYABLE_BACKOFF_SECONDS,
) -> float:
    """Fetch last traded price for one tradingsymbol on `exchange`, with retry on transient failures."""
    key = f"{exchange}:{tradingsymbol}"
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            result = client.ltp([key])
            return float(result[key]["last_price"])
        except BrokerAuthError:
            raise
        except BrokerError as exc:
            last_exc = exc
            logger.warning("ltp(%s) failed (attempt %d/%d): %s", tradingsymbol, attempt, max_attempts, exc)
            if attempt < max_attempts:
                time.sleep(backoff_seconds)
    raise RuntimeError(f"Failed to fetch LTP for {tradingsymbol} after {max_attempts} attempts: {last_exc}")


# --------------------------------------------------------------------------
# Order execution
# --------------------------------------------------------------------------


def fetch_fill_price(
    client: JainamClient,
    order_id: str,
    logger: logging.Logger,
    poll_interval: int = ORDER_FILL_POLL_INTERVAL_SECONDS,
    max_attempts: int = ORDER_FILL_POLL_MAX_ATTEMPTS,
) -> float:
    """Poll order_history(order_id) until COMPLETE and return average_price.

    Raises RuntimeError (never silently guesses) if the order is
    REJECTED/CANCELLED, or doesn't reach COMPLETE within the poll budget.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            history = client.order_history(order_id)
        except BrokerError as exc:
            logger.warning(
                "order_history(%s) failed (attempt %d/%d): %s", order_id, attempt, max_attempts, exc
            )
            time.sleep(poll_interval)
            continue

        if history:
            latest = history[-1]
            status = latest.get("status")
            if status == "COMPLETE":
                return float(latest["average_price"])
            if status in ("REJECTED", "CANCELLED"):
                msg = f"Order {order_id} ended with status {status}: {latest.get('status_message')}"
                logger.critical(msg)
                raise RuntimeError(msg)
        time.sleep(poll_interval)

    msg = (
        f"Order {order_id} did not reach COMPLETE status within "
        f"{max_attempts * poll_interval}s -- cannot confirm fill."
    )
    logger.critical(msg)
    raise RuntimeError(msg)


def execute_order(
    client: JainamClient,
    logger: logging.Logger,
    tradingsymbol: str,
    transaction_type: str,
    quantity: int,
    live: bool,
    exchange: str,
) -> float:
    """Place (live) or simulate (paper) a NRML market order on `exchange`; return the fill price.

    product=NRML, not MIS -- these positions are explicitly held overnight,
    and MIS positions are auto-squared-off by the broker near end-of-day,
    which would silently break either strategy.

    Never retried on failure: a failed/ambiguous order is logged CRITICAL
    and raised rather than blindly retried (which could risk a duplicate
    order).
    """
    if not live:
        simulated_price = fetch_ltp(client, tradingsymbol, logger, exchange)
        logger.info(
            "[PAPER] %s %d x %s @ ~%.2f (simulated market fill, no real order placed)",
            transaction_type, quantity, tradingsymbol, simulated_price,
        )
        return simulated_price

    try:
        order_id = client.place_order(
            variety=client.VARIETY_REGULAR,
            exchange=exchange,
            tradingsymbol=tradingsymbol,
            transaction_type=transaction_type,
            quantity=quantity,
            product=client.PRODUCT_NRML,
            order_type=client.ORDER_TYPE_MARKET,
        )
    except BrokerError as exc:
        logger.critical(
            "ORDER PLACEMENT FAILED: %s %d x %s -- %s. Halting automation for this leg; "
            "manual intervention required.",
            transaction_type, quantity, tradingsymbol, exc,
        )
        raise RuntimeError(f"Order placement failed for {tradingsymbol} ({transaction_type}): {exc}") from exc

    logger.info(
        "Order placed: %s %d x %s -- order_id=%s. Waiting for fill confirmation...",
        transaction_type, quantity, tradingsymbol, order_id,
    )
    fill_price = fetch_fill_price(client, order_id, logger)
    logger.info(
        "Order %s CONFIRMED FILLED: %s %d x %s @ %.2f", order_id, transaction_type, quantity, tradingsymbol, fill_price
    )
    return fill_price


# --------------------------------------------------------------------------
# Stop-loss / re-entry (identical mechanics across strategies; only the
# stop-loss trigger threshold differs, and that check is made by the caller
# before invoking handle_stop_loss)
# --------------------------------------------------------------------------


def handle_stop_loss(
    client: JainamClient, logger: logging.Logger, leg_key: str, leg: dict, quantity: int, live: bool, exchange: str
) -> dict:
    """Square off (BUY) a leg that hit its stop-loss; mark it watching_for_reentry.

    entry_price is intentionally NOT overwritten here -- it stays as the
    level to watch for re-entry.
    """
    exit_price = execute_order(client, logger, leg["tradingsymbol"], "BUY", quantity, live, exchange)
    pnl = (leg["entry_price"] - exit_price) * quantity
    leg["realized_pnl"] = leg.get("realized_pnl", 0.0) + pnl
    leg["stop_count"] = leg.get("stop_count", 0) + 1
    leg["status"] = "watching_for_reentry"
    leg.setdefault("history", []).append(
        {
            "type": "stop_loss",
            "tradingsymbol": leg["tradingsymbol"],
            "exit_price": exit_price,
            "entry_price_at_stop": leg["entry_price"],
            "pnl": pnl,
            "timestamp": datetime.now(IST).isoformat(),
        }
    )
    logger.info(
        "STOP-LOSS: %s %s bought back @ %.2f (entry was %.2f, segment P&L %+.2f). "
        "Now watching for re-entry <= %.2f.",
        leg_key, leg["tradingsymbol"], exit_price, leg["entry_price"], pnl,
        leg["entry_price"],
    )
    return leg


def handle_reentry(
    client: JainamClient, logger: logging.Logger, leg_key: str, leg: dict, quantity: int, live: bool, exchange: str
) -> dict:
    """Re-sell a flat, watching leg at market; the new fill becomes its new entry_price."""
    new_entry_price = execute_order(client, logger, leg["tradingsymbol"], "SELL", quantity, live, exchange)
    leg["entry_price"] = new_entry_price
    leg["reentry_count"] = leg.get("reentry_count", 0) + 1
    leg["status"] = "open"
    leg.setdefault("history", []).append(
        {
            "type": "reentry",
            "tradingsymbol": leg["tradingsymbol"],
            "entry_price": new_entry_price,
            "reentry_count": leg["reentry_count"],
            "timestamp": datetime.now(IST).isoformat(),
        }
    )
    logger.info(
        "RE-ENTRY #%d: %s %s sold @ %.2f.", leg["reentry_count"], leg_key, leg["tradingsymbol"], new_entry_price
    )
    return leg


def apply_stop_loss_or_reentry(
    client: JainamClient,
    logger: logging.Logger,
    leg_key: str,
    leg: dict,
    ltp: float,
    quantity: int,
    live: bool,
    stop_loss_multiplier: float,
    max_reentries: Optional[int],
    exchange: str,
) -> dict:
    """Shared decision + action step used by any strategy once a leg is 'open'
    or 'watching_for_reentry': apply the stop-loss check (entry_price *
    stop_loss_multiplier) or the re-entry check (ltp <= entry_price, capped at
    max_reentries), leaving the leg untouched if neither condition is met.

    max_reentries is enforced against leg["reentry_count"] as persisted in
    state (reloaded on restart), not an in-memory counter -- so the cap holds
    correctly even if the script is restarted mid-cycle.
    """
    if leg["status"] == "open" and ltp >= leg["entry_price"] * stop_loss_multiplier:
        leg = handle_stop_loss(client, logger, leg_key, leg, quantity, live, exchange)
    elif leg["status"] == "watching_for_reentry" and ltp <= leg["entry_price"]:
        reentry_count = leg.get("reentry_count", 0)
        if max_reentries is not None and reentry_count >= max_reentries:
            logger.info(
                "%s leg reached max_reentries=%d (reentry_count=%d) -- staying flat for the rest of this cycle.",
                leg_key, max_reentries, reentry_count,
            )
            leg["status"] = "capped_flat"
        else:
            leg = handle_reentry(client, logger, leg_key, leg, quantity, live, exchange)
    return leg


# --------------------------------------------------------------------------
# Live-mode warning
# --------------------------------------------------------------------------


def print_live_warning_banner() -> None:
    print("!" * 78)
    print("!!!  LIVE TRADING MODE -- REAL ORDERS WILL BE PLACED ON YOUR JAINAM ACCOUNT  !!!")
    print("!" * 78)
