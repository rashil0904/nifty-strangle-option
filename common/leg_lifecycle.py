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
import math
import os
import time
from datetime import date, datetime, timedelta
from datetime import time as dtime
from typing import Optional

from jainam_client import JainamClient
from broker_errors import BrokerAuthError, BrokerError
import telegram_notify

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


# --------------------------------------------------------------------------
# Broker-side (resting) stop-loss
#
# Live mode only. After a short leg is sold, a BUY stop-market is parked at the
# broker at entry * multiplier so the stop works even if this process or the PC
# dies. It is a DAY order, so it expires at the close and is re-placed on the
# next session's first poll. The software stop (polling LTP) remains as the
# fallback whenever no resting order exists.
# --------------------------------------------------------------------------

STOP_TICK = 0.05
STOP_LIMIT_BUFFER = 1.03  # resting stop is a stop-LIMIT (stop-market is not permitted on this account): limit = trigger * this
MAX_STOP_ARM_FAILURES = 3
SOFTWARE_FALLBACK_BUFFER = 1.05  # resting stop still unfilled with LTP this far beyond its trigger -> close at market


def stop_trigger_price(entry_price: float, multiplier: float) -> float:
    return round(math.ceil(entry_price * multiplier / STOP_TICK - 1e-9) * STOP_TICK, 2)


def arm_stop(
    client: JainamClient,
    logger: logging.Logger,
    leg_key: str,
    leg: dict,
    ltp: float,
    quantity: int,
    live: bool,
    multiplier: float,
    exchange: str,
) -> dict:
    """Park a BUY stop-market at entry * multiplier for an open short leg (stop-limit; no-op in paper mode,
    if one is already resting, if arming has failed repeatedly, or if the market is already
    through the trigger -- the software stop closes that case at market)."""
    if not live or leg.get("sl_order_id") or leg.get("sl_arm_failures", 0) >= MAX_STOP_ARM_FAILURES:
        return leg
    trigger = stop_trigger_price(leg["entry_price"], multiplier)
    if ltp >= trigger:
        return leg
    limit = stop_trigger_price(trigger, STOP_LIMIT_BUFFER)
    try:
        order_id = client.place_order(
            variety=client.VARIETY_REGULAR,
            exchange=exchange,
            tradingsymbol=leg["tradingsymbol"],
            transaction_type="BUY",
            quantity=quantity,
            product=client.PRODUCT_NRML,
            order_type=client.ORDER_TYPE_STOP_LIMIT,
            stop_price=trigger,
            limit_price=limit,
        )
    except BrokerError as exc:
        leg["sl_arm_failures"] = leg.get("sl_arm_failures", 0) + 1
        logger.warning(
            "Could not place resting stop for %s %s @ %.2f (failure %d/%d): %s -- software stop remains active.",
            leg_key, leg["tradingsymbol"], trigger, leg["sl_arm_failures"], MAX_STOP_ARM_FAILURES, exc,
        )
        return leg
    leg["sl_order_id"] = str(order_id)
    leg["sl_price"] = trigger
    leg["sl_placed_at"] = datetime.now(IST).isoformat()
    logger.info(
        "STOP ORDER PLACED: %s %s BUY stop-limit trigger %.2f / limit %.2f (entry %.2f x %.2f) -- order_id=%s",
        leg_key, leg["tradingsymbol"], trigger, limit, leg["entry_price"], multiplier, order_id,
    )
    return leg


def record_stop_loss(
    logger: logging.Logger, leg_key: str, leg: dict, quantity: int, exit_price: float, live: bool = True
) -> dict:
    """Book a stop-out at exit_price: P&L, counters, and flip the leg to watching_for_reentry."""
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
        leg_key, leg["tradingsymbol"], exit_price, leg["entry_price"], pnl, leg["entry_price"],
    )
    telegram_notify.send(
        f"[{'LIVE' if live else 'PAPER'}] [{logger.name}] STOP-LOSS\n"
        f"{leg_key.upper()}: {leg['tradingsymbol']}\n"
        f"Bought back @ {exit_price:.2f} (entry was {leg['entry_price']:.2f})\n"
        f"Now watching for re-entry <= {leg['entry_price']:.2f}",
        logger,
    )
    return leg


def _order_unknown(exc: Exception) -> bool:
    """True if the broker says it has no such order (e.g. a previous day's DAY stop, purged after the close)."""
    text = str(exc)
    return "e-orders-0001" in text or "not found" in text.lower()


def _stop_is_old(leg: dict, min_age_seconds: int = 120, key: str = "sl_placed_at") -> bool:
    """A 'not found' answer only means 'expired' if the order was placed a while ago (a just-placed order can
    be briefly unknown to the order-history API)."""
    placed = leg.get(key)
    if not placed:
        return True
    try:
        return (datetime.now(IST) - datetime.fromisoformat(placed)).total_seconds() > min_age_seconds
    except ValueError:
        return True


def check_broker_stop(client: JainamClient, logger: logging.Logger, leg_key: str, leg: dict, quantity: int) -> bool:
    """Look at the resting stop. True iff it has FILLED (the leg is then booked as stopped out).
    A cancelled/expired stop is simply forgotten so it can be re-placed; a rejected one counts
    as an arming failure."""
    order_id = leg.get("sl_order_id")
    if not order_id:
        return False
    try:
        history = client.order_history(order_id)
    except BrokerError as exc:
        if _order_unknown(exc) and _stop_is_old(leg):
            logger.warning(
                "Resting stop %s for %s is unknown to the broker (expired) -- forgetting it so it is re-placed.", order_id, leg_key
            )
            leg["sl_order_id"] = None
            return False
        logger.warning("order_history(%s) for %s stop failed: %s", order_id, leg_key, exc)
        return False
    if not history:
        return False
    latest = history[-1]
    status = latest.get("status")
    if status == "COMPLETE":
        leg["sl_order_id"] = None
        record_stop_loss(logger, leg_key, leg, quantity, float(latest["average_price"]), live=True)
        return True
    if status in ("REJECTED", "CANCELLED"):
        leg["sl_order_id"] = None
        if status == "REJECTED":
            leg["sl_arm_failures"] = leg.get("sl_arm_failures", 0) + 1
        logger.warning(
            "Resting stop %s for %s ended %s (%s) -- %s.",
            order_id, leg_key, status, latest.get("status_message"),
            "will be re-placed" if status == "CANCELLED" else "software stop covers it",
        )
    return False


def cancel_stop(client: JainamClient, logger: logging.Logger, leg_key: str, leg: dict, quantity: int) -> bool:
    """Cancel the resting stop before this process closes the leg itself. Returns True if the stop had
    already FILLED (leg now booked as stopped out -- the caller must NOT send another BUY). Raises
    RuntimeError if the stop can neither be confirmed filled nor cancelled."""
    order_id = leg.get("sl_order_id")
    if not order_id:
        return False
    if check_broker_stop(client, logger, leg_key, leg, quantity):
        return True
    if not leg.get("sl_order_id"):
        return False  # the stop had already expired/been cancelled (check_broker_stop forgot it) -- nothing to cancel
    try:
        client.cancel_order(order_id)
    except BrokerError as exc:
        if check_broker_stop(client, logger, leg_key, leg, quantity):
            return True
        if _order_unknown(exc) and _stop_is_old(leg):
            leg["sl_order_id"] = None
            logger.warning("Resting stop %s for %s no longer exists at the broker -- treating as already gone.", order_id, leg_key)
            return False
        raise RuntimeError(f"Could not cancel resting stop {order_id} for {leg_key}: {exc}") from exc
    if check_broker_stop(client, logger, leg_key, leg, quantity):
        return True
    leg["sl_order_id"] = None
    logger.info("Resting stop %s for %s cancelled.", order_id, leg_key)
    return False


# --------------------------------------------------------------------------
# Broker-side (resting) ENTRY orders -- live mode, only when RESTING_ENTRY_ORDERS is switched on
# (--resting-entries). A re-entry ("sell when price falls back to the old entry") and a breakout entry
# ("sell when price falls below the frozen low") are both "sell once price falls to X", so a SELL stop-limit
# is parked at X instead of reacting to a 15-second poll with a market order. The software path stays as the
# fallback: no resting order (placement failed / price already through X) -> market order as before, and a
# resting order the market has run away from (LTP below its limit) is cancelled and replaced by a market SELL.
# --------------------------------------------------------------------------

RESTING_ENTRY_ORDERS = False
ENTRY_LIMIT_BUFFER = 0.97  # SELL stop-limit: limit = trigger * this
MAX_ENTRY_ARM_FAILURES = 3


def entry_trigger_price(level: float) -> float:
    """Nearest tick to `level` (the price at/below which the SELL should trigger)."""
    return round(round(level / STOP_TICK) * STOP_TICK, 2)


def arm_entry(
    client: JainamClient,
    logger: logging.Logger,
    leg_key: str,
    leg: dict,
    ltp: float,
    quantity: int,
    live: bool,
    trigger: float,
    exchange: str,
) -> dict:
    """Park a SELL stop-limit that triggers when price falls to `trigger`. No-op in paper mode, with the feature
    off, when one is already resting, after repeated failures, or when the market is already at/through the
    trigger (the caller then enters at market)."""
    if (not live or not RESTING_ENTRY_ORDERS or leg.get("entry_order_id")
            or leg.get("entry_arm_failures", 0) >= MAX_ENTRY_ARM_FAILURES):
        return leg
    trigger = entry_trigger_price(trigger)
    if trigger <= STOP_TICK or ltp <= trigger:
        return leg
    limit = round(math.floor(trigger * ENTRY_LIMIT_BUFFER / STOP_TICK + 1e-9) * STOP_TICK, 2)
    try:
        order_id = client.place_order(
            variety=client.VARIETY_REGULAR,
            exchange=exchange,
            tradingsymbol=leg["tradingsymbol"],
            transaction_type="SELL",
            quantity=quantity,
            product=client.PRODUCT_NRML,
            order_type=client.ORDER_TYPE_STOP_LIMIT,
            stop_price=trigger,
            limit_price=limit,
        )
    except BrokerError as exc:
        leg["entry_arm_failures"] = leg.get("entry_arm_failures", 0) + 1
        logger.warning(
            "Could not place resting entry for %s %s @ %.2f (failure %d/%d): %s -- software entry remains active.",
            leg_key, leg["tradingsymbol"], trigger, leg["entry_arm_failures"], MAX_ENTRY_ARM_FAILURES, exc,
        )
        return leg
    leg["entry_order_id"] = str(order_id)
    leg["entry_trigger"] = trigger
    leg["entry_limit"] = limit
    leg["entry_placed_at"] = datetime.now(IST).isoformat()
    logger.info(
        "ENTRY ORDER PLACED: %s %s SELL stop-limit trigger %.2f / limit %.2f -- order_id=%s",
        leg_key, leg["tradingsymbol"], trigger, limit, order_id,
    )
    return leg


def _forget_entry_order(leg: dict) -> None:
    for k in ("entry_order_id", "entry_trigger", "entry_limit", "entry_placed_at"):
        leg.pop(k, None)


def check_entry_order(client: JainamClient, logger: logging.Logger, leg_key: str, leg: dict) -> Optional[float]:
    """Look at the resting entry order. Returns its fill price if it FILLED (the order is then forgotten and the
    caller books the entry); None otherwise. A cancelled/expired order is forgotten so it is re-placed; a
    rejected one counts as an arming failure (software entry covers it)."""
    order_id = leg.get("entry_order_id")
    if not order_id:
        return None
    try:
        history = client.order_history(order_id)
    except BrokerError as exc:
        if _order_unknown(exc) and _stop_is_old(leg, key="entry_placed_at"):
            logger.warning("Resting entry %s for %s is unknown to the broker (expired) -- forgetting it.", order_id, leg_key)
            _forget_entry_order(leg)
            return None
        logger.warning("order_history(%s) for %s entry failed: %s", order_id, leg_key, exc)
        return None
    if not history:
        return None
    latest = history[-1]
    status = latest.get("status")
    if status == "COMPLETE":
        _forget_entry_order(leg)
        return float(latest["average_price"])
    if status in ("REJECTED", "CANCELLED"):
        _forget_entry_order(leg)
        if status == "REJECTED":
            leg["entry_arm_failures"] = leg.get("entry_arm_failures", 0) + 1
        logger.warning(
            "Resting entry %s for %s ended %s (%s) -- %s.", order_id, leg_key, status, latest.get("status_message"),
            "will be re-placed" if status == "CANCELLED" else "software entry covers it",
        )
    return None


def cancel_entry_order(client: JainamClient, logger: logging.Logger, leg_key: str, leg: dict) -> Optional[float]:
    """Cancel the resting entry order before this process enters (or exits) itself. Returns the fill price if
    the order had already FILLED (the caller must book that entry and NOT send another SELL); None once it is
    gone. Raises RuntimeError if it can neither be confirmed filled nor cancelled."""
    order_id = leg.get("entry_order_id")
    if not order_id:
        return None
    fill = check_entry_order(client, logger, leg_key, leg)
    if fill is not None or not leg.get("entry_order_id"):
        return fill
    try:
        client.cancel_order(order_id)
    except BrokerError as exc:
        fill = check_entry_order(client, logger, leg_key, leg)
        if fill is not None:
            return fill
        if _order_unknown(exc) and _stop_is_old(leg, key="entry_placed_at"):
            _forget_entry_order(leg)
            logger.warning("Resting entry %s for %s no longer exists at the broker -- treating as gone.", order_id, leg_key)
            return None
        raise RuntimeError(f"Could not cancel resting entry {order_id} for {leg_key}: {exc}") from exc
    fill = check_entry_order(client, logger, leg_key, leg)   # a fill that raced the cancel
    if fill is not None:
        return fill
    _forget_entry_order(leg)
    logger.info("Resting entry %s for %s cancelled.", order_id, leg_key)
    return None


def entry_order_stuck(leg: dict, ltp: float) -> bool:
    """True if a resting SELL stop-limit exists but the market has run below its limit price (it cannot fill
    unless price recovers) -- the caller should cancel it and enter at market instead."""
    return bool(leg.get("entry_order_id")) and ltp < leg.get("entry_limit", 0.0)


def handle_stop_loss(
    client: JainamClient, logger: logging.Logger, leg_key: str, leg: dict, quantity: int, live: bool, exchange: str
) -> dict:
    """Square off (BUY) a leg that hit its stop-loss; mark it watching_for_reentry.

    entry_price is intentionally NOT overwritten here -- it stays as the
    level to watch for re-entry. Any resting broker stop is cancelled first
    (or recognised as already filled, in which case no second BUY is sent).
    """
    if live and cancel_stop(client, logger, leg_key, leg, quantity):
        return leg
    exit_price = execute_order(client, logger, leg["tradingsymbol"], "BUY", quantity, live, exchange)
    return record_stop_loss(logger, leg_key, leg, quantity, exit_price, live=live)


PROFIT_TARGET_FRACTION = 0.10  # book profit once LTP <= entry * this (90% of premium captured)


def handle_profit_target(
    client: JainamClient, logger: logging.Logger, leg_key: str, leg: dict, quantity: int, live: bool, exchange: str
) -> dict:
    """Buy back a short that has captured 90% of its premium; the leg then stays flat (target_hit) for the
    rest of the cycle. Any resting broker stop is cancelled first (or recognised as already filled, in
    which case no second BUY is sent)."""
    if live and cancel_stop(client, logger, leg_key, leg, quantity):
        return leg
    exit_price = execute_order(client, logger, leg["tradingsymbol"], "BUY", quantity, live, exchange)
    pnl = (leg["entry_price"] - exit_price) * quantity
    leg["realized_pnl"] = leg.get("realized_pnl", 0.0) + pnl
    leg["status"] = "target_hit"
    leg.setdefault("history", []).append(
        {
            "type": "profit_target",
            "tradingsymbol": leg["tradingsymbol"],
            "exit_price": exit_price,
            "entry_price_at_target": leg["entry_price"],
            "pnl": pnl,
            "timestamp": datetime.now(IST).isoformat(),
        }
    )
    logger.info(
        "PROFIT TARGET: %s %s bought back @ %.2f (entry was %.2f, segment P&L %+.2f). Staying flat for the rest of this cycle.",
        leg_key, leg["tradingsymbol"], exit_price, leg["entry_price"], pnl,
    )
    telegram_notify.send(
        f"[{'LIVE' if live else 'PAPER'}] [{logger.name}] PROFIT TARGET\n"
        f"{leg_key.upper()}: {leg['tradingsymbol']}\n"
        f"Bought back @ {exit_price:.2f} (entry was {leg['entry_price']:.2f}, P&L {pnl:+.2f})",
        logger,
    )
    return leg


def handle_reentry(
    client: JainamClient, logger: logging.Logger, leg_key: str, leg: dict, quantity: int, live: bool, exchange: str
) -> dict:
    """Re-sell a flat, watching leg at market; the new fill becomes its new entry_price."""
    new_entry_price = execute_order(client, logger, leg["tradingsymbol"], "SELL", quantity, live, exchange)
    return book_reentry(logger, leg_key, leg, quantity, new_entry_price, live)


def book_reentry(logger: logging.Logger, leg_key: str, leg: dict, quantity: int, new_entry_price: float, live: bool) -> dict:
    """Record a re-entry SELL filled at new_entry_price (market order or resting entry order)."""
    leg["entry_price"] = new_entry_price
    leg["reentry_count"] = leg.get("reentry_count", 0) + 1
    leg["status"] = "open"
    leg["sl_arm_failures"] = 0
    leg["entry_arm_failures"] = 0
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
    telegram_notify.send(
        f"[{'LIVE' if live else 'PAPER'}] [{logger.name}] RE-ENTRY #{leg['reentry_count']}\n"
        f"{leg_key.upper()}: {leg['tradingsymbol']}\n"
        f"Sold @ {new_entry_price:.2f}",
        logger,
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
    or 'watching_for_reentry'.

    Open leg: keep a resting broker stop-market parked (live mode); if it has filled, book the
    stop-out; otherwise the software stop (ltp >= entry * multiplier) closes the leg at market
    whenever no resting stop exists, or if the resting one is still unfilled with LTP well past
    its trigger. Watching leg: re-enter when ltp <= entry_price, capped at max_reentries.

    max_reentries is enforced against leg["reentry_count"] as persisted in
    state (reloaded on restart), not an in-memory counter -- so the cap holds
    correctly even if the script is restarted mid-cycle.
    """
    if leg["status"] == "open":
        if live:
            if check_broker_stop(client, logger, leg_key, leg, quantity):
                return leg
            leg = arm_stop(client, logger, leg_key, leg, ltp, quantity, live, stop_loss_multiplier, exchange)
        resting = leg.get("sl_order_id")
        trigger = leg.get("sl_price") or leg["entry_price"] * stop_loss_multiplier
        if (not resting and ltp >= leg["entry_price"] * stop_loss_multiplier) or (
            resting and ltp >= trigger * SOFTWARE_FALLBACK_BUFFER
        ):
            leg = handle_stop_loss(client, logger, leg_key, leg, quantity, live, exchange)
        elif ltp <= leg["entry_price"] * PROFIT_TARGET_FRACTION:
            leg = handle_profit_target(client, logger, leg_key, leg, quantity, live, exchange)
    elif leg["status"] == "watching_for_reentry":
        reentry_count = leg.get("reentry_count", 0)
        at_cap = max_reentries is not None and reentry_count >= max_reentries
        if live and RESTING_ENTRY_ORDERS and not at_cap:
            fill = check_entry_order(client, logger, leg_key, leg)
            if fill is not None:
                return book_reentry(logger, leg_key, leg, quantity, fill, live)
            leg = arm_entry(client, logger, leg_key, leg, ltp, quantity, live, leg["entry_price"], exchange)
        resting = bool(leg.get("entry_order_id"))
        if ltp <= leg["entry_price"] and (not resting or entry_order_stuck(leg, ltp)):
            if at_cap:
                logger.info(
                    "%s leg reached max_reentries=%d (reentry_count=%d) -- staying flat for the rest of this cycle.",
                    leg_key, max_reentries, reentry_count,
                )
                leg["status"] = "capped_flat"
            else:
                if resting:
                    fill = cancel_entry_order(client, logger, leg_key, leg)
                    if fill is not None:
                        return book_reentry(logger, leg_key, leg, quantity, fill, live)
                leg = handle_reentry(client, logger, leg_key, leg, quantity, live, exchange)
    return leg


# --------------------------------------------------------------------------
# Live-mode warning
# --------------------------------------------------------------------------


def print_live_warning_banner() -> None:
    print("!" * 78)
    print("!!!  LIVE TRADING MODE -- REAL ORDERS WILL BE PLACED ON YOUR JAINAM ACCOUNT  !!!")
    print("!" * 78)
