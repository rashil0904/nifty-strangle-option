"""
delta_breakdown_strategy.py
=============================

Live short-options "breakdown" strategy on NIFTY weekly options via Jainam
(XTS Connect). Runs alongside (not instead of) delta_strangle_strategy.py --
separate contracts, separate state file (breakdown_strategy_state.json),
separate process. The two scripts are independent and never touch each
other's positions or state.

Strategy summary
--------------------------------------------------------------------------
1. 10:45 IST -- contract selection (locked for the day): compute Black-76
   delta for the nearest weekly expiry's full chain (reusing
   nifty_delta_model.py's math/data functions) and lock in the call strike
   closest to +0.30 delta and the put strike closest to -0.30 delta. These
   two contracts do not change again for the rest of the cycle, even if the
   0.3-delta strike shifts later in the day.

2. 10:45-11:29:59 IST -- tracking window: poll each locked contract's LTP
   on --poll-seconds and maintain a running minimum, tracked_low, per leg.
   At exactly 11:29:59, tracked_low is frozen and never updated again.

3. 11:30 IST onward -- breakout entry: keep polling; the first time (and
   only the first time) a leg's LTP drops strictly below its frozen
   tracked_low, SELL that leg at market and record the fill as entry_price.
   A leg that never breaks its tracked_low before market close (15:30) that
   day is NOT dropped: it keeps watching the same frozen low on the next trading day from the
   open (a gap-down open below the low sells at the open) until the exit deadline.

4. Stop-loss / re-entry (identical mechanics to delta_strangle_strategy.py,
   different thresholds/timing): once a leg is open, a stop-loss at
   entry_price * 1.30 buys it back and starts watching for re-entry; while
   watching, the leg re-enters (SELL) once (--max-reentries, default 1) as
   soon as LTP <= entry_price -- no upper bound, no percentage band. After
   the cap is used, a further stop-out leaves the leg flat for the rest of
   the cycle. This continues overnight and resumes polling once the market
   reopens.

5. 11:15 IST the next trading day -- unconditional exit: any leg still
   "open" is bought back regardless of P&L. Legs that are flat (no entry,
   or stopped out with the re-entry cap exhausted) need no action.

Each launch day uses its own state file (<name>_YYYY-MM-DD.json), so yesterday's cycle (still
running until its next-day exit) and today's new 10:45 cycle run as separate processes in parallel.

Safety default: this script only ever PAPER TRADES (computes and logs every
intended order, never calls the broker's order-placement call) unless
--live is explicitly passed. A loud warning banner prints whenever --live
is active.

Broker: trades via common/jainam_client.py's JainamClient (JAINAM_*
environment variables) -- see delta_strangle_strategy.py's docstring for
the full explanation.

Reused from nifty_delta_model.py (imported, not duplicated): get_jainam_client,
fetch_instruments, select_option_chain_instruments, select_nearest_option_expiry,
select_nearest_future, fetch_option_chain_quotes, fetch_spot_and_futures,
time_to_expiry_years, black76_price, implied_vol, black76_delta.

Shared mechanics (state persistence, market-hours/scheduling, retryable
broker calls, order execution, stop-loss -> re-entry) live in
common/leg_lifecycle.py and are reused (not duplicated) here and in
delta_strangle_strategy.py, so those mechanics cannot silently diverge
between the two strategies: both scripts call the exact same
handle_stop_loss/handle_reentry/execute_order functions underneath, just
with different thresholds and timing passed in.

Assumptions / known limitations (flagged explicitly rather than silently
assumed)
--------------------------------------------------------------------------
1. Re-entry trigger threshold: identical to delta_strangle_strategy.py --
   re-entry fires as soon as LTP <= entry_price, no upper bound, no
   percentage multiplier. A leg watching for re-entry is never re-sold
   while its LTP is still above the price it was originally sold at.

2. Order product is NRML (carry-forward), not MIS (intraday) -- deliberate
   and safety-critical, since a breakout entry can happen as late as 15:29
   on day D and the position may be held to 11:15 on day D+1.

3. No NSE holiday calendar is incorporated -- only weekends are skipped
   when computing the next trading day. A mid-week exchange holiday isn't
   specially handled; quote/order calls would simply fail cleanly that day.

4. The risk-free rate r used for the Black-76 delta calculation is a fixed
   constant (DEFAULT_R, matching nifty_delta_model.py's own default) rather
   than a CLI argument -- this script's CLI surface is intentionally
   limited to --lots, --poll-seconds, --max-reentries, --live per spec.

5. If a leg has not yet broken out by market close (15:30) on the selection
   day, it carries over and keeps watching the same frozen low the next day
   until the exit deadline. tracked_low itself is specific to the
   10:45-11:29:59 window of that single trading day.

6. If the script is launched after 10:45 on a trading day (but before market
   close), it runs the contract-selection step immediately using whatever
   chain is live at that moment -- the tracked_low window is then whatever
   remains between launch time and 11:29:59 that day. If launched after
   11:29:59, tracked_low freezes at (effectively) the single LTP read at
   selection time, since no further updates occur before the freeze check.

7. Partial-selection failure (one leg entered on breakout, the other's
   breakout order fails) is not auto-recovered -- the script logs CRITICAL
   and halts monitoring only for the failed leg (status -> "error"), same
   discipline as delta_strangle_strategy.py.

8. All orders (entry, stop-loss, re-entry, final exit) are MARKET orders.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from datetime import time as dtime
from typing import Optional

import numpy as np
import pandas as pd

# nifty_delta_model.py lives in ../helpers, and leg_lifecycle.py (shared with
# Sensex) lives in ../../common -- neither is on the default import path, so
# add both explicitly (paths are resolved from this script's own location,
# so this works regardless of the caller's cwd).
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_THIS_DIR, "..", "helpers"))
sys.path.insert(0, os.path.join(_THIS_DIR, "..", "..", "common"))

from jainam_client import JainamClient
import telegram_notify
from nifty_delta_model import (
    IST,
    NFO_EXCHANGE,
    get_jainam_client,
    fetch_instruments,
    select_option_chain_instruments,
    select_nearest_option_expiry,
    select_nearest_future,
    fetch_option_chain_quotes,
    fetch_spot_and_futures,
    time_to_expiry_years,
    black76_price,
    black76_delta,
    implied_vol,
)
import leg_lifecycle as _leg_lifecycle
from leg_lifecycle import (
    MARKET_CLOSE_TIME,
    save_state as _lifecycle_save_state,
    load_state as _lifecycle_load_state,
    clear_state as _lifecycle_clear_state,
    is_trading_day,
    is_market_hours,
    next_market_open,
    deadline_next_trading_day,
    sleep_until,
    retry_call as _lifecycle_retry_call,
    fetch_ltp as _lifecycle_fetch_ltp,
    execute_order as _lifecycle_execute_order,
    handle_stop_loss as _lifecycle_handle_stop_loss,
    handle_reentry as _lifecycle_handle_reentry,
    cancel_stop as _lifecycle_cancel_stop,
    cancel_entry_order as _lifecycle_cancel_entry_order,
    check_entry_order as _lifecycle_check_entry_order,
    arm_entry as _lifecycle_arm_entry,
    entry_order_stuck as _lifecycle_entry_order_stuck,
    book_reentry as _lifecycle_book_reentry,
    STOP_TICK as _STOP_TICK,
    apply_stop_loss_or_reentry,
    print_live_warning_banner,
)

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

STATE_FILE = "breakdown_strategy_state.json"
LOG_FILE = "breakdown_strategy_log.txt"

SELECTION_TIME = dtime(10, 45)
TRACKED_LOW_FREEZE_TIME = dtime(11, 29, 59)
BREAKOUT_START_TIME = dtime(11, 30)
EXIT_TIME = dtime(11, 15)  # on the next trading day after selection
EXPIRY_DAY_EXIT_TIME = dtime(15, 15)  # when the contract expires today: square off before it settles
SAME_DAY_EXPIRY = False  # set by --same-day-expiry

TARGET_DELTA_CALL = 0.30
TARGET_DELTA_PUT = -0.30
STOP_LOSS_MULTIPLIER = 1.30
# Re-entry has no multiplier: it fires as soon as LTP <= entry_price (see
# docstring point 1). Compared directly against leg["entry_price"] in monitor_leg below.

DEFAULT_R = 0.065  # matches nifty_delta_model.py's default; not CLI-configurable here (see docstring point 4)

# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------

logger = logging.getLogger("delta_breakdown_strategy")
logger.setLevel(logging.INFO)
logger.propagate = False
_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_formatter)
logger.addHandler(_console_handler)
_file_handler = logging.FileHandler(LOG_FILE)
_file_handler.setFormatter(_formatter)
logger.addHandler(_file_handler)


# --------------------------------------------------------------------------
# State persistence (thin wrappers around leg_lifecycle, bound to STATE_FILE)
# --------------------------------------------------------------------------


def save_state(state: dict) -> None:
    _lifecycle_save_state(state, STATE_FILE)


def load_state() -> Optional[dict]:
    return _lifecycle_load_state(STATE_FILE, logger)


def clear_state() -> None:
    _lifecycle_clear_state(STATE_FILE)


# --------------------------------------------------------------------------
# Market-hours / scheduling helpers (is_trading_day, is_market_hours,
# next_market_open, sleep_until are reused directly from leg_lifecycle)
# --------------------------------------------------------------------------


def next_selection_datetime(now: datetime) -> datetime:
    """Next 10:45 IST contract-selection trigger: today's if not yet passed on
    a trading day; 'now' itself if today is a trading day, past 10:45, but
    still within the session (so a late-morning launch selects immediately
    off whatever chain is live -- see docstring point 6); otherwise the next
    trading day's 10:45."""
    d = now.date()
    if is_trading_day(d):
        today_selection = datetime.combine(d, SELECTION_TIME, tzinfo=IST)
        if now <= today_selection:
            return today_selection
        if now.time() < MARKET_CLOSE_TIME:
            return now
    d = d + timedelta(days=1)
    while not is_trading_day(d):
        d += timedelta(days=1)
    return datetime.combine(d, SELECTION_TIME, tzinfo=IST)


def exit_deadline_from_selection(selection_dt: datetime) -> datetime:
    """11:15 IST on the next trading day after selection_dt's date."""
    return deadline_next_trading_day(selection_dt.date(), EXIT_TIME)


# --------------------------------------------------------------------------
# Retryable broker calls / order execution (thin wrappers around
# leg_lifecycle, bound to this module's logger)
# --------------------------------------------------------------------------


def _retry_call(func, *args, description: str = "call", **kwargs):
    return _lifecycle_retry_call(logger, func, *args, description=description, **kwargs)


def fetch_ltp(client: JainamClient, tradingsymbol: str) -> float:
    return _lifecycle_fetch_ltp(client, tradingsymbol, logger, NFO_EXCHANGE)


def _execute_order(client: JainamClient, tradingsymbol: str, transaction_type: str, quantity: int, live: bool) -> float:
    return _lifecycle_execute_order(client, logger, tradingsymbol, transaction_type, quantity, live, NFO_EXCHANGE)


# --------------------------------------------------------------------------
# Strike selection (locked for the day)
# --------------------------------------------------------------------------


def select_entry_strikes(client: JainamClient, instruments: pd.DataFrame) -> dict:
    """Nearest weekly NIFTY expiry -> option chain + futures price -> Black-76
    delta per strike/side -> the call closest to +0.30 delta and the put
    closest to -0.30 delta (by absolute difference). Identical selection
    method to delta_strangle_strategy.py, but this is an entirely separate
    pair of contracts/positions.

    Returns {"expiry": date, "call": {...}, "put": {...}}, each leg dict
    containing strike, option_type, tradingsymbol, delta, price, lot_size.
    """
    expiry = select_nearest_option_expiry(instruments)
    chain_instruments = select_option_chain_instruments(instruments, expiry)
    future_row = select_nearest_future(instruments)

    chain = _retry_call(
        fetch_option_chain_quotes, client, chain_instruments, description="fetch_option_chain_quotes"
    )
    spot, futures_price = _retry_call(
        fetch_spot_and_futures, client, future_row["tradingsymbol"], description="fetch_spot_and_futures"
    )
    T = time_to_expiry_years(expiry)

    chain = chain.copy()
    chain["delta"] = np.nan
    for idx, row in chain.iterrows():
        strike, side, price = row["strike"], row["instrument_type"], row["price"]
        iv = implied_vol(lambda s, K=strike, opt=side: black76_price(futures_price, K, T, DEFAULT_R, s, opt), price)
        if np.isfinite(iv):
            chain.at[idx, "delta"] = black76_delta(futures_price, strike, T, DEFAULT_R, iv, side)

    calls = chain[chain["instrument_type"] == "CE"].dropna(subset=["delta"])
    puts = chain[chain["instrument_type"] == "PE"].dropna(subset=["delta"])
    if calls.empty or puts.empty:
        raise RuntimeError("Could not compute a valid delta for any call or put strike -- cannot select entry strikes.")

    call_row = calls.loc[(calls["delta"] - TARGET_DELTA_CALL).abs().idxmin()]
    put_row = puts.loc[(puts["delta"] - TARGET_DELTA_PUT).abs().idxmin()]

    def _lot_size(tradingsymbol: str) -> int:
        match = chain_instruments.loc[chain_instruments["tradingsymbol"] == tradingsymbol, "lot_size"]
        if match.empty:
            raise RuntimeError(f"Could not find lot_size for {tradingsymbol} in the instrument master.")
        return int(match.iloc[0])

    def _leg(row: pd.Series) -> dict:
        return {
            "strike": float(row["strike"]),
            "option_type": row["instrument_type"],
            "tradingsymbol": row["tradingsymbol"],
            "delta": float(row["delta"]),
            "price": float(row["price"]),
            "lot_size": _lot_size(row["tradingsymbol"]),
        }

    return {"expiry": expiry, "call": _leg(call_row), "put": _leg(put_row)}


# --------------------------------------------------------------------------
# Contract locking + tracked_low
# --------------------------------------------------------------------------


def track_low(existing_low: Optional[float], ltp: float) -> float:
    """Running-minimum update; existing_low may be None on the very first call."""
    if existing_low is None:
        return ltp
    return min(existing_low, ltp)


def run_selection_scan(client: JainamClient, lots: int, live: bool, max_reentries: Optional[int]) -> dict:
    """10:45 sequence: lock the call/put contracts for the day and seed each
    leg's tracked_low with an initial LTP read. No orders are placed here --
    a leg only becomes a position once it breaks out (see check_breakout)."""
    logger.info("=" * 70)
    logger.info("CONTRACT SELECTION starting...")
    # Skip an expiry that is today itself -- a breakout entry any time after
    # 11:30 today could still be open and held overnight to tomorrow's
    # 11:15 exit, so a contract expiring at today's 15:30 cutoff would
    # already be settled mid-hold. Require strictly next week's expiry.
    # --same-day-expiry lifts that floor: trade today's expiry and square off before it settles.
    floor = datetime.now(IST).date() if SAME_DAY_EXPIRY else (datetime.now(IST) + timedelta(days=1)).date()
    instruments = _retry_call(fetch_instruments, client, min_expiry=floor, description="fetch_instruments")
    selection = select_entry_strikes(client, instruments)

    call_lot, put_lot = selection["call"]["lot_size"], selection["put"]["lot_size"]
    if call_lot != put_lot:
        raise RuntimeError(f"Call and put lot sizes differ ({call_lot} vs {put_lot}) -- refusing to proceed.")
    quantity = call_lot * lots

    now = datetime.now(IST)
    state = {
        "selection_time": now.isoformat(),
        "expiry": selection["expiry"].isoformat(),
        "quantity": quantity,
        "lots": lots,
        "exit_deadline": (datetime.combine(now.date(), EXPIRY_DAY_EXIT_TIME, tzinfo=IST)
                          if selection["expiry"] == now.date() else exit_deadline_from_selection(now)).isoformat(),
        "live": live,
        "max_reentries": max_reentries,
        "legs": {},
    }

    for leg_key in ("call", "put"):
        leg_selection = selection[leg_key]
        initial_ltp = fetch_ltp(client, leg_selection["tradingsymbol"])
        state["legs"][leg_key] = {
            "tradingsymbol": leg_selection["tradingsymbol"],
            "strike": leg_selection["strike"],
            "option_type": leg_selection["option_type"],
            "delta_at_selection": leg_selection["delta"],
            "tracked_low": track_low(None, initial_ltp),
            "tracked_low_frozen": False,
            "tracking_date": now.date().isoformat(),
            "entry_price": None,
            "entry_time": None,
            "status": "watching_breakout",
            "stop_count": 0,
            "reentry_count": 0,
            "realized_pnl": 0.0,
            "history": [
                {
                    "type": "selection",
                    "tradingsymbol": leg_selection["tradingsymbol"],
                    "delta_at_selection": leg_selection["delta"],
                    "initial_ltp": initial_ltp,
                    "timestamp": now.isoformat(),
                }
            ],
        }
        logger.info(
            "LOCKED: %s %s (strike %.1f, delta %+.3f), initial tracked_low=%.2f",
            leg_key, leg_selection["tradingsymbol"], leg_selection["strike"], leg_selection["delta"], initial_ltp,
        )
        telegram_notify.send(
            f"[{'LIVE' if live else 'PAPER'}] [NIFTY BREAKDOWN] CONTRACT LOCKED\n"
            f"{leg_key.upper()}: {leg_selection['tradingsymbol']}\n"
            f"Strike {leg_selection['strike']:.0f}, delta {leg_selection['delta']:+.3f}\n"
            f"Initial tracked_low = {initial_ltp:.2f}",
            logger,
        )

    save_state(state)
    logger.info(
        "CONTRACT SELECTION complete. Tracking until %s, exit deadline %s.",
        TRACKED_LOW_FREEZE_TIME, state["exit_deadline"],
    )
    return state


# --------------------------------------------------------------------------
# Breakout detection
# --------------------------------------------------------------------------


def check_breakout(leg: dict, ltp: float) -> bool:
    """True iff this leg is still watching, its tracked_low is frozen, and
    ltp has dropped strictly below that frozen tracked_low."""
    return (
        leg["status"] == "watching_breakout"
        and leg.get("tracked_low_frozen", False)
        and ltp < leg["tracked_low"]
    )


def place_entry(
    client: JainamClient, leg_key: str, leg: dict, quantity: int, live: bool, fill_price: Optional[float] = None
) -> dict:
    """Breakout SELL entry for a leg that just broke below its frozen tracked_low."""
    entry_price = fill_price if fill_price is not None else _execute_order(
        client, leg["tradingsymbol"], "SELL", quantity, live
    )
    now = datetime.now(IST).isoformat()
    leg["entry_price"] = entry_price
    leg["entry_time"] = now
    leg["status"] = "open"
    leg.setdefault("history", []).append(
        {
            "type": "breakout_entry",
            "tradingsymbol": leg["tradingsymbol"],
            "entry_price": entry_price,
            "tracked_low": leg["tracked_low"],
            "timestamp": now,
        }
    )
    logger.info(
        "BREAKOUT ENTRY: %s %s SOLD @ %.2f x%d (broke below frozen tracked_low %.2f)",
        leg_key, leg["tradingsymbol"], entry_price, quantity, leg["tracked_low"],
    )
    telegram_notify.send(
        f"[{'LIVE' if live else 'PAPER'}] [NIFTY BREAKDOWN] BREAKOUT ENTRY\n"
        f"{leg_key.upper()}: {leg['tradingsymbol']}\n"
        f"Sold @ {entry_price:.2f} x{quantity} (broke below frozen tracked_low {leg['tracked_low']:.2f})",
        logger,
    )
    return leg


# --------------------------------------------------------------------------
# Monitoring: tracking window -> breakout window -> stop-loss / re-entry
# --------------------------------------------------------------------------


def monitor_leg(
    client: JainamClient, leg_key: str, leg: dict, quantity: int, max_reentries: Optional[int], live: bool, now: datetime
) -> dict:
    """One polling check for a single leg, dispatched by its current phase:

    - watching_breakout, before the freeze time: update tracked_low.
    - watching_breakout, at/after the freeze time: freeze tracked_low (once),
      then check for a breakout entry. If market close passes with no
      breakout, keep watching the same frozen low on later days until the exit deadline.
    - open / watching_for_reentry: identical stop-loss / re-entry mechanics
      to delta_strangle_strategy.py, just parameterized with
      STOP_LOSS_MULTIPLIER=1.30 here.

    An order failure here is caught, logged CRITICAL, and halts only this
    leg (status -> "error") rather than crashing the whole process or the
    other leg's monitoring.
    """
    if leg["status"] not in ("watching_breakout", "open", "watching_for_reentry"):
        return leg

    try:
        ltp = fetch_ltp(client, leg["tradingsymbol"])
    except RuntimeError as exc:
        logger.warning(
            "Could not fetch LTP for %s (%s) this poll: %s -- will retry next cycle.",
            leg_key, leg["tradingsymbol"], exc,
        )
        return leg

    leg["last_ltp"] = ltp
    leg["last_checked"] = now.isoformat()

    try:
        if leg["status"] == "watching_breakout":
            today = now.date().isoformat()
            tracking_date = leg.get("tracking_date", today)
            if not leg.get("tracked_low_frozen", False) and today == tracking_date and now.time() < TRACKED_LOW_FREEZE_TIME:
                leg["tracked_low"] = track_low(leg["tracked_low"], ltp)
            else:
                if not leg.get("tracked_low_frozen", False):
                    leg["tracked_low_frozen"] = True
                    logger.info(
                        "%s tracked_low FROZEN at %.2f (%s).", leg_key, leg["tracked_low"], TRACKED_LOW_FREEZE_TIME
                    )
                    telegram_notify.send(
                        f"[{'LIVE' if live else 'PAPER'}] [NIFTY BREAKDOWN] TRACKED_LOW FROZEN\n"
                        f"{leg_key.upper()}: {leg['tradingsymbol']}\n"
                        f"Frozen at {leg['tracked_low']:.2f} ({TRACKED_LOW_FREEZE_TIME})",
                        logger,
                    )
                # Carry-over: a leg that never broke out stays in watching_breakout past the close and
                # keeps watching the same frozen low on later days from the open (a gap-down open
                # below the low sells at the open) until the cycle's exit deadline.
                if today > tracking_date or now.time() >= BREAKOUT_START_TIME:
                    if live and _leg_lifecycle.RESTING_ENTRY_ORDERS:
                        # Resting SELL stop-limit one tick under the frozen low (== 'ltp < tracked_low').
                        fill = _lifecycle_check_entry_order(client, logger, leg_key, leg)
                        if fill is not None:
                            return place_entry(client, leg_key, leg, quantity, live, fill_price=fill)
                        leg = _lifecycle_arm_entry(
                            client, logger, leg_key, leg, ltp, quantity, live, leg["tracked_low"] - _STOP_TICK, NFO_EXCHANGE
                        )
                    resting = bool(leg.get("entry_order_id"))
                    if check_breakout(leg, ltp) and (not resting or _lifecycle_entry_order_stuck(leg, ltp)):
                        if resting:
                            fill = _lifecycle_cancel_entry_order(client, logger, leg_key, leg)
                            if fill is not None:
                                return place_entry(client, leg_key, leg, quantity, live, fill_price=fill)
                        leg = place_entry(client, leg_key, leg, quantity, live)
        elif leg["status"] in ("open", "watching_for_reentry"):
            leg = apply_stop_loss_or_reentry(
                client, logger, leg_key, leg, ltp, quantity, live, STOP_LOSS_MULTIPLIER, max_reentries, NFO_EXCHANGE
            )
    except RuntimeError as exc:
        logger.critical(
            "AUTOMATION HALTED for %s leg (%s) after an order failure: %s. This leg will no longer be "
            "monitored automatically -- check your Jainam positions manually.",
            leg_key, leg["tradingsymbol"], exc,
        )
        telegram_notify.send(
            f"[{'LIVE' if live else 'PAPER'}] [NIFTY BREAKDOWN] AUTOMATION HALTED\n"
            f"{leg_key.upper()}: {leg['tradingsymbol']}\n"
            f"Order failure: {exc}\nCheck Jainam positions manually.",
            logger,
        )
        leg["status"] = "error"
        leg["error"] = str(exc)

    return leg


def handle_stop_loss(client: JainamClient, leg_key: str, leg: dict, quantity: int, live: bool) -> dict:
    return _lifecycle_handle_stop_loss(client, logger, leg_key, leg, quantity, live, NFO_EXCHANGE)


def handle_reentry(client: JainamClient, leg_key: str, leg: dict, quantity: int, live: bool) -> dict:
    return _lifecycle_handle_reentry(client, logger, leg_key, leg, quantity, live, NFO_EXCHANGE)


def run_monitoring_loop(
    client: JainamClient, state: dict, live: bool, max_reentries: Optional[int], poll_seconds: int
) -> dict:
    """Poll every non-terminal leg every poll_seconds until the exit deadline,
    sleeping straight through non-market hours."""
    exit_deadline = datetime.fromisoformat(state["exit_deadline"])
    quantity = state["quantity"]
    active_statuses = ("watching_breakout", "open", "watching_for_reentry")

    while True:
        now = datetime.now(IST)
        if now >= exit_deadline:
            logger.info("Reached exit deadline %s.", exit_deadline.isoformat())
            break

        active = [k for k, leg in state["legs"].items() if leg["status"] in active_statuses]
        if not active:
            logger.info("No active legs remain -- ending monitoring early.")
            break

        if not is_market_hours(now):
            sleep_target = min(next_market_open(now), exit_deadline)
            logger.info("Outside market hours -- sleeping until %s.", sleep_target.isoformat())
            sleep_until(sleep_target)
            continue

        for leg_key in list(state["legs"]):
            leg = state["legs"][leg_key]
            if leg["status"] in active_statuses:
                state["legs"][leg_key] = monitor_leg(client, leg_key, leg, quantity, max_reentries, live, now)

        save_state(state)
        time.sleep(poll_seconds)

    return state


# --------------------------------------------------------------------------
# Exit
# --------------------------------------------------------------------------


def exit_all(client: JainamClient, state: dict, live: bool) -> dict:
    """Square off any leg still 'open' regardless of P&L; leave flat legs
    untouched. Logs final per-leg P&L and stop/re-entry counts."""
    quantity = state["quantity"]
    for leg_key, leg in state["legs"].items():
        if live and leg.get("entry_order_id"):
            # A resting entry order must not outlive the cycle; if it filled, the leg is now open and is closed below.
            try:
                fill = _lifecycle_cancel_entry_order(client, logger, leg_key, leg)
                if fill is not None:
                    if leg["status"] == "watching_breakout":
                        place_entry(client, leg_key, leg, quantity, live, fill_price=fill)
                    else:
                        _lifecycle_book_reentry(logger, leg_key, leg, quantity, fill, live)
            except RuntimeError as exc:
                logger.critical(
                    "FINAL EXIT: could not clear the resting entry order for %s (%s): %s. Check Jainam manually.",
                    leg_key, leg["tradingsymbol"], exc,
                )
                leg["status"] = "error"
                leg["error"] = str(exc)
        if leg["status"] == "open" and live:
            # Cancel the resting broker stop first; if it already filled the leg is flat -- no second BUY.
            try:
                _lifecycle_cancel_stop(client, logger, leg_key, leg, quantity)
            except RuntimeError as exc:
                logger.critical(
                    "FINAL EXIT: could not clear the resting stop for %s (%s): %s. Check Jainam manually.",
                    leg_key, leg["tradingsymbol"], exc,
                )
                leg["status"] = "error"
                leg["error"] = str(exc)
        if leg["status"] == "open":
            try:
                exit_price = _execute_order(client, leg["tradingsymbol"], "BUY", quantity, live)
                pnl = (leg["entry_price"] - exit_price) * quantity
                leg["realized_pnl"] = leg.get("realized_pnl", 0.0) + pnl
                leg["status"] = "closed"
                leg.setdefault("history", []).append(
                    {
                        "type": "final_exit",
                        "tradingsymbol": leg["tradingsymbol"],
                        "exit_price": exit_price,
                        "pnl": pnl,
                        "timestamp": datetime.now(IST).isoformat(),
                    }
                )
                logger.info(
                    "FINAL EXIT: %s %s bought back @ %.2f (segment P&L %+.2f).",
                    leg_key, leg["tradingsymbol"], exit_price, pnl,
                )
                telegram_notify.send(
                    f"[{'LIVE' if live else 'PAPER'}] [NIFTY BREAKDOWN] FINAL EXIT\n"
                    f"{leg_key.upper()}: {leg['tradingsymbol']}\n"
                    f"Bought back @ {exit_price:.2f}",
                    logger,
                )
            except RuntimeError as exc:
                logger.critical(
                    "FINAL EXIT FAILED for %s leg (%s): %s. This leg may still be OPEN on your Jainam "
                    "account -- check and close it manually.",
                    leg_key, leg["tradingsymbol"], exc,
                )
                telegram_notify.send(
                    f"[{'LIVE' if live else 'PAPER'}] [NIFTY BREAKDOWN] FINAL EXIT FAILED\n"
                    f"{leg_key.upper()}: {leg['tradingsymbol']}\n"
                    f"{exc}\nMay still be OPEN -- check Jainam manually.",
                    logger,
                )
                leg["status"] = "error"
                leg["error"] = str(exc)
        elif leg["status"] in ("watching_for_reentry", "capped_flat", "no_entry", "watching_breakout"):
            logger.info(
                "%s (%s) ended the cycle flat -- no position, no action needed.", leg_key, leg["tradingsymbol"]
            )
            leg["status"] = "closed_flat"

        logger.info(
            "%s SUMMARY: stop_outs=%d re-entries=%d total_realized_pnl=%+.2f",
            leg_key, leg.get("stop_count", 0), leg.get("reentry_count", 0), leg.get("realized_pnl", 0.0),
        )

    save_state(state)
    return state


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def run(args: argparse.Namespace) -> None:
    # One state file per launch day, so a cycle carried into the next day (breakout watch / open
    # positions until the next-day exit) keeps running in its own process while the new day's
    # 10:45 cycle starts independently, instead of resuming yesterday's state.
    global STATE_FILE, SAME_DAY_EXPIRY
    SAME_DAY_EXPIRY = args.same_day_expiry
    if args.state_file:
        STATE_FILE = args.state_file
    else:
        root, ext = os.path.splitext(STATE_FILE)
        STATE_FILE = f"{root}_{datetime.now(IST).date().isoformat()}{ext}"

    _leg_lifecycle.RESTING_ENTRY_ORDERS = args.resting_entries
    client = get_jainam_client()

    state = load_state()
    if state is not None:
        live = state.get("live", False)
        max_reentries = state.get("max_reentries")
        logger.info(
            "Existing cycle found in %s (selection_time=%s) -- resuming (live=%s, max_reentries=%s).",
            STATE_FILE, state.get("selection_time"), live, max_reentries,
        )
        # A resumed process has no instrument map yet; load the saved cycle's expiry grid so its
        # contracts can be quoted and ordered.
        _retry_call(
            fetch_instruments, client, min_expiry=datetime.fromisoformat(state["expiry"]).date(),
            description="fetch_instruments (resume)",
        )
        if live != args.live:
            logger.warning(
                "This invocation passed --live=%s but the active cycle was started with live=%s -- "
                "using the ORIGINAL cycle's setting for consistency. Let this cycle finish (it exits "
                "automatically at 11:15am) before starting a new one in a different mode.",
                args.live, live,
            )
        if live:
            print_live_warning_banner()
    else:
        live = args.live
        max_reentries = args.max_reentries
        if live:
            print_live_warning_banner()
            logger.warning("LIVE MODE ACTIVE -- real orders will be placed on this Jainam account.")
        else:
            logger.info("Running in PAPER mode -- no real orders will be placed. Pass --live to trade for real.")

        now = datetime.now(IST)
        selection_dt = next_selection_datetime(now)
        if selection_dt > now:
            logger.info("Waiting until %s IST for contract selection...", selection_dt.isoformat())
            sleep_until(selection_dt)

        state = run_selection_scan(client, args.lots, live, max_reentries)

    state = run_monitoring_loop(client, state, live, max_reentries, args.poll_seconds)

    logger.info("Squaring off any remaining open legs for final exit.")
    state = exit_all(client, state, live)

    if any(leg["status"] == "error" for leg in state["legs"].values()):
        # A leg may still be open at the broker: keep the state file instead of discarding the only record.
        logger.critical(
            "One or more legs ended in ERROR (a position may still be OPEN). %s is KEPT -- reconcile manually "
            "on Jainam before starting a new cycle.", STATE_FILE,
        )
        return

    clear_state()
    logger.info("Cycle complete. %s cleared. Ready for the next cycle.", STATE_FILE)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Live short-options breakdown strategy on NIFTY weekly options via Jainam (XTS Connect)."
    )
    parser.add_argument(
        "--lots", type=int, default=1, help="Number of NIFTY lots per leg (quantity = lot_size * lots). Default: 1."
    )
    parser.add_argument(
        "--poll-seconds", type=int, default=15, help="Seconds between LTP polls during market hours. Default: 15."
    )
    parser.add_argument(
        "--max-reentries",
        type=int,
        default=1,
        help="Cap on re-entries per leg after a stop-loss. Default: 1 (re-enter once; if that re-entered "
        "position stops out again, it stays flat for the rest of the cycle). Enforced against the "
        "persisted per-leg reentry_count in breakdown_strategy_state.json, so it holds across restarts too. "
        "Pass a higher number to allow more cycles, or a very large number for effectively unlimited.",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Place real orders. Default (omitted): paper mode -- compute and log everything, never "
        "call the broker's order-placement endpoint.",
    )
    parser.add_argument(
        "--state-file",
        default=None,
        help="Resume the saved cycle in this specific state file (used to re-attach to positions left open by "
        "an earlier outage) instead of this launch's default state file.",
    )
    parser.add_argument(
        "--same-day-expiry",
        action="store_true",
        help="When the nearest weekly expiry is today, trade it (instead of skipping to next week) and square off "
        "at 15:15 today rather than carrying overnight.",
    )
    parser.add_argument(
        "--resting-entries",
        action="store_true",
        help="Live only: park re-entries and breakout entries as broker-side SELL stop-limit orders instead of reacting to the poll "
        "with a market order (the market-order path stays as the fallback).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> int:
    args = parse_args(argv)
    try:
        run(args)
    except KeyboardInterrupt:
        logger.warning("Interrupted by user (Ctrl-C). State has been saved as of the last poll -- rerun to resume.")
        return 1
    except (EnvironmentError, RuntimeError) as exc:
        logger.critical("FATAL: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
