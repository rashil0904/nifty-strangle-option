"""
delta_strangle_strategy.py
============================

Live short-strangle strategy on NIFTY weekly options via Jainam (XTS
Connect).

Strategy summary
--------------------------------------------------------------------------
At 11:00 IST, scan the nearest weekly NIFTY option chain, compute Black-76
delta for every strike (reusing nifty_delta_model.py's math/data functions),
and SELL 1 lot each of the call strike closest to +0.30 delta and the put
strike closest to -0.30 delta. Each leg is then monitored independently
until 10:30 IST the next trading day:

  - Stop-loss: if a leg's LTP reaches entry_price * 1.20, buy it back
    immediately and start watching for a re-entry.
  - Re-entry: while flat and watching, if LTP falls back to at/near the
    original entry_price, sell it again at market. Capped at --max-reentries
    re-entries per leg (default 1: re-enter once, then stay flat for the
    rest of the cycle if it stops out again).
  - At 10:30 next day, whatever is still open gets bought back regardless
    of P&L; legs that are flat (watching_for_reentry or capped) need no
    action.

Safety default: this script only ever PAPER TRADES (computes and logs every
intended order, never calls the broker's order-placement call) unless
--live is explicitly passed. A loud warning banner prints whenever --live
is active.

Broker: trades via common/jainam_client.py's JainamClient (JAINAM_*
environment variables). Its order-placement path is unverified against a
live Jainam session -- see that module's docstring (the CONFIRM BEFORE LIVE
section) before passing --live. Paper mode never calls place_order()/
order_history(), so a full dry run is always safe regardless.

Reused from nifty_delta_model.py (imported, not duplicated): get_jainam_client,
fetch_instruments, select_option_chain_instruments, select_nearest_option_expiry,
select_nearest_future, fetch_option_chain_quotes, fetch_spot_and_futures,
time_to_expiry_years, black76_price, implied_vol, black76_delta.

Shared mechanics (state persistence, market-hours/scheduling, retryable
broker calls, order execution, stop-loss -> re-entry) live in
common/leg_lifecycle.py and are reused (not duplicated) here and in
delta_breakdown_strategy.py, so those mechanics cannot silently diverge
between the two strategies.

Assumptions / known limitations (flagged explicitly rather than silently
assumed)
--------------------------------------------------------------------------
1. Re-entry trigger threshold: re-entry fires as soon as LTP <= entry_price
   -- no upper bound, no separate percentage multiplier. "Up to 1% below"
   in the originating spec described a point already inside that
   LTP <= entry_price range (i.e. slightly below entry, which naturally
   satisfies the condition), not a wider threshold sitting above entry.
   A leg watching for re-entry is never re-sold while its LTP is still
   above the price it was originally sold at.

2. Order product is NRML (carry-forward), not MIS (intraday). This is
   deliberate and safety-critical: these positions are explicitly held
   overnight (11am entry to next-day 10:30am exit), and MIS positions are
   auto-squared-off by the broker near end-of-day, which would silently
   break the whole strategy.

3. No NSE holiday calendar is incorporated -- only weekends are skipped
   when computing the next market open / next entry / exit deadline. A
   mid-week exchange holiday isn't specially handled; quote/order calls
   would simply fail cleanly on that day (no live data or order acceptance),
   not silently misbehave, but the script won't automatically skip to the
   next real trading day.

4. The risk-free rate r used for the Black-76 delta calculation is a fixed
   constant (DEFAULT_R, matching nifty_delta_model.py's own default) rather
   than a CLI argument -- this script's CLI surface is intentionally
   limited to --lots, --poll-seconds, --max-reentries, --live per spec.

5. One process instance manages exactly one entry-to-exit cycle
   (~11am to ~10:30am the next day) and then exits. For continuous
   day-after-day operation, relaunch it each morning (or run it under a
   supervisor); it will auto-wait for the next 11:00 entry if launched
   early, or resume an in-progress cycle from strategy_state.json if one
   exists.

6. Partial-entry failure (one leg's SELL order succeeds, the other's
   fails) is not auto-recovered -- the script logs CRITICAL, saves
   whatever state exists, and stops rather than guessing. This needs
   manual reconciliation (e.g. closing the one leg that did fill) before
   starting a new cycle.

7. All orders (entry, stop-loss, re-entry, final exit) are MARKET orders,
   for guaranteed execution rather than risking an unfilled limit order
   during a square-off.
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

STATE_FILE = "strategy_state.json"
LOG_FILE = "strategy_log.txt"

ENTRY_TIME = dtime(11, 0)
EXIT_TIME = dtime(10, 30)  # on the next trading day after entry
EXPIRY_DAY_EXIT_TIME = dtime(15, 15)  # when the contract expires today: square off before it settles
SAME_DAY_EXPIRY = False  # set by --same-day-expiry

TARGET_DELTA_CALL = 0.30
TARGET_DELTA_PUT = -0.30
STOP_LOSS_MULTIPLIER = 1.20
# Re-entry has no multiplier: it fires as soon as LTP <= entry_price (see
# docstring point 1). Compared directly against leg["entry_price"] in monitor_leg below.

DEFAULT_R = 0.065  # matches nifty_delta_model.py's default; not CLI-configurable here (see docstring point 4)

# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------

logger = logging.getLogger("delta_strangle_strategy")
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


def next_entry_datetime(now: datetime) -> datetime:
    """Next 11:00 IST entry trigger: today's if not yet passed on a trading day;
    'now' itself if today is a trading day, past 11:00, but still within the
    session (so a late-morning launch enters immediately); otherwise the next
    trading day's 11:00."""
    d = now.date()
    if is_trading_day(d):
        today_entry = datetime.combine(d, ENTRY_TIME, tzinfo=IST)
        if now <= today_entry:
            return today_entry
        if now.time() < MARKET_CLOSE_TIME:
            return now
    d = d + timedelta(days=1)
    while not is_trading_day(d):
        d += timedelta(days=1)
    return datetime.combine(d, ENTRY_TIME, tzinfo=IST)


def exit_deadline_from_entry(entry_dt: datetime) -> datetime:
    """10:30 IST on the next trading day after entry_dt's date."""
    return deadline_next_trading_day(entry_dt.date(), EXIT_TIME)


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
# Strike selection
# --------------------------------------------------------------------------


def select_entry_strikes(client: JainamClient, instruments: pd.DataFrame) -> dict:
    """Nearest weekly NIFTY expiry -> option chain + futures price -> Black-76
    delta per strike/side -> the call closest to +0.30 delta and the put
    closest to -0.30 delta (by absolute difference).

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
# Entry
# --------------------------------------------------------------------------


def place_entry(client: JainamClient, leg_key: str, leg_selection: dict, quantity: int, live: bool) -> dict:
    """Place the entry SELL order for one leg and build its initial state dict."""
    entry_price = _execute_order(client, leg_selection["tradingsymbol"], "SELL", quantity, live)
    now = datetime.now(IST).isoformat()
    leg_state = {
        "tradingsymbol": leg_selection["tradingsymbol"],
        "strike": leg_selection["strike"],
        "option_type": leg_selection["option_type"],
        "delta_at_selection": leg_selection["delta"],
        "entry_price": entry_price,
        "entry_time": now,
        "status": "open",
        "stop_count": 0,
        "reentry_count": 0,
        "realized_pnl": 0.0,
        "history": [
            {
                "type": "entry",
                "tradingsymbol": leg_selection["tradingsymbol"],
                "entry_price": entry_price,
                "delta_at_selection": leg_selection["delta"],
                "timestamp": now,
            }
        ],
    }
    logger.info(
        "ENTRY: %s %s (strike %.1f, delta %+.3f) SOLD @ %.2f x%d",
        leg_key, leg_selection["tradingsymbol"], leg_selection["strike"], leg_selection["delta"], entry_price, quantity,
    )
    telegram_notify.send(
        f"[{'LIVE' if live else 'PAPER'}] [NIFTY STRANGLE] ENTRY\n"
        f"{leg_key.upper()}: {leg_selection['tradingsymbol']}\n"
        f"Strike {leg_selection['strike']:.0f}, delta {leg_selection['delta']:+.3f}\n"
        f"Sold @ {entry_price:.2f} x{quantity}",
        logger,
    )
    return leg_state


def run_entry_scan(client: JainamClient, lots: int, live: bool, max_reentries: Optional[int]) -> dict:
    """Full 11am entry sequence: select strikes, place both legs, build fresh state."""
    logger.info("=" * 70)
    logger.info("ENTRY SCAN starting...")
    # Skip an expiry that is today itself -- entering a contract that expires
    # before tonight's hold is even finished would leave nothing to carry
    # overnight, so require strictly next week's expiry in that case.
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
        "entry_time": now.isoformat(),
        "expiry": selection["expiry"].isoformat(),
        "quantity": quantity,
        "lots": lots,
        "exit_deadline": (datetime.combine(now.date(), EXPIRY_DAY_EXIT_TIME, tzinfo=IST)
                          if selection["expiry"] == now.date() else exit_deadline_from_entry(now)).isoformat(),
        "live": live,
        "max_reentries": max_reentries,
        "legs": {},
    }

    try:
        for leg_key in ("call", "put"):
            state["legs"][leg_key] = place_entry(client, leg_key, selection[leg_key], quantity, live)
    except RuntimeError:
        if state["legs"]:
            save_state(state)
            logger.critical(
                "PARTIAL ENTRY: only the %s leg(s) were entered (%s) before this failure. The strangle "
                "is INCOMPLETE. State saved to %s -- manual reconciliation required (consider closing "
                "the entered leg manually) before starting a new cycle.",
                len(state["legs"]), list(state["legs"].keys()), STATE_FILE,
            )
            telegram_notify.send(
                f"[{'LIVE' if live else 'PAPER'}] [NIFTY STRANGLE] PARTIAL ENTRY FAILURE\n"
                f"Only entered: {list(state['legs'].keys())}. Manual reconciliation required.",
                logger,
            )
        raise

    save_state(state)
    logger.info("ENTRY SCAN complete. Monitoring until exit deadline %s.", state["exit_deadline"])
    return state


# --------------------------------------------------------------------------
# Monitoring: stop-loss / re-entry
# --------------------------------------------------------------------------


def handle_stop_loss(client: JainamClient, leg_key: str, leg: dict, quantity: int, live: bool) -> dict:
    return _lifecycle_handle_stop_loss(client, logger, leg_key, leg, quantity, live, NFO_EXCHANGE)


def handle_reentry(client: JainamClient, leg_key: str, leg: dict, quantity: int, live: bool) -> dict:
    return _lifecycle_handle_reentry(client, logger, leg_key, leg, quantity, live, NFO_EXCHANGE)


def monitor_leg(
    client: JainamClient, leg_key: str, leg: dict, quantity: int, max_reentries: Optional[int], live: bool
) -> dict:
    """One polling check for a single leg: fetch LTP, apply stop-loss / re-entry logic.

    max_reentries is enforced against leg["reentry_count"] as persisted in
    state (reloaded from strategy_state.json on restart), not an in-memory
    counter -- so the cap holds correctly even if the script is restarted
    mid-cycle.

    An order failure here is caught, logged CRITICAL, and halts only this
    leg (status -> "error") rather than crashing the whole process or the
    other leg's monitoring.
    """
    if leg["status"] not in ("open", "watching_for_reentry"):
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
    leg["last_checked"] = datetime.now(IST).isoformat()

    try:
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
            f"[{'LIVE' if live else 'PAPER'}] [NIFTY STRANGLE] AUTOMATION HALTED\n"
            f"{leg_key.upper()}: {leg['tradingsymbol']}\n"
            f"Order failure: {exc}\nCheck Jainam positions manually.",
            logger,
        )
        leg["status"] = "error"
        leg["error"] = str(exc)

    return leg


def run_monitoring_loop(
    client: JainamClient, state: dict, live: bool, max_reentries: Optional[int], poll_seconds: int
) -> dict:
    """Poll every open/watching leg every poll_seconds during market hours until
    the exit deadline, sleeping straight through non-market hours."""
    exit_deadline = datetime.fromisoformat(state["exit_deadline"])
    quantity = state["quantity"]

    while True:
        now = datetime.now(IST)
        if now >= exit_deadline:
            logger.info("Reached exit deadline %s.", exit_deadline.isoformat())
            break

        active = [k for k, leg in state["legs"].items() if leg["status"] in ("open", "watching_for_reentry")]
        if not active:
            logger.info("No open or watching_for_reentry legs remain -- ending monitoring early.")
            break

        if not is_market_hours(now):
            sleep_target = min(next_market_open(now), exit_deadline)
            logger.info("Outside market hours -- sleeping until %s.", sleep_target.isoformat())
            sleep_until(sleep_target)
            continue

        for leg_key in list(state["legs"]):
            leg = state["legs"][leg_key]
            if leg["status"] in ("open", "watching_for_reentry"):
                state["legs"][leg_key] = monitor_leg(client, leg_key, leg, quantity, max_reentries, live)

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
                    f"[{'LIVE' if live else 'PAPER'}] [NIFTY STRANGLE] FINAL EXIT\n"
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
                    f"[{'LIVE' if live else 'PAPER'}] [NIFTY STRANGLE] FINAL EXIT FAILED\n"
                    f"{leg_key.upper()}: {leg['tradingsymbol']}\n"
                    f"{exc}\nMay still be OPEN -- check Jainam manually.",
                    logger,
                )
                leg["status"] = "error"
                leg["error"] = str(exc)
        elif leg["status"] in ("watching_for_reentry", "capped_flat", "target_hit"):
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
    global STATE_FILE, SAME_DAY_EXPIRY
    if args.state_file:
        STATE_FILE = args.state_file
    SAME_DAY_EXPIRY = args.same_day_expiry

    _leg_lifecycle.RESTING_ENTRY_ORDERS = args.resting_entries
    client = get_jainam_client()

    state = load_state()
    if state is not None:
        live = state.get("live", False)
        max_reentries = state.get("max_reentries")
        logger.info(
            "Existing cycle found in %s (entry_time=%s) -- resuming (live=%s, max_reentries=%s).",
            STATE_FILE, state.get("entry_time"), live, max_reentries,
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
                "automatically at 10:30am) before starting a new one in a different mode.",
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
        entry_dt = next_entry_datetime(now)
        if entry_dt > now:
            logger.info("Waiting until %s IST for the entry scan...", entry_dt.isoformat())
            sleep_until(entry_dt)

        state = run_entry_scan(client, args.lots, live, max_reentries)

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
        description="Live short-strangle strategy on NIFTY weekly options via Jainam (XTS Connect)."
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
        "persisted per-leg reentry_count in strategy_state.json, so it holds across restarts too. "
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
        help="Live only: park re-entries as broker-side SELL stop-limit orders instead of reacting to the poll "
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
