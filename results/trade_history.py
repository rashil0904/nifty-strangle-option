"""
trade_history.py
==================

Append-only, permanent ledger of every CLOSED leg across all 4 live
strategies (Nifty/Sensex strangle + breakdown). Same role as
nifty-option-btst's results/trade_history.py: each strategy's own state
file only ever holds the CURRENT cycle (overwritten/cleared every cycle,
gitignored as local run state -- see leg_lifecycle.save_state/clear_state).
Something has to accumulate history once a leg's cycle ends, or there is
no P&L to report. This is that something.

Unlike BTST (one long+short pair = one trade), this project's unit of
"closed" is a single LEG (call or put) of a single strategy's cycle,
because a leg can stop out and re-enter several times before its cycle's
own final exit -- leg_lifecycle.py's state already accumulates all of that
into leg["realized_pnl"], so gross_pnl here is read directly off the
leg, never recomputed (that's the one thing simpler than BTST, which has
to derive P&L from separate long/short entry/exit prices).

trade_history.json lives alongside this file, at results/trade_history.json,
and is tracked in git (a permanent ledger, not transient run state).

open_positions.json (also here) is the companion live view: a snapshot of
every leg still mid-cycle (not yet archived above), refreshed on every
entry/stop-loss/re-entry event by write_open_position_snapshot() and
removed by remove_open_position() once exit_all() finally archives it.
Unlike trade_history.json this is NOT an append-only ledger -- it's
overwritten in place, same as a strategy's own state file, and is
gitignored for the same reason (current run state, not history).

Both writers live inside common/leg_lifecycle.py's shared event functions
(record_stop_loss, handle_profit_target, book_reentry) and each script's
own place_entry()/exit_all(), which only ever know their own logger, not a
strategy display name -- _LOGGER_TO_STRATEGY resolves logger.name (already
in scope at every one of those call sites) to the same display names
trade_history.json's "strategy" field uses, so no function signature
anywhere needs a new parameter threaded through it.
"""

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
HISTORY_PATH = Path(__file__).resolve().parent / "trade_history.json"
OPEN_POSITIONS_PATH = Path(__file__).resolve().parent / "open_positions.json"

_LOGGER_TO_STRATEGY = {
    "delta_strangle_strategy": "Nifty Strangle",
    "sensex_delta_strangle_strategy": "Sensex Strangle",
    "delta_breakdown_strategy": "Nifty Breakdown",
    "sensex_delta_breakdown_strategy": "Sensex Breakdown",
}


def append_closed_leg(strategy: str, leg_key: str, leg: dict, quantity: int, lots: int) -> dict:
    """Append one record for a leg that just reached a terminal status in exit_all()
    ("closed", "closed_flat", or "error"). Callers must only call this for a leg that
    actually entered at least once this cycle (leg["entry_price"] is not None) --
    a leg that never entered isn't a trade, there's nothing to log.

    Returns the record it wrote.
    """
    status = leg.get("status")
    realized_pnl = leg.get("realized_pnl", 0.0)
    # An "error" leg may still be open on the broker -- only report a figure if
    # something was actually realized (a stop-loss fired before the failure);
    # otherwise 0.0 would misleadingly read as "closed flat, no P&L" instead of
    # "unknown, check Jainam manually".
    gross_pnl = None if (status == "error" and leg.get("stop_count", 0) == 0) else realized_pnl

    record = {
        "strategy": strategy,
        "leg_key": leg_key,
        "tradingsymbol": leg.get("tradingsymbol"),
        "strike": leg.get("strike"),
        "option_type": leg.get("option_type"),
        "entry_date": leg["entry_time"][:10] if leg.get("entry_time") else None,
        "entry_price": leg.get("entry_price"),
        "exit_date": datetime.now(IST).date().isoformat(),
        "status": status,
        "stop_count": leg.get("stop_count", 0),
        "reentry_count": leg.get("reentry_count", 0),
        "quantity": quantity,
        "lots": lots,
        "gross_pnl": gross_pnl,
        "archived_at": datetime.now(IST).isoformat(),
    }

    HISTORY_PATH.parent.mkdir(exist_ok=True)
    history = []
    if HISTORY_PATH.exists():
        try:
            history = json.loads(HISTORY_PATH.read_text())
        except (json.JSONDecodeError, OSError):
            history = []
    history.append(record)

    tmp = HISTORY_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(history, indent=2) + "\n")
    tmp.replace(HISTORY_PATH)
    return record


def load_trade_history() -> list[dict]:
    """Every archived leg, oldest first. A leg closed by hand mid-cycle is archived at that moment and
    again by its strategy's exit_all() at the deadline; the second copy is dropped here."""
    if not HISTORY_PATH.exists():
        return []
    try:
        history = json.loads(HISTORY_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return []
    seen = set()
    unique = []
    for rec in history:
        key = (rec.get("strategy"), rec.get("leg_key"), rec.get("tradingsymbol"), rec.get("entry_date"), rec.get("entry_price"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(rec)
    return unique


def _read_open_positions() -> dict:
    if not OPEN_POSITIONS_PATH.exists():
        return {}
    try:
        return json.loads(OPEN_POSITIONS_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _write_open_positions(data: dict) -> None:
    OPEN_POSITIONS_PATH.parent.mkdir(exist_ok=True)
    tmp = OPEN_POSITIONS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n")
    tmp.replace(OPEN_POSITIONS_PATH)


def write_open_position_snapshot(logger_name: str, leg_key: str, leg: dict, quantity: int) -> None:
    """Record/refresh the live snapshot for one leg right after an entry, stop-loss,
    profit-target, or re-entry event. Safe to call every time one of those happens --
    it always fully replaces this leg's own entry, never accumulates."""
    strategy = _LOGGER_TO_STRATEGY.get(logger_name, logger_name)
    data = _read_open_positions()
    data.setdefault(strategy, {})[leg_key] = {
        "tradingsymbol": leg.get("tradingsymbol"),
        "strike": leg.get("strike"),
        "option_type": leg.get("option_type"),
        "entry_price": leg.get("entry_price"),
        "status": leg.get("status"),
        "stop_count": leg.get("stop_count", 0),
        "reentry_count": leg.get("reentry_count", 0),
        "realized_pnl": leg.get("realized_pnl", 0.0),
        "quantity": quantity,
        "updated_at": datetime.now(IST).isoformat(),
    }
    _write_open_positions(data)


def remove_open_position(logger_name: str, leg_key: str) -> None:
    """Drop a leg's live snapshot once exit_all() has archived it to trade_history.json --
    a no-op if it was never there (e.g. a leg that never entered this cycle)."""
    strategy = _LOGGER_TO_STRATEGY.get(logger_name, logger_name)
    data = _read_open_positions()
    legs = data.get(strategy)
    if legs and leg_key in legs:
        del legs[leg_key]
        if not legs:
            del data[strategy]
        _write_open_positions(data)


def load_open_positions() -> dict:
    return _read_open_positions()
