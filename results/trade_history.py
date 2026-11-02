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
"""

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
HISTORY_PATH = Path(__file__).resolve().parent / "trade_history.json"


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
    if not HISTORY_PATH.exists():
        return []
    try:
        return json.loads(HISTORY_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return []
