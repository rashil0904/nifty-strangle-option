"""
3:28pm overnight hedge for the four live strategies -- run from cron at 15:27:

    27 15 * * 1-5 ... python3 scripts/hedge_328.py --live >> logs/hedge_328.log 2>&1

Holds until 15:28:00 IST, then for every OPEN short leg in the four strategies'
state files buys a same-side, further-OTM option of the same expiry whose
premium is 10-15% of that leg's LTP at 3:28 (the one nearest the short
strike; same quantity as the short; no hedge, and an alert, if nothing is in
the band). The leg's entry price is logged for context only.

Paper by default (no orders); pass --live to place real NRML market buys.
The hedges are NOT tracked by the strategies -- nothing here squares them off.
A marker file stops a re-run the same day from buying them twice.

Only legs with status "open" in the strategies' own state files are hedged
(the broker positions list is entity-wide and includes other projects' trades).
Anything else that is short -- e.g. a manual trade -- can be added with
--extra SYMBOL:QTY[:ENTRY_PRICE].
"""

from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import sys
import time
from datetime import date, datetime, time as dtime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)  # for the `results` package imported by common/leg_lifecycle.py
sys.path.insert(0, os.path.join(ROOT, "common"))
sys.path.insert(0, os.path.join(ROOT, "Nifty", "helpers"))
sys.path.insert(0, os.path.join(ROOT, "Sensex", "helpers"))

import leg_lifecycle as ll  # noqa: E402
import nifty_delta_model as nifty  # noqa: E402
import sensex_delta_model as sensex  # noqa: E402
import telegram_notify  # noqa: E402

FIRE_AT = dtime(15, 28)
LATEST = dtime(15, 29, 30)
HEDGE_MIN_FRACTION = 0.10
HEDGE_MAX_FRACTION = 0.15
MARKER = os.path.join(ROOT, "logs", ".hedge_328_{date}.json")

UNDERLYINGS = {"NIFTY": (nifty, "NFO"), "SENSEX": (sensex, "BFO")}
# (label, underlying, state-file glob)
STATE_FILES = [
    ("NIFTY STRANGLE", "NIFTY", "strategy_state.json"),
    ("SENSEX STRANGLE", "SENSEX", "sensex_strategy_state.json"),
    ("NIFTY BREAKDOWN", "NIFTY", "breakdown_strategy_state_*.json"),
    ("SENSEX BREAKDOWN", "SENSEX", "sensex_breakdown_strategy_state_*.json"),
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("hedge_328")


def open_shorts(extras: list[str]) -> list[dict]:
    """Open short legs: {label, underlying, tradingsymbol, option_type, strike, expiry, qty, entry_price}."""
    legs = []
    for label, underlying, pattern in STATE_FILES:
        for path in sorted(glob.glob(os.path.join(ROOT, pattern))):
            with open(path) as fh:
                state = json.load(fh)
            for key, leg in state.get("legs", {}).items():
                if leg.get("status") == "open":
                    legs.append({
                        "label": f"{label} {key}", "underlying": underlying, "tradingsymbol": leg["tradingsymbol"],
                        "option_type": leg["option_type"], "strike": float(leg["strike"]),
                        "expiry": date.fromisoformat(str(state["expiry"])[:10]),
                        "qty": int(state["quantity"]), "entry_price": leg.get("entry_price"),
                    })
                elif leg.get("status") == "error":
                    logger.warning("%s %s %s is in ERROR state -- not hedged (use --extra if it is still short).",
                                   label, key, leg.get("tradingsymbol"))
    legs_by_symbol = {leg["tradingsymbol"]: leg for leg in legs}
    for extra in extras:
        symbol, qty, *entry = extra.split(":")
        base = legs_by_symbol.get(symbol)
        if base is None:
            base = _leg_from_symbol(symbol)
            legs.append(base)
        base = dict(base, label=f"EXTRA {symbol}", qty=int(qty), entry_price=float(entry[0]) if entry else None)
        legs.append(base)
        legs.remove(legs_by_symbol[symbol]) if symbol in legs_by_symbol else None
    return legs


def _leg_from_symbol(symbol: str) -> dict:
    """Resolve an --extra symbol (not in any state file) from the instrument master."""
    for underlying, (module, exchange) in UNDERLYINGS.items():
        if symbol.startswith(underlying):
            client = module.get_jainam_client()
            instruments = module.fetch_instruments(client)
            row = instruments[instruments["tradingsymbol"] == symbol].iloc[0]
            return {"label": symbol, "underlying": underlying, "tradingsymbol": symbol,
                    "option_type": row["instrument_type"], "strike": float(row["strike"]),
                    "expiry": row["expiry"], "qty": 0, "entry_price": None}
    raise SystemExit(f"--extra: cannot tell the underlying of {symbol}")


def pick_hedge(chain, leg: dict, ltp: float):
    """Same-side, further-OTM row priced 10-15% of ltp -- the one nearest the short strike, or None."""
    side = chain[chain["instrument_type"] == leg["option_type"]]
    side = side[(side["strike"] > leg["strike"]) if leg["option_type"] == "CE" else (side["strike"] < leg["strike"])]
    in_band = side[(side["price"] >= HEDGE_MIN_FRACTION * ltp) & (side["price"] <= HEDGE_MAX_FRACTION * ltp)]
    if in_band.empty:
        return None
    return in_band.iloc[(in_band["strike"] - leg["strike"]).abs().argsort().iloc[0]]


def wait_until_fire() -> None:
    now = datetime.now(ll.IST)
    if now.time() > LATEST:
        raise SystemExit(f"Started at {now:%H:%M:%S}, past {LATEST} -- too late to hedge.")
    fire = datetime.combine(now.date(), FIRE_AT, tzinfo=ll.IST)
    if now < fire:
        logger.info("Waiting until %s IST...", FIRE_AT)
        time.sleep((fire - now).total_seconds())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--live", action="store_true", help="place real orders (default: paper)")
    parser.add_argument("--now", action="store_true", help="skip the wait for 15:28 (testing)")
    parser.add_argument("--extra", action="append", default=[], metavar="SYMBOL:QTY[:ENTRY]",
                        help="also hedge a short that is not in a state file")
    args = parser.parse_args()

    today = datetime.now(ll.IST).date().isoformat()
    marker_path = MARKER.format(date=today)
    done = set(json.load(open(marker_path))) if os.path.exists(marker_path) else set()

    legs = open_shorts(args.extra)
    if not legs:
        logger.info("No open short legs -- nothing to hedge.")
        return 0
    if not args.now:
        wait_until_fire()

    logger.info("%s MODE -- %d open short leg(s).", "LIVE" if args.live else "PAPER", len(legs))
    lines, clients = [], {}
    for leg in legs:
        hedge_key = f"{leg['label']}|{leg['tradingsymbol']}"
        if args.live and hedge_key in done:
            logger.info("%s already hedged today -- skipping.", hedge_key)
            continue
        try:
            module, exchange = UNDERLYINGS[leg["underlying"]]
            if leg["underlying"] not in clients:
                clients[leg["underlying"]] = (module.get_jainam_client(), {})
            client, grids = clients[leg["underlying"]]
            # the broker resolves only one expiry grid (nearest on/after min_expiry), so load each leg's own
            if leg["expiry"] not in grids:
                grids[leg["expiry"]] = module.fetch_instruments(client, min_expiry=leg["expiry"])
            instruments = grids[leg["expiry"]]
            chain = module.fetch_option_chain_quotes(
                client, module.select_option_chain_instruments(instruments, leg["expiry"])
            )
            ltp = float(chain.loc[chain["tradingsymbol"] == leg["tradingsymbol"], "price"].iloc[0])
            hedge = pick_hedge(chain, leg, ltp)
            if hedge is None:
                raise RuntimeError(f"no further-OTM option priced {HEDGE_MIN_FRACTION:.0%}-{HEDGE_MAX_FRACTION:.0%} of LTP {ltp:.2f}")
            logger.info(
                "%s short %s x%d: entry %s, LTP %.2f; hedge BUY %s (%.2f = %.1f%% of LTP).",
                leg["label"], leg["tradingsymbol"], leg["qty"], leg["entry_price"], ltp,
                hedge["tradingsymbol"], hedge["price"], 100 * hedge["price"] / ltp,
            )
            fill = ll.execute_order(client, logger, hedge["tradingsymbol"], "BUY", leg["qty"], args.live, exchange)
            lines.append(f"{leg['label']}: short {leg['tradingsymbol']} (entry {leg['entry_price']}, LTP {ltp:.2f}) "
                         f"-> BUY {leg['qty']} x {hedge['tradingsymbol']} @ {fill:.2f}")
            if args.live:
                done.add(hedge_key)
                with open(marker_path, "w") as fh:
                    json.dump(sorted(done), fh)
        except Exception as exc:  # one failed leg must not stop the others
            logger.critical("Hedge FAILED for %s %s: %s", leg["label"], leg["tradingsymbol"], exc)
            lines.append(f"{leg['label']}: HEDGE FAILED for {leg['tradingsymbol']} -- {exc}")

    if args.live and lines:
        telegram_notify.send("3:28 hedges\n" + "\n".join(lines), logger)
    return 0


if __name__ == "__main__":
    sys.exit(main())
