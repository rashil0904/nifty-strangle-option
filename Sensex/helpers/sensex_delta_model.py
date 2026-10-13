"""
sensex_delta_model.py
=======================

Pulls a live SENSEX option chain from Jainam's XTS Connect API (via
common/jainam_client.py) and computes implied volatility (IV) and delta
two ways for every strike/side:

  1. Spot-based, via Black-Scholes, using the BSE SENSEX index spot price.
  2. Futures-based, via Black-76, using the nearest-expiry SENSEX futures
     LTP directly as F.

Both legs share the same math module (bs_price/black76_price, bs_delta/
black76_delta, implied_vol) so the only thing that differs between them is
the input (S vs F) and the corresponding pricing formula.

This is the SENSEX/BSE counterpart of nifty_delta_model.py: identical math
and data-fetching logic, with the index-specific constants swapped --
SENSEX contracts and futures trade on the "BFO" (BSE F&O) exchange segment
rather than "NFO", and the spot symbol is BSE:SENSEX rather than
NSE:NIFTY 50.

Why spot-based BS needs a dividend-yield guess and futures-based Black-76
does not
--------------------------------------------------------------------------
Black-Scholes prices an option off the *spot* price, so it has to separately
account for the cost of carry between today and expiry: financing cost (r)
minus the yield an option holder forgoes by not holding the underlying (q).
SENSEX's dividend yield isn't a single observable number -- it's a blend of
~30 constituents' dividend timing and amounts, most of which is uncertain
and time-varying -- so q is necessarily an estimate (here, a flat CLI
parameter). Any error in that guess biases the S*e^{-qT} carry term, and
therefore every d1, IV, and delta computed from spot.

Black-76 sidesteps this entirely by pricing off the *futures* price, which
is a market-observed number. The futures price already embeds the market's
own aggregate view of carry, dividends, financing cost, and any basis/
specialness effects through arbitrage -- there is no separate dividend
assumption to get wrong.

Why the two deltas are expected to diverge more for longer-dated expiries
--------------------------------------------------------------------------
Two separate mechanisms drive this, and both scale with T -- confirmed by
direct instrumentation on the NIFTY counterpart of this model, not just
derivation (T itself is bit-for-bit identical between the two legs within a
run; it is never the cause of any divergence). The mechanics are identical
here since the math is shared, just applied to SENSEX inputs.

1. Yield-guess error (the dominant effect in practice). The dividend-yield
   guess enters the spot-based model through terms that scale with T (e.g.
   (r - q)T inside d1, and e^{-qT} in the delta itself). For a near-week
   expiry, T is small, so even a meaningfully wrong q barely moves d1 -- the
   error has little time to compound. For a monthly or quarterly expiry, the
   same q error compounds over a much longer horizon, pulling the
   spot-implied forward (S*e^{(r-q)T}) further from the market's actual
   futures price, which shows up as a growing IV (and therefore delta) gap.

2. A structural discount-factor gap (present even with a *perfect* q guess).
   Delta_spot = e^{-qT}*N(d1) and delta_fut = e^{-rT}*N(d1) are different
   Greeks -- sensitivity to spot vs. sensitivity to futures -- so they
   discount by q and r respectively, not by the same rate. Even when d1 is
   identical in both formulas (i.e. F is exactly the theoretically-correct
   S*e^{(r-q)T}, so mechanism 1 contributes nothing), delta_diff still equals
   (e^{-rT} - e^{-qT})*N(d1) exactly. This term grows with T on its own and
   is not an error to correct; it is inherent to comparing "delta w.r.t.
   spot" against "delta w.r.t. futures."

Why F is the raw futures LTP, not a parity-calibrated forward
--------------------------------------------------------------------------
This version uses the futures last-traded price directly as F in the
Black-76 leg, rather than backing out a put-call-parity-implied forward
from the option chain. That's simpler, but it trades away robustness: an
LTP can be stale (no recent trade) or sit at a wide, one-sided print
relative to where the chain is actually quoting, so the futures-based leg
is more timing-sensitive than a parity-calibrated forward would be --
results can shift noticeably if the futures contract is thinly traded at
the moment of the pull. SENSEX futures/options are generally less liquid
than NIFTY's, so this staleness risk is worth keeping in mind more here
than on the NIFTY side.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sys
from datetime import date, datetime
from datetime import time as dtime
from typing import Callable, Optional

import numpy as np
import pandas as pd
from scipy.optimize import brentq
from scipy.stats import norm

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9 fallback
    from backports.zoneinfo import ZoneInfo  # type: ignore

# broker_errors.py / jainam_client.py live in ../../common relative to this
# file (shared with Nifty and every strategy script) -- not on the default
# import path when this module is run standalone, so add it explicitly.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "common"))

from broker_errors import BrokerAuthError, BrokerError
from jainam_client import JainamClient

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

IST = ZoneInfo("Asia/Kolkata")
EXPIRY_SETTLEMENT_TIME = dtime(15, 30)  # exchange F&O expiry cutoff, IST
DAYS_PER_YEAR = 365.0  # calendar-day convention; swap for a trading-day count later
MAX_INSTRUMENTS_PER_QUOTE = 50  # per-call cap for quote() batching -- verified live limit (e-quotes-0003)
IV_LOWER_BOUND = 0.01
IV_UPPER_BOUND = 3.0
VOL_SOLVER_XTOL = 1e-6

SENSEX_SPOT_SYMBOL = "BSE:SENSEX"
UNDERLYING_NAME = "SENSEX"
BFO_EXCHANGE = "BFO"  # BSE F&O segment: where SENSEX options/futures trade

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Session-level cache for the (large) instrument master, keyed by exchange.
_instrument_cache: dict[str, pd.DataFrame] = {}


# --------------------------------------------------------------------------
# Auth
# --------------------------------------------------------------------------


def get_jainam_client() -> JainamClient:
    """Build a JainamClient from JAINAM_* environment variables (see
    common/jainam_client.py). Jainam's login is a direct, non-interactive
    appKey/secretKey POST, so this logs in fresh every call -- no session
    cache to manage."""
    return JainamClient.from_env()


# --------------------------------------------------------------------------
# Instrument master + quote fetching
# --------------------------------------------------------------------------


def fetch_instruments(client: JainamClient, exchange: str = BFO_EXCHANGE, use_cache: bool = True) -> pd.DataFrame:
    """Fetch (and cache for the session) the instrument master for `exchange`."""
    if use_cache and exchange in _instrument_cache:
        return _instrument_cache[exchange]

    try:
        raw = client.instruments(exchange)
    except BrokerAuthError as exc:
        raise RuntimeError(
            "Jainam credentials expired or invalid while fetching instruments. Regenerate them and retry."
        ) from exc
    except BrokerError as exc:
        raise RuntimeError(f"Jainam API error while fetching instruments: {exc}") from exc

    df = pd.DataFrame(raw)
    if df.empty:
        raise RuntimeError(f"Jainam returned no instruments for exchange {exchange!r}.")
    df["expiry"] = pd.to_datetime(df["expiry"]).dt.date

    if use_cache:
        _instrument_cache[exchange] = df
    return df


def select_option_chain_instruments(instruments: pd.DataFrame, expiry_date: date) -> pd.DataFrame:
    """Filter the instrument master down to SENSEX CE/PE contracts for expiry_date."""
    chain = instruments[
        (instruments["name"] == UNDERLYING_NAME)
        & (instruments["instrument_type"].isin(["CE", "PE"]))
        & (instruments["expiry"] == expiry_date)
    ].copy()

    if chain.empty:
        available = sorted(
            instruments.loc[
                (instruments["name"] == UNDERLYING_NAME) & (instruments["instrument_type"].isin(["CE", "PE"])),
                "expiry",
            ].unique()
        )
        raise ValueError(
            f"No SENSEX option instruments found for expiry {expiry_date}. Available expiries: {available}"
        )
    return chain


def select_nearest_option_expiry(instruments: pd.DataFrame, as_of: Optional[date] = None) -> date:
    """Return the nearest unexpired SENSEX option expiry date in the instrument master."""
    as_of = as_of or datetime.now(IST).date()
    expiries = instruments.loc[
        (instruments["name"] == UNDERLYING_NAME)
        & (instruments["instrument_type"].isin(["CE", "PE"]))
        & (instruments["expiry"] >= as_of),
        "expiry",
    ]
    if expiries.empty:
        raise ValueError("No unexpired SENSEX option expiries found in the instrument master.")
    return expiries.min()


def select_nearest_future(instruments: pd.DataFrame, as_of: Optional[date] = None) -> pd.Series:
    """Return the instrument row for the nearest unexpired SENSEX futures contract."""
    as_of = as_of or datetime.now(IST).date()
    futs = instruments[
        (instruments["name"] == UNDERLYING_NAME)
        & (instruments["instrument_type"] == "FUT")
        & (instruments["expiry"] >= as_of)
    ]
    if futs.empty:
        raise ValueError("No unexpired SENSEX futures contracts found in the instrument master.")
    return futs.sort_values("expiry").iloc[0]


def select_future_by_expiry(instruments: pd.DataFrame, futures_expiry: date) -> pd.Series:
    """Return the instrument row for the SENSEX futures contract expiring exactly on futures_expiry."""
    futs = instruments[
        (instruments["name"] == UNDERLYING_NAME)
        & (instruments["instrument_type"] == "FUT")
        & (instruments["expiry"] == futures_expiry)
    ]
    if futs.empty:
        available = sorted(
            instruments.loc[
                (instruments["name"] == UNDERLYING_NAME) & (instruments["instrument_type"] == "FUT"),
                "expiry",
            ].unique()
        )
        raise ValueError(
            f"No SENSEX futures contract found expiring on {futures_expiry}. Available futures expiries: {available}"
        )
    return futs.iloc[0]


def select_future(
    instruments: pd.DataFrame,
    futures_expiry: Optional[date] = None,
    as_of: Optional[date] = None,
) -> pd.Series:
    """Pick the futures contract: exact match on futures_expiry if given, else nearest unexpired."""
    if futures_expiry is not None:
        return select_future_by_expiry(instruments, futures_expiry)
    return select_nearest_future(instruments, as_of=as_of)


def _chunk(seq: list, size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _price_and_liquidity(quote: dict) -> tuple[float, bool]:
    """Best bid/ask midpoint from depth; falls back to last_price if depth is empty."""
    depth = quote.get("depth", {}) or {}
    bids = depth.get("buy", []) or []
    asks = depth.get("sell", []) or []
    best_bid = next((lvl["price"] for lvl in bids if lvl.get("quantity", 0) > 0), None)
    best_ask = next((lvl["price"] for lvl in asks if lvl.get("quantity", 0) > 0), None)

    if best_bid and best_ask:
        return (best_bid + best_ask) / 2.0, False
    return quote.get("last_price", np.nan), True


def fetch_option_chain_quotes(client: JainamClient, chain_instruments: pd.DataFrame) -> pd.DataFrame:
    """Batch-quote every CE/PE tradingsymbol in chain_instruments through client.quote().

    Chunks the request if it exceeds MAX_INSTRUMENTS_PER_QUOTE. Returns a
    DataFrame with columns: strike, instrument_type, tradingsymbol, price,
    low_liquidity.
    """
    symbols = [f"{BFO_EXCHANGE}:{ts}" for ts in chain_instruments["tradingsymbol"]]

    quotes: dict = {}
    for batch in _chunk(symbols, MAX_INSTRUMENTS_PER_QUOTE):
        try:
            quotes.update(client.quote(batch))
        except BrokerAuthError as exc:
            raise RuntimeError("Jainam credentials expired or invalid while fetching option quotes.") from exc
        except BrokerError as exc:
            raise RuntimeError(f"Jainam API error fetching option quotes: {exc}") from exc

    rows = []
    for _, inst in chain_instruments.iterrows():
        key = f"{BFO_EXCHANGE}:{inst['tradingsymbol']}"
        q = quotes.get(key)
        if q is None:
            logger.warning("No quote returned for %s; skipping.", key)
            continue
        price, low_liquidity = _price_and_liquidity(q)
        rows.append(
            {
                "strike": inst["strike"],
                "instrument_type": inst["instrument_type"],
                "tradingsymbol": inst["tradingsymbol"],
                "price": price,
                "low_liquidity": low_liquidity,
            }
        )

    if not rows:
        raise RuntimeError("Jainam returned no usable option quotes for the requested expiry.")
    return pd.DataFrame(rows)


def fetch_spot_and_futures(client: JainamClient, futures_tradingsymbol: str) -> tuple[float, float]:
    """Fetch the SENSEX index spot price and the nearest-expiry future's price."""
    fut_key = f"{BFO_EXCHANGE}:{futures_tradingsymbol}"
    try:
        result = client.quote([SENSEX_SPOT_SYMBOL, fut_key])
    except BrokerAuthError as exc:
        raise RuntimeError("Jainam credentials expired or invalid while fetching spot/futures quote.") from exc
    except BrokerError as exc:
        raise RuntimeError(f"Jainam API error fetching spot/futures quote: {exc}") from exc

    if SENSEX_SPOT_SYMBOL not in result or fut_key not in result:
        raise RuntimeError("Jainam did not return both spot and futures quotes; response was incomplete.")

    spot = result[SENSEX_SPOT_SYMBOL]["last_price"]
    futures_price = result[fut_key]["last_price"]
    return spot, futures_price


# --------------------------------------------------------------------------
# Time to expiry
# --------------------------------------------------------------------------


def time_to_expiry_years(
    expiry_date: date,
    days_per_year: float = DAYS_PER_YEAR,
    now: Optional[datetime] = None,
) -> float:
    """Years from now (IST) to expiry_date at EXPIRY_SETTLEMENT_TIME IST, calendar-day convention."""
    now = now or datetime.now(IST)
    expiry_dt = datetime.combine(expiry_date, EXPIRY_SETTLEMENT_TIME, tzinfo=IST)
    seconds_remaining = (expiry_dt - now).total_seconds()
    if seconds_remaining <= 0:
        raise ValueError(f"Expiry {expiry_date} at {EXPIRY_SETTLEMENT_TIME} IST has already passed.")
    return seconds_remaining / (days_per_year * 24 * 3600)


# --------------------------------------------------------------------------
# Pricing math: Black-Scholes (spot) and Black-76 (futures)
# --------------------------------------------------------------------------


def bs_price(S: float, K: float, T: float, r: float, q: float, sigma: float, option_type: str) -> float:
    """Black-Scholes price with a continuous dividend yield q."""
    if sigma <= 0 or T <= 0:
        return float("nan")
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if option_type == "CE":
        return S * math.exp(-q * T) * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    elif option_type == "PE":
        return K * math.exp(-r * T) * norm.cdf(-d2) - S * math.exp(-q * T) * norm.cdf(-d1)
    raise ValueError(f"Unknown option_type {option_type!r}")


def bs_delta(S: float, K: float, T: float, r: float, q: float, sigma: float, option_type: str) -> float:
    if sigma <= 0 or T <= 0:
        return float("nan")
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    if option_type == "CE":
        return math.exp(-q * T) * norm.cdf(d1)
    elif option_type == "PE":
        return math.exp(-q * T) * (norm.cdf(d1) - 1)
    raise ValueError(f"Unknown option_type {option_type!r}")


def black76_price(F: float, K: float, T: float, r: float, sigma: float, option_type: str) -> float:
    """Black-76 price off futures price F; no dividend-yield input needed."""
    if sigma <= 0 or T <= 0:
        return float("nan")
    d1 = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if option_type == "CE":
        return math.exp(-r * T) * (F * norm.cdf(d1) - K * norm.cdf(d2))
    elif option_type == "PE":
        return math.exp(-r * T) * (K * norm.cdf(-d2) - F * norm.cdf(-d1))
    raise ValueError(f"Unknown option_type {option_type!r}")


def black76_delta(F: float, K: float, T: float, r: float, sigma: float, option_type: str) -> float:
    if sigma <= 0 or T <= 0:
        return float("nan")
    d1 = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))
    if option_type == "CE":
        return math.exp(-r * T) * norm.cdf(d1)
    elif option_type == "PE":
        return -math.exp(-r * T) * norm.cdf(-d1)
    raise ValueError(f"Unknown option_type {option_type!r}")


def implied_vol(
    price_func: Callable[[float], float],
    market_price: float,
    lower: float = IV_LOWER_BOUND,
    upper: float = IV_UPPER_BOUND,
) -> float:
    """Solve for sigma such that price_func(sigma) == market_price via brentq.

    price_func is any single-argument (sigma -> price) closure, so this works
    for both bs_price and black76_price without knowing which one it's given.
    Returns NaN (rather than raising) if market_price is unusable or falls
    outside the no-arbitrage bounds implied by [lower, upper].
    """
    if market_price is None or not np.isfinite(market_price) or market_price <= 0:
        return float("nan")

    def objective(sigma: float) -> float:
        return price_func(sigma) - market_price

    try:
        low_val = objective(lower)
        high_val = objective(upper)
        if not np.isfinite(low_val) or not np.isfinite(high_val) or low_val * high_val > 0:
            return float("nan")
        return brentq(objective, lower, upper, xtol=VOL_SOLVER_XTOL)
    except (ValueError, RuntimeError):
        return float("nan")


# --------------------------------------------------------------------------
# Greeks table
# --------------------------------------------------------------------------


def build_greeks_table(chain: pd.DataFrame, S: float, F: float, T: float, r: float, q: float) -> pd.DataFrame:
    """Compute spot-based and futures-based IV/delta for every strike in chain."""
    pivot_price = chain.pivot_table(index="strike", columns="instrument_type", values="price")
    pivot_liq = chain.pivot_table(index="strike", columns="instrument_type", values="low_liquidity", aggfunc="max")

    rows = []
    for strike in sorted(pivot_price.index):
        price_row = pivot_price.loc[strike]
        liq_row = pivot_liq.loc[strike]

        call_mid = price_row.get("CE", np.nan)
        put_mid = price_row.get("PE", np.nan)
        call_low_liq = bool(liq_row.get("CE")) if pd.notna(liq_row.get("CE")) else False
        put_low_liq = bool(liq_row.get("PE")) if pd.notna(liq_row.get("PE")) else False

        call_iv_spot = implied_vol(lambda s, K=strike: bs_price(S, K, T, r, q, s, "CE"), call_mid)
        put_iv_spot = implied_vol(lambda s, K=strike: bs_price(S, K, T, r, q, s, "PE"), put_mid)
        call_iv_fut = implied_vol(lambda s, K=strike: black76_price(F, K, T, r, s, "CE"), call_mid)
        put_iv_fut = implied_vol(lambda s, K=strike: black76_price(F, K, T, r, s, "PE"), put_mid)

        call_delta_spot = bs_delta(S, strike, T, r, q, call_iv_spot, "CE") if np.isfinite(call_iv_spot) else float("nan")
        put_delta_spot = bs_delta(S, strike, T, r, q, put_iv_spot, "PE") if np.isfinite(put_iv_spot) else float("nan")
        call_delta_fut = black76_delta(F, strike, T, r, call_iv_fut, "CE") if np.isfinite(call_iv_fut) else float("nan")
        put_delta_fut = black76_delta(F, strike, T, r, put_iv_fut, "PE") if np.isfinite(put_iv_fut) else float("nan")

        call_delta_spot = round(call_delta_spot, 2)
        put_delta_spot = round(put_delta_spot, 2)
        call_delta_fut = round(call_delta_fut, 2)
        put_delta_fut = round(put_delta_fut, 2)

        rows.append(
            {
                "strike": strike,
                "low_liquidity": call_low_liq or put_low_liq,
                "call_iv_spot": call_iv_spot,
                "call_delta_spot": call_delta_spot,
                "put_iv_spot": put_iv_spot,
                "put_delta_spot": put_delta_spot,
                "call_iv_fut": call_iv_fut,
                "call_delta_fut": call_delta_fut,
                "put_iv_fut": put_iv_fut,
                "put_delta_fut": put_delta_fut,
                "delta_diff_call": round(call_delta_fut - call_delta_spot, 2),
                "delta_diff_put": round(put_delta_fut - put_delta_spot, 2),
            }
        )

    return pd.DataFrame(rows)


# --------------------------------------------------------------------------
# Output: console diagnostics
# --------------------------------------------------------------------------


def print_diagnostics(
    spot: float,
    futures_price: float,
    q: float,
    r: float,
    T: float,
    futures_tradingsymbol: str,
    futures_contract_expiry: date,
    futures_source: str,
    expiry_date: Optional[date] = None,
) -> None:
    print("=" * 78)
    print("MODEL INPUTS" if expiry_date is None else f"MODEL INPUTS (expiry {expiry_date})")
    print("=" * 78)
    print(f"SENSEX spot price (S, spot-based leg):               {spot:.2f}")
    print(f"Futures contract used for F ({futures_source}):")
    print(f"  tradingsymbol: {futures_tradingsymbol}")
    print(f"  expiry:        {futures_contract_expiry}")
    print(f"Futures LTP (F, futures-based leg):                  {futures_price:.2f}")
    print(f"Time to expiry (years, {DAYS_PER_YEAR:.0f}-day convention):       {T:.5f}")
    print(f"Risk-free rate r:                                    {r:.4%}")
    print(f"Assumed dividend yield q (spot-based leg only):      {q:.4%}")
    print("=" * 78)


def print_summary(summary: pd.DataFrame) -> None:
    print("=" * 78)
    print("DELTA DIVERGENCE BY EXPIRY (futures-based minus spot-based)")
    print("=" * 78)
    print(summary.to_string(index=False, float_format=lambda x: f"{x:.6f}"))
    print("=" * 78)


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def run_single_expiry(
    client: JainamClient,
    instruments: pd.DataFrame,
    expiry_date: date,
    r: float,
    q: float,
    days_per_year: float = DAYS_PER_YEAR,
    futures_expiry: Optional[date] = None,
) -> tuple[pd.DataFrame, float, float, float, pd.Series, str]:
    """Run the fetch/solve pipeline for one expiry.

    futures_expiry, if given, pins the exact futures contract (expiry ==
    futures_expiry) used for F instead of auto-selecting the nearest
    unexpired one.

    Returns (results, spot, futures_price, T, future_row, futures_source).
    """
    chain_instruments = select_option_chain_instruments(instruments, expiry_date)

    if futures_expiry is not None:
        future_row = select_future_by_expiry(instruments, futures_expiry)
        futures_source = "override via --futures-expiry"
    else:
        future_row = select_nearest_future(instruments)
        futures_source = "auto-selected: nearest unexpired"

    logger.info(
        "Futures contract used for F: %s (expiry %s, %s)",
        future_row["tradingsymbol"],
        future_row["expiry"],
        futures_source,
    )

    logger.info("Fetching option chain quotes for %d instruments...", len(chain_instruments))
    chain = fetch_option_chain_quotes(client, chain_instruments)

    logger.info("Fetching spot and futures quotes...")
    spot, futures_price = fetch_spot_and_futures(client, future_row["tradingsymbol"])

    T = time_to_expiry_years(expiry_date, days_per_year=days_per_year)

    results = build_greeks_table(chain, S=spot, F=futures_price, T=T, r=r, q=q)
    return results, spot, futures_price, T, future_row, futures_source


def run(
    expiry_dates: list,
    r: float,
    q: float,
    days_per_year: float = DAYS_PER_YEAR,
    futures_expiry: Optional[date] = None,
) -> pd.DataFrame:
    """Run the model for one or more expiries. Prints results to the console only.

    expiry_dates: if empty/None, the nearest unexpired SENSEX option expiry is
    auto-selected (single-expiry run) -- so running with no --expiry-date at
    all, at any point during market hours, just works off whatever is
    currently nearest. Pass one or more explicit dates to override.

    Single expiry: prints the full per-strike table and returns it.

    Multiple expiries: runs the same per-expiry pipeline for each date (full
    per-strike table printed for each), then additionally prints a
    delta-divergence-vs-T summary and returns that summary instead.

    futures_expiry, if given, pins the exact SENSEX futures contract used for
    F (applies to every expiry in expiry_dates) instead of auto-selecting
    the nearest unexpired one.

    Only the nearest option expiry is ever resolvable (see
    common/jainam_client.py's module docstring, including the CONFIRM
    BEFORE LIVE note on BSE segment IDs); passing multiple or non-nearest
    --expiry-date values will raise a "no instruments found for expiry"
    error.
    """
    client = get_jainam_client()

    logger.info("Fetching BFO instrument master...")
    instruments = fetch_instruments(client)

    if not expiry_dates:
        nearest_expiry = select_nearest_option_expiry(instruments)
        logger.info("No --expiry-date given; auto-selected nearest unexpired option expiry: %s", nearest_expiry)
        expiry_dates = [nearest_expiry]
    else:
        expiry_dates = sorted(set(expiry_dates))

    summary_rows = []
    last_results: Optional[pd.DataFrame] = None

    for expiry_date in expiry_dates:
        logger.info("Processing expiry %s...", expiry_date)
        results, spot, futures_price, T, future_row, futures_source = run_single_expiry(
            client, instruments, expiry_date, r=r, q=q, days_per_year=days_per_year, futures_expiry=futures_expiry
        )
        last_results = results

        print_diagnostics(
            spot=spot,
            futures_price=futures_price,
            q=q,
            r=r,
            T=T,
            futures_tradingsymbol=future_row["tradingsymbol"],
            futures_contract_expiry=future_row["expiry"],
            futures_source=futures_source,
            expiry_date=expiry_date,
        )
        print(results.to_string(index=False))

        summary_rows.append(
            {
                "expiry_date": expiry_date,
                "T": T,
                "mean_abs_delta_diff_call": results["delta_diff_call"].abs().mean(),
                "mean_abs_delta_diff_put": results["delta_diff_put"].abs().mean(),
                "max_abs_delta_diff_call": results["delta_diff_call"].abs().max(),
                "max_abs_delta_diff_put": results["delta_diff_put"].abs().max(),
            }
        )

    if len(expiry_dates) == 1:
        return last_results

    summary = pd.DataFrame(summary_rows)
    print_summary(summary)
    return summary


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute SENSEX option IV and delta, spot-based (Black-Scholes) vs futures-based (Black-76)."
    )
    parser.add_argument(
        "--expiry-date",
        dest="expiry_dates",
        action="append",
        type=date.fromisoformat,
        default=None,
        help="Option expiry date, YYYY-MM-DD. Repeat the flag for multiple expiries "
        "(e.g. --expiry-date 2026-10-01 --expiry-date 2026-10-29) to get a "
        "multi-expiry delta-divergence-vs-T summary instead of single-expiry output. "
        "If omitted entirely, the nearest unexpired SENSEX option expiry is auto-selected "
        "-- so running the script with no --expiry-date during market hours just works "
        "off whatever expiry is currently nearest.",
    )
    parser.add_argument(
        "--futures-expiry",
        type=date.fromisoformat,
        default=None,
        help="Optional: exact SENSEX futures contract expiry (YYYY-MM-DD) to use for F in the "
        "Black-76 leg. If omitted, the nearest unexpired futures contract is "
        "auto-selected (previous behavior). Errors out if no futures contract expires on this "
        "exact date, rather than silently falling back.",
    )
    parser.add_argument(
        "--r",
        type=float,
        default=0.065,
        help="Risk-free rate, annualized decimal (default: 0.065 = 6.5%%).",
    )
    parser.add_argument(
        "--q",
        type=float,
        default=0.013,
        help="Dividend yield for the spot-based Black-Scholes leg, annualized decimal (default: 0.013 = 1.3%%).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> int:
    args = parse_args(argv)
    try:
        run(
            expiry_dates=args.expiry_dates,
            r=args.r,
            q=args.q,
            futures_expiry=args.futures_expiry,
        )
    except (EnvironmentError, RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
