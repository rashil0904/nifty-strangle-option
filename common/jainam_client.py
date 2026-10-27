"""
jainam_client.py
==================

Jainam (XTS-based) broker client -- the sole broker client this project
trades through. leg_lifecycle.py and both delta models only ever call
`.instruments(exchange)`, `.quote(keys)`, `.ltp(keys)`, `.place_order(...)`,
`.order_history(order_id)`, and read the constants `VARIETY_REGULAR`,
`PRODUCT_NRML`, `ORDER_TYPE_MARKET` off the client -- this class implements
that exact surface backed by Jainam's XTS Connect API, so every strategy
script, leg_lifecycle.py's stop-loss/re-entry engine, and both delta
models' math/selection logic call a single, consistent client interface.

Jainam white-labels Symphony Fintech's XTS Connect API -- same platform,
same route table, as documented at developers.symphonyfintech.in. The
read-side endpoints below (login, index list, quotes, expiry/strike/symbol
resolution) mirror the ones already used in this account's other project
(Nifty-option-BTST/jainam/broker/xts_client.py), which cross-checked these
specific routes against Symphony's reference Python SDK's route table
rather than trusting the prose docs alone (the two disagreed on at least
one path). Order placement/status below is new -- neither that project nor
this one has ever exercised it against a live Jainam session. See the
CONFIRM BEFORE LIVE list at the bottom of this docstring.

Talks to the documented REST endpoints directly with `requests`; does not
vendor Symphony's own SDK (github.com/symphonyfintech/xts-pythonclient-api-sdk),
since that repo carries no open-source license despite being public. No
python-dotenv dependency either, unlike the reference project -- this
project's existing convention (see get_jainam_client() in
nifty_delta_model.py / sensex_delta_model.py) is plain environment
variables, not a .env file, so JainamClient.from_env() follows that same
convention.

Why only the nearest option expiry is ever fully resolved
--------------------------------------------------------------------------
XTS has no bulk instrument-dump route among the ones this account has
verified -- only point lookups (list expiries for a symbol/series, list
strikes for one expiry, resolve one option symbol at a time). Fully
resolving *every* strike of *every* listed expiry just to build an
equivalent bulk instruments() table would be hundreds of extra round trips
for expiries nothing in this project ever uses. Since every strategy script
here only ever trades the nearest weekly expiry, instruments() instead: (a)
fully resolves every listed futures contract (there are only ever a
handful), and (b) fully resolves the full CE/PE strike grid of the single
nearest unexpired option expiry. That's exactly what
select_nearest_option_expiry + select_option_chain_instruments will end up
using anyway. Requesting a *different* expiry (only possible via the
standalone --expiry-date override on the delta-model CLIs, not from any of
the five live strategy scripts) raises the same "no instruments found for
expiry" ValueError that a genuinely delisted expiry would -- correctly
refusing to guess, just for a different reason.

CONFIRM BEFORE LIVE -- built from API documentation and the patterns in
this account's other Jainam integration, not verified against a live
Jainam session (same caveat that project's own xts_client.py carries for
every read call). Before trusting this for real trading:
  - JAINAM_BASE_URL must be Jainam's own production host from their API
    dashboard, not developers.symphonyfintech.in (Symphony's sandbox).
  - BSE_CM_SEGMENT / BSE_FO_SEGMENT segment IDs (11 / 12 below) are a
    best-effort inference from common XTS deployments -- the reference
    integration only ever exercised NSE segments (1 / 2). Confirm these
    against Jainam's own segment table before trading SENSEX on Jainam.
  - The BSE index list containing "SENSEX" under that exact bare name
    (mirroring "NIFTY 50" / "INDIA VIX" on NSE) is assumed, not verified.
  - The OPTIDX option-expiry-list call (`series="OPTIDX"` against the same
    /instrument/expiryDate route the reference verified for FUTIDX) is an
    extrapolation, not a route independently confirmed working.
  - Order placement (place_order) and order status (order_history) are
    entirely new: field names (exchangeSegment as int vs. string enum,
    productType/orderType/orderSide values, timeInForce, the response
    shape and its OrderStatus vocabulary) follow commonly-documented XTS
    Interactive Order API conventions but are UNTESTED against a live
    Jainam account. Test in the default PAPER mode first -- paper mode
    never calls place_order()/order_history(), so it fully exercises the
    (verified-pattern) read side without ever touching this unverified
    write side.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import date, datetime

import requests

from broker_errors import BrokerAuthError, BrokerError

# --------------------------------------------------------------------------
# Exchange segments (XTS market-data convention: small integers)
# --------------------------------------------------------------------------

NSE_CM_SEGMENT = 1  # verified: used for NIFTY 50 / INDIA VIX index lookups
NSE_FO_SEGMENT = 2  # verified: used for NIFTY futures/options

# CONFIRM BEFORE LIVE -- inferred, not exercised against a live Jainam session.
BSE_CM_SEGMENT = 11
BSE_FO_SEGMENT = 12

# The Interactive (order) API takes the segment as a string name, not the market-data integer.
_ORDER_SEGMENT_NAME = {NSE_CM_SEGMENT: "NSECM", NSE_FO_SEGMENT: "NSEFO", BSE_CM_SEGMENT: "BSECM", BSE_FO_SEGMENT: "BSEFO"}

# NFO/BFO (this project's own exchange constants) -> (underlying symbol, F&O
# segment, cash segment, spot index name). The spot index name is what XTS's
# indexlist endpoint is expected to key SENSEX/NIFTY 50 under.
_UNDERLYING_BY_EXCHANGE = {
    "NFO": {"symbol": "NIFTY", "fut_series": "FUTIDX", "opt_series": "OPTIDX", "fo_segment": NSE_FO_SEGMENT, "cm_segment": NSE_CM_SEGMENT, "spot_index_name": "NIFTY 50"},
    "BFO": {"symbol": "SENSEX", "fut_series": "IF", "opt_series": "IO", "fo_segment": BSE_FO_SEGMENT, "cm_segment": BSE_CM_SEGMENT, "spot_index_name": "SENSEX"},
}

# Spot-quote key prefixes ("NSE:NIFTY 50", "BSE:SENSEX") map to the
# underlying's cash segment, keyed by the *cash* exchange prefix rather
# than NFO/BFO.
_CASH_SEGMENT_BY_PREFIX = {"NSE": NSE_CM_SEGMENT, "BSE": BSE_CM_SEGMENT}
_INDEX_NAME_BY_CASH_SEGMENT = {NSE_CM_SEGMENT: "NIFTY 50", BSE_CM_SEGMENT: "SENSEX"}


# --------------------------------------------------------------------------
# Shared login session
#
# XTS allows only one live token per API key: logging in again invalidates the
# previous token ("Invalid Token", e-session-0007). Several strategy scripts run
# as separate processes with the same keys, so they share one token pair through
# a small cache file instead of each logging in on its own. A call that fails
# with an invalid token re-logs in under a file lock (or adopts a token another
# process already refreshed) and retries once.
# --------------------------------------------------------------------------

_SESSION_FILE = os.environ.get("JAINAM_SESSION_FILE") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", ".jainam_session.json"
)
_LOCK_FILE = _SESSION_FILE + ".lock"
_LOCK_TIMEOUT_SECONDS = 30
_LOCK_STALE_SECONDS = 60


class _FileLock:
    def __enter__(self):
        deadline = time.time() + _LOCK_TIMEOUT_SECONDS
        while True:
            try:
                fd = os.open(_LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                return self
            except FileExistsError:
                try:
                    if time.time() - os.path.getmtime(_LOCK_FILE) > _LOCK_STALE_SECONDS:
                        os.remove(_LOCK_FILE)
                        continue
                except OSError:
                    pass
                if time.time() > deadline:
                    raise BrokerError(f"Timed out waiting for the Jainam session lock {_LOCK_FILE}.")
                time.sleep(0.2)

    def __exit__(self, *exc):
        try:
            os.remove(_LOCK_FILE)
        except OSError:
            pass


def _read_session_cache(base_url: str) -> dict | None:
    try:
        with open(_SESSION_FILE) as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return None
    if data.get("base_url") != base_url or not data.get("market_token") or not data.get("interactive_token"):
        return None
    return data


def _write_session_cache(base_url: str, market_token: str, interactive_token: str) -> None:
    tmp = _SESSION_FILE + ".tmp"
    with open(tmp, "w") as fh:
        json.dump({"base_url": base_url, "market_token": market_token, "interactive_token": interactive_token}, fh)
    os.replace(tmp, _SESSION_FILE)


def _is_invalid_token(exc: BrokerError) -> bool:
    text = str(exc)
    return isinstance(exc, BrokerAuthError) or "Invalid Token" in text or "e-session" in text


def _fmt_expiry(d: date) -> str:
    """XTS instrument-lookup endpoints take expiry as DDMonYYYY, e.g. 30Jan2025."""
    return d.strftime("%d%b%Y")


class JainamClient:
    """Broker client backed by Jainam's XTS Connect API.

    Construct via JainamClient.from_env() -- the constructor itself just
    takes already-obtained tokens.
    """

    # Constants referenced by leg_lifecycle.py's execute_order. Values are
    # self-consistent within this class only -- they're passed straight
    # back into place_order() below.
    VARIETY_REGULAR = "regular"
    PRODUCT_NRML = "NRML"
    PRODUCT_MIS = "MIS"
    ORDER_TYPE_MARKET = "MARKET"
    ORDER_TYPE_STOP_MARKET = "StopMarket"
    ORDER_TYPE_STOP_LIMIT = "StopLimit"

    def __init__(self, base_url: str, market_token: str, interactive_token: str):
        self.base_url = base_url.rstrip("/")
        self.market_token = market_token
        self.interactive_token = interactive_token
        self._session = requests.Session()
        # Trading account the orders are placed under (JAINAM_CLIENT_ID); sent as clientID on order calls.
        self.client_id: str | None = None
        self._index_list_cache: dict[int, dict[str, int]] = {}
        self._instrument_cache: dict[tuple[str, date], list[dict]] = {}
        # "EXCHANGE:tradingsymbol_or_index_name" -> {"segment": int, "id": int}
        self._symbol_map: dict[str, dict] = {}

    @classmethod
    def from_env(cls) -> "JainamClient":
        """Build a client from JAINAM_* environment variables and log in.

        XTS login is a direct, non-interactive appKey/secretKey POST, so
        this logs in fresh each time a script starts -- no manual browser
        login step and no session cache to manage.
        """
        base_url = os.environ.get("JAINAM_BASE_URL")
        market_api_key = os.environ.get("JAINAM_MARKET_API_KEY")
        market_api_secret = os.environ.get("JAINAM_MARKET_API_SECRET")
        interactive_api_key = os.environ.get("JAINAM_INTERACTIVE_API_KEY")
        interactive_api_secret = os.environ.get("JAINAM_INTERACTIVE_API_SECRET")
        source = os.environ.get("JAINAM_SOURCE", "WEBAPI")

        required = {
            "JAINAM_BASE_URL": base_url,
            "JAINAM_MARKET_API_KEY": market_api_key,
            "JAINAM_MARKET_API_SECRET": market_api_secret,
            "JAINAM_INTERACTIVE_API_KEY": interactive_api_key,
            "JAINAM_INTERACTIVE_API_SECRET": interactive_api_secret,
        }
        missing = [name for name, val in required.items() if not val]
        if missing:
            raise EnvironmentError(
                f"Missing Jainam environment variable(s): {', '.join(missing)}."
            )

        session = requests.Session()
        client = cls(base_url=base_url, market_token="", interactive_token="")
        client._session = session
        client.client_id = os.environ.get("JAINAM_CLIENT_ID") or None
        client._creds = {
            "market": ("/apibinarymarketdata/auth/login", market_api_key, market_api_secret, source),
            "interactive": ("/interactive/user/session", interactive_api_key, interactive_api_secret, source),
        }
        # Reuse the shared token pair if another process already logged in; otherwise log in once
        # (under the lock, so concurrent starts don't each log in and invalidate one another).
        with _FileLock():
            cached = _read_session_cache(base_url)
            if cached:
                client.market_token = cached["market_token"]
                client.interactive_token = cached["interactive_token"]
            else:
                client.market_token = _login(session, base_url, *client._creds["market"])
                client.interactive_token = _login(session, base_url, *client._creds["interactive"])
                _write_session_cache(base_url, client.market_token, client.interactive_token)
        return client

    def _refresh_token(self, kind: str, stale_token: str) -> None:
        """Replace an invalid `kind` ("market"/"interactive") token: adopt one another process already
        refreshed, else log in again and share the new token."""
        with _FileLock():
            cached = _read_session_cache(self.base_url)
            tokens = {"market": self.market_token, "interactive": self.interactive_token}
            if cached and cached[f"{kind}_token"] != stale_token:
                tokens[kind] = cached[f"{kind}_token"]
            else:
                tokens[kind] = _login(self._session, self.base_url, *self._creds[kind])
            other = "interactive" if kind == "market" else "market"
            if cached:
                tokens[other] = cached[f"{other}_token"]
            self.market_token, self.interactive_token = tokens["market"], tokens["interactive"]
            _write_session_cache(self.base_url, self.market_token, self.interactive_token)

    def _authed(self, kind: str, call):
        """Run call(token); on an invalid-token failure refresh that token and retry once."""
        token = self.market_token if kind == "market" else self.interactive_token
        try:
            return call(token)
        except BrokerError as exc:
            if not _is_invalid_token(exc):
                raise
            self._refresh_token(kind, token)
            token = self.market_token if kind == "market" else self.interactive_token
            return call(token)

    # --- low-level request helpers ----------------------------------------

    def _market_get(self, path: str, params: dict) -> dict:
        return self._authed("market", lambda t: _request(self._session, "GET", self.base_url, path, t, params=params))

    def _market_post(self, path: str, body: dict) -> dict:
        return self._authed("market", lambda t: _request(self._session, "POST", self.base_url, path, t, json_body=body))

    def _interactive_get(self, path: str, params: dict) -> dict:
        return self._authed("interactive", lambda t: _request(self._session, "GET", self.base_url, path, t, params=params))

    def _interactive_delete(self, path: str, params: dict) -> dict:
        return self._authed("interactive", lambda t: _request(self._session, "DELETE", self.base_url, path, t, params=params))

    def _interactive_post(self, path: str, body: dict) -> dict:
        return self._authed("interactive", lambda t: _request(self._session, "POST", self.base_url, path, t, json_body=body))

    # --- index list / spot resolution --------------------------------------

    def _index_list(self, exchange_segment: int) -> dict[str, int]:
        """Name -> exchangeInstrumentID for all indices in a segment."""
        if exchange_segment not in self._index_list_cache:
            data = self._market_get("/apibinarymarketdata/instruments/indexlist", {"exchangeSegment": exchange_segment})
            mapping = {}
            for entry in data["result"]["indexList"]:
                name, _, instrument_id = entry.rpartition("_")
                mapping[name] = int(instrument_id)
            self._index_list_cache[exchange_segment] = mapping
        return self._index_list_cache[exchange_segment]

    def _resolve_spot_key(self, key: str) -> None:
        """Populate self._symbol_map[key] for a spot index key like 'NSE:NIFTY 50'."""
        prefix, _, _ = key.partition(":")
        cash_segment = _CASH_SEGMENT_BY_PREFIX.get(prefix)
        if cash_segment is None:
            raise BrokerError(f"Don't know how to resolve spot key {key!r} -- unrecognized exchange prefix {prefix!r}.")
        index_name = _INDEX_NAME_BY_CASH_SEGMENT[cash_segment]
        indices = self._index_list(cash_segment)
        if index_name not in indices:
            raise BrokerError(f"{index_name!r} not found in index list for segment {cash_segment}.")
        self._symbol_map[key] = {"segment": cash_segment, "id": indices[index_name]}

    # --- instrument / chain resolution --------------------------------------

    def _get_expiries(self, symbol: str, fo_segment: int, series: str) -> list[date]:
        data = self._market_get(
            "/apibinarymarketdata/instruments/instrument/expiryDate",
            {"exchangeSegment": fo_segment, "series": series, "symbol": symbol},
        )
        return [datetime.fromisoformat(e).date() for e in data["result"]]

    def _resolve_future(self, symbol: str, fo_segment: int, expiry: date, series: str) -> dict:
        data = self._market_get(
            "/apibinarymarketdata/instruments/instrument/futureSymbol",
            {"exchangeSegment": fo_segment, "series": series, "symbol": symbol, "expiryDate": _fmt_expiry(expiry)},
        )
        results = data["result"]
        if not results:
            raise BrokerError(f"No {symbol} future found for expiry={expiry}.")
        result = results[0]
        return {"tradingsymbol": result["Description"], "instrument_id": int(result["ExchangeInstrumentID"])}

    def _get_strike_list(self, symbol: str, fo_segment: int, expiry: date, series: str) -> list[float]:
        data = self._market_get(
            "/apibinarymarketdata/instruments/instrument/strikePrice",
            {
                "exchangeSegment": fo_segment,
                "series": series,
                "symbol": symbol,
                "expiryDate": _fmt_expiry(expiry),
                "optionType": "CE",
            },
        )
        return sorted({float(s) for s in data["result"]})

    def _resolve_option(
        self, symbol: str, fo_segment: int, expiry: date, strike: float, option_type: str, series: str
    ) -> dict:
        data = self._market_get(
            "/apibinarymarketdata/instruments/instrument/optionsymbol",
            {
                "exchangeSegment": fo_segment,
                "series": series,
                "symbol": symbol,
                "expiryDate": _fmt_expiry(expiry),
                "optionType": option_type,
                "strikePrice": strike,
            },
        )
        results = data.get("result")
        if not results:
            raise BrokerError(f"No {symbol} {option_type} found for strike={strike} expiry={expiry}.")
        result = results[0]
        return {
            "tradingsymbol": result["Description"],
            "instrument_id": int(result["ExchangeInstrumentID"]),
            "lot_size": int(result["LotSize"]),
        }

    def instruments(self, exchange: str, use_cache: bool = True, min_expiry: date | None = None) -> list[dict]:
        """Instrument rows for `exchange` ("NFO" or "BFO").

        Only one option expiry's full CE/PE grid is resolved -- see the
        module docstring for why. That's the nearest unexpired expiry on or
        after `min_expiry` (default: today), so a fresh overnight entry can
        require min_expiry=tomorrow to skip an expiry that is today itself
        (which wouldn't leave anything to hold overnight) and get next
        week's grid instead. All listed futures contracts are fully
        resolved regardless (there are only ever a few).
        """
        today = datetime.now().date()
        floor = min_expiry if min_expiry is not None else today
        cache_key = (exchange, floor)
        if use_cache and cache_key in self._instrument_cache:
            return self._instrument_cache[cache_key]

        underlying = _UNDERLYING_BY_EXCHANGE.get(exchange)
        if underlying is None:
            raise BrokerError(f"Unknown exchange {exchange!r} -- expected 'NFO' or 'BFO'.")
        symbol, fo_segment = underlying["symbol"], underlying["fo_segment"]
        fut_series, opt_series = underlying["fut_series"], underlying["opt_series"]

        rows: list[dict] = []

        fut_expiries = sorted(set(self._get_expiries(symbol, fo_segment, fut_series)))
        for expiry in fut_expiries:
            fut = self._resolve_future(symbol, fo_segment, expiry, fut_series)
            key = f"{exchange}:{fut['tradingsymbol']}"
            self._symbol_map[key] = {"segment": fo_segment, "id": fut["instrument_id"]}
            rows.append(
                {
                    "name": symbol,
                    "instrument_type": "FUT",
                    "expiry": expiry,
                    "strike": float("nan"),
                    "tradingsymbol": fut["tradingsymbol"],
                    "lot_size": None,
                }
            )

        opt_expiries = sorted(self._get_expiries(symbol, fo_segment, opt_series))
        unexpired = [e for e in opt_expiries if e >= floor]
        if not unexpired:
            raise BrokerError(f"No {symbol} option expiries on or after {floor} returned by Jainam.")
        nearest_expiry = unexpired[0]

        strikes = self._get_strike_list(symbol, fo_segment, nearest_expiry, opt_series)
        for strike in strikes:
            for option_type in ("CE", "PE"):
                opt = self._resolve_option(symbol, fo_segment, nearest_expiry, strike, option_type, opt_series)
                key = f"{exchange}:{opt['tradingsymbol']}"
                self._symbol_map[key] = {"segment": fo_segment, "id": opt["instrument_id"]}
                rows.append(
                    {
                        "name": symbol,
                        "instrument_type": option_type,
                        "expiry": nearest_expiry,
                        "strike": strike,
                        "tradingsymbol": opt["tradingsymbol"],
                        "lot_size": opt["lot_size"],
                    }
                )

        if use_cache:
            self._instrument_cache[cache_key] = rows
        return rows

    # --- quotes --------------------------------------------------------------

    def quote(self, keys: list[str]) -> dict:
        """quote() -- {key: {"last_price": ..., "depth": {}}}, omitting any key
        XTS returned no quote for (e.g. a strike with zero trades) rather than
        raising -- callers that need every key present (ltp()) check for gaps
        themselves; callers scanning a whole chain (fetch_option_chain_quotes)
        already tolerate and skip individual missing strikes.

        depth is always empty: XTS's touchline response only demonstrably
        carries LastTradedPrice in this account's verified usage, so
        low-liquidity mid-price computation (nifty_delta_model.py's
        _price_and_liquidity) always falls back to last_price.
        """
        instruments_payload = []
        for key in keys:
            if key not in self._symbol_map:
                self._resolve_spot_key(key)
            entry = self._symbol_map[key]
            instruments_payload.append({"exchangeSegment": entry["segment"], "exchangeInstrumentID": entry["id"]})

        data = self._market_post(
            "/apibinarymarketdata/instruments/quotes",
            {"instruments": instruments_payload, "xtsMessageCode": 1501, "publishFormat": "JSON"},
        )
        quotes_by_id = {}
        import json as _json

        for q in data["result"]["listQuotes"]:
            if isinstance(q, str):
                q = _json.loads(q)
            quotes_by_id[int(q["ExchangeInstrumentID"])] = q

        result = {}
        for key in keys:
            entry = self._symbol_map[key]
            q = quotes_by_id.get(entry["id"])
            if q is None:
                continue
            result[key] = {"last_price": float(q["LastTradedPrice"]), "depth": {}}
        return result

    def resolve_key(self, exchange: str, tradingsymbol: str) -> dict:
        """Already-resolved {"segment": int, "id": int} for an instrument instruments()/
        quote() has already looked up. Raises BrokerError if not yet resolved -- callers
        (e.g. market_feed.py's subscribe()) must have gone through instruments() for this
        exchange first, same precondition place_order() already relies on."""
        key = f"{exchange}:{tradingsymbol}"
        entry = self._symbol_map.get(key)
        if entry is None:
            raise BrokerError(f"No resolved instrument for {key} -- call instruments()/quote() for it first.")
        return dict(entry)

    def ltp(self, keys: list[str]) -> dict:
        quotes = self.quote(keys)
        missing = [key for key in keys if key not in quotes]
        if missing:
            raise BrokerError(f"No quote returned for {', '.join(missing)}.")
        return {key: {"last_price": q["last_price"]} for key, q in quotes.items()}

    # --- order placement / status (UNVERIFIED -- see module docstring) -----

    def place_order(
        self,
        variety: str,
        exchange: str,
        tradingsymbol: str,
        transaction_type: str,
        quantity: int,
        product: str,
        order_type: str,
        stop_price: float = 0,
        limit_price: float = 0,
    ) -> str:
        """CONFIRM BEFORE LIVE -- untested against a live Jainam session.

        Only paper mode (the default everywhere in this project) is
        exercised in practice without --live; this path only runs when a
        strategy script is launched with --live.
        """
        key = f"{exchange}:{tradingsymbol}"
        entry = self._symbol_map.get(key)
        if entry is None:
            raise BrokerError(
                f"No resolved instrument for {key} -- it must be returned by instruments() before an order "
                "can be placed for it."
            )
        body = {
            "exchangeSegment": _ORDER_SEGMENT_NAME[entry["segment"]],
            "exchangeInstrumentID": entry["id"],
            "productType": product,
            "orderType": order_type,
            "orderSide": transaction_type,
            "timeInForce": "DAY",
            "disclosedQuantity": 0,
            "orderQuantity": quantity,
            "limitPrice": limit_price,
            "stopPrice": stop_price,
            "orderUniqueIdentifier": uuid.uuid4().hex[:20],
        }
        if self.client_id:
            body["clientID"] = self.client_id
        data = self._interactive_post("/interactive/orders", body)
        try:
            return str(data["result"]["AppOrderID"])
        except KeyError as exc:
            raise BrokerError(f"Order placement response did not contain AppOrderID: {data}") from exc

    def cancel_order(self, order_id: str) -> None:
        """Cancel a resting order (used for the broker-side stop-loss). UNVERIFIED against a live session."""
        params = {"appOrderID": order_id}
        if self.client_id:
            params["clientID"] = self.client_id
        self._interactive_delete("/interactive/orders", params)

    _STATUS_MAP = {
        "Filled": "COMPLETE",
        "Rejected": "REJECTED",
        "Cancelled": "CANCELLED",
        "PendingCancel": "CANCELLED",
    }

    def order_history(self, order_id: str) -> list[dict]:
        """CONFIRM BEFORE LIVE -- untested against a live Jainam session.

        Normalizes XTS's OrderStatus vocabulary to this project's own
        ("COMPLETE" / "REJECTED" / "CANCELLED") so fetch_fill_price() in
        leg_lifecycle.py has one consistent vocabulary to check. Unrecognized
        statuses (e.g.
        "New", "PartiallyFilled") pass through unmapped, which correctly
        tells the caller's poll loop to keep waiting rather than matching
        any terminal state by accident.
        """
        params = {"appOrderID": order_id}
        if self.client_id:
            params["clientID"] = self.client_id
        data = self._interactive_get("/interactive/orders", params)
        result = data.get("result")
        rows = result if isinstance(result, list) else [result] if result else []
        history = []
        for row in rows:
            raw_status = row.get("OrderStatus")
            history.append(
                {
                    "status": self._STATUS_MAP.get(raw_status, raw_status),
                    "average_price": row.get("OrderAverageTradedPrice"),
                    "status_message": row.get("CancelRejectReason") or row.get("OrderStatusMessage"),
                }
            )
        return history


def _login(session: requests.Session, base_url: str, path: str, app_key: str, secret_key: str, source: str) -> str:
    data = _request(
        session, "POST", base_url, path, token=None, json_body={"appKey": app_key, "secretKey": secret_key, "source": source}
    )
    try:
        return data["result"]["token"]
    except KeyError as exc:
        raise BrokerAuthError(f"Login to {path} did not return a token: {data}") from exc


def _request(
    session: requests.Session,
    method: str,
    base_url: str,
    path: str,
    token: str | None,
    params: dict | None = None,
    json_body: dict | None = None,
) -> dict:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = token
    try:
        response = session.request(
            method, base_url.rstrip("/") + path, headers=headers, params=params, json=json_body, timeout=10
        )
    except requests.RequestException as exc:
        raise BrokerError(f"Network error calling Jainam {path}: {exc}") from exc

    if response.status_code in (401, 403):
        raise BrokerAuthError(f"Jainam rejected credentials on {path} (HTTP {response.status_code}): {response.text}")
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise BrokerError(f"Jainam API error on {path} (HTTP {response.status_code}): {response.text}") from exc

    data = response.json()
    if data.get("type") == "error":
        raise BrokerError(f"Jainam API error on {path}: {data.get('description')}")
    return data
