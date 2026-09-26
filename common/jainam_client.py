"""
jainam_client.py
==================

Jainam (XTS-based) broker client, built to be a drop-in stand-in for
kiteconnect.KiteConnect wherever this project's code takes a `kite`
parameter: leg_lifecycle.py and both delta models only ever call
`.instruments(exchange)`, `.quote(keys)`, `.ltp(keys)`, `.place_order(...)`,
`.order_history(order_id)`, and read the constants `VARIETY_REGULAR`,
`PRODUCT_NRML`, `ORDER_TYPE_MARKET` off the client -- this class implements
that exact surface backed by Jainam's XTS Connect API instead of Kite, so
every strategy script, leg_lifecycle.py's stop-loss/re-entry engine, and
both delta models' math/selection logic run completely unmodified against
either broker. Nothing outside this file (and broker_client.py, which
constructs it) knows or cares that Jainam isn't Kite.

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
project's existing convention (see get_kite_client() in nifty_delta_model.py
/ sensex_delta_model.py) is plain environment variables, not a .env file,
so JainamClient.from_env() follows that same convention.

Why only the nearest option expiry is ever fully resolved
--------------------------------------------------------------------------
Kite's instruments(exchange) is a single bulk dump of every NFO instrument,
which is why fetch_instruments() in the delta models can cache it once and
have select_nearest_option_expiry/select_option_chain_instruments/etc. do
pure local DataFrame filtering with no further I/O. XTS has no equivalent
bulk dump among the routes this account has verified -- only point lookups
(list expiries for a symbol/series, list strikes for one expiry, resolve
one option symbol at a time). Fully resolving *every* strike of *every*
listed expiry just to build an equivalent bulk instruments() table would be
hundreds of extra round trips for expiries nothing in this project ever
uses. Since every strategy script here only ever trades the nearest weekly
expiry, instruments() instead: (a) fully resolves every listed futures
contract (there are only ever a handful), and (b) fully resolves the full
CE/PE strike grid of the single nearest unexpired option expiry. That's
exactly what select_nearest_option_expiry + select_option_chain_instruments
will end up using anyway. Requesting a *different* expiry (only possible
via the standalone --expiry-date override on the delta-model CLIs, not from
any of the five live strategy scripts) raises the same "no instruments
found for expiry" ValueError the Kite path already raises for a genuinely
delisted expiry -- correctly refusing to guess, just for a different reason.

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
    Jainam account. Test with --broker jainam in the default PAPER mode
    first -- paper mode never calls place_order()/order_history() on any
    broker, so it fully exercises the (verified-pattern) read side without
    ever touching this unverified write side.
"""

from __future__ import annotations

import os
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

# NFO/BFO (this project's own exchange constants) -> (underlying symbol, F&O
# segment, cash segment, spot index name). The spot index name is what XTS's
# indexlist endpoint is expected to key SENSEX/NIFTY 50 under.
_UNDERLYING_BY_EXCHANGE = {
    "NFO": {"symbol": "NIFTY", "fo_segment": NSE_FO_SEGMENT, "cm_segment": NSE_CM_SEGMENT, "spot_index_name": "NIFTY 50"},
    "BFO": {"symbol": "SENSEX", "fo_segment": BSE_FO_SEGMENT, "cm_segment": BSE_CM_SEGMENT, "spot_index_name": "SENSEX"},
}

# Spot-quote key prefixes ("NSE:NIFTY 50", "BSE:SENSEX") map to the
# underlying's cash segment, keyed by the *cash* exchange prefix rather
# than NFO/BFO.
_CASH_SEGMENT_BY_PREFIX = {"NSE": NSE_CM_SEGMENT, "BSE": BSE_CM_SEGMENT}
_INDEX_NAME_BY_CASH_SEGMENT = {NSE_CM_SEGMENT: "NIFTY 50", BSE_CM_SEGMENT: "SENSEX"}


def _fmt_expiry(d: date) -> str:
    """XTS instrument-lookup endpoints take expiry as DDMonYYYY, e.g. 30Jan2025."""
    return d.strftime("%d%b%Y")


class JainamClient:
    """Kite-compatible client backed by Jainam's XTS Connect API.

    Construct via JainamClient.from_env() -- the constructor itself just
    takes already-obtained tokens, mirroring KiteConnect's own
    set_access_token() pattern.
    """

    # Kite-compatible constants referenced by leg_lifecycle.py's execute_order.
    # Values are self-consistent within this class only -- they're passed
    # straight back into place_order() below, never compared against Kite's
    # own literal values, so they don't need to match Kite's.
    VARIETY_REGULAR = "regular"
    PRODUCT_NRML = "NRML"
    PRODUCT_MIS = "MIS"
    ORDER_TYPE_MARKET = "MARKET"

    def __init__(self, base_url: str, market_token: str, interactive_token: str):
        self.base_url = base_url.rstrip("/")
        self.market_token = market_token
        self.interactive_token = interactive_token
        self._session = requests.Session()
        self._index_list_cache: dict[int, dict[str, int]] = {}
        self._instrument_cache: dict[str, list[dict]] = {}
        # "EXCHANGE:tradingsymbol_or_index_name" -> {"segment": int, "id": int}
        self._symbol_map: dict[str, dict] = {}

    @classmethod
    def from_env(cls) -> "JainamClient":
        """Build a client from JAINAM_* environment variables and log in.

        Unlike Kite, XTS login is a direct appKey/secretKey POST with no
        interactive browser step -- there is deliberately no Jainam
        equivalent of kite_login_helper.py's daily manual login or session
        cache; this logs in fresh each time a script starts.
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
        market_token = _login(session, base_url, "/apimarketdata/auth/login", market_api_key, market_api_secret, source)
        interactive_token = _login(
            session, base_url, "/interactive/user/session", interactive_api_key, interactive_api_secret, source
        )
        client = cls(base_url=base_url, market_token=market_token, interactive_token=interactive_token)
        client._session = session
        return client

    # --- low-level request helpers ----------------------------------------

    def _market_get(self, path: str, params: dict) -> dict:
        return _request(self._session, "GET", self.base_url, path, self.market_token, params=params)

    def _market_post(self, path: str, body: dict) -> dict:
        return _request(self._session, "POST", self.base_url, path, self.market_token, json_body=body)

    def _interactive_get(self, path: str, params: dict) -> dict:
        return _request(self._session, "GET", self.base_url, path, self.interactive_token, params=params)

    def _interactive_post(self, path: str, body: dict) -> dict:
        return _request(self._session, "POST", self.base_url, path, self.interactive_token, json_body=body)

    # --- index list / spot resolution --------------------------------------

    def _index_list(self, exchange_segment: int) -> dict[str, int]:
        """Name -> exchangeInstrumentID for all indices in a segment."""
        if exchange_segment not in self._index_list_cache:
            data = self._market_get("/apimarketdata/instruments/indexlist", {"exchangeSegment": exchange_segment})
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
            "/apimarketdata/instruments/instrument/expiryDate",
            {"exchangeSegment": fo_segment, "series": series, "symbol": symbol},
        )
        return [datetime.fromisoformat(e).date() for e in data["result"]]

    def _resolve_future(self, symbol: str, fo_segment: int, expiry: date) -> dict:
        data = self._market_get(
            "/apimarketdata/instruments/instrument/futureSymbol",
            {"exchangeSegment": fo_segment, "series": "FUTIDX", "symbol": symbol, "expiryDate": _fmt_expiry(expiry)},
        )
        result = data["result"]
        return {"tradingsymbol": result["Description"], "instrument_id": int(result["ExchangeInstrumentID"])}

    def _get_strike_list(self, symbol: str, fo_segment: int, expiry: date) -> list[float]:
        data = self._market_get(
            "/apimarketdata/instruments/instrument/strikePrice",
            {
                "exchangeSegment": fo_segment,
                "series": "OPTIDX",
                "symbol": symbol,
                "expiryDate": _fmt_expiry(expiry),
                "optionType": "CE",
            },
        )
        return sorted({float(s) for s in data["result"]})

    def _resolve_option(self, symbol: str, fo_segment: int, expiry: date, strike: float, option_type: str) -> dict:
        data = self._market_get(
            "/apimarketdata/instruments/instrument/optionsymbol",
            {
                "exchangeSegment": fo_segment,
                "series": "OPTIDX",
                "symbol": symbol,
                "expiryDate": _fmt_expiry(expiry),
                "optionType": option_type,
                "strikePrice": strike,
            },
        )
        result = data.get("result")
        if not result:
            raise BrokerError(f"No {symbol} {option_type} found for strike={strike} expiry={expiry}.")
        return {
            "tradingsymbol": result["Description"],
            "instrument_id": int(result["ExchangeInstrumentID"]),
            "lot_size": int(result["LotSize"]),
        }

    def instruments(self, exchange: str, use_cache: bool = True) -> list[dict]:
        """Kite-shaped instrument rows for `exchange` ("NFO" or "BFO").

        Only the nearest unexpired option expiry's full CE/PE grid is
        resolved -- see the module docstring for why. All listed futures
        contracts are fully resolved (there are only ever a few).
        """
        if use_cache and exchange in self._instrument_cache:
            return self._instrument_cache[exchange]

        underlying = _UNDERLYING_BY_EXCHANGE.get(exchange)
        if underlying is None:
            raise BrokerError(f"Unknown exchange {exchange!r} -- expected 'NFO' or 'BFO'.")
        symbol, fo_segment = underlying["symbol"], underlying["fo_segment"]

        rows: list[dict] = []

        fut_expiries = self._get_expiries(symbol, fo_segment, "FUTIDX")
        for expiry in fut_expiries:
            fut = self._resolve_future(symbol, fo_segment, expiry)
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

        opt_expiries = sorted(self._get_expiries(symbol, fo_segment, "OPTIDX"))
        today = datetime.now().date()
        unexpired = [e for e in opt_expiries if e >= today]
        if not unexpired:
            raise BrokerError(f"No unexpired {symbol} option expiries returned by Jainam.")
        nearest_expiry = unexpired[0]

        strikes = self._get_strike_list(symbol, fo_segment, nearest_expiry)
        for strike in strikes:
            for option_type in ("CE", "PE"):
                opt = self._resolve_option(symbol, fo_segment, nearest_expiry, strike, option_type)
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
            self._instrument_cache[exchange] = rows
        return rows

    # --- quotes --------------------------------------------------------------

    def quote(self, keys: list[str]) -> dict:
        """Kite-shaped quote() -- {key: {"last_price": ..., "depth": {}}}.

        depth is always empty: XTS's touchline response only demonstrably
        carries LastTradedPrice in this account's verified usage, so
        low-liquidity mid-price computation (nifty_delta_model.py's
        _price_and_liquidity) always falls back to last_price for Jainam,
        same as it would for any Kite quote with no resting bid/ask.
        """
        instruments_payload = []
        for key in keys:
            if key not in self._symbol_map:
                self._resolve_spot_key(key)
            entry = self._symbol_map[key]
            instruments_payload.append({"exchangeSegment": entry["segment"], "exchangeInstrumentID": entry["id"]})

        data = self._market_post(
            "/apimarketdata/instruments/quotes",
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
                raise BrokerError(f"No quote returned for {key}.")
            result[key] = {"last_price": float(q["LastTradedPrice"]), "depth": {}}
        return result

    def ltp(self, keys: list[str]) -> dict:
        quotes = self.quote(keys)
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
    ) -> str:
        """CONFIRM BEFORE LIVE -- untested against a live Jainam session.

        Only paper mode (the default everywhere in this project) is
        exercised in practice without --live; this path only runs when a
        strategy script is launched with both --live and --broker jainam.
        """
        key = f"{exchange}:{tradingsymbol}"
        entry = self._symbol_map.get(key)
        if entry is None:
            raise BrokerError(
                f"No resolved instrument for {key} -- it must be returned by instruments() before an order "
                "can be placed for it."
            )
        body = {
            "exchangeSegment": entry["segment"],
            "exchangeInstrumentID": entry["id"],
            "productType": product,
            "orderType": order_type,
            "orderSide": transaction_type,
            "timeInForce": "DAY",
            "disclosedQuantity": 0,
            "orderQuantity": quantity,
            "limitPrice": 0,
            "stopPrice": 0,
            "orderUniqueIdentifier": uuid.uuid4().hex[:20],
        }
        data = self._interactive_post("/interactive/orders", body)
        try:
            return str(data["result"]["AppOrderID"])
        except KeyError as exc:
            raise BrokerError(f"Order placement response did not contain AppOrderID: {data}") from exc

    _STATUS_MAP = {
        "Filled": "COMPLETE",
        "Rejected": "REJECTED",
        "Cancelled": "CANCELLED",
        "PendingCancel": "CANCELLED",
    }

    def order_history(self, order_id: str) -> list[dict]:
        """CONFIRM BEFORE LIVE -- untested against a live Jainam session.

        Normalizes XTS's OrderStatus vocabulary to Kite's ("COMPLETE" /
        "REJECTED" / "CANCELLED") so fetch_fill_price() in leg_lifecycle.py
        needs no broker-specific branching. Unrecognized statuses (e.g.
        "New", "PartiallyFilled") pass through unmapped, which correctly
        tells the caller's poll loop to keep waiting rather than matching
        any terminal state by accident.
        """
        data = self._interactive_get("/interactive/orders", {"appOrderID": order_id})
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
