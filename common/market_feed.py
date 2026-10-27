"""
market_feed.py
==================

Shared XTS Connect market-data websocket feed -- replaces the 15-second REST
LTP poll with push updates, used by every strategy script's monitoring loop
(delta_strangle_strategy.py / delta_breakdown_strategy.py, both indices).

Exactly ONE underlying socket connection is held across however many of the
4 strategy processes happen to be running at once: whichever process starts
first becomes the "owner" (opens the real socket, maintains the
subscription list, writes every tick to a shared cache file); every other
process is a "follower" that only declares which instruments it wants (via
its own small subscription-request file) and reads the owner's cache. If
the owner dies or exits, a follower notices the stale heartbeat and takes
over -- no process needs to know in advance whether it will be the owner.

This mirrors the cross-process sharing jainam_client.py already does for
the login session (`.jainam_session.json` + `_FileLock`), just for a
different resource. It intentionally does NOT reuse that file lock (it is
hardcoded to the session file's path) or that JainamClient instance (the
feed builds its own dedicated client for the socket's connect/reconnect/
token-refresh, so the caller's own JainamClient -- used for orders and the
REST fallback below -- is never touched from more than one thread).

Correctness note on cache trust: an instrument's cached price is trusted
whenever the feed itself is live (owner heartbeat fresh + connected) and
*any* tick has ever arrived for that key -- regardless of how old that
tick is. LTP is correct until the next trade, so a quiet/illiquid strike
going a long time between ticks is not "stale data", and gating trust on
tick-recency would cause spurious REST fallbacks for exactly the strikes
this project trades (0.3-delta weekly options, not always liquid).

CONFIRM BEFORE LIVE -- same caveat class as jainam_client.py's own
unverified surface, built from documented XTS Connect Socket.IO
conventions, not verified against a live Jainam session:
  - SOCKET_PATH ("/apibinarymarketdata/socket.io") mirrors this account's
    already-confirmed REST route prefix quirk (vs. the documented
    "/apimarketdata/"), but the socket path itself has never been
    exercised against a live Jainam session.
  - The tick event name ("1501-json-full") and the "userID" query param
    (set to JAINAM_MARKET_API_KEY, per explicit instruction -- there is no
    separately-confirmed XTS "userID" distinct from the API key for this
    account) are best-effort from commonly-documented XTS Connect
    conventions, not confirmed working.
  - If the socket never connects for any reason (bad route, bad auth,
    python-socketio not installed), every strategy script keeps working
    correctly via the REST fallback in get_ltp() below -- this module
    degrades safely by design, it does not replace REST polling so much
    as make it the exception path instead of the common one.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from typing import Optional

from jainam_client import JainamClient
from leg_lifecycle import fetch_ltp as _rest_fetch_ltp

_logger = logging.getLogger("market_feed")

# --------------------------------------------------------------------------
# Shared files (siblings of .jainam_session.json)
# --------------------------------------------------------------------------

_REPO_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
_FEED_OWNER_FILE = os.path.join(_REPO_ROOT, ".jainam_feed_owner.json")
_FEED_CACHE_FILE = os.path.join(_REPO_ROOT, ".jainam_feed_cache.json")
_FEED_SUBS_DIR = os.path.join(_REPO_ROOT, ".jainam_feed_subs")
_FEED_LOCK_FILE = _FEED_OWNER_FILE + ".lock"

_LOCK_TIMEOUT_SECONDS = 10
_LOCK_STALE_SECONDS = 30

HEARTBEAT_INTERVAL_SECONDS = 2
OWNER_STALE_SECONDS = 20  # generous vs. heartbeat interval -- avoids false failover during a slow reconnect
SUB_STALE_SECONDS = 20
REST_FALLBACK_FLOOR_SECONDS = 15  # the requested buffer: never REST-poll a key faster than this while degraded

SOCKET_PATH = "/apibinarymarketdata/socket.io"
XTS_MESSAGE_CODE = 1501  # same touchline code quote() already uses
TICK_EVENT = "1501-json-full"


class _FileLock:
    """Own lock for feed-ownership election -- jainam_client.py's _FileLock is hardcoded
    to the session file's path, so this is a separate (identically-shaped) one."""

    def __enter__(self):
        deadline = time.time() + _LOCK_TIMEOUT_SECONDS
        while True:
            try:
                fd = os.open(_FEED_LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                return self
            except FileExistsError:
                try:
                    if time.time() - os.path.getmtime(_FEED_LOCK_FILE) > _LOCK_STALE_SECONDS:
                        os.remove(_FEED_LOCK_FILE)
                        continue
                except OSError:
                    pass
                if time.time() > deadline:
                    raise RuntimeError(f"Timed out waiting for the market feed lock {_FEED_LOCK_FILE}.")
                time.sleep(0.1)

    def __exit__(self, *exc):
        try:
            os.remove(_FEED_LOCK_FILE)
        except OSError:
            pass


def _read_json(path: str) -> Optional[dict]:
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _write_json_atomic(path: str, data: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def _read_owner_file() -> Optional[dict]:
    return _read_json(_FEED_OWNER_FILE)


def _owner_is_stale(owner: Optional[dict]) -> bool:
    return owner is None or (time.time() - owner.get("heartbeat", 0)) > OWNER_STALE_SECONDS


class MarketFeed:
    """One process-local handle onto the shared feed. Safe to construct once per
    process and reuse for every leg that process monitors."""

    def __init__(self) -> None:
        self._pid = os.getpid()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._wanted_lock = threading.Lock()
        self._wanted: dict[str, dict] = {}  # "EXCHANGE:tradingsymbol" -> {"segment":.., "id":..}

        self._rest_last_fetch: dict[str, float] = {}
        self._rest_last_value: dict[str, float] = {}

        self._is_owner = False
        self._epoch: Optional[str] = None
        self._sio = None  # socketio.Client, created lazily only once this process becomes owner
        self._ever_connected = False  # once True, reconnection is left entirely to socketio's own
        # background reconnect thread (reconnection=True) -- calling .connect() ourselves on top of
        # that double-connects the same token, which the server treats as a second login and kicks
        # the socket with "logged out by another user". We only drive .connect() ourselves before the
        # very first successful connection, where no such background thread exists yet.
        self._feed_client: Optional[JainamClient] = None
        self._subscribed_keys: set[str] = set()
        self._id_to_key: dict[int, str] = {}
        self._cache_lock = threading.Lock()

    @classmethod
    def from_env(cls) -> "MarketFeed":
        return cls()

    # --- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run_loop, name="market-feed", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        try:
            own_sub_file = os.path.join(_FEED_SUBS_DIR, f"{self._pid}.json")
            if os.path.exists(own_sub_file):
                os.remove(own_sub_file)
        except OSError:
            pass
        if self._sio is not None:
            try:
                self._sio.disconnect()
            except Exception:
                pass

    # --- public API used by strategy scripts ----------------------------------

    def subscribe(self, exchange: str, tradingsymbol: str, client: JainamClient) -> None:
        """Declare that this process wants `tradingsymbol` streamed. Safe to call repeatedly
        (e.g. once per leg at entry, again on re-entry) -- it's a plain dict update."""
        entry = client.resolve_key(exchange, tradingsymbol)
        key = f"{exchange}:{tradingsymbol}"
        with self._wanted_lock:
            self._wanted[key] = entry
        self._write_own_sub_file()

    def get_ltp(self, exchange: str, tradingsymbol: str, client: JainamClient, logger: logging.Logger) -> float:
        """Primary LTP source for the monitoring loop: cache hit if the feed is live and has
        ever seen a tick for this key, else a REST fallback floored at REST_FALLBACK_FLOOR_SECONDS
        per key (see module docstring for why tick recency itself is not the trust signal)."""
        key = f"{exchange}:{tradingsymbol}"
        if self._feed_is_live():
            cached = self._read_cache_entry(key)
            if cached is not None:
                return float(cached["last_price"])
        return self._rest_fallback(key, tradingsymbol, exchange, client, logger)

    # --- shared-file plumbing -------------------------------------------------

    def _write_own_sub_file(self) -> None:
        # Both the main thread (subscribe()) and the background loop thread (owner/follower
        # tick) call this; _wanted_lock also serializes the write itself (not just the dict
        # snapshot) so two threads never race on the same tmp filename underneath
        # _write_json_atomic -- one thread's os.replace() would otherwise consume the other's
        # in-flight tmp file out from under it.
        os.makedirs(_FEED_SUBS_DIR, exist_ok=True)
        path = os.path.join(_FEED_SUBS_DIR, f"{self._pid}.json")
        with self._wanted_lock:
            _write_json_atomic(path, {"pid": self._pid, "heartbeat": time.time(), "instruments": dict(self._wanted)})

    def _read_subscription_union(self) -> dict[str, dict]:
        union: dict[str, dict] = {}
        if not os.path.isdir(_FEED_SUBS_DIR):
            return union
        now = time.time()
        for name in os.listdir(_FEED_SUBS_DIR):
            if not name.endswith(".json"):
                continue
            data = _read_json(os.path.join(_FEED_SUBS_DIR, name))
            if not data or now - data.get("heartbeat", 0) > SUB_STALE_SECONDS:
                continue
            union.update(data.get("instruments", {}))
        return union

    def _read_cache_entry(self, key: str) -> Optional[dict]:
        data = _read_json(_FEED_CACHE_FILE) or {}
        return data.get(key)

    def _feed_is_live(self) -> bool:
        owner = _read_owner_file()
        if _owner_is_stale(owner):
            return False
        return bool(owner.get("connected"))

    def _write_owner_file(self, connected: bool) -> None:
        _write_json_atomic(
            _FEED_OWNER_FILE, {"pid": self._pid, "epoch": self._epoch, "heartbeat": time.time(), "connected": connected}
        )

    # --- REST fallback ---------------------------------------------------------

    def _rest_fallback(
        self, key: str, tradingsymbol: str, exchange: str, client: JainamClient, logger: logging.Logger
    ) -> float:
        now = time.time()
        last_fetch = self._rest_last_fetch.get(key)
        if last_fetch is not None and now - last_fetch < REST_FALLBACK_FLOOR_SECONDS and key in self._rest_last_value:
            return self._rest_last_value[key]
        value = _rest_fetch_ltp(client, tradingsymbol, logger, exchange)
        self._rest_last_fetch[key] = now
        self._rest_last_value[key] = value
        return value

    # --- background loop: election + owner/follower roles ----------------------

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                if self._is_owner:
                    self._owner_tick()
                else:
                    self._follower_tick()
            except Exception:
                _logger.exception("market_feed loop iteration failed")
            self._stop_event.wait(HEARTBEAT_INTERVAL_SECONDS)

    def _follower_tick(self) -> None:
        self._write_own_sub_file()
        owner = _read_owner_file()
        if _owner_is_stale(owner):
            self._try_claim_ownership()

    def _try_claim_ownership(self) -> None:
        with _FileLock():
            owner = _read_owner_file()
            if not _owner_is_stale(owner):
                return  # someone else claimed it first -- stay a follower
            self._epoch = uuid.uuid4().hex
            self._write_owner_file(connected=False)
        self._is_owner = True
        self._subscribed_keys = set()
        _logger.info("market_feed: became owner (pid=%d, epoch=%s)", self._pid, self._epoch)

    def _demote(self) -> None:
        self._is_owner = False
        if self._sio is not None:
            try:
                self._sio.disconnect()
            except Exception:
                pass
            self._sio = None
        self._ever_connected = False  # a future re-claim must drive its own fresh connect
        self._subscribed_keys = set()
        _logger.warning("market_feed: fenced out (another process's record won) -- demoting to follower")

    def _owner_tick(self) -> None:
        owner = _read_owner_file()
        if owner is None or owner.get("pid") != self._pid or owner.get("epoch") != self._epoch:
            self._demote()
            return
        self._write_own_sub_file()
        self._write_owner_file(connected=self._socket_connected())
        if self._socket_connected():
            self._refresh_subscriptions()
        elif not self._ever_connected:
            # Only before the very first successful connect do we drive .connect() ourselves --
            # once connected at least once, socketio's own reconnection=True background thread
            # owns reconnecting, and calling .connect() again on top of it would attempt a second
            # concurrent connection on the same token.
            self._attempt_connect()

    def _socket_connected(self) -> bool:
        return self._sio is not None and bool(getattr(self._sio, "connected", False))

    def _attempt_connect(self) -> None:
        try:
            import socketio
        except ImportError:
            _logger.warning(
                "market_feed: python-socketio not installed -- staying on REST fallback. "
                "Install with: pip install \"python-socketio[client]\""
            )
            return

        if self._feed_client is None:
            self._feed_client = JainamClient.from_env()
        market_api_key = os.environ.get("JAINAM_MARKET_API_KEY")
        if not market_api_key:
            _logger.warning("market_feed: JAINAM_MARKET_API_KEY not set -- cannot connect socket.")
            return

        if self._sio is None:
            self._sio = socketio.Client(reconnection=True, reconnection_delay=2, reconnection_delay_max=10)
            self._sio.on("connect", self._on_connect)
            self._sio.on(TICK_EVENT, self._on_tick)

        token = self._feed_client.market_token
        url = f"{self._feed_client.base_url}/?token={token}&userID={market_api_key}&publishFormat=JSON&broadcastMode=Full"
        try:
            self._sio.connect(url, socketio_path=SOCKET_PATH, transports=["websocket"], wait_timeout=10)
        except Exception as exc:
            text = str(exc)
            if "Invalid Token" in text or "e-session" in text or "401" in text:
                try:
                    self._feed_client._refresh_token("market", token)
                except Exception:
                    _logger.warning("market_feed: token refresh for socket reconnect failed: %s", exc)
            _logger.warning("market_feed: socket connect failed: %s", exc)

    def _on_connect(self) -> None:
        self._ever_connected = True
        self._subscribed_keys = set()
        self._refresh_subscriptions()

    def _refresh_subscriptions(self) -> None:
        union = self._read_subscription_union()
        self._id_to_key = {entry["id"]: key for key, entry in union.items()}
        new_keys = [key for key in union if key not in self._subscribed_keys]
        if not new_keys or not self._socket_connected():
            return
        payload = [{"exchangeSegment": union[key]["segment"], "exchangeInstrumentID": union[key]["id"]} for key in new_keys]
        try:
            self._sio.emit("subscribe", {"instruments": payload, "xtsMessageCode": XTS_MESSAGE_CODE})
            self._subscribed_keys.update(new_keys)
        except Exception as exc:
            _logger.warning("market_feed: subscribe emit failed: %s", exc)

    def _on_tick(self, data) -> None:
        rows = data if isinstance(data, list) else [data]
        now = time.time()
        updates: dict[str, dict] = {}
        for row in rows:
            if isinstance(row, str):
                try:
                    row = json.loads(row)
                except ValueError:
                    continue
            try:
                instrument_id = int(row["ExchangeInstrumentID"])
                price = float(row["LastTradedPrice"])
            except (KeyError, TypeError, ValueError):
                continue
            key = self._id_to_key.get(instrument_id)
            if key:
                updates[key] = {"last_price": price, "ts": now}
        if not updates:
            return
        with self._cache_lock:
            current = _read_json(_FEED_CACHE_FILE) or {}
            current.update(updates)
            _write_json_atomic(_FEED_CACHE_FILE, current)
