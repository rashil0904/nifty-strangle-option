"""
kite_login_helper.py
=====================

One-time (really: once-per-trading-day) interactive helper to generate a
Kite Connect access token via the login flow.

This lives in common/ (not under Nifty/ or Sensex/) because it's genuinely
shared: a Kite access token is issued per Zerodha account, not per index, so
one login here is enough for both Nifty/helpers/nifty_delta_model.py and
Sensex/helpers/sensex_delta_model.py (and the strategy scripts that use
them) on the same trading day.

Usage:
    export KITE_API_KEY=...
    export KITE_API_SECRET=...
    python3 kite_login_helper.py
    # open the printed URL, log in, paste back the request_token
    # the token is now cached for the rest of today -- both index delta
    # models will pick it up automatically, no export needed

Kite Connect access tokens expire daily (Kite invalidates them every day,
typically overnight) -- you must re-run this helper once each trading day
before running either index's strategy or delta-model scripts.

The access token is cached to ~/.kite_session.json (chmod 600, owner-only)
so you don't have to re-export it for every command -- get_kite_client() in
nifty_delta_model.py / sensex_delta_model.py reads it automatically if
KITE_ACCESS_TOKEN isn't set in the environment. The cache is keyed to
today's IST calendar date and is ignored (treated as absent) once the date
rolls over, so a stale token is never silently reused. The API secret is
never written anywhere; only the API key and access token are cached.
"""

import json
import os
import stat
import sys
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9 fallback
    from backports.zoneinfo import ZoneInfo  # type: ignore

try:
    from kiteconnect import KiteConnect
    from kiteconnect.exceptions import KiteException
except ImportError as exc:
    raise SystemExit(
        "kiteconnect is not installed. Install it with `pip install kiteconnect`."
    ) from exc

IST = ZoneInfo("Asia/Kolkata")
SESSION_CACHE_PATH = os.path.expanduser("~/.kite_session.json")


def main() -> int:
    api_key = os.environ.get("KITE_API_KEY")
    api_secret = os.environ.get("KITE_API_SECRET")
    if not api_key or not api_secret:
        print(
            "ERROR: KITE_API_KEY and KITE_API_SECRET must be set as environment variables.",
            file=sys.stderr,
        )
        return 1

    kite = KiteConnect(api_key=api_key)

    print("Open this URL in a browser, log in, and approve access:\n")
    print(f"  {kite.login_url()}\n")
    print(
        "After login you'll be redirected to your app's redirect URL with a "
        "'request_token' query parameter in it -- copy just that value.\n"
    )

    request_token = input("Paste the request_token here: ").strip()
    if not request_token:
        print("ERROR: no request_token entered.", file=sys.stderr)
        return 1

    try:
        session = kite.generate_session(request_token, api_secret=api_secret)
    except KiteException as exc:
        print(f"ERROR: Kite rejected the session exchange: {exc}", file=sys.stderr)
        return 1

    access_token = session["access_token"]
    today = datetime.now(IST).date().isoformat()

    cache_data = {"api_key": api_key, "access_token": access_token, "date": today}
    with open(SESSION_CACHE_PATH, "w") as f:
        json.dump(cache_data, f)
    os.chmod(SESSION_CACHE_PATH, stat.S_IRUSR | stat.S_IWUSR)  # chmod 600, owner-only

    # Access tokens expire daily -- re-run this script each trading morning.
    # The cache above is only valid for today's IST date; get_kite_client()
    # in either index's delta model ignores it once the date rolls.
    print("\n" + "=" * 70)
    print("ACCESS TOKEN")
    print("=" * 70)
    print(f"KITE_ACCESS_TOKEN = {access_token}")
    print()
    print(f"Cached for today ({today}) at {SESSION_CACHE_PATH} (chmod 600).")
    print("Both nifty_delta_model.py and sensex_delta_model.py will pick it up automatically -- no export needed.")
    print()
    print("If you'd rather not rely on the cache, export it manually instead:")
    print(f"  export KITE_ACCESS_TOKEN={access_token}")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(main())
