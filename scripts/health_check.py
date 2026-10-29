"""
Health check for the four live strategies -- run from cron every 2 minutes
during market hours:

    */2 9-15 * * 1-5 ... python3 scripts/health_check.py >> logs/health_check.log 2>&1

Alert-only (Telegram): it never restarts or trades anything.

2026-10-28/29: the server rebooted overnight and every strategy process died
silently; the same afternoon, Jainam rate-limited our order-status calls for
~21 minutes (12:43-13:05) and nobody noticed. This checks from the outside:

  [DEAD]   a state file exists (a cycle is in progress -- every strategy
           deletes its state file when its cycle completes) but no live
           process owns it.
  [STALE]  in market hours, the owning process hasn't recorded a price check
           for STALE_MINUTES (stuck, or every quote failing).
  [API]    the strategy logs show rate limiting (HTTP 429), failed LTP
           fetches, or ERROR/FAILED lines since the last run.

A problem that persists is re-alerted every REPEAT_MINUTES; a [RECOVERED]
message follows once it clears. One morning message confirms the check
itself is running.
"""

from __future__ import annotations

import glob
import json
import os
import re
import subprocess
import sys
from datetime import datetime, time, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "common"))

import telegram_notify  # noqa: E402
from leg_lifecycle import IST, is_market_hours  # noqa: E402

STALE_MINUTES = 3
REPEAT_MINUTES = 15
RATE_LIMIT_MIN_HITS = 3
LTP_FAIL_MIN_HITS = 2
LOG_DIR = os.path.join(ROOT, "logs")
ALERT_STATE = os.path.join(LOG_DIR, ".health_state.json")

# script path -> (label, default state file, whether the default is dated by the process's start day)
STRATEGIES = {
    "Nifty/strategy/delta_strangle_strategy.py": ("NIFTY STRANGLE", "strategy_state.json", False),
    "Sensex/strategy/delta_strangle_strategy.py": ("SENSEX STRANGLE", "sensex_strategy_state.json", False),
    "Nifty/strategy/delta_breakdown_strategy.py": ("NIFTY BREAKDOWN", "breakdown_strategy_state.json", True),
    "Sensex/strategy/delta_breakdown_strategy.py": ("SENSEX BREAKDOWN", "sensex_breakdown_strategy_state.json", True),
}
STATE_GLOBS = ["strategy_state.json", "sensex_strategy_state.json",
               "breakdown_strategy_state_*.json", "sensex_breakdown_strategy_state_*.json"]

LOG_PATTERNS = {
    "rate_limit": (re.compile(r"HTTP 429"), RATE_LIMIT_MIN_HITS, "Jainam rate limit (HTTP 429)"),
    "ltp_fail": (re.compile(r"Could not fetch LTP"), LTP_FAIL_MIN_HITS, "price fetch failed after retries"),
    "error": (re.compile(r" ERROR |FAILED"), 1, "ERROR / FAILED"),
}
LOG_TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


def log(msg: str) -> None:
    print(f"[{datetime.now(IST).isoformat(timespec='seconds')}] {msg}", flush=True)


def running_state_files() -> dict[str, int]:
    """State file -> pid, for every live strategy process."""
    out = subprocess.run(["ps", "-eo", "pid=,lstart=,args="], capture_output=True, text=True).stdout
    owners = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) < 7:
            continue
        pid, lstart, args = int(parts[0]), " ".join(parts[1:6]), parts[6:]
        script = next((a for a in args if a in STRATEGIES), None)
        if script is None:
            continue
        _, default, dated = STRATEGIES[script]
        if "--state-file" in args:
            path = args[args.index("--state-file") + 1]
        elif dated:
            started = datetime.strptime(lstart, "%a %b %d %H:%M:%S %Y").date()
            root, ext = os.path.splitext(default)
            path = f"{root}_{started.isoformat()}{ext}"
        else:
            path = default
        owners[os.path.basename(path)] = pid
    return owners


def label_for(state_file: str) -> str:
    for script, (label, default, _) in STRATEGIES.items():
        root = os.path.splitext(default)[0]
        if state_file == default or re.fullmatch(re.escape(root) + r"_\d{4}-\d{2}-\d{2}\.json", state_file):
            return label
    return state_file


def check_processes(now: datetime) -> dict[str, str]:
    problems = {}
    owners = running_state_files()
    for pattern in STATE_GLOBS:
        for path in glob.glob(os.path.join(ROOT, pattern)):
            name = os.path.basename(path)
            label = label_for(name)
            if name not in owners:
                problems[f"dead:{name}"] = f"[DEAD] {label}: cycle in progress ({name}) but no process is running it."
                continue
            if not is_market_hours(now) or now.time() < time(9, 18):
                continue
            try:
                with open(path) as f:
                    legs = json.load(f).get("legs", {})
            except (OSError, json.JSONDecodeError):
                continue
            checks = [datetime.fromisoformat(leg["last_checked"]) for leg in legs.values() if leg.get("last_checked")]
            if checks and now - max(checks) > timedelta(minutes=STALE_MINUTES):
                age = int((now - max(checks)).total_seconds() // 60)
                problems[f"stale:{name}"] = f"[STALE] {label}: no price check for {age} min (pid {owners[name]})."
    return problems


def check_logs(since: datetime, now: datetime) -> dict[str, str]:
    problems = {}
    for path in sorted(glob.glob(os.path.join(LOG_DIR, "*.log"))):
        if os.path.basename(path) == "health_check.log":
            continue
        hits = {key: 0 for key in LOG_PATTERNS}
        sample = {}
        with open(path, errors="replace") as f:
            f.seek(max(0, os.path.getsize(path) - 2_000_000))
            for line in f:
                m = LOG_TS.match(line)
                if not m:
                    continue
                ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
                if not (since < ts <= now):
                    continue
                for key, (regex, _, _) in LOG_PATTERNS.items():
                    if regex.search(line):
                        hits[key] += 1
                        sample.setdefault(key, line.strip()[:200])
        name = os.path.basename(path)
        for key, (_, threshold, desc) in LOG_PATTERNS.items():
            if hits[key] >= threshold:
                problems[f"{key}:{name}"] = f"[API] {name}: {hits[key]}x {desc} since {since:%H:%M}.\n{sample[key]}"
    return problems


def main() -> int:
    now = datetime.now(IST)
    try:
        with open(ALERT_STATE) as f:
            state = json.load(f)
    except (OSError, json.JSONDecodeError):
        state = {}
    last_run = datetime.fromisoformat(state["last_run"]) if state.get("last_run") else now - timedelta(minutes=2)
    since = max(last_run, now - timedelta(minutes=10))
    sent = state.get("sent", {})

    problems = check_processes(now)
    problems.update(check_logs(since, now))

    for key, msg in problems.items():
        last = sent.get(key)
        if last is None or now - datetime.fromisoformat(last) >= timedelta(minutes=REPEAT_MINUTES):
            telegram_notify.send(f"[HEALTH] {msg}")
            sent[key] = now.isoformat()
            log(f"ALERT {msg.splitlines()[0]}")
    # Log-based problems are per-window, so only process/staleness problems get a RECOVERED message.
    for key in [k for k in sent if k not in problems]:
        if key.startswith(("dead:", "stale:")):
            telegram_notify.send(f"[HEALTH] [RECOVERED] {key.split(':', 1)[1]} is back to normal.")
            log(f"RECOVERED {key}")
        del sent[key]

    today = now.date().isoformat()
    if state.get("morning_ok") != today and is_market_hours(now):
        owners = running_state_files()
        telegram_notify.send(
            f"[HEALTH] Monitoring active: {len(owners)} strategy process(es) running"
            f"{', ' + str(len(problems)) + ' problem(s)' if problems else ', all OK'}."
        )
        state["morning_ok"] = today

    state.update(last_run=now.isoformat(), sent=sent)
    with open(ALERT_STATE, "w") as f:
        json.dump(state, f, indent=2)
    if not problems:
        log("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
