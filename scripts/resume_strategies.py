"""
Restart strategy processes that died while a cycle was still in progress.

Run from cron (see crontab):

    @reboot             (after a short delay)
    */2 9-15 * * 1-5

A strategy deletes its state file when its cycle completes, so a state file
with a live leg (open / watching_breakout / watching_for_reentry) and no
process owning it means the process died -- most often the overnight server
reboot, which is what left 2026-11-02's carried positions with no bot and no
stop on 2026-11-03.

The process is relaunched against that exact state file (--state-file), so a
cycle carried over from an earlier day resumes instead of starting fresh. The
resumed bot sleeps until the open, re-places its DAY stops, and still exits at
the cycle's original deadline (immediately, if that has already passed).

Guards against a crash loop: a file is relaunched at most MAX_LAUNCHES times a
day, at least MIN_GAP_MINUTES apart. Legs in "error" are never auto-resumed --
a position may be open at the broker with nothing known about it.
"""

from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.path.insert(0, os.path.join(ROOT, "common"))

import telegram_notify  # noqa: E402
from health_check import IST, LOG_DIR, STATE_GLOBS, STRATEGIES, label_for, running_state_files  # noqa: E402

LIVE_STATUSES = {"open", "watching_breakout", "watching_for_reentry"}
MAX_LAUNCHES = 3
MIN_GAP_MINUTES = 5
MARKER = os.path.join(LOG_DIR, ".resume_state.json")

STRATEGY_LOGS = {
    "SENSEX BREAKDOWN": "sensex_breakdown.log",
    "NIFTY BREAKDOWN": "nifty_breakdown.log",
    "SENSEX STRANGLE": "sensex_strangle.log",
    "NIFTY STRANGLE": "nifty_strangle.log",
}


def log(msg: str) -> None:
    print(f"[{datetime.now(IST).isoformat(timespec='seconds')}] {msg}", flush=True)


def script_for(label: str) -> str:
    return next(script for script, (lbl, _, _) in STRATEGIES.items() if lbl == label)


def resumable(path: str) -> tuple[bool, dict]:
    try:
        with open(path) as f:
            state = json.load(f)
    except (OSError, json.JSONDecodeError):
        return False, {}
    return any(leg.get("status") in LIVE_STATUSES for leg in state.get("legs", {}).values()), state


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    now = datetime.now(IST)
    try:
        with open(MARKER) as f:
            marker = json.load(f)
    except (OSError, json.JSONDecodeError):
        marker = {}
    owners = running_state_files()
    today = now.date().isoformat()

    for pattern in STATE_GLOBS:
        for path in sorted(glob.glob(os.path.join(ROOT, pattern))):
            name = os.path.basename(path)
            if name in owners:
                continue
            ok, state = resumable(path)
            if not ok:
                continue
            label = label_for(name)
            rec = marker.get(name, {})
            launches = rec.get("count", 0) if rec.get("day") == today else 0
            last = datetime.fromisoformat(rec["last"]) if rec.get("last") else None
            if launches >= MAX_LAUNCHES:
                log(f"{label} ({name}): already relaunched {launches}x today; not trying again.")
                continue
            if last and now - last < timedelta(minutes=MIN_GAP_MINUTES):
                continue

            cmd = [sys.executable, "-u", script_for(label), "--state-file", path]
            if state.get("live"):
                cmd.insert(3, "--live")
            if dry_run:
                log(f"DRY RUN: would restart {label}: {' '.join(cmd)}")
                continue
            with open(os.path.join(LOG_DIR, STRATEGY_LOGS[label]), "a") as out:
                out.write(f"{now.isoformat(timespec='seconds')} INFO resume_strategies: relaunching {label} on {name}\n")
                proc = subprocess.Popen(cmd, cwd=ROOT, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
            marker[name] = {"day": today, "count": launches + 1, "last": now.isoformat()}
            log(f"RESTARTED {label} on {name} (pid {proc.pid}, launch {launches + 1}/{MAX_LAUNCHES}).")
            telegram_notify.send(
                f"[RESUME] {label} was not running with a cycle in progress ({name}); restarted it "
                f"(pid {proc.pid}, launch {launches + 1}/{MAX_LAUNCHES}). It re-arms its stops at the open."
            )

    if not dry_run:
        with open(MARKER, "w") as f:
            json.dump(marker, f, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
