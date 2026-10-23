"""
carryover.py -- 09:13 launcher for cycles carried over from a previous day.

Scans the strategies' saved state files; any cycle that started on an earlier day, is not already
being run by a live process, and is still unfinished is re-attached by launching its strategy script
with --state-file (live flag taken from the saved state). That puts stop-loss, re-entry and the
next-day exit back under script control BEFORE the 09:15 open -- the 10:43 / 10:58 launches then start
the day's own new cycles independently.

Guards against double-managing a cycle (which would send duplicate orders):
  * a cycle whose started day is today is skipped (today's own process owns it);
  * a cycle is skipped if a process for the same script is already running that either names the
    state file, or was started on an earlier day with no --state-file (an overnight process that
    is simply sleeping until the open).
An overdue cycle (exit time already passed) IS launched: the script's own loop then goes straight to
its final exit and closes whatever is still open.

Usage:  python scheduler/carryover.py [--dry-run] [--today YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import subprocess
import sys
from datetime import date, datetime

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# (log name, script, state-file glob)
JOBS = [
    ("nifty_strangle", "Nifty/strategy/delta_strangle_strategy.py", "strategy_state.json"),
    ("sensex_strangle", "Sensex/strategy/delta_strangle_strategy.py", "sensex_strategy_state.json"),
    ("nifty_breakdown", "Nifty/strategy/delta_breakdown_strategy.py", "breakdown_strategy_state_*.json"),
    ("sensex_breakdown", "Sensex/strategy/delta_breakdown_strategy.py", "sensex_breakdown_strategy_state_*.json"),
]


def load_env() -> None:
    path = os.path.join(REPO, ".env")
    if not os.path.exists(path):
        return
    for line in open(path, encoding="utf-8"):
        line = line.rstrip("\r\n")
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v


def running_python() -> list[dict]:
    """[{cmd, created(date)}] for every python process, via PowerShell/WMI."""
    ps = (
        "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
        "ForEach-Object { [pscustomobject]@{cmd=$_.CommandLine; created=$_.CreationDate.ToString('yyyy-MM-dd')} } | "
        "ConvertTo-Json -Compress"
    )
    out = subprocess.run(["powershell", "-NoProfile", "-Command", ps], capture_output=True, text=True).stdout.strip()
    if not out:
        return []
    data = json.loads(out)
    return data if isinstance(data, list) else [data]


def started_day(state: dict) -> date:
    return datetime.fromisoformat(state.get("entry_time") or state["selection_time"]).date()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--today", default=None, help="Pretend today is this date (testing).")
    ap.add_argument("--ignore-processes", action="store_true", help="Pretend no strategy process is running (testing).")
    args = ap.parse_args()

    os.chdir(REPO)
    load_env()
    now = datetime.now().astimezone()
    today = date.fromisoformat(args.today) if args.today else now.date()
    procs = [] if args.ignore_processes else running_python()
    launch = []

    for name, script, pattern in JOBS:
        for path in sorted(glob.glob(pattern)):
            try:
                state = json.load(open(path))
                day = started_day(state)
                deadline = datetime.fromisoformat(state["exit_deadline"])
            except (OSError, ValueError, KeyError) as exc:
                print(f"SKIP {path}: unreadable ({exc})")
                continue
            if day >= today:
                print(f"skip {path}: started {day} (today's own cycle)")
                continue
            script_win = script.replace("/", "\\")     # full path: Nifty and Sensex share file names
            alive = [
                p for p in procs
                if p["cmd"] and script_win in p["cmd"].replace("/", "\\")
                and (os.path.basename(path) in p["cmd"] or ("--state-file" not in p["cmd"] and date.fromisoformat(p["created"]) < today))
            ]
            if alive:
                print(f"skip {path}: already running ({len(alive)} process)")
                continue
            statuses = {k: l["status"] for k, l in state["legs"].items()}
            print(f"LAUNCH {name}: {path}  legs={statuses}  exit={deadline:%d-%b %H:%M}"
                  + ("  [OVERDUE -> will exit immediately]" if deadline <= now else ""))
            launch.append((name, script, path, state))

    if args.dry_run or not launch:
        print("dry run -- nothing launched" if args.dry_run else "nothing to carry over")
        return 0

    children = []
    for name, script, path, state in launch:
        for leg in state["legs"].values():           # yesterday's DAY stop orders have expired
            leg["sl_order_id"] = None
            leg["sl_arm_failures"] = 0
        tmp = path + ".tmp"
        json.dump(state, open(tmp, "w"), indent=2)
        os.replace(tmp, path)
        log = os.path.join(REPO, f"cron_carry_{name}_{today.isoformat()}.log")
        cmd = [sys.executable, script] + (["--live"] if state.get("live") else []) + ["--state-file", path]
        children.append(subprocess.Popen(
            cmd, cwd=REPO, stdout=open(log, "a"), stderr=open(log.replace(".log", ".err.log"), "a"),
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        ))
        print(f"started {name} (pid {children[-1].pid})")
    for c in children:
        c.wait()
    return 0


if __name__ == "__main__":
    sys.exit(main())
