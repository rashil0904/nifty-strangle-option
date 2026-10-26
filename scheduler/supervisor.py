"""
supervisor.py -- one windowless process that keeps every strategy cycle alive.

Why it exists: strategy processes launched by Task Scheduler inside visible PowerShell console windows were
killed together by a console signal (exit code 0xC000013A) on 22 and 26 Oct, leaving live positions unmanaged
(only the broker-side stops protected them). This supervisor:

  1. runs with no console window (pythonw) and starts every strategy as a detached, windowless process in its
     own process group, so closing a window / Ctrl+C / a console signal can no longer reach them;
  2. RESUMES any unfinished cycle (a state file exists) whose process is not running -- carried-over cycles at
     boot/09:00, and ANY cycle that dies mid-day (the watchdog);
  3. LAUNCHES each day's new cycles at their scheduled times (replacing the per-strategy scheduled tasks);
  4. reports every launch/restart/failure to supervisor_log.txt, supervisor_alerts.txt and, if configured,
     Telegram (common/telegram_notify.py).

Guards: a fresh cycle is launched at most once per day per job (a rejected entry is never auto-retried every
minute); fresh launches are skipped if more than a few minutes late; resumed processes are restarted at most
MAX_RESTARTS times per RESTART_WINDOW before the supervisor stops and alerts; a process already running a
cycle (including processes started by the old tasks, which carry no --state-file) is never duplicated.

Usage:  pythonw scheduler/supervisor.py                (loop until 15:35 on trading days)
        python scheduler/supervisor.py --once --dry-run [--now ISO] [--ignore-processes]   (show the plan, launch nothing)
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import subprocess
import sys
import time
from datetime import date, datetime, time as dtime, timedelta

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "common"))

LIVE = os.environ.get("SUPERVISOR_PAPER", "") != "1"

TICK_SECONDS = 30
ACTIVE_FROM = dtime(8, 55)      # supervisor works from here ...
ACTIVE_UNTIL = dtime(15, 35)    # ... until here on trading days (cycles themselves run on, detached)
MAX_RESTARTS = 5
RESTART_WINDOW = timedelta(minutes=30)

# name, script (repo-relative), state-file pattern ("{d}" = ISO date for dated files), fresh launch time, minutes of tolerated lateness
JOBS = [
    {"name": "nifty_strangle", "script": "Nifty/strategy/delta_strangle_strategy.py", "glob": "strategy_state.json",
     "fresh_state": "strategy_state.json", "at": dtime(10, 58), "late_min": 10},
    {"name": "sensex_strangle", "script": "Sensex/strategy/delta_strangle_strategy.py", "glob": "sensex_strategy_state.json",
     "fresh_state": "sensex_strategy_state.json", "at": dtime(10, 58), "late_min": 10},
    {"name": "nifty_breakdown", "script": "Nifty/strategy/delta_breakdown_strategy.py", "glob": "breakdown_strategy_state_*.json",
     "fresh_state": "breakdown_strategy_state_{d}.json", "at": dtime(10, 43), "late_min": 15},
    {"name": "sensex_breakdown", "script": "Sensex/strategy/delta_breakdown_strategy.py", "glob": "sensex_breakdown_strategy_state_*.json",
     "fresh_state": "sensex_breakdown_strategy_state_{d}.json", "at": dtime(10, 43), "late_min": 15},
]

LOG_FILE = os.path.join(REPO, "supervisor_log.txt")
ALERT_FILE = os.path.join(REPO, "supervisor_alerts.txt")
MEMORY_FILE = os.path.join(REPO, "supervisor_memory.json")


def log(msg: str, alert: bool = False) -> None:
    line = f"{datetime.now():%Y-%m-%d %H:%M:%S} {'ALERT ' if alert else ''}{msg}"
    for path in ([LOG_FILE] + ([ALERT_FILE] if alert else [])):
        try:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except OSError:
            pass
    if sys.stdout is not None:          # pythonw has no console
        print(line, flush=True)
    if alert:
        try:
            import telegram_notify
            telegram_notify.send(f"SUPERVISOR: {msg}")
        except Exception:
            pass


def load_env() -> None:
    path = os.path.join(REPO, ".env")
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            line = line.rstrip("\r\n")
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v


def python_processes() -> list[str]:
    ps = ("Get-CimInstance Win32_Process -Filter \"Name='python.exe' OR Name='pythonw.exe'\" | "
          "ForEach-Object { $_.CommandLine } | ConvertTo-Json -Compress")
    try:
        out = subprocess.run(["powershell", "-NoProfile", "-Command", ps], capture_output=True, text=True,
                             timeout=30, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0)).stdout.strip()
    except (subprocess.SubprocessError, OSError):
        return []
    if not out:
        return []
    data = json.loads(out)
    return [c.replace("/", "\\") for c in (data if isinstance(data, list) else [data]) if c]


def is_alive(job: dict, state_file: str, procs: list[str]) -> bool:
    """A process runs this cycle if it runs the script with this --state-file, or runs the script with no
    --state-file at all (a process started by the old per-strategy tasks, which owns the script's own state)."""
    script = job["script"].replace("/", "\\")
    base = os.path.basename(state_file)
    for c in procs:
        if script in c and ("--state-file" not in c or base in c):
            return True
    return False


def load_memory() -> dict:
    try:
        return json.load(open(MEMORY_FILE))
    except (OSError, ValueError):
        return {}


def save_memory(mem: dict) -> None:
    tmp = MEMORY_FILE + ".tmp"
    json.dump(mem, open(tmp, "w"), indent=1)
    os.replace(tmp, MEMORY_FILE)


def launch(job: dict, state_file: str, live: bool, dry: bool, why: str) -> bool:
    cmd = [sys.executable.replace("pythonw.exe", "python.exe"), job["script"]] + (["--live"] if live else []) + ["--state-file", state_file]
    log(f"{'WOULD LAUNCH' if dry else 'LAUNCH'} {job['name']} ({why}): --state-file {state_file}")
    if dry:
        return True
    logbase = os.path.join(REPO, f"cron_sup_{job['name']}_{date.today().isoformat()}")
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    for extra in (getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0), 0):   # leave any Task Scheduler job if allowed
        try:
            subprocess.Popen(cmd, cwd=REPO, stdout=open(logbase + ".log", "a"), stderr=open(logbase + ".err.log", "a"),
                             stdin=subprocess.DEVNULL, creationflags=flags | extra)
            return True
        except OSError as exc:
            if not extra:
                log(f"launch of {job['name']} FAILED: {exc}", alert=True)
                return False
    return False


def tick(now: datetime, dry: bool, ignore_procs: bool) -> None:
    procs = [] if ignore_procs else python_processes()
    mem = load_memory()
    today = now.date().isoformat()
    restarts = mem.setdefault("restarts", {})
    fresh = mem.setdefault("fresh", {})
    for k in [k for k in fresh if not k.startswith(today)]:
        fresh.pop(k)                                   # forget previous days

    for job in JOBS:
        files = sorted(glob.glob(os.path.join(REPO, job["glob"])))
        # ---- 1+2: resume / watchdog: any existing state file = an unfinished cycle
        for path in files:
            name = os.path.relpath(path, REPO)
            try:
                state = json.load(open(path))
            except (OSError, ValueError) as exc:
                log(f"state file {name} unreadable ({exc}) -- not touched", alert=True)
                continue
            if is_alive(job, name, procs):
                continue
            key = f"{job['name']}|{name}"
            hist = [datetime.fromisoformat(t) for t in restarts.get(key, [])]
            hist = [t for t in hist if now - t < RESTART_WINDOW]
            if len(hist) >= MAX_RESTARTS:
                if not mem.setdefault("gave_up", {}).get(key):
                    log(f"{job['name']} cycle {name} keeps dying ({MAX_RESTARTS} restarts in {RESTART_WINDOW}) -- "
                        f"supervisor STOPPED restarting it; check the positions manually", alert=True)
                    mem["gave_up"][key] = True
                continue
            statuses = {k: l["status"] for k, l in state["legs"].items()}
            first_start = not hist and key not in restarts
            ok = launch(job, name, state.get("live", LIVE), dry, f"cycle found with no process, legs={statuses}")
            if ok and not dry:
                hist.append(now)
                restarts[key] = [t.isoformat() for t in hist]
                log(f"{job['name']}: process was not running -- restarted from {name} (legs {statuses})", alert=True)
        # ---- 3: today's new cycle at its scheduled time
        state_name = job["fresh_state"].format(d=today)
        fkey = f"{today}|{job['name']}"
        due = datetime.combine(now.date(), job["at"], tzinfo=now.tzinfo)
        if now >= due and fkey not in fresh:
            if os.path.exists(os.path.join(REPO, state_name)):
                pass        # a state file already exists for this cycle: the resume/watchdog branch above owns it
            elif is_alive(job, state_name, procs):
                log(f"{job['name']}: a process still runs this script (previous cycle?) -- today's cycle waits for it to finish")
            elif now > due + timedelta(minutes=job["late_min"]):
                if not dry:
                    fresh[fkey] = "skipped-late"
                log(f"{job['name']}: today's cycle NOT launched -- {int((now - due).total_seconds() / 60)} min past its "
                    f"start (limit {job['late_min']}); start it manually if you still want it", alert=True)
            elif launch(job, state_name, LIVE, dry, "today's scheduled cycle") and not dry:
                fresh[fkey] = "launched"
    if not dry:
        save_memory(mem)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--now", default=None, help="ISO datetime to pretend it is (testing)")
    ap.add_argument("--ignore-processes", action="store_true")
    args = ap.parse_args()
    os.chdir(REPO)
    load_env()
    log(f"supervisor started (live={LIVE}, dry={args.dry_run})")
    while True:
        now = datetime.fromisoformat(args.now).astimezone() if args.now else datetime.now().astimezone()
        if now.weekday() < 5 and ACTIVE_FROM <= now.time() <= ACTIVE_UNTIL:
            try:
                tick(now, args.dry_run, args.ignore_processes)
            except Exception as exc:                       # never let the watchdog itself die on a bad tick
                log(f"supervisor tick error: {exc!r}", alert=True)
        elif not args.once and now.time() > ACTIVE_UNTIL:
            log("past 15:35 -- supervisor exiting (cycles keep running; next start 09:00 / boot / logon)")
            return 0
        if args.once:
            return 0
        time.sleep(TICK_SECONDS)


if __name__ == "__main__":
    sys.exit(main())
