# --- Auto-backup section for job-scraper project ---

import html as _html
import html as html_lib
import shutil
import subprocess
import textwrap
import builtins
import logging
import traceback
import csv
import json
import urllib.parse as up
import re
from config import geo_constants
from config.location_chips import tokenize_location_chips, derive_locked_location_chips
from config.locality import matches_locality
from config.geo_constants import US_STATE_ABBRS as GEO_US_STATE_ABBRS
from config.geo_constants import (
    US_STATE_ABBRS,          # lowercase set like {"wa","ca","tx",...,"dc"}
    US_STATE_CHIPS,          # uppercase set like {"WA","CA","TX",...,"DC"}
    US_STATE_ABBR_TO_NAME,
    US_STATE_NAME_TO_ABBR,
    CAN_PROV_MAP,              # uppercase map like {"BC": "British Columbia", ...}
    CAN_PROV_ABBRS,          # uppercase set like {"BC","ON",...}
    CAN_PROV_ABBRS_LOWER,    # lowercase set like {"bc","on",...}
    CAN_PROV_CHIPS,
    REGION_SYNONYMS,
    ALLOWED_REGION_TOKENS, ALLOWED_LOCATION_CHIPS,
    APPLICANT_REGION_TOKENS,
    LOCATION_CHIP_SYNONYMS,
    LOCALITY_HINTS,
    PATH_LOC_MAP,
    _COUNTRY_CODE_TO_NAME, _COUNTRY_CODE_TO_WORDS, normalize_country_field_value,
    DEFAULT_TARGET_COUNTRY_CODES, DEFAULT_TARGET_COUNTRY_WORDS, DEFAULT_TARGET_COUNTRY_CHIPS, DEFAULT_TARGET_STATE_PROV_CHIPS, DEFAULT_TARGET_LOCATION_CHIPS,
)
CAN_PROV_RX = re.compile(
    r"\b(?:%s)\b" % "|".join(sorted(map(re.escape, CAN_PROV_ABBRS_LOWER))),
    flags=re.IGNORECASE,
)
from config.geo_regex import (
    CAN_PROV_RX,
    PLACEHOLDER_RX,
    COUNTRY_RX,
    CA_HINTS,
    REMOTE_KEYWORDS,
    ONSITE_BLOCKERS,
    SINGLE_CITY_PATTERNS, NON_TARGET_COUNTRY_WORD_RX, NON_TARGET_COUNTRY_CODE_RX, TARGET_US_RX, TARGET_CAN_RX,
)
from contextlib import contextmanager
from urllib import robotparser
from playwright.sync_api import sync_playwright
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List, Iterable
from urllib.parse import urlparse, urljoin, urlsplit, parse_qs, urlunparse, parse_qsl, urlencode
from pathlib import Path
from datetime import datetime, timedelta
from dateutil import parser as dateparser
from bs4 import BeautifulSoup
from contextlib import contextmanager
from classification_rules import ClassificationConfig, classify_keep_or_skip, classify_work_mode, _as_listish
from edsurge_jobs import scrape_edsurge_jobs
from gsheets_utils import (
    init_gs_libs,
    log_startup_warning_if_needed,
    log_final_reminder_if_needed,
    to_keep_sheet_row,
    to_skipped_sheet_row,
    push_results_to_sheets,
    fetch_prior_decisions,
    push_rows_to_google_sheet,
)
from logging_utils import (
    info,
    warn,
    debug,
    error,
    done_log,
    progress,
    trace_chips,
    progress_clear_if_needed,
    log_line, trace_chips,
)
from config.debug_flags import debug_print, load_debug_config
DEBUG_CFG = load_debug_config()

DOT  = "."              # 1 dot
DOT2 = ".."             # 2 dots
DOT3 = "..."            # 3 dots
DOT4 = "...."           # 4 dots
DOT5 = "....."          # 5 dots
DOT6 = "......"         # 6 dots
DOTW = "⚠️ "            # warning dots
DOTR = "🛠️ "            # DE-DUPE wrench
DOTC = "💭 "            # Reason cloud
DOTL = "🐞"             # Debug lady bug
DOTY = "❓"             # Debug


import sys, os, time

# for the live progress spinner thread
import threading


# --- Unified CLI args & run configuration (single parse, early) ---
import argparse

DEBUG_LOCATION = False



# keep this function where your logger utils live, near _p / _box / _progress_print
# ================= PROGRESS LINE =================
_P = {"active_len": 0, "start": None, "total": 0}


PUSH_MODE = "ask"   # "auto" | "ask" | "off"

SMOKE = False
PAGE_CAP = None
LINK_CAP = None
LIST_LINKS = False
SALARY_FLOOR = 110_000
SOFT_SALARY_FLOOR = 90_000
SALARY_CEIL = None






def base_row_from_listing(listing: dict, board_name: str, detail_url: str) -> dict:
    """Build the base spreadsheet row from a listing dict."""

    # Start with all the columns your Sheet expects
    row = {
        "Applied?": "",
        "Reason": "",
        "Date Scraped": datetime.now().strftime("%Y-%m-%d"),
        "Title": listing.get("title") or "",
        "Job ID (Vendor)": listing.get("job_id") or "",
        "Job ID (Numeric)": "",
        "Job Key": "",
        "Company": listing.get("company") or "",
        "Career Board": board_name,
        "Location": listing.get("location") or "",
        "Posted": "",
        "Posting Date": listing.get("posting_date") or "",
        "Valid Through": listing.get("valid_through") or "",
        "Job URL": detail_url,
        "Apply URL": "",
        "Apply URL Note": "",
        "Description Snippet": listing.get("snippet") or "",
        "WA Rule": "",
        "BC Rule": "",
        "ON Rule": "",
        "Remote Rule": "",
        "US Rule": "",
        "Canada Rule": "",
        "Salary Max Detected": "",
        "Salary Rule": "",
        "Salary Near Min": "",
        "Salary Status": "",
        "Salary Note": "",
        "Salary Est. (Low-High)": "",
        "Location Chips": "",
        "Applicant Regions": "",
        "Applicant Regions Source": "",
        "Visibility Status": "",
        "Confidence Score": "",
        "Confidence Mark": "",
    }

    return row



def _ansi_ok() -> bool:
    try:
        import sys, os
        return sys.stdout.isatty() and os.environ.get("NO_COLOR") is None
    except Exception:
        return False

def log(section: str, msg: str) -> None:
    # clear any active progress line if your progress system uses it
    try:
        progress_clear_if_needed()
    except NameError:
        pass
    _bk_log_wrap(_paint(section), msg)



def _bkts() -> str:
    from datetime import datetime
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")



# width used by wrapped logger
_LOG_WRAP_WIDTH = 120  # or detect terminal width if you prefer  (hard wrap)

def _bk_log_wrap(section: str, msg: str, indent: int = 1, width: int | None = None) -> None:
    """Timestamped, wrapped logger that plays nice with progress lines."""
    import shutil, textwrap, builtins, datetime as _dt, re
    try:
        progress_clear_if_needed()
    except NameError:
        pass
    # Avoid double timestamps if msg is already prefixed (e.g., upstream logger)
    #already_ts = bool(re.match(r"\[\d{4}-\d{2}-\d{2}", str(msg).lstrip()))
    head = f"{section:<22}"  # log_print already adds the timestamp

    TS_PREFIX_WIDTH = 22  # "[YYYY-MM-DD HH:MM:SS] " (added by log_print)
    sub_indent = " " * (TS_PREFIX_WIDTH + len(head))

    width = width or shutil.get_terminal_size(fallback=(120, 22)).columns
    body = textwrap.fill(
        str(msg),
        width=width,
        subsequent_indent=sub_indent,
        break_long_words=False,
        break_on_hyphens=False,
    )
    line = f"{head}{body}"

    # Color the entire line when ANSI is OK, using the level token inside section.
    if _ansi_ok():
        m = re.search(r"\b(INFO|WARN|ERROR|KEEP|SKIP|DONE|GS)\b", section.upper())
        label = m.group(1) if m else ""
        color = LEVEL_COLOR.get(label, RESET)
        line = f"{color}{line}{RESET}"

    log_print(line)
    try:
        progress_refresh_after_log()
    except NameError:
        pass

# colors + painter
RESET="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"; RED="\033[31m"; YELLOW="\033[33m"; GREEN="\033[32m"; CYAN="\033[36m"; BLUE="\033[34m"; MAGENTA="\033[35m"
BRIGHT_GREEN = "\033[92m"

def _ansi_ok() -> bool:
    import sys, os
    return sys.stdout.isatty() and os.environ.get("NO_COLOR") is None

def FG256(n: int) -> str:
    return f"\033[38;5;{n}m"

#DARK_GREEN = FG256(22)   # deep forest green
# other nice greens: 28, 34, 40

LEVEL_COLOR = {
    "ERROR": RED, "WARN": YELLOW, "INFO": CYAN,
    "KEEP": DIM + GREEN, "SKIP": DIM + MAGENTA, "SETUP": RESET,
    "ENV": RESET, "BACKUP": RESET, "DONE": RESET, "GS": CYAN,
}
# Accent colors to make KEEP/SKIP title lines pop
KEEP_TITLE_COLOR = GREEN
SKIP_TITLE_COLOR = MAGENTA

def _paint(label: str) -> str:
    color = LEVEL_COLOR.get(label.upper(), RESET)
    if not _ansi_ok():
        return f"[{label:<22}]"
    return f"{color}[{label:<22}]{RESET}"

def log_line(label: str, msg: str, width: int | None = None) -> None:
    _bk_log_wrap(_paint(label), msg, width=width or _LOG_WRAP_WIDTH)

# convenient shorthands (use these everywhere)
def env(msg: str) -> None:
    c = LEVEL_COLOR.get("ENV", RESET)
    log_line("ENV", f"{c}{msg}{RESET}")
def info(msg: str) -> None:
    c = LEVEL_COLOR.get("INFO", RESET)
    log_line("INFO", f"{c}{msg}{RESET}")
def warn(msg: str) -> None:
    c = LEVEL_COLOR.get("WARN", RESET)
    log_line("WARN", f"{c}{msg}{RESET}")
def setup(msg: str) -> None:    log_line("SETUP", msg)
_DEBUG_ROW_STACK: list[dict] = []

def debug(msg: str) -> None:
    """Route DEBUG lines into the active job context when available."""
    if _DEBUG_ROW_STACK:
        target = _DEBUG_ROW_STACK[-1]
        if isinstance(target, dict):
            target.setdefault("__debug_rows", []).append(str(msg))
            return
def backup(msg: str) -> None:   log_line("BACKUP", msg)
def keep_log(msg: str) -> None: log_line("KEEP", msg)
def skip_log(msg: str) -> None: log_line("SKIP", msg)
def done_log(msg: str) -> None: log_line("DONE", msg)


def _append_debug_row(target: dict | None, msg: str) -> None:
    """Queue a DEBUG message on the row so it can be printed with the job summary."""
    if not msg:
        return
    if not isinstance(target, dict):
        debug(msg)
        return
    target.setdefault("__debug_rows", []).append(str(msg))


def _inherit_debug_rows(target: dict | None, *sources: dict | None) -> None:
    """Copy queued DEBUG rows from any source dictionaries onto the target dict."""
    if not isinstance(target, dict):
        return
    for src in sources:
        if not isinstance(src, dict):
            continue
        rows = src.get("__debug_rows")
        if rows:
            target.setdefault("__debug_rows", []).extend(rows)


def _print_debug_rows_for(data: dict | None, *, color: str | None = None) -> None:
    """Flush any queued DEBUG rows so they appear just before the DONE line."""
    if not isinstance(data, dict):
        return
    rows = data.get("__debug_rows")
    if not rows:
        return

    # Reuse the main wrapped logger so DEBUG rows word-wrap like other lines.
    for row in rows:
        # DOTL prefix keeps the visual style you already use in other messages.
        log_line("DEBUG", f"{DOTL}{row}")


@contextmanager
def capture_debug_rows(target: dict | None):
    """Temporarily funnel debug() output into the provided dict."""
    if not isinstance(target, dict):
        yield
        return
    _DEBUG_ROW_STACK.append(target)
    try:
        yield
    finally:
        if _DEBUG_ROW_STACK and _DEBUG_ROW_STACK[-1] is target:
            _DEBUG_ROW_STACK.pop()
        else:
            try:
                _DEBUG_ROW_STACK.remove(target)
            except ValueError:
                pass

def _yc_trace(details: dict, label: str, dot: str = "") -> None:
    try:
        url = (details.get("job_url") or details.get("Job URL") or "")
        if "ycombinator.com" not in (url or "").lower():
            return

        log_line(label, (
            f"{dot}"
            f"Location={details.get('Location')!r} | "
            f"LocRaw={details.get('Location Raw')!r} | "
            f"LocChips={details.get('Location Chips')!r} | "
            f"AppRegions={details.get('Applicant Regions')!r} | "
            f"CountryChips={details.get('Country Chips')!r} | "
            f"USRule={details.get('US Rule')!r} | "
            f"RemoteRule={details.get('Remote Rule')!r}"
        ))
    except Exception:
        pass


# Only create the short alias `log` if it’s safe to do so
if 'log' not in globals() or isinstance(globals()['log'], logging.Logger):
    log = log_line


# --- live progress state + helpers (single place, top-level) ---
import shutil
from wcwidth import wcswidth
from threading import Event, Thread, Lock
_IO_LOCK = Lock()




def _dispw(s: str) -> int:
    w = wcswidth(s)
    return w if w >= 0 else len(s)

_SPINNER_FRAMES = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
_PROGRESS_STATE = {
    "active": False,
    "current": 0,
    "total": 0,
    "kept": 0,
    "skip": 0,
    "start": 0.0,
    "spinner": 0,
    "last_line": "",
    "last_width": 0,
    "last_render": 0.0,
    "needs_redraw": False,
}
_SPINNER_THREAD: Thread | None = None
_SPINNER_STOP = Event()
kept_count = 0
skip_count = 0


def _progress_now() -> str:
    state = _PROGRESS_STATE
    total = state["total"] or max(state["current"], 1)
    elapsed = max(time.time() - state["start"], 0.0001)
    frame = _SPINNER_FRAMES[state["spinner"] % len(_SPINNER_FRAMES)]
    label = f"{frame} PROGRESS"[:LEVEL_WIDTH].ljust(LEVEL_WIDTH)
    if _ansi_ok():
        color = LEVEL_COLOR.get("PROGRESS", RESET)
        head = f"{color}[{label}]{RESET}"
    else:
        head = f"[{label}]"
    return (
        f"{_bkts()} {head} "
        f"{state['current']:>3}/{total:<3}  "
        f"Kept {state['kept']:<3}  Skip {state['skip']:<3}"
    )


def _progress_render(force: bool = False) -> None:
    state = _PROGRESS_STATE
    if not state["active"]:
        return
    now = time.time()
    if not force and (now - state["last_render"]) < 0.1:
        return
    state["spinner"] = (state["spinner"] + 1) % len(_SPINNER_FRAMES)
    line = _progress_now()

    cols = shutil.get_terminal_size(fallback=(120, 20)).columns
    pad_to = max(cols - 1, 1)  # leave a little breathing room

    visible = _dispw(line)
    if visible < pad_to:
        out = line + (" " * (pad_to - visible))
    else:
        out = line

    sys.stdout.write("\r" + out)
    sys.stdout.flush()
    state["last_line"] = line
    state["last_width"] = pad_to
    state["last_render"] = now
    state["needs_redraw"] = False


def progress_set_total(n: int) -> None:
    _PROGRESS_STATE["total"] = max(0, int(n or 0))


def start_spinner(n: int) -> None:
    progress_start(n)


def progress_start(total: int) -> None:
    state = _PROGRESS_STATE
    state.update(
        active=True,
        current=0,
        kept=0,
        skip=0,
        total=max(0, int(total or 0)),
        start=time.time(),
        spinner=-1,
        last_line="",
        last_width=0,
        last_render=0.0,
        needs_redraw=False,
    )
    _progress_render(force=True)
    _spinner_start()


def progress_tick(i: int | None = None, kept: int | None = None, skip: int | None = None) -> None:
    state = _PROGRESS_STATE
    if not state["active"]:
        return
    if i is None:
        state["current"] += 1
    else:
        state["current"] = max(0, int(i))
    if kept is not None:
        state["kept"] = max(0, int(kept))
    if skip is not None:
        state["skip"] = max(0, int(skip))
    _progress_render()


def progress_clear_if_needed(permanent: bool = False) -> None:
    state = _PROGRESS_STATE
    if not state["last_line"] and not state["active"]:
        return
    cols = shutil.get_terminal_size(fallback=(120, 20)).columns
    pad_to = max(cols - 1, 1)

    sys.stdout.write("\r" + (" " * pad_to) + "\r")
    sys.stdout.flush()
    state["needs_redraw"] = not permanent
    if permanent:
        state["active"] = False
        state["last_line"] = ""
        state["last_width"] = 0


def progress_refresh_after_log(force: bool = False) -> None:
    state = _PROGRESS_STATE
    if not state["active"]:
        return
    if force or state["needs_redraw"]:
        _progress_render(force=True)


def progress_done() -> None:
    if not _PROGRESS_STATE["active"]:
        return
    progress_clear_if_needed(permanent=True)
    _spinner_stop_thread()
    _SPINNER_STOP.clear()


def _spinner_loop() -> None:
    while not _SPINNER_STOP.is_set():
        _progress_render()
        time.sleep(0.1)
    # final refresh so the last counters remain visible
    _progress_render(force=True)


def _spinner_start() -> None:
    global _SPINNER_THREAD
    if _SPINNER_THREAD and _SPINNER_THREAD.is_alive():
        return
    _SPINNER_STOP.clear()
    _SPINNER_THREAD = Thread(target=_spinner_loop, daemon=True, name="progress-spinner")
    _SPINNER_THREAD.start()


def _spinner_stop_thread() -> None:
    global _SPINNER_THREAD
    if not _SPINNER_THREAD:
        return
    _SPINNER_STOP.set()
    _SPINNER_THREAD.join(timeout=0.5)
    _SPINNER_THREAD = None


# timestamped print that respects the progress line
_builtin_print = print


def log_print(msg: str, color: str | None = None, color_prefix: bool = False) -> None:
    # Avoid double timestamps if msg already contains one
    has_ts = bool(re.match(r"^\s*\[\d{4}-\d{2}-\d{2}", str(msg)))
    prefix = "" if has_ts else _bkts()
    progress_clear_if_needed()
    lead = f"{prefix + ' ' if prefix else ''}"
    body = f"{msg}"
    if color and _ansi_ok():
        if color_prefix:
            line = f"{color}{lead}{body}{RESET}"
        else:
            line = f"{lead}{color}{body}{RESET}"
    else:
        line = f"{lead}{body}"

    # IMPORTANT: send logs to stderr so stdout is reserved for the spinner line
    print(line, file=sys.stderr)
    progress_refresh_after_log()



def _parse_args():
    p = argparse.ArgumentParser(
        prog="po_job_scraper.py",
        description="Product jobs scraper – full run or quick smoke."
    )
    # run shape
    p.add_argument("--smoke", action="store_true",
                   help="Fast run: only probing the first site and capping detail links.")
    p.add_argument("--only", type=str, default="",
                   help="Comma list of site keywords to include (e.g. 'greenhouse,workday,hubspot')")
    p.add_argument("--limit-pages", type=int, default=0,
                   help="Hard cap on listing pages visited (0 = unlimited)")
    p.add_argument("--limit-links", type=int, default=0,
                   help="Hard cap on job detail links visited (0 = unlimited)")
    p.add_argument("--list-links", action="store_true",
                   help="Only discover/dedupe detail links, print them, then exit.")
                    # to run, enter this into the Terminal: python3 po_job_scraper.py --smoke --list-links
    p.add_argument(
        "--test-url",
        type=str,
        default="",
        help="Fetch and parse a single job detail URL, print fields, then exit.")
    p.add_argument(
    "--only-url",
    dest="only_url",
    default="",
    help="Process exactly one job detail URL (skips board link discovery).",)

    # salary knobs
    p.add_argument("--floor", type=int, default=110_000,
                   help="Minimum target salary filter")
    p.add_argument("--soft-floor", type=int, default=90_000,
                   help="Lower bound for soft-keep salaries (0 disables the soft floor)")
    p.add_argument("--ceil",  type=int, default=0,
                   help="Optional salary ceiling; 0 means no ceiling")

    return p.parse_args()


# Push control (leave as-is if you prefer to be prompted)
PUSH_MODE = "ask"     # "auto" | "ask" | "off"

# ---------- I/O + logging setup ----------
import os, sys, time, shutil, logging

# help the terminal show progress updates promptly
try:
    os.environ["PYTHONUNBUFFERED"] = "1"
except Exception:
    pass

try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)  # one call is enough
except Exception:
    pass

# send logs to stderr so they do not step on the progress line (stdout)
logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
log = logging.getLogger("scraper")
# ---------- end setup ----------




# ---- fixed-width, wrapped logger for long lines ----
_LOG_WRAP_WIDTH = 120
width = _LOG_WRAP_WIDTH



GIT_PUSH_MODE = "prompt"    # change to "prompt" or "auto" when you want

def _is_interactive() -> bool:
    import os, sys
    if os.environ.get("CI"):
        return False
    try:
        return sys.stdin.isatty() and sys.stdout.isatty()
    except Exception:
        return False



def git_commit_and_push(message: str) -> None:
    repo_dir = Path(__file__).resolve().parent
    def _run(cmd):
        return subprocess.run(cmd, cwd=repo_dir, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    # stage changes
    _run(["git", "add", "-A"])
    # commit (no-op if nothing to commit)
    commit = _run(["git", "commit", "-m", message])
    if "nothing to commit" in commit.stdout.lower():
        log_print("[GIT", "].No changes to commit.")
        return

    # push
    push = _run(["git", "push"])
    if push.returncode == 0:
        log_print("GIT", "].Pushed to remote.")
    else:
        log_print("GITERR", ".Push failed:")
        for ln in (push.stdout or "").splitlines():
            log_print("GITERR", ".{ln}")

def git_prompt_then_push(default_msg: str) -> None:
    if PUSH_MODE != "ask" or not _is_interactive():
        return
    try:
        progress_clear_if_needed()
    except NameError:
        pass
    try:
        ans = input("[GIT] Push changes to GitHub now? (y/n) ").strip().lower()
    except EOFError:
        return
    if ans in ("y", "yes"):
        git_commit_and_push(default_msg)
    else:
        log_print("GIT", "Skipped push.")

# If True, keep jobs even when no salary is detected; if False, treat as SKIP.
KEEP_UNKNOWN_SALARY = False


# Set this to True later if you want to STOP creating "backup of backup" files.
BACKUP_SKIP_ARCHIVED = False

def backup_all_py_to_archive(keep_last: int | None = None, max_age_days: int | None = None):
    """
    Back up every .py file in this folder into 'Code Archive' with a timestamp.

    Safety:
      - Skip if running inside 'Code Archive'.
    Retention:
      - keep_last=None keeps all backups.
      - keep_last=N keeps only the N most recent per file.
      - max_age_days=M deletes backups older than M days.
      - If both are set, enforce both.
    """
    here = Path(__file__).resolve().parent
    if "Code Archive" in str(here):
        #_bk_log_wrap("[BACKUP", "].Skipped (running inside Code Archive)")
        backup(f"{DOT3}.Skipped (running inside Code Archive)")
        return

    archive_dir = here / "Code Archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    for src in here.glob("*.py"):
        # Only skip previously archived files if/when you flip the toggle
        if BACKUP_SKIP_ARCHIVED and "_backup_" in src.name:
            continue

        stem = src.stem
        dest = archive_dir / f"{stem}_backup_{ts}{src.suffix}"
        shutil.copy2(src, dest)
        #_bk_log_wrap("[BACKUP", f"].Saved {dest}")
        backup(f".Saved to: {dest}")

        # Collect backups for this file
        pattern = f"{stem}_backup_*{src.suffix}"
        backups = sorted(archive_dir.glob(pattern))

        # Age-based pruning
        if max_age_days is not None:
            cutoff = datetime.now() - timedelta(days=max_age_days)
            for b in list(backups):
                try:
                    ts_str = b.stem.split("_backup_")[1]
                    b_dt = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
                    if b_dt < cutoff:
                        b.unlink()
                        #_bk_log_wrap("[BACKUP", f"].Pruned by age {b.name}")
                        backup(f"{DOT3}Pruned by age {b.name}")
                except Exception:
                    # ignore filenames that don’t match the pattern
                    pass
            backups = sorted(archive_dir.glob(pattern))

        # Count-based pruning
        if keep_last is not None:
            while len(backups) > keep_last:
                old = backups.pop(0)
                try:
                    old.unlink()
                    #_bk_log_wrap("[BACKUP", f"].Pruned by count {old.name}")
                    backup(f"{DOT3}Pruned by count {old.name}")
                except OSError as e:
                    progress_clear_if_needed()
                    log_line("WARN", f".Could not remove {old.name}: {e}")


# --- HOW TO USE (uncomment exactly one) ---
backup_all_py_to_archive()                  # 1) keep ALL backups
# backup_all_py_to_archive(keep_last=10)    # 2) keep last 10 per file
# backup_all_py_to_archive(max_age_days=30) # 3) delete backups older than 30 days
# backup_all_py_to_archive(keep_last=10, max_age_days=60)  # combine both
# --- End auto-backup section ---



# --- Auto-install dependencies if missing (venv-friendly) ---
import sys, subprocess, os
import builtins, datetime as _dt
_builtin_print = builtins.print


def ts_pprint(*args, **kwargs):
    ts = _dt.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    if args:
        args = (f"{ts} {args[0]}",) + args[1:]
    else:
        args = (ts,)
    return _builtin_print(*args, **kwargs)

REQUIRED_PACKAGES = [
    "requests",
    "requests-cache",
    "beautifulsoup4",
    "python-dateutil",
    "playwright",
    "wcwidth",
    #"gspread",
    #"google-auth",
]

BLOCKED_DOMAINS = {
    "glassdoor.com", "www.glassdoor.com",
    "indeed.com", "www.indeed.com",
    "careerbuilder.com", "www.careerbuilder.com",
    "producthunt.com", "www.producthunt.com",
    "jobspresso.co", "www.jobspresso.co",
}


IN_VENV = (sys.prefix != sys.base_prefix)

def _stamp() -> str:
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

def _ensure(pkg: str):
    """Import if available; otherwise pip install and stream output with timestamps."""
    try:
        __import__(pkg.replace("-", "_"))
        setup(f".Requirement already satisfied: {pkg}")
    except ImportError:
        setup(f".Installing missing dependency: {pkg}")
        cmd = [sys.executable, "-m", "pip", "install", pkg]
        # If not in a venv, fallback to --user to avoid Homebrew's externally-managed error
        if not IN_VENV:
            # one of these will satisfy different macOS setups
            cmd.append("--break-system-packages")
            # or fallback:
            # cmd.append("--user")

        proc = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        for line in proc.stdout.splitlines():
            if line.strip():
                setup(f".{line.strip()}")

def _log_env_sanity(setup):
    import os
    import sys

    setup(f".Python executable: {sys.executable}")
    setup(f".Python version: {sys.version.split()[0]}")
    setup(f".sys.prefix: {sys.prefix}")
    setup(f".VIRTUAL_ENV: {os.environ.get('VIRTUAL_ENV','')}")
    setup(f".IN_VENV: {IN_VENV}")

def _ensure_dependencies():
    _log_env_sanity(setup)
    _ensure_dependencies()

"""
po_job_scraper.py

Starter script to find Product Owner roles that mention "remote".
Polite by default: checks robots.txt, honors crawl-delay, uses rate limiting.

Note: Update `STARTING_PAGES` with the listing pages you want to crawl.
Do not use this script to collect personal data or bypass site protections.
"""


# Treat these domains as remote-first boards
REMOTE_BOARDS = {"remote.co", "weworkremotely.com", "remotive.com", "nodesk.co", "workingnomads.com"}
SOFT_US_FOR_REMOTE_BOARDS = True  # if True, assume US unless page screams non-US

# Optional: widen role match to include PM titles
ALLOW_PM = True  # set True if you want Product Manager roles too

# Sites that are *boards/aggregators* (do NOT use their site_name as company)
BOARD_HOSTS = {
    "remotive.com", "weworkremotely.com", "nodesk.co", "workingnomads.com",
    "remoteok.com", "builtin.com", "builtinseattle.com","builtinvancouver.com",
    "simplyhired.com","themuse.com", "jobright.ai",
    "ycombinator.com", "remote.co", "angel.co", "wellfound.com", "stackoverflow.com",
    "jobspresso.co", "powertofly.com", "landing.jobs", "careerjet.com",
    "jobboard.io", "authenticjobs.com", "jobbatical.com", "workew.com",
    "techfetch.com", "dice.com", "jobserve.com", "jobs.github.com",
    "edtech.com", "edtechjobs.com", "hired.com", "talent.com", "joblift.com",
}

# Common ATS/company career hosts (OK to use site_name as company)
ATS_HOSTS = {
    "boards.greenhouse.io", "job-boards.greenhouse.io",
    "jobs.lever.co", "greenhouse.io",
    "myworkdayjobs.com", "workday.com",
    "icims.com", "smartrecruiters.com", "jobs.ashbyhq.com"
}


import time
import random
import re

import requests
from bs4 import BeautifulSoup
from dateutil import parser
_STATE_ABBR = {"AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY",
               "LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND",
               "OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"}

def safe_parse_dt(value: str):
    s = (value or "").strip()
    # Remove bare state abbreviations that the parser mistakes for tz names
    s = re.sub(r"\b(" + "|".join(_STATE_ABBR) + r")\b", "", s)
    try:
        # fuzzy to ignore leftovers, ignoretz to avoid tz warnings
        return parser.parse(s, fuzzy=True, ignoretz=True)
    except Exception:
        return None

import warnings
try:
    from dateutil.parser import UnknownTimezoneWarning
    warnings.filterwarnings("ignore", category=UnknownTimezoneWarning)
except Exception:
    pass
# --- Safe date parsing to avoid UnknownTimezoneWarning ---
from dateutil.tz import tzoffset
from dateutil.parser import UnknownTimezoneWarning
warnings.filterwarnings("ignore", category=UnknownTimezoneWarning)

from urllib.parse import urlparse, parse_qsl, urlencode

CASE_SENSITIVE_PATH_HOSTS = {
    "ycombinator.com",
}

def _host_is_case_sensitive(host: str) -> bool:
    h = (host or "").lower()
    return any(dom in h for dom in CASE_SENSITIVE_PATH_HOSTS)

def normalize_url_for_key(u: str) -> str:
    """Stable URL for dedupe and job keys. Lowercase host always.
    Lowercase path except for known case sensitive hosts (YC).
    """
    if not u:
        return ""
    p = urlparse(u.strip())

    scheme = (p.scheme or "https").lower()
    host = (p.netloc or "").lower()

    path_raw = (p.path or "/").rstrip("/") or "/"
    path = path_raw if _host_is_case_sensitive(host) else path_raw.lower()

    # filter and sort query params for stability
    drop = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content",
            "ref","referrer","source","_hsmi","_hsenc","gh_src","page","p","start"}
    kept = [(k.lower(), v) for k, v in parse_qsl(p.query, keep_blank_values=True) if k.lower() not in drop]
    kept.sort()

    query = urlencode(kept, doseq=True)
    return urlunparse((scheme, host, path, "", query, ""))


def _job_key(details: dict, link: str) -> str:
    """
    Build a stable key so we only process the same job once per run.

    Normalize the URL so tracking/pagination params don't create new keys
    for the same job, while preserving path casing (needed for some boards).
    """
    job_url = (details.get("Job URL") or details.get("job_url") or link or "").strip()
    if not job_url:
        return ""

    try:
        p = urlparse(job_url)

        scheme = (p.scheme or "https").lower()
        host = (p.netloc or "").lower()

        # Preserve path casing, only normalize trailing slash
        path = (p.path or "/").rstrip("/") or "/"

        drop = {
            "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
            "ref", "referrer", "source", "_hsmi", "_hsenc", "gh_src", "page", "p", "start",
        }

        kept = []
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            k_norm = (k or "").lower()
            if k_norm in drop:
                continue
            kept.append((k_norm, v))

        kept.sort()
        query = urlencode(kept, doseq=True)

        # Built In cross-board dedupe:
        # Use path + normalized query only, so the key does not imply a specific host.
        if host in {"builtin.com", "www.builtin.com", "builtinseattle.com", "www.builtinseattle.com", "builtinvancouver.org", "www.builtinvancouver.org"}:
            return f"{path}?{query}" if query else path

        clean = urlunparse((scheme, host, path, "", query, ""))
        return clean or job_url

    except Exception:
        # Last resort: preserve original URL casing
        return job_url

def parse_date_relaxed(s):
    """Parse many date strings while neutralizing stray 'tzname' tokens like 'WI' or 'IL'.
    Some job pages concatenate a location right after a date, which confuses dateutil
    and emits UnknownTimezoneWarning. We map any unknown tzname to a zero offset.
    Returns ISO date string when possible, else the original string.
    """
    try:
        dt = parser.parse(str(s), fuzzy=True, ignoretz=True)
        return dt.date().isoformat()
    except Exception:
        return s

import re

UTC_ISO_RX = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$"
)

DATE_ONLY_RX = re.compile(r"\d{4}-\d{2}-\d{2}$")


# Date normalization rule:
# Canonical "Posting Date" and "Valid Through" are computed using the UTC day encoded
# in ISO timestamps. This avoids timezone drift across machines and locales.

def utc_date_from_iso(value: str) -> str | None:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None

    # Already YYYY-MM-DD
    if len(s) >= 10 and s[4] == "-" and s[7] == "-" and "T" not in s:
        return s[:10]

    s2 = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s2)
    except Exception:
        try:
            d = parse_date_relaxed(s)
            return d if d else None
        except Exception:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc).date().isoformat()


def normalize_posted_label(value: str) -> str:
    """
    Normalizes 'Posted' text for consistency across boards.
    Examples:
      'Job Posted 16 Days' -> 'Posted 16 Days'
      'Job posted 6 days ago |' -> 'Posted 6 days ago'
      'Posted 12 days ago |' -> 'Posted 12 days ago'
    """
    s = (value or "").strip()
    if not s:
        return ""

    # Remove trailing separators Dice style
    s = s.replace("|", " ").strip()

    # Remove leading 'Job ' only when it is part of 'Job Posted ...'
    s = re.sub(r"(?i)^\s*job\s+posted\s+", "Posted ", s).strip()

    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s



def progress(i: int, total: int, kept: int, skipped: int) -> None:
    """
    Legacy wrapper retained for callers that still expect the old signature.
    """
    global kept_count, skip_count
    kept_count = kept
    skip_count = skipped
    progress_set_total(total)
    progress_tick(i=i, kept=kept_count, skip=skip_count)

# --- Environment banner ---
env(f".Using Python from: {sys.executable}")
env(f".Virtual environment: {'Yes' if IN_VENV else 'No'}")
env(f".Working directory: {os.getcwd()}")

DEBUG_REMOTE = False  # flip to True only when tuning remote rules


# --- Company helpers (JSON-LD first) -----------------------------------------

def _company_from_ldnode(d: dict) -> str:
    """Pull 'hiringOrganization.name' out of a JobPosting JSON-LD node (including @graph)."""
    if not isinstance(d, dict):
        return ""
    # Direct JobPosting node
    if d.get("@type") == "JobPosting":
        org = d.get("hiringOrganization") or d.get("hiringOrganisation")
        if isinstance(org, dict):
            name = (org.get("name") or "").strip()
            if name:
                return name
    # Sometimes data is under a @graph list
    if isinstance(d.get("@graph"), list):
        for n in d["@graph"]:
            name = _company_from_ldnode(n)
            if name:
                return name
    return ""

def company_from_jsonld(soup) -> str:
    """Search all <script type='application/ld+json'> and return company if present."""
    import json
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            payload = s.string or s.get_text() or ""
            if not payload.strip():
                continue
            data = json.loads(payload)
        except Exception:
            continue
        # List or single object
        if isinstance(data, list):
            for d in data:
                name = _company_from_ldnode(d)
                if name:
                    return name
        else:
            name = _company_from_ldnode(data)
            if name:
                return name
    return ""

_YC_JOB_PATH_RX = re.compile(r"^/companies/[^/]+/jobs/([^/?#]+)")

def _infer_yc_title_from_job_url(job_url: str) -> str | None:
    """
    YC job URLs often follow:
      /companies/<company-slug>/jobs/<job-slug>

    Use the job slug as a fallback Title only when Title is missing.
    """
    if not job_url:
        return None

    try:
        path = up.urlparse(job_url).path or ""
    except Exception:
        return None

    m = _YC_JOB_PATH_RX.match(path)
    if not m:
        return None

    slug = (m.group(1) or "").strip()
    if not slug:
        return None

    raw = slug.replace("-", " ").strip()
    if not raw:
        return None

    words = [w for w in raw.split() if w]
    if not words:
        return None

    acronyms = {"ai", "ml", "ui", "ux", "qa", "pm", "po", "hr", "us", "uk", "eu"}
    out = []
    for w in words:
        out.append(w.upper() if w.lower() in acronyms else w.capitalize())

    return " ".join(out).strip() or None

import re

_INVESTOR_CUES = (
    "ventures",
    "capital",
    "angel",
    "investor",
    "fund",
    "partners",
)

def _is_plausible_yc_location(loc: str | None) -> bool:
    if not loc:
        return False

    s = " ".join(loc.split()).strip()
    if not s:
        return False

    # Too long for a location line
    if len(s) > 80:
        return False

    # Investor list signature: lots of commas and parentheses
    if s.count(",") >= 3 and ("(" in s or ")" in s):
        return False

    low = s.lower()
    if any(cue in low for cue in _INVESTOR_CUES):
        return False

    # Locations usually do not contain many personal name patterns like "First Last("
    if re.search(r"\b[A-Z][a-z]+\s+[A-Z][a-z]+\(", s):
        return False

    return True

import json

def _yc_location_from_next_data(soup) -> str | None:
    node = soup.find("script", id="__NEXT_DATA__")
    if not node or not node.string:
        return None

    try:
        data = json.loads(node.string)
    except Exception:
        return None

    def walk(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                lk = str(k).lower()
                if lk in {"location", "joblocation", "job_location", "locationtext"}:
                    if isinstance(v, str) and v.strip():
                        return v.strip()
                found = walk(v)
                if found:
                    return found
        elif isinstance(obj, list):
            for item in obj:
                found = walk(item)
                if found:
                    return found
        return None

    return walk(data)

def _yc_extract_location_from_embedded_job_payload(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    root = soup.select_one('[id^="WaasShowJobPage-react-component-"][data-page]')
    if not root:
        return None

    raw = root.get("data-page") or ""
    if not raw.strip():
        return None

    try:
        data = json.loads(html.unescape(raw))
    except Exception:
        return None

    props = data.get("props") or {}
    job = props.get("job") or {}
    company = props.get("company") or {}

    job_loc = str(job.get("location") or "").strip()
    company_loc = str(company.get("location") or "").strip()
    job_loc_low = job_loc.lower()
    if "canada" in job_loc_low:
        return "Canada"

    # Prefer job-specific location truth over company HQ.
    if job_loc:
        return job_loc

    # Only fall back to company HQ if there is truly no job location.
    if company_loc:
        return company_loc

    return None

def _yc_extract_location_from_jsonld(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    for script in soup.select('script[type="application/ld+json"]'):
        raw = (script.string or "").strip()
        if not raw:
            continue

        try:
            data = json.loads(raw)
        except Exception:
            continue

        objs = data if isinstance(data, list) else [data]
        for obj in objs:
            if not isinstance(obj, dict):
                continue

            # 1. Explicit structured job location
            job_locs = obj.get("jobLocation")
            if job_locs:
                first = job_locs[0] if isinstance(job_locs, list) and job_locs else job_locs
                if isinstance(first, dict):
                    addr = first.get("address") or {}
                    if isinstance(addr, dict):
                        city = addr.get("addressLocality")
                        region = addr.get("addressRegion")
                        country = normalize_country_field_value(addr.get("addressCountry"))
                        parts = [p for p in [city, region, country] if isinstance(p, str) and p.strip()]
                        if parts:
                            return ", ".join(parts)

            # 2. Remote with applicant location restriction
            job_loc_type = str(obj.get("jobLocationType") or "").strip().upper()
            applicant_req = obj.get("applicantLocationRequirements")

            if job_loc_type == "TELECOMMUTE" and applicant_req:
                req = applicant_req[0] if isinstance(applicant_req, list) and applicant_req else applicant_req
                if isinstance(req, dict):
                    # schema sometimes uses name directly on Country
                    name = normalize_country_field_value(req.get("name"))
                    if name:
                        return name

                    addr = req.get("address") or {}
                    if isinstance(addr, dict):
                        city = addr.get("addressLocality")
                        region = addr.get("addressRegion")
                        country = normalize_country_field_value(addr.get("addressCountry"))
                        parts = [p for p in [city, region, country] if isinstance(p, str) and p.strip()]
                        if parts:
                            return ", ".join(parts)

            # 3. Remote only
            if job_loc_type == "TELECOMMUTE":
                return "Remote"

    return None

def _dice_extract_location_from_soup(soup: BeautifulSoup) -> str:
    """
    Dice job pages include a header line like:
      'Hybrid in New York, NY, US' or 'Remote in REMOTE WORK, VA, US'
    We extract the part after ' in ' and lightly normalize placeholders.
    """
    try:
        header = soup.select_one('[data-testid="job-detail-header-card"]')
        if not header:
            return ""

        # The first span inside the light text block is the location line.
        # Example: <span>Hybrid in New York, NY, US</span>
        loc_span = header.select_one('span.text-font-light > span')
        if not loc_span:
            return ""

        raw = loc_span.get_text(" ", strip=True)
        if not raw:
            return ""

        # Split "Hybrid in X" -> X
        if " in " in raw:
            loc = raw.split(" in ", 1)[1].strip()
        else:
            loc = raw.strip()

        # Normalize placeholder locality like "REMOTE WORK, VA, US"
        # Keep "VA, US"
        parts = [p.strip() for p in loc.split(",") if p.strip()]
        if len(parts) >= 3 and parts[0].lower() in {"remote", "remote work", "work from home", "wfh"}:
            parts = parts[1:]  # drop placeholder locality
        loc = ", ".join(parts)

        return loc
    except Exception:
        return ""

def _apply_builtinvancouver_overrides(
    details: dict,
    job_url: str,
    loc_low: str,
    chips: list[str],
    applicant_regions: list[str],
) -> tuple[list[str], list[str]]:
    """
    Built In Vancouver has Canada signals polluted by generic defaults and site chrome.
    If we detect Canada, force Canada pass and US fail, and ensure CAN is present in chips
    and 'can' is present in applicant regions.
    """
    is_biv = "builtinvancouver.org" in (job_url or "").lower()
    if not is_biv:
        return chips, applicant_regions

    # Normalize inputs defensively
    chips_list = [str(c).strip().upper() for c in (chips or []) if str(c).strip()]
    app_list = [str(r).strip().lower() for r in (applicant_regions or []) if str(r).strip()]

    is_canada = bool(
        re.search(r"\bcanada\b", loc_low or "")
        or re.search(r"(?:^|[,\s])can(?:$|[,\s])", loc_low or "")
        or _has_can_province_signal(loc_low or "")
        or ("CAN" in chips_list)
    )

    # Only stamp the source when the override actually triggers
    if not is_canada:
        return chips_list, app_list

    if not (details.get("Applicant Regions Source") or "").strip():
        details["Applicant Regions Source"] = "BIV_RULES"

    details["US Rule"] = "Fail"
    details["WA Rule"] = "Fail"
    details["Canada Rule"] = "Pass"

    existing_notes = (details.get("Eligibility Notes") or "").strip()
    if "BIV_CANADA_OVERRIDE" not in existing_notes:
        details["Eligibility Notes"] = (existing_notes + "|BIV_CANADA_OVERRIDE").strip("|")

    # Ensure CAN chip, then filter to allowed chips
    if "CAN" not in chips_list:
        chips_list.append("CAN")
    chips_list = [c for c in chips_list if c in ALLOWED_LOCATION_CHIPS]

    # Ensure 'can' region
    if not app_list:
        app_list = ["can"]
    elif "can" not in app_list:
        app_list.append("can")

    return chips_list, app_list

def _is_built_in_host(host: str) -> bool:
    h = (host or "").lower()
    return "builtin" in h  # catches builtin.com + builtinvancouver.org + other Built In city sites

def _builtin_hero_location_token(soup) -> str:
    """
    Built In hero scope only.
    Extract the token from the hero location row:
    'Hiring Remotely in <TOKEN>' or similar.
    """
    if not soup:
        return ""

    hero_blocks = soup.select("div.col-12.col-lg-3, div.container.d-lg-none")
    for hero in hero_blocks:
        icon = hero.select_one("i.fa-location-dot")
        if not icon:
            continue

        container = icon.find_parent("div", class_=re.compile(r"\bd-flex\b", re.I))
        if not container:
            continue

        spans = container.find_all("span")
        if not spans:
            continue

        for i, sp in enumerate(spans):
            txt = sp.get_text(" ", strip=True)
            if re.search(r"\bHiring\s+Remotely\s+in\b", txt, re.I):
                if i + 1 < len(spans):
                    token = spans[i + 1].get_text(" ", strip=True)
                    return (token or "").strip()

    return ""

def _apply_builtin_family_hero_lock(details: dict, host: str, soup) -> None:
    if not soup:
        return

    token = _builtin_hero_location_token(soup)
    if not token:
        return

    h = (host or "").lower()
    if "builtinvancouver.org" in h:
        norm = _normalize_biv_location_value(token)  # CAN / USA and remote cleanup
        # For Vancouver, CAN means Canada
        if norm == "CAN":
            details["Location"] = "Canada"
        elif norm == "USA":
            details["Location"] = "United States"
        else:
            details["Location"] = norm

        details["LocationRaw"] = norm
        details["Location Chips"] = norm
        details["Location Chips Source"] = "HERO"
        details["_LOCK_LOCATION_CHIPS"] = True
        details["Remote Rule"] = "Remote"

    elif "builtin.com" in h:
        # builtin.com: be conservative with CA
        norm = (token or "").strip().upper()

        # If the hero token is CAN or USA, lock immediately
        if norm in {"CAN", "USA"}:
            details["Location Chips"] = norm
            details["Location Chips Source"] = "HERO"
            details["_LOCK_LOCATION_CHIPS"] = True

        # If token is CA, assume California unless strong Canada context exists
        elif norm == "CA":
            ca_is_canada = bool(
                re.search(r"\bCanada\b", (details.get("Location") or ""), re.I)
                or (details.get("Canada Rule") or "").strip().lower() == "pass"
            )
            if ca_is_canada:
                details["Location Chips Source"] = (details.get("Location Chips Source") or "") + "|HERO_LOCK"
                _set_loc_chips(details, "CAN", "HERO_LOCK set CAN only")
            else:
                details["Location Chips"] = "CA"  # California chip
            details["Location Chips Source"] = "HERO"
            details["_LOCK_LOCATION_CHIPS"] = True

        # If token is a state or province code you allow, lock it
        elif norm in ALLOWED_LOCATION_CHIPS:
            details["Location Chips"] = norm
            details["Location Chips Source"] = "HERO"
            details["_LOCK_LOCATION_CHIPS"] = True

    # If we locked chips, keep Location display consistent
    if details.get("_LOCK_LOCATION_CHIPS"):
        try:
            if not details.get("_DERIVE_LOCATION_RULES_DONE"):
                details = _derive_location_rules(details)
        except Exception:
            pass

def workday_links_from_listing(listing_url: str, max_results: int = 250) -> list[str]:
    """
    Convert a Workday listing URL into job detail links by querying the cxs JSON API.
    Handles both myworkdaysite and myworkdayjobs patterns.
    """
    # If this is already a detail page, just return it
    if "/job/" in (p.path or ""):
        return [listing_url]


    p = up.urlparse(listing_url)
    host = (p.netloc or "").lower()
    parts = [s for s in (p.path or "").split("/") if s]
    qs = up.parse_qs(p.query)
    search = " ".join(qs.get("q", [])).strip() or ""

    # Normalize locale prefix in path: /en-us/recruiting/... -> /recruiting/...
    if parts and re.fullmatch(r"[a-z]{2}-[a-z]{2}", parts[0], re.I):
        parts = parts[1:]


    def _html_fallback_links(listing_url: str) -> list[str]:
        html = get_html(listing_url) or ""
        if not html:
            return []
        links = find_job_links(html, listing_url) or []
        # keep only likely Workday detail links
        out = []
        for lk in links:
            if "/job/" in lk or "/details/" in lk:
                out.append(lk)

        # de-dupe preserve order
        seen, deduped = set(), []
        for u in out:
            if u not in seen:
                seen.add(u)
                deduped.append(u)
        return deduped

    # If host is wdN.myworkdayjobs.com, try to discover the real tenant host
    if re.fullmatch(r"wd\d+\.myworkdayjobs\.com", host, re.I):
        try:
            html = get_html(listing_url) or ""
            if not html:
                log_line("WARN", f".[WORKDAY] Could not fetch listing HTML for tenant sniff (url={listing_url})")
            else:
                # Sniff a tenant host from HTML (embedded URLs)
                m = re.search(r'https?://([a-z0-9-]+)\.(wd\d+)\.myworkdayjobs\.com', html, re.I)
                if m:
                    tenant_guess, wd_num = m.group(1), m.group(2)
                    host = f"{tenant_guess}.{wd_num}.myworkdayjobs.com"
                    # optional: log_line("DEBUG", f".[WORKDAY] Sniffed tenant host: {host}")
        except Exception as e:
            log_print("WARN", f".[WORKDAY] tenant sniff failed (error={e}, url={listing_url})")

    # Infer tenant and site
    tenant = None
    site = None

    if parts and parts[0] == "recruiting" and len(parts) >= 3:
        tenant = parts[1]
        site = parts[2]
    else:
        sub = host.split(".")[0]
        if not re.fullmatch(r"wd\d+", sub, re.I):
            tenant = sub
        if not tenant and len(parts) >= 2:
            tenant = parts[0]
        if len(parts) >= 1:
            site = parts[0]

    if not tenant:
        log_print(
            "WARN",
            f".[WORKDAY] Could not infer tenant (host={host}, parts={parts}, url={listing_url}). "
            "Falling back to HTML link extraction (page 1 only)."
        )
        return _html_fallback_links(listing_url)

    if not site:
        site = tenant

    api_host = _workday_api_host(host)
    jobs = _wd_jobs(api_host, tenant, site, search, limit=50, max_results=max_results)

    if not jobs:
        return _html_fallback_links(listing_url)

    out: list[str] = []
    for j in jobs:
        ext = j.get("externalUrl") or j.get("externalPath") or j.get("url")
        if not ext:
            continue
        out.append(up.urljoin(f"https://{host}/", ext))

    # De-dupe while preserving order
    seen, deduped = set(), []
    for u in out:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    return deduped

def _workday_api_host(ui_host: str) -> str:
    """
    Convert a Workday UI host to the likely API host.
    Examples:
      velera.wd5.myworkdayjobs.com -> wd5.myworkday.com
      wd5.myworkdaysite.com        -> wd5.myworkday.com
      wd5.myworkday.com            -> wd5.myworkday.com
    """
    h = (ui_host or "").lower()

    m = re.search(r"(wd\d+)\.", h)
    if m:
        return f"{m.group(1)}.myworkday.com"

    # last resort, keep original
    return ui_host


def _wd_jobs(host: str, tenant: str, site: str, search: str, limit: int = 50, max_results: int = 250) -> list[dict]:
    """
    Query Workday cxs jobs endpoint and return raw job dicts.
    Correct path is: /wday/cxs/{tenant}/jobs
    """
    #log_line("DEBUG", f".[WORKDAY] _wd_jobs entered (tenant={tenant}, site={site}, host={host})")

    url = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
    out, offset = [], 0
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0"
    }

    while True:
        remaining = max_results - len(out)
        if remaining <= 0:
            break

        current_limit = min(limit, remaining)
        payload = {"limit": current_limit, "offset": offset, "searchText": search}

        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)

        data = _safe_resp_json(r, context=f".[WORKDAY] cxs jobs (api={url})")
        if not data:
            # returning [] triggers your HTML fallback upstream
            return []

        total_hint = (
            data.get("total")
            or data.get("totalCount")
            or data.get("totalHits")
            or data.get("totalRecords")
        )

        items = data.get("jobPostings") or data.get("jobs") or data.get("data") or []
        if isinstance(items, dict):
            items = []
        if not isinstance(items, list):
            items = []
        if not items:
            break

        out.extend(items)

        if len(out) >= max_results:
            break
        if total_hint is not None and len(out) >= int(total_hint):
            break
        if total_hint is None and len(items) < current_limit:
            break

        offset += len(items)

    return out



def _set_qp(url: str, **updates) -> str:
    p = up.urlparse(url)
    q = up.parse_qs(p.query)
    for k, v in updates.items():
        q[str(k)] = [str(v)]
    new_q = up.urlencode({k:v[0] for k,v in q.items()})
    return up.urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, p.fragment))

# BeautifulSoup is available already in your file
from bs4 import BeautifulSoup

def _extract_json_ld(soup):
    """Return parsed JSON-LD objects found on the page as a list."""
    out = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            raw = script.string or ""
            data = json.loads(raw)
            # some pages put an array at top-level
            if isinstance(data, list):
                out.extend(data)
            else:
                out.append(data)
        except Exception:
            # ignore parse errors
            continue
    return out

def parse_remotive(soup, job_url):
    """
    Remotive parser returns dict keys: posting_date, valid_through, Posted (optional)
    """
    out = {}
    # JSON-LD first
    for obj in _extract_json_ld(soup):
        if obj.get("@type") in ("JobPosting", "JobPostingContract", "Job"):
            # job posting JSON-LD
            dt = obj.get("datePosted") or obj.get("datePublished")
            if dt:
                # prefer ISO, strip time to YYYY-MM-DD
                out["posting_date"] = dt.split("T", 1)[0]
            vt = obj.get("validThrough")
            if vt:
                out["valid_through"] = vt.split("T", 1)[0]
            # sometimes they provide a relative label
            posted_label = obj.get("publishedLabel") or obj.get("datePostedLabel")
            if posted_label:
                out["Posted"] = posted_label
            return out

    # Fallback to meta tags
    meta_pub = soup.find("meta", {"property": "article:published_time"}) or soup.find("meta", {"name": "publication_date"})
    if meta_pub and meta_pub.get("content"):
        out["posting_date"] = meta_pub["content"].split("T", 1)[0]

    # Some Remotive pages include data in a script block under window.__INITIAL_STATE__ or a global variable.
    # Attempt a simple search for ISO datestamps in page text if above not found
    text = soup.get_text(" ", strip=True)
    import re
    m = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', text)
    if m and not out.get("posting_date"):
        out["posting_date"] = m.group(1).split("T", 1)[0]

    if out.get("posting_date") and not out.get("Posting Date"):
        out["Posting Date"] = out["posting_date"]
    if out.get("valid_through") and not out.get("Valid Through"):
        out["Valid Through"] = out["valid_through"]

    return out

def collect_dice_links(listing_url: str, max_pages: int = 25) -> list[str]:
    """Walk Dice /jobs?page=N pagination and dedupe links across pages."""
    seen, out = set(), []

    # Try to start from whatever page the URL already has
    try:
        start_page = int((up.parse_qs(up.urlparse(listing_url).query).get("page") or ["1"])[0])
    except Exception:
        start_page = 1

    page = start_page
    while page <= max_pages:
        url = _set_qp(listing_url, page=page)
        set_source_tag(url)

        html = get_html(url)  # will use Playwright for dice.com in your setup
        if not html:
            break

        links = find_job_links(html, url)

        added = 0
        for lk in links:
            if lk not in seen:
                seen.add(lk)
                out.append(lk)
                added += 1

        # stop when a page contributes nothing new
        if added == 0:
            break

        random_delay()
        page += 1

    return out



def parse_dice(soup, job_url):
    out = {}

    # 1) JSON-LD (keep this)
    for obj in _extract_json_ld(soup):
        if obj.get("@type") == "JobPosting":
            # Keep raw ISO strings (do not split here)
            dt = obj.get("datePosted") or obj.get("datePublished")
            if dt:
                out["posting_date"] = str(dt).strip()

            vt = obj.get("validThrough")
            if vt:
                out["valid_through"] = str(vt).strip()

            # NEW CODE GOES RIGHT HERE (you did this correctly)
            if not out.get("valid_through"):
                dd = obj.get("dateDeactivated")
                if dd:
                    out["valid_through"] = str(dd).strip()

            posted_label = obj.get("publishedLabel")
            if posted_label:
                out["Posted"] = posted_label

            org = obj.get("hiringOrganization") or {}
            if isinstance(org, dict) and org.get("name"):
                out["Company"] = org["name"]

            break

    # 2) Dice-specific fallback: <dhi-time-ago posted-date="...">
    if not out.get("posting_date"):
        tag = soup.select_one("dhi-time-ago[posted-date]")
        if tag:
            raw = (tag.get("posted-date") or "").strip()
            if raw:
                out["posting_date"] = raw   # keep raw, do not split

    # 3) Dice pages often also include JSON with "datePosted"
    if not out.get("posting_date"):
        html = soup.decode() if hasattr(soup, "decode") else str(soup)
        m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html)
        if m:
            out["posting_date"] = m.group(1).strip()

    # 4) meta fallback
    if not out.get("posting_date"):
        meta_pub = soup.find("meta", {"name": "date"})
        if meta_pub and meta_pub.get("content"):
            out["posting_date"] = meta_pub["content"].split("T", 1)[0]

    # 5) Dice-specific: dateDeactivated → Valid Through (HTML regex fallback)
    if not out.get("valid_through"):
        html = soup.decode() if hasattr(soup, "decode") else str(soup)
        m = re.search(r'"dateDeactivated"\s*:\s*"([^"]+)"', html)
        if m:
            out["valid_through"] = m.group(1).strip()

    return out


def muse_company_from_header(html: str) -> str:
    """
    The Muse: <a class="job-header_jobHeaderCompanyNameProgrammatic ...">Equinix, Inc</a>
    """
    try:
        soup = BeautifulSoup(html or "", "html.parser")
        a = soup.select_one("a.job-header_jobHeaderCompanyNameProgrammatic")
        if a and a.get_text(strip=True):
            return a.get_text(strip=True)
    except Exception:
        pass
    return ""

MUSE_DATE_POSTED_RX = re.compile(r'"datePosted"\s*:\s*"([^"]+)"')
MUSE_VALID_THROUGH_RX = re.compile(r'"validThrough"\s*:\s*"([^"]+)"')

def _extract_muse_dates_from_html(html: str) -> dict:
    """Pull datePosted / validThrough from The Muse job pages using regex."""
    out: dict = {}
    if not html:
        return out

    def _norm(raw: str) -> str:
        raw = str(raw)
        m = re.match(r"(\d{4}-\d{2}-\d{2})", raw)
        return m.group(1) if m else raw

    m = MUSE_DATE_POSTED_RX.search(html)
    if m:
        iso = _norm(m.group(1))
        out["date_posted"] = iso
        out["posting_date"] = iso

    m = MUSE_VALID_THROUGH_RX.search(html)
    if m:
        iso = _norm(m.group(1))
        out["valid_through"] = iso

    return out


# -------- Generic HTML fallback for dates (board-agnostic) --------

# Common schema.org style key names used across many boards
GENERIC_DATE_PATTERNS = [
    # Posting date variants
    (re.compile(r'"datePosted"\s*:\s*"([^"]+)"', re.IGNORECASE), "posting_date"),
    (re.compile(r'"date_posted"\s*:\s*"([^"]+)"', re.IGNORECASE), "posting_date"),
    (re.compile(r'"postingDate"\s*:\s*"([^"]+)"', re.IGNORECASE), "posting_date"),
    # Valid through / expiration date variants
    (re.compile(r'"validThrough"\s*:\s*"([^"]+)"', re.IGNORECASE), "valid_through"),
    (re.compile(r'"valid_through"\s*:\s*"([^"]+)"', re.IGNORECASE), "valid_through"),
]

def extract_generic_dates(html: str) -> dict:
    """Board-agnostic fallback that pulls posting / valid dates out of raw HTML."""
    out: dict = {}
    if not html:
        return out

    text = html
    for rx, key in GENERIC_DATE_PATTERNS:
        m = rx.search(text)
        if not m:
            continue

        raw = m.group(1)
        try:
            iso = parse_date_relaxed(raw)
        except Exception:
            iso = raw

        if key == "posting_date":
            # Keep both aliases consistent for the normalizer
            out.setdefault("date_posted", iso)
            out.setdefault("posting_date", iso)
        elif key == "valid_through":
            out.setdefault("valid_through", iso)

    return out


import re
def collect_hubspot_links(listing_url: str, max_pages: int = 25) -> list[str]:
    """Walk HubSpot /careers/jobs?page=N pagination without double-counting page 1, with per-page logs."""
    seen, out = set(), []
    try:
        start_page = int((up.parse_qs(up.urlparse(listing_url).query).get("page") or ["1"])[0])
    except Exception:
        start_page = 1

    page = start_page
    while page <= max_pages:
        url = _set_qp(listing_url, page=page)
        set_source_tag(url)

        t0 = time.time()
        progress_clear_if_needed()

        html = get_html(url)
        if not html:
            #log_print("[WARN", f"]{DOT3}{DOTW} Warning: Failed to GET listing page: {listing_url}")
            progress_clear_if_needed()
            break

        links = parse_hubspot_list_page(html, url)
        elapsed = time.time() - t0
        progress_clear_if_needed()

        # de-dupe across pages
        added = 0
        for u in links:
            if u not in seen:
                seen.add(u)
                out.append(u)
                added += 1

        progress_clear_if_needed()

        # stop when a page contributes nothing new
        if added == 0:
            break

        # gentle pause before next page
        random_delay()
        page += 1

    return out


def collect_uw_links() -> list[str]:
    """
    UW Workday focused pull:
      - role queries only
      - prefer Remote/Seattle/WA in text to cut volume
    """
    host, tenant = "wd5.myworkdaysite.com", "uw"

    role_terms = [
        "product manager", "product owner",
        "business analyst", "system analyst", "systems analyst", "business systems analyst",
        "scrum master"
    ]
    # We bias toward remote or Seattle in the search itself
    queries = [f"{rt} remote" for rt in role_terms] + [f"{rt} seattle" for rt in role_terms]

    links = []
    for q in queries:
        #for j in _wd           #may not be necessary
        return []

def write_rows_csv(path: str, rows: list[dict], header_fields: list[str]):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    new_file = not Path(path).exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header_fields, extrasaction="ignore")
        if new_file:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)

ROLE_RX = re.compile(
    r"(?i)\b("
    r"(group|staff|principal)?\s*product\s*[- ]*\s*(owner|manager)|"
    r"product\s+lead(?!\s*gen)|"          # allow Product Lead, but not lead gen
    r"technical\s+product\s+owner|"
    r"product\s+management|"
    r"product\b.{0,30}\b(owner|manager)"  # allow more in-between noise
    r")\b"
)



import urllib.parse

MAX_PAGES_SIMPLYHIRED = 2  # bump later if you like


# -----------------------------
# HubSpot careers (PUBLIC page)
# example listing page: https://www.hubspot.com/careers/jobs?page=1
# job detail pages can be on hubspot.com or redirect to an ATS (Greenhouse/Lever/etc.)
# -----------------------------


def parse_hubspot_list_page(html: str, base: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[str] = []

    for a in soup.select('a[href]'):
        txt  = a.get_text(" ", strip=True).lower()
        href = a.get("href", "")

        # Skip generic listing CTAs
        if "all open positions" in txt:

            continue

        # Accept only real job details like /careers/jobs/<slug or id>
        if "/careers/jobs/" in href:
            p = up.urlparse(up.urljoin(base, href))
            segs = [s for s in p.path.split("/") if s]
            if len(segs) >= 3:  # careers / jobs / <slug-or-id>
                out.append(up.urljoin(base, href))

        # Still allow direct ATS links
        if any(k in href for k in ("greenhouse.io", "lever.co", "workday", "smartrecruiters.com")):
            out.append(up.urljoin(base, href))

    # de-dupe while preserving order
    seen, deduped = set(), []
    for u in out:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    return deduped


import re
from datetime import datetime
from html import unescape



def enrich_dice_fields(details: dict, raw_html: str) -> dict:
    """
    Normalize a Dice job-detail page.

    - `details["Apply URL"]` should already be the job-detail URL.
    - `raw_html` should be the full HTML (what you see in view-source).
    """

    html = raw_html or ""
    soup = BeautifulSoup(html, "html.parser")

    try:
        board_data = parse_dice(soup, details.get("Job URL") or details.get("Apply URL") or "") or {}

        # Safer merge: do not overwrite canonical fields
        for k, v in board_data.items():
            if not v:
                continue
            if k in ("Posting Date", "Valid Through"):
                continue
            details.setdefault(k, v)

    except Exception as e:
        log_line("WARN", f"parse_dice failed in enrich_dice_fields: {e}")

    # Prefer location from Dice header line (more reliable than og:title)
    header_loc = _dice_extract_location_from_soup(soup)

    def _is_us_only(val: str) -> bool:
        v = (val or "").strip().lower()
        return v in {"us", "usa", "united states", "united states of america"}

    if header_loc:
        # Optional: drop trailing ", US" for your sheet Location display
        loc_parts = [p.strip() for p in header_loc.split(",") if p.strip()]
        if len(loc_parts) >= 2 and loc_parts[-1].lower() in {"us", "usa"}:
            header_loc_display = ", ".join(loc_parts[:-1])
        else:
            header_loc_display = header_loc

        # Override only if Location is missing or currently just "US"
        cur_loc = details.get("Location") or details.get("LocationRaw") or ""
        if not cur_loc or _is_us_only(cur_loc):
            details["Location"] = header_loc_display
            details["LocationRaw"] = header_loc

    # 1. Title / Company / Location from og:title
    #    Example:
    #    <meta property="og:title"
    #      content="Workday Federal - Talent, Learning and Recruiting Lead - Navigant Consulting - McLean, VA">
    title_meta = re.search(
        r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if title_meta:
        raw_title = unescape(title_meta.group(1)).strip()
        parts = [p.strip() for p in raw_title.split(" - ") if p.strip()]

        if len(parts) >= 3:
            # Everything except the last two pieces is the full job title
            title = " - ".join(parts[:-2])
            company_from_title = parts[-2]
            location_from_title = parts[-1]
        elif len(parts) == 2:
            title, company_from_title = parts
            location_from_title = None
        else:
            title = raw_title
            company_from_title = None
            location_from_title = None

        details.setdefault("Title", title)

        if company_from_title:
            details.setdefault("Company", company_from_title)

        if location_from_title:
            details.setdefault("Location", location_from_title)
            details.setdefault("Location Raw", location_from_title)

    # 2. Prefer branded company name from JSON if present
    #    ... "company_name":"Guidehouse" ...
    brand_match = re.search(r'"company_name"\s*:\s*"([^"]+)"', html)
    if brand_match:
        brand = unescape(brand_match.group(1)).strip()
        if brand:
            # Always upgrade to the branded name
            details["Company"] = brand

    # 3. Company fallback from the company link if still missing
    #    data-cy="companyNameLink">REDLEO SOFTWARE INC.</a>
    if not details.get("Company"):
        m = re.search(
            r'companyNameLink"\s*>\s*([^<]+)</a>',
            html,
            flags=re.DOTALL | re.IGNORECASE,
        )
        if m:
            company = unescape(re.sub(r"\s+", " ", m.group(1))).strip()
            details.setdefault("Company", company)

    # 4. Strong location fallback from header line
    #    <li data-cy="location">McLean, VA</li>
    if not details.get("Location") or "Consulting" in details.get("Location", ""):
        m = re.search(
            r'<li[^>]+data-cy=["\']location["\'][^>]*>([^<]+)</li>',
            html,
            flags=re.DOTALL | re.IGNORECASE,
        )
        if m:
            loc = unescape(re.sub(r"\s+", " ", m.group(1))).strip()
            if loc:
                details["Location"] = loc
                details.setdefault("Location Raw", loc)

    # 5. Location chip fallback (Remote, On Site, Hybrid) if still missing
    #    <span id="location: Remote">Remote</span>
    if not details.get("Location"):
        m = re.search(
            r'id="location:\s*[^"]*"\s*>\s*([^<]+)</span>',
            html,
            flags=re.DOTALL | re.IGNORECASE,
        )
        if m:
            loc = unescape(re.sub(r"\s+", " ", m.group(1))).strip()
            details.setdefault("Location", loc)
            details.setdefault("Location Raw", loc)

    # 6. "Posted ..." line from the timeAgo span (best effort)
    #    <span id="timeAgo">Posted 4 days ago | Updated 4 days ago</span>
    if not details.get("Posted Raw") or not details.get("Posted"):
        posted_raw = None

        # Try the span first
        m = re.search(
            r'id="timeAgo"[^>]*>([^<]+)',
            html,
            flags=re.DOTALL | re.IGNORECASE,
        )
        if m:
            posted_raw = unescape(re.sub(r"\s+", " ", m.group(1))).strip()

        # Fallback to JSON "timeAgo" if needed
        if not posted_raw:
            m = re.search(
                r'"timeAgo"\s*:\s*"([^"]+)"',
                html,
                flags=re.DOTALL | re.IGNORECASE,
            )
            if m:
                posted_raw = unescape(re.sub(r"\s+", " ", m.group(1))).strip()

        if posted_raw:
            details.setdefault("Posted Raw", posted_raw)
            details.setdefault("Posted", posted_raw)


    # 7. Basic description fallback so salary logic still has text
    if not details.get("Description"):
        text_only = re.sub(r"<[^>]+>", " ", html)
        text_only = re.sub(r"\s+", " ", text_only).strip()

        max_desc_len = 2000
        details["Description"] = text_only[:max_desc_len]
        details["Description Snippet"] = text_only[:300]

    # 8. Job URL and board markers
    if "Job URL" not in details and "Apply URL" in details:
        details["Job URL"] = details["Apply URL"]

    details["Career Board"] = details.get("Career Board") or "Dice"
    details["CareerBoard"] = details.get("CareerBoard") or "Dice"

    return details


def parse_hubspot_detail(html_or_soup, job_url: str) -> dict:
    """Extract title/company/location/snippet; capture Apply link if present."""
    from bs4 import BeautifulSoup

    UNHELPFUL_TITLES = {
        "all open positions",
        "open positions",
        "all positions",
        "jobs at hubspot",
    }

    # Accept either raw HTML or an already-parsed soup
    if hasattr(html_or_soup, "find"):
        soup = html_or_soup
    else:
        soup = BeautifulSoup(html_or_soup or "", "html.parser")

    # Title heuristics: prefer <h2> headings that are not "All Open Positions"
    title = ""
    h2_list = [
        h for h in soup.find_all("h2")
        if h and h.get_text(strip=True) and h.get_text(strip=True).lower() != "all open positions"
    ]
    h2 = h2_list[0] if h2_list else None
    if h2:
        title = h2.get_text(strip=True)
    if not title:
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            title = h1.get_text(strip=True)
    if not title:
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            title = og["content"].strip()
    if not title and soup.title and soup.title.string:
        title = soup.title.string.strip()

    company = "HubSpot"

    # Location: try meta/labels or visible labels
    loc = ""
    # If we found an <h2>, prefer a sibling/nearby <h3> as location
    if h2:
        parent = h2.parent
        if parent:
            h3 = parent.find("h3")
            if h3 and h3.get_text(strip=True):
                loc = h3.get_text(strip=True)

    # Try to derive Posting Date from visible text (best effort)
    posted = ""
    try:
        import re as _re
        date_re = _re.compile(
            r"posted\s+(?:on\s+)?(?P<d>([A-Za-z]{3,9}\s+\d{1,2},?\s*\d{4})|\d{4}-\d{2}-\d{2})",
            _re.I,
        )
        text_block = soup.get_text(" ", strip=True)
        m = date_re.search(text_block)
        if m:
            posted = m.group("d").strip()
    except Exception:
        posted = ""

    # Job ID / key from URL path
    job_id_vendor = ""
    segs = [s for s in up.urlparse(job_url).path.split("/") if s]
    if segs:
        job_id_vendor = segs[-1]

    # Employment type from text near header
    emp_type = ""
    header_block = ""
    if h2 and h2.parent:
        header_block = h2.parent.get_text(" ", strip=True)
    block = (header_block or soup.get_text(" ", strip=True)).lower()
    for tag in ("full-time", "part-time", "contract", "intern"):
        if tag in block:
            emp_type = tag
            break

    # Remote / onsite signal (prefer explicit Hybrid over Remote/onsite)
    remote_flag = "unknown_or_onsite"

    # Workday detail pages often expose a labeled "remote type" row; grab it directly if present
    try:
        import re as _re
        rt_label = soup.find(string=_re.compile(r"remote\s*type", _re.I))
        if rt_label:
            dd = rt_label.find_next("dd")
            if dd and dd.get_text(strip=True):
                remote_flag = dd.get_text(strip=True)
    except Exception:
        pass

    details["remote_flag"] = remote_flag  # <--- add this

    # Remote rule
    remote_rule = (details.get("Remote Rule") or details.get("remote_flag") or "Unknown").strip()
    remote_rule_norm = remote_rule.strip().title()

    badge_text = " ".join([ details.get("workplace_type") or "", details.get("work_mode") or "", details.get("workplace_badge") or "", ]).strip()

    job_url_low = (details.get("Job URL") or "").lower()
    is_biv = "builtinvancouver.org" in job_url_low

    # If we already have an explicit rule, keep it
    if remote_rule_norm not in {"Remote", "Hybrid", "Onsite"}:
        if is_biv:
            remote_rule_norm = classify_work_mode(badge_text) if badge_text else "Unknown"
        else:
            text_for_rules = locals().get("text_for_rules", "") or ""
            t = f"{text_for_rules} {badge_text}".lower()
            remote_rule_norm = classify_work_mode(t)

    details["Remote Rule"] = remote_rule_norm

    for sel in [
        '[data-test-id="location"]',
        ".job-location",
        'meta[property="og:locale"]',
    ]:
        node = soup.select_one(sel)
        if not node:
            continue

        txt = node.get("content") if node.name == "meta" else node.get_text(" ", strip=True)
        if txt:
            loc = txt.strip()
            break

    # Workday HTML fallback: data-automation-id="locations" often holds the main location
    if not loc:
        loc_node = soup.find(attrs={"data-automation-id": "locations"})
        if loc_node:
            loc_txt = loc_node.get_text(" ", strip=True)
            if loc_txt:
                loc = loc_txt

    # Description snippet (lightweight)
    desc = ""
    md = soup.find("meta", attrs={"name": "description"})
    if md and md.get("content"):
        desc = md["content"].strip()
    if not desc:
        p = soup.find("p")
        if p:
            desc = p.get_text(" ", strip=True)[:300]

    # Salary hint (generic, no board-specific markup)
    salary_text_blob = soup.get_text(" ", strip=True)
    sal_lo, sal_hi = extract_salary_from_text(salary_text_blob)
    salary_range = ""
    if sal_lo and sal_hi:
        salary_range = f"${sal_lo:,} - ${sal_hi:,}"
    elif sal_hi:
        salary_range = f"${sal_hi:,}"

    # Apply URL: look for outbound ATS links, else stick with job_url
    apply_url = ""
    apply_note = ""
    ats_links: list[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        label = a.get_text(" ", strip=True).lower()
        if "apply" in label or "submit" in label or "apply now" in label:
            apply_url = up.urljoin(job_url, href)
            ats_links.append(apply_url)
            break
        if any(x in href for x in ("greenhouse.io", "lever.co", "myworkdayjobs.com", "smartrecruiters.com")):
            apply_url = up.urljoin(job_url, href)
            ats_links.append(apply_url)
            break
    if not apply_url:
        apply_url = job_url
        apply_note = "Apply link missing or job unavailable"

    # Canonical / og:url candidates (often the real job URL)
    canonical = ""
    tag = soup.find("link", rel="canonical")
    if tag and tag.get("href"):
        canonical = tag["href"].strip()
    ogu = soup.find("meta", property="og:url")
    if ogu and ogu.get("content"):
        canonical = canonical or ogu["content"].strip()

    # Follow once if title looks unhelpful; try ATS links first, then canonical/og:url
    if not title or title.strip().lower() in UNHELPFUL_TITLES:
        candidates = []
        candidates.extend(ats_links)
        if canonical and canonical != job_url:
            candidates.append(canonical)

        for tgt in candidates:
            if not tgt or tgt == job_url:
                continue
            try:
                html2 = get_html(tgt)
                if not html2:
                    continue
                d2 = extract_job_details(html2, tgt)
                d2 = enrich_salary_fields(d2, page_host=up.urlparse(tgt).netloc)
                new_title = (d2.get("Title") or "").strip()
                if new_title and new_title.lower() not in UNHELPFUL_TITLES:
                    title = new_title
                    company = d2.get("Company") or company
                    loc = d2.get("Location") or loc
                    job_url = tgt
                    apply_url = d2.get("apply_url", apply_url)
                    break
            except Exception:
                continue

    # Final slug-based fallback if still unhelpful
    if not title or title.strip().lower() in UNHELPFUL_TITLES:
        seg = up.urlparse(job_url).path.rstrip("/").split("/")[-1]
        seg = seg.split("?")[0]
        if seg:
            slug = seg.replace("-", " ").replace("_", " ").strip()
            if slug:
                title = slug.title()

    details = {
        "Title": title,
        "Company": "HubSpot",
        "Location": loc,
        "job_url": job_url,
        "apply_url": apply_url,
        "Apply URL Note": apply_note or "",
        "description_snippet": desc,
        "Description": desc,
        "Posting Date": posted,
        "Posted": posted,
        "Job ID (Vendor)": job_id_vendor,
        "Job Key": job_id_vendor,
        "employment_type": emp_type,
        "is_remote_flag": remote_flag,
        "Remote Rule": remote_flag,
        "Location Chips": "",
        "Applicant Regions": "",
        "Applicant Regions Source": "",
        "Salary Range": salary_range,
        "Salary Est. (Low-High)": salary_range,
        "career_board": "HubSpot (Public)",
    }

    # Final title cleanup with company context
    details["Title"] = normalize_title(details.get("Title"), details.get("Company"))
    return details


def collect_simplyhired_links(listing_url: str) -> list[str]:
    """Collect job detail links from a SimplyHired search listing."""
    found: list[str] = []
    seen = set()

    page_url = listing_url
    pages = 0
    while page_url and pages < MAX_PAGES_SIMPLYHIRED:
        set_source_tag(listing_url)
        html = get_html(page_url)
        if not html:
            #log_print("[WARN", f" ]{DOT3}{DOTW} Warning: Failed to GET listing page: {listing_url}")
            break

        soup = BeautifulSoup(html, "html.parser")

        # Job cards: links look like /job/<slug-or-id>...
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("/job/"):
                url = urllib.parse.urljoin(SIMPLYHIRED_BASE, href)
                if url not in seen:
                    seen.add(url)
                    found.append(url)

                    # Capture the card title so we can still log something
                    # even if the detail page 403s later.
                    title_text = a.get_text(" ", strip=True) or ""
                    if title_text:
                        SIMPLYHIRED_TITLES[url] = title_text

        # Follow pagination via rel=next or aria-label="Next"
        next_link = soup.find("a", rel=lambda v: v and "next" in v) or \
                    soup.find("a", attrs={"aria-label": "Next"})
        if next_link and next_link.get("href"):
            page_url = urllib.parse.urljoin(SIMPLYHIRED_BASE, next_link["href"])
            pages += 1
        else:
            break

    return found

ID_LIKE_RX = re.compile(r"^[\d._-]{5,}$")

from bs4 import BeautifulSoup
import urllib.parse as up

def _prune_non_job_sections(soup, host: str) -> None:
    # generic sidebars and recommendation blocks
    generic = [
        "aside",
        ".sidebar",
        'div:-soup-contains("Similar Jobs")',
        'section:-soup-contains("Similar Jobs")',
        'div:-soup-contains("Recommended jobs")',
        'section:-soup-contains("Recommended jobs")',
        'div:-soup-contains("People also viewed")',
        'section:-soup-contains("People also viewed")',
        '[aria-label*="Similar"]',
    ]
    for sel in generic:
        try:
            for n in soup.select(sel):
                n.decompose()
        except Exception:
            pass

    # site specific cleanups
    if "edtech.com" in host:
        for sel in [
            '*:div:-soup-contains("Here are some similar Product Development jobs")',
            'div:has(h2:div:-soup-contains("Similar Product Development jobs"))',
        ]:
            try:
                for n in soup.select(sel):
                    n.decompose()
            except Exception:
                pass

    if "themuse.com" in host:
        for sel in ['[role="complementary"]']:
            try:
                for n in soup.select(sel):
                    n.decompose()
            except Exception:
                pass

# The Muse: pull 'Client-provided Location(s)' text when present.
def _extract_muse_location(soup: BeautifulSoup) -> str | None:
    import re as _re
    try:
        label = soup.find(string=lambda t: isinstance(t, str) and "client-provided location" in t.lower())
        container = None
        if label:
            container = label.parent
            if container and container.name in ("b", "strong"):
                container = container.parent
        if container:
            label_text = label.strip() if isinstance(label, str) else ""
            text = container.get_text(" ", strip=True)
            if text.lower().startswith(label_text.lower()):
                text = text[len(label_text):].lstrip(" :·-").strip()
            if text:
                return text
            next_block = container.find_next(string=lambda t: isinstance(t, str) and t.strip())
            if next_block:
                return next_block.strip()
        meta_loc = soup.find("meta", attrs={"name": "jobLocation"})
        if meta_loc and meta_loc.get("content"):
            return meta_loc["content"].strip()
        loc_candidate = soup.find(lambda tag: tag.name in ("span", "div") and tag.get_text() and _re.search(r",\\s*[A-Za-z]{2}", tag.get_text()))
        if loc_candidate:
            return loc_candidate.get_text(" ", strip=True)
    except Exception:
        return None
    return None

def _debug_biv(details: dict, host: str, label: str) -> None:
    if "builtinvancouver.org" not in (host or "").lower():
        return


    #log_print(f"{_box('BIV DEBUG ')}{DOT6}Location              : {details.get('Location')}")

    # ✅ ADD THIS
    biv_locs = details.get("BIV Tooltip Locations")
    biv_count = details.get("BIV Tooltip Location Count")
    if biv_count is None and isinstance(biv_locs, list):
        biv_count = len(biv_locs)
    #log_print(f"{_box('BIV DEBUG ')}{DOT6}BIV Location Count    : {biv_count}")

    #log_print(f"{_box('BIV DEBUG ')}{DOT6}Canada                : {details.get('Canada Rule')}")
    #log_print(f"{_box('BIV DEBUG ')}{DOT6}US                    : {details.get('US Rule')}")
    #log_print(f"{_box('BIV DEBUG ')}{DOT6}WA                    : {details.get('WA Rule')}")
    #log_print(f"{_box('BIV DEBUG ')}{DOT6}Remote                : {details.get('Remote Rule')}")
    #log_print(f"{_box('BIV DEBUG ')}{DOT6}Chips                 : {details.get('Location Chips')}")
    #log_print(f"{_box('BIV DEBUG ')}{DOT6}Regions               : {details.get('Applicant Regions')}")
    #log_print(f"{_box('BIV DEBUG ')}{DOT6}💲 Salary             : {details.get('Salary Range')}")
    #log_print(f"{_box('BIV DEBUG ')}{DOT6}💲 Salary Text        : {details.get('Salary Text')}")
    #log_print(f"{_box('BIV DEBUG ')}{DOT6}💲 Salary Range       : {details.get('Salary Range')}")
    #log_print(f"{_box('BIV DEBUG ')}{DOT6}💲 Salary Status      : {details.get('Salary Source')}")
    #log_print(f"{_box('BIV DEBUG ')}{DOT6}💲 Salary Placeholder : {details.get('Salary Placeholder')}")


def _debug_biv_loc(stage: str, details: dict, extra: dict | None = None) -> None:
    if not DEBUG_LOCATION:
        return
    extra = extra or {}
    #log_line("BIV DEBUG", f"{DOTL}LOC STAGE          : {stage}")
    #log_line("BIV DEBUG", f"{DOTL}..Location         : {details.get('Location')!r}")
    #log_line("BIV DEBUG", f"{DOTL}..Location Raw     : {details.get('Location Raw')!r}")
    #log_line("BIV DEBUG", f"{DOTL}..Remote Rule      : {details.get('Remote Rule')!r}")
    #log_line("BIV DEBUG", f"{DOTL}..Location Chips   : {details.get('Location Chips')!r}")
    #log_line("BIV DEBUG", f"{DOTL}..Locations Text   : {details.get('Locations Text')!r}")
    #log_line("BIV DEBUG", f"{DOTL}..Regions          : {details.get('Regions')!r}")

    # NEW: always show minimal signal when Built In jobs are missing title/company
    try:
        # Prefer URL signal because Board is often not set yet when this helper is called.
        job_url = ""
        if extra and isinstance(extra, dict):
            job_url = (extra.get("job_url") or extra.get("url") or "").strip().lower()

        is_builtin = ("builtin.com" in job_url) or ("builtinvancouver.org" in job_url) or ("builtinseattle.com" in job_url)

        t_ok = bool((details.get("Title") or "").strip())
        c_ok = bool((details.get("Company") or "").strip())

        if is_builtin and not (t_ok and c_ok):
            log_line(
                "DEBUG",
                f"[BIVDBG] {stage} title={details.get('Title')} company={details.get('Company')} extra={extra or {}}",
            )
    except Exception:
        pass

    for k, v in extra.items():
        s = str(v)
        if len(s) > 220:
            s = s[:220] + "...(trunc)"
        #log_line("BIV DEBUG", f"{DOTL}..{k:<16}: {s}")

_BUILTIN_RANGE_RX = re.compile(
    r"""
    (?:
        compensation(?:\s+details)? |
        canada\s+pay\s+range |
        pay\s+range
    )
    [^\$]{0,40}
    \$\s*(?P<min>[\d,]+(?:\.\d{1,2})?)
    \s*(?:-|–|—|to)\s*
    \$\s*(?P<max>[\d,]+(?:\.\d{1,2})?)
    (?:\s*(?P<cur>CAD|USD))?
    """,
    re.IGNORECASE | re.VERBOSE,
)

_BUILTIN_CUR_RX = re.compile(r"\b(CAD|USD)\b", re.IGNORECASE)
_BUILTIN_HOURLY_RX = re.compile(r"\b(per\s*hour|hourly|/hr|/hour)\b", re.IGNORECASE)

def _to_float_num(s: str) -> Optional[float]:
    try:
        return float(s.replace(",", "").strip())
    except Exception:
        return None

def _walk_json(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk_json(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from _walk_json(it)

def _extract_salary_from_jsonld(page_text: str) -> Optional[Dict[str, Any]]:
    # Grab all ld+json blocks, parse any that load
    blocks = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        page_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not blocks:
        # Sometimes you only have “view-source” text or simplified content
        blocks = re.findall(
            r'"@type"\s*:\s*"JobPosting".*?\}\s*\}|\{.*?"@type"\s*:\s*"JobPosting".*?\}',
            page_text,
            flags=re.IGNORECASE | re.DOTALL,
        )

    for raw in blocks:
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        for node in _walk_json(data):
            if not isinstance(node, dict):
                continue
            if str(node.get("@type") or "").lower() != "jobposting":
                continue

            # Common schema patterns
            sal = node.get("baseSalary") or node.get("estimatedSalary")
            if not isinstance(sal, dict):
                continue

            cur = (sal.get("currency") or node.get("salaryCurrency") or "").strip().upper() or None
            val = sal.get("value")

            min_v = max_v = unit = None

            if isinstance(val, dict):
                unit = (val.get("unitText") or "").strip().upper() or None
                min_v = val.get("minValue")
                max_v = val.get("maxValue")
                if min_v is None and isinstance(val.get("value"), (int, float)):
                    min_v = val.get("value")
                if max_v is None and isinstance(val.get("value"), (int, float)):
                    max_v = val.get("value")
            elif isinstance(val, (int, float)):
                min_v = val
                max_v = val

            if isinstance(min_v, (int, float)) and isinstance(max_v, (int, float)):
                salary_text = f"{cur + ' ' if cur else ''}{min_v:,.0f} - {max_v:,.0f}".strip()
                return {
                    "Salary From": float(min_v),
                    "Salary To": float(max_v),
                    "Salary Currency": cur,
                    "Salary Unit": unit or "YEAR",
                    "Salary Text": salary_text,
                    "Salary Range": salary_text,
                    "Salary Source": "builtin_jsonld",
                }

    return None

def _extract_salary_from_visible_text(page_text: str, fallback_currency: Optional[str] = None) -> Optional[Dict[str, Any]]:
    m = _BUILTIN_RANGE_RX.search(page_text or "")
    if not m:
        return None

    lo = _to_float_num(m.group("min") or "")
    hi = _to_float_num(m.group("max") or "")
    if lo is None or hi is None:
        return None

    # Currency: explicit wins, otherwise infer
    cur = (m.group("cur") or "").upper().strip() or None
    if not cur:
        # Look nearby for CAD/USD, else fallback
        window = (page_text[m.start(): m.end() + 80] if page_text else "")
        cur2 = _BUILTIN_CUR_RX.search(window)
        cur = (cur2.group(1).upper() if cur2 else None) or fallback_currency

    # Unit: hourly if obvious, else year
    window2 = (page_text[m.start(): m.end() + 120] if page_text else "")
    unit = "HOUR" if _BUILTIN_HOURLY_RX.search(window2) else "YEAR"

    # NEW: avoid .00 for annual salaries
    if unit == "YEAR":
        lo = int(lo)
        hi = int(hi)

    # NEW: format annual salaries without decimals
    if unit == "YEAR":
        salary_text = f"{cur + ' ' if cur else ''}{lo:,} - {hi:,}".strip()
    else:
        salary_text = f"{cur + ' ' if cur else ''}{lo:,.2f} - {hi:,.2f}".strip()

    return {
        "Salary From": lo,
        "Salary To": hi,
        "Salary Currency": cur,
        "Salary Unit": unit,
        "Salary Text": salary_text,
        "Salary Range": salary_text,
        "Salary Source": "builtin_text",
    }


def _money_to_int(s: str) -> Optional[int]:
    if not s:
        return None
    # remove commas and currency symbols
    s2 = re.sub(r"[^\d.]", "", s)
    if not s2:
        return None
    # If it looks like a float, allow it but cast to int
    try:
        return int(float(s2))
    except Exception:
        return None

def _normalize_salary_unit(unit_text: str) -> str:
    u = (unit_text or "").strip().lower()
    if "hour" in u:
        return "hour"
    if "week" in u:
        return "week"
    if "month" in u:
        return "month"
    if "year" in u or "ann" in u:
        return "year"
    return u or "year"

def _format_salary(min_amt: Optional[int], max_amt: Optional[int], currency: str, unit: str) -> str:
    cur = (currency or "").upper() or "CAD"
    u = _normalize_salary_unit(unit)

    if min_amt is None and max_amt is None:
        return ""

    # If only one side exists, treat it as a single value
    if min_amt is None:
        min_amt = max_amt
    if max_amt is None:
        max_amt = min_amt

    if min_amt == max_amt:
        return f"{cur} {min_amt:,}/{u}"

    lo, hi = sorted([min_amt, max_amt])
    return f"{cur} {lo:,}–{hi:,}/{u}"

def _builtin_fill_title_company_from_builtinsignals(details: dict, soup: BeautifulSoup, html: str | None = None) -> None:
    if (details.get("Title") or "").strip() and (details.get("Company") or "").strip():
        return

    html_text = html or str(soup)

    # 1) Builtin.jobPostInit: parse with brace matching (more reliable than regex capture)
    try:
        import json
        import re

        m = re.search(r"Builtin\.jobPostInit\(\s*\{", html_text)
        if m:
            i = m.end() - 1  # points at the "{"
            depth = 0
            in_str = False
            esc = False
            j = i

            while j < len(html_text):
                ch = html_text[j]

                if in_str:
                    if esc:
                        esc = False
                    elif ch == "\\":
                        esc = True
                    elif ch == '"':
                        in_str = False
                else:
                    if ch == '"':
                        in_str = True
                    elif ch == "{":
                        depth += 1
                    elif ch == "}":
                        depth -= 1
                        if depth == 0:
                            j += 1
                            break
                j += 1

            if depth == 0 and j > i:
                blob = html_text[i:j]
                data = json.loads(blob)
                job = data.get("job") if isinstance(data, dict) else None
                details["_builtin_title_company_source"] = "jobPostInit"
                if isinstance(job, dict):
                    t = (job.get("title") or "").strip()
                    c = (job.get("companyName") or "").strip()

                    if t and not (details.get("Title") or "").strip():
                        details["Title"] = t
                    if c and not (details.get("Company") or "").strip():
                        details["Company"] = c

                    if (details.get("Title") or "").strip() and (details.get("Company") or "").strip():
                        return
    except Exception:
        pass

    # 2) JSON-LD JobPosting: title + hiringOrganization.name
    try:
        import json

        for node in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = (node.get_text() or "").strip()
            if not raw:
                continue
            data = json.loads(raw)

            candidates = []
            if isinstance(data, dict) and isinstance(data.get("@graph"), list):
                candidates = data["@graph"]
            elif isinstance(data, list):
                candidates = data
            elif isinstance(data, dict):
                candidates = [data]

            for item in candidates:
                if not isinstance(item, dict):
                    continue
                if item.get("@type") != "JobPosting":
                    continue

                t = (item.get("title") or "").strip()
                org = item.get("hiringOrganization") or {}
                c = (org.get("name") or "").strip() if isinstance(org, dict) else ""

                if t and not (details.get("Title") or "").strip():
                    details["Title"] = t
                if c and not (details.get("Company") or "").strip():
                    details["Company"] = c
                if (details.get("Title") or "").strip() and (details.get("Company") or "").strip():
                    return
                    details["_builtin_title_company_source"] = "jsonld"

    except Exception:
        pass

    # 3) HTML <title> as a last resort
    try:
        tnode = soup.find("title")
        if tnode:
            raw_title = (tnode.get_text() or "").strip()
            # Typical: "Role - Company | Built In" or "Role - Company - Company | Built In"
            if raw_title and "| Built In" in raw_title:
                left = raw_title.split("| Built In", 1)[0].strip()
                parts = [p.strip() for p in left.split(" - ") if p.strip()]
                if parts:
                    role = parts[0]
                    company = parts[-1] if len(parts) > 1 else ""
                    if role and not (details.get("Title") or "").strip():
                        details["Title"] = role
                    if company and not (details.get("Company") or "").strip():
                        details["Company"] = company
                        details["_builtin_title_company_source"] = "html_title"
    except Exception:
        pass

def _builtin_visible_location_from_html(html: str) -> str:
    """
    Built In fallback: extract a single visible location shown beside the location icon.
    Handles patterns like:
      <span>Peoria, IL</span>
    and also:
      <span>Hiring Remotely in </span><span>Los Angeles, CA</span>
    """
    h = html or ""
    if not h:
        return ""

    # Narrow to a small window after the location icon
    m = re.search(r"fa-location-dot[^>]*></i>(.{0,900})", h, flags=re.I | re.S)
    if not m:
        return ""

    window = m.group(1)

    spans = re.findall(
        r"<span[^>]*>\s*([^<]{1,140}?)\s*</span>",
        window,
        flags=re.I | re.S,
    )
    if not spans:
        return ""

    cleaned: list[str] = []
    for s in spans:
        txt = re.sub(r"\s+", " ", s).strip()
        if not txt:
            continue

        low = txt.lower()

        # junk tokens
        if low in {"remote", "hybrid", "locations", "job locations"}:
            continue
        if low.startswith("hiring remotely"):
            continue
        if _builtin_is_count_label(txt):
            continue

        cleaned.append(txt)

    if not cleaned:
        return ""

    # choose the last candidate that looks like a place
    for txt in reversed(cleaned):
        low = txt.lower().strip()

        if "," in txt:
            return txt

        if low in {"us", "usa", "united states", "united states of america"}:
            return "US"

        if low == "canada":
            return "CA"

    return ""

def _debug_builtin_page_fingerprint(details: dict, host: str, job_url: str, html: str) -> None:
    try:
        import re
        title_ok = bool(re.search(r"<title>.*?</title>", html or "", re.I | re.S))
        h1_ok = bool(re.search(r"<h1\b", html or "", re.I))
        jsonld_ok = bool(re.search(r'application/ld\+json', html or "", re.I)) and bool(re.search(r'"@type"\s*:\s*"JobPosting"', html or "", re.I))
        init_ok = bool(re.search(r"Builtin\.jobPostInit\(", html or ""))
        length = len(html or "")
        _debug_biv_loc(
            "BUILTIN PAGE FP",
            details,
            {
                "host": host,
                "url": job_url,
                "len": length,
                "has_title": title_ok,
                "has_h1": h1_ok,
                "has_jsonld_jobposting": jsonld_ok,
                "has_jobpostinit": init_ok,
            },
        )
    except Exception:
        pass

def _builtin_is_count_label(s: str) -> bool:
    return bool(re.match(r"^\s*\d+\s+Locations?\s*$", (s or "").strip(), re.I))

import re

def _extract_biv_hiring_remotely_location(html: str) -> Optional[str]:
    if not html:
        return None

    # Handles: <span>Hiring Remotely in </span><span>CAN</span>
    m = re.search(r"Hiring\s+Remotely\s+in\s*</span>\s*<span>\s*([A-Z]{2,3})\s*</span>", html, re.IGNORECASE)
    if not m:
        return None
    code = m.group(1).upper()
    return f"Hiring Remotely in {code}"

def _extract_biv_hiring_remotely_country_chips(html: str) -> list[str]:
    if not html:
        return []

    # Capture multiple country spans if they exist
    # This is intentionally conservative: it only pulls 2 to 3 letter uppercase codes following the phrase.
    block = re.search(r"Hiring\s+Remotely\s+in.*?</div>", html, re.IGNORECASE | re.DOTALL)
    if not block:
        return []

    codes = re.findall(r"<span>\s*([A-Z]{2,3})\s*</span>", block.group(0))
    # Filter to plausible country codes used by your pipeline (CAN, USA, PRT, ESP, etc)
    out = []
    for c in codes:
        c = c.upper()
        if c in {"USA", "CAN", "PRT", "ESP", "UK"}:
            out.append(c)
    # de-dupe preserve order
    seen = set()
    uniq = []
    for c in out:
        if c not in seen:
            uniq.append(c)
            seen.add(c)
    return uniq

def _extract_biv_hiring_remotely_location(html: str) -> str | None:
    """
    BuiltInVancouver: detect a headline style phrase like:
      "Hiring Remotely in CAN"
      "Hiring Remotely in Canada"
      "Hiring Remotely in CAN and USA"
    Returns the exact normalized phrase we want to persist, or None.
    """
    text = " ".join((html or "").split())
    if not text:
        return None

    # Look for the phrase in raw text first (fast and resilient)
    m = re.search(r"\bHiring\s+Remotely\s+in\s+([^<]{1,80})", text, re.I)
    if not m:
        # Fallback: try soup text
        soup = BeautifulSoup(html or "", "html.parser")
        soup_text = " ".join(soup.get_text(" ", strip=True).split())
        m = re.search(r"\bHiring\s+Remotely\s+in\s+(.{1,80})", soup_text, re.I)
        if not m:
            return None

    tail = (m.group(1) or "").strip()

    # Stop the capture at common boundary words or separators.
    # This avoids accidentally pulling the whole page.
    # Example tail might be: "CAN Mid level Remote 3 locations..."
    tail = re.split(r"\b(Job|Posted|Mid\s+level|Senior|Role|Team|What\s+you|About)\b", tail, maxsplit=1, flags=re.I)[0]
    tail = tail.strip(" -|/•·,;:").strip()

    if not tail:
        return None

    # Normalize common values to your preferred display
    # You said Lumos should be exactly "Hiring Remotely in CAN"
    tail_norm = tail

    # Map "Canada" variants to CAN
    if re.fullmatch(r"canada", tail_norm, re.I):
        tail_norm = "CAN"
    # Map "United States" variants to USA
    if re.fullmatch(r"(united\s+states|u\.?s\.?a\.?|u\.?s\.?)", tail_norm, re.I):
        tail_norm = "USA"

    # If it contains both CAN + USA in any form, normalize those tokens
    # Keep it readable, but still deterministic.
    # Examples:
    #  "Canada and USA" -> "CAN and USA"
    #  "CAN, USA" -> "CAN, USA"
    tail_norm = re.sub(r"\bCanada\b", "CAN", tail_norm, flags=re.I)
    tail_norm = re.sub(r"\bUnited\s+States\b", "USA", tail_norm, flags=re.I)
    tail_norm = re.sub(r"\bU\.?S\.?A\.?\b", "USA", tail_norm, flags=re.I)
    tail_norm = re.sub(r"\bU\.?S\.?\b", "USA", tail_norm, flags=re.I)

    # Guardrail: only accept if it still looks like a region phrase,
    # not a sentence.
    if len(tail_norm) > 50:
        return None

    return f"Hiring Remotely in {tail_norm}"

def _extract_biv_hiring_remotely_country_chips(html: str) -> list[str]:
    """
    Given the same page HTML, return country chips inferred from the
    "Hiring Remotely in ..." phrase.

    Output tokens are the scraper's country chips: USA, CAN, etc.
    """
    loc = _extract_biv_hiring_remotely_location(html)
    if not loc:
        return []

    low = loc.lower()

    chips: list[str] = []
    if re.search(r"\b(can|canada)\b", low):
        chips.append("CAN")
    if re.search(r"\b(usa|united states|u\.?s\.?)\b", low):
        chips.append("USA")
    if re.search(r"\b(portugal|pt)\b", low):
        chips.append("PRT")
    if re.search(r"\b(spain|es)\b", low):
        chips.append("ESP")

    # De dupe preserve order
    seen = set()
    out: list[str] = []
    for c in chips:
        if c not in seen:
            seen.add(c)
            out.append(c)

    return out

def _builtin_tooltip_locations_from_html(html: str) -> list[str]:
    """
    Extract job locations from Built In tooltip HTML.

    Strict rules:
    - Only accept known location containers inside the tooltip content:
      div.col-lg-6 or div.text-truncate
    - Never fall back to selecting all divs
    - Choose the best candidate tooltip by plausibility scoring
    """
    import html as _html
    soup0 = BeautifulSoup(html or "", "html.parser")
    candidates = soup0.select("[data-bs-toggle='tooltip'], [data-toggle='tooltip']")

    def _clean(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip())

    def _is_plausible_location(s: str) -> bool:
        # Reject obvious non location content
        low = s.lower()
        if any(x in low for x in ["posted", "mid level", "junior", "senior", "days ago", "locations"]):
            return False

        # Too long is usually leakage
        if len(s) > 60:
            return False

        # City, ST, USA style: allow up to 2 commas (eg "Chicago, IL, USA" = 2)
        if s.count(",") > 2:
            return False

        # Otherwise allow simple country names (eg "Canada", "Spain", "Portugal")
        # Letters and spaces only, short-ish
        if "," not in s:
            return bool(re.fullmatch(r"[A-Za-z][A-Za-z ]{1,40}[A-Za-z]?", s))

        return True

    best: list[str] = []
    best_score = -10_000

    for node in candidates:
        label_text = _clean(node.get_text(" ", strip=True))
        aria_label = _clean(node.get("aria-label") or "")
        label = label_text or aria_label

        # We are looking for the Locations tooltip
        if not re.search(r"\bLocations?\b", label, re.I) and "job locations" not in label.lower():
            continue

        raw = (
            _clean(node.get("data-bs-original-title") or "")
            or _clean(node.get("data-bs-title") or "")
            or _clean(node.get("data-original-title") or "")
            or _clean(node.get("title") or "")
        )
        if not raw:
            continue

        unesc = _html.unescape(_html.unescape(raw))
        inner = BeautifulSoup(unesc, "html.parser")

        # Strict extraction: only known containers
        locs = [d.get_text(" ", strip=True) for d in inner.select("div.col-lg-6")]
        #if not locs:
            #locs = [d.get_text(" ", strip=True) for d in inner.select("div.text-truncate")]
        locs = [_clean(x) for x in locs if _clean(x)]

        if not locs:
            continue

        # De dupe preserve order
        seen = set()
        uniq: list[str] = []
        for x in locs:
            k = x.lower()
            if k not in seen:
                seen.add(k)
                uniq.append(x)

        # Plausibility filter
        plausible = [x for x in uniq if _is_plausible_location(x)]
        invalid = [x for x in uniq if not _is_plausible_location(x)]

        # Score candidate
        # Prefer:
        # - more plausible items
        # - fewer invalid items
        # - smaller lists when plausibility ties (avoids “vacuum” lists)
        score = (len(plausible) * 10) - (len(invalid) * 50) - len(uniq)

        if plausible and score > best_score:
            best_score = score
            best = plausible
        
        def _ok(s: str) -> bool:
            s = (s or "").strip()
            if not s:
                return False
            if s.count(",") > 2:
                return False
            if len(s) > 60:
                return False
            return True

        locs = [x for x in locs if _ok(x)]

    return best

def _builtin_primary_locations_from_html(html: str) -> list[str]:
    """
    Extract the job's own location tooltip by anchoring on the location icon
    within the job detail card. Avoids "Similar Jobs" tooltips.
    """
    import html as _html

    soup = BeautifulSoup(html or "", "html.parser")

    # Find the first location icon in the main job section
    # This is much more specific than scanning all tooltips.
    icon = soup.select_one("i.fa-location-dot, i.fa-regular.fa-location-dot")
    if not icon:
        return []

    # Walk up to a reasonable container, then search within it for the tooltip span
    container = icon
    for _ in range(6):
        if container is None:
            break
        # Heuristic: stop at a card-like wrapper
        cls = " ".join(container.get("class", []))
        if any(x in cls for x in ["bg-white", "rounded", "container", "row"]):
            break
        container = container.parent

    scope = container or soup

    # Look for the tooltip element near the icon inside this scope
    node = scope.select_one("[data-bs-toggle='tooltip'][data-html='true'], [data-toggle='tooltip'][data-html='true']")
    if not node:
        node = scope.select_one("[data-bs-toggle='tooltip'], [data-toggle='tooltip']")
    if not node:
        return []

    raw = (
        (node.get("data-bs-original-title") or "").strip()
        or (node.get("data-bs-title") or "").strip()
        or (node.get("data-original-title") or "").strip()
        or (node.get("title") or "").strip()
    )
    if not raw:
        return []

    unesc = _html.unescape(_html.unescape(raw))
    inner = BeautifulSoup(unesc, "html.parser")

    # Built In Vancouver uses col-lg-6 in the tooltip HTML you pasted
    locs = [d.get_text(" ", strip=True) for d in inner.select("div.col-lg-6")]
    if not locs:
        locs = [d.get_text(" ", strip=True) for d in inner.select("div.text-truncate")]

    locs = [x.strip() for x in locs if x and x.strip()]
    return locs

def extract_builtin_salary(
    ld_json: Optional[Dict[str, Any]],
    page_text: str = "",
) -> Tuple[str, Optional[int], Optional[int], str, str]:
    """
    Returns:
      salary_display, salary_min, salary_max, salary_currency, salary_unit
    """
    # 1) JSON-LD baseSalary
    if isinstance(ld_json, dict):
        bs = ld_json.get("baseSalary") or ld_json.get("estimatedSalary") or None
        if isinstance(bs, dict):
            currency = bs.get("currency") or bs.get("salaryCurrency") or "CAD"
            unit = _normalize_salary_unit(
                (bs.get("value") or {}).get("unitText") if isinstance(bs.get("value"), dict) else bs.get("unitText")
            )

            val = bs.get("value")
            if isinstance(val, dict):
                # Can be minValue/maxValue OR value
                min_amt = _money_to_int(str(val.get("minValue") or ""))
                max_amt = _money_to_int(str(val.get("maxValue") or ""))
                if min_amt is None and max_amt is None:
                    single = _money_to_int(str(val.get("value") or val.get("amount") or ""))
                    min_amt = single
                    max_amt = single

                salary_display = _format_salary(min_amt, max_amt, currency, unit)
                if salary_display:
                    return salary_display, min_amt, max_amt, (currency or "CAD"), unit

            # Sometimes baseSalary is flat
            min_amt = _money_to_int(str(bs.get("minValue") or ""))
            max_amt = _money_to_int(str(bs.get("maxValue") or ""))
            if min_amt is None and max_amt is None:
                single = _money_to_int(str(bs.get("value") or bs.get("amount") or ""))
                min_amt = single
                max_amt = single
            salary_display = _format_salary(min_amt, max_amt, currency, unit)
            if salary_display:
                return salary_display, min_amt, max_amt, (currency or "CAD"), unit

    # 2) Text fallback
    t = page_text or ""
    # Examples to catch:
    # "Salary: $120,000 - $160,000"
    # "$55–$70/hr"
    # "CAD 110,000–140,000"
    m = re.search(
        r"(?i)\b(?:salary|pay)\b[^\d]{0,40}((?:CAD|USD)?\s*\$?\s*[\d,]+(?:\.\d+)?)\s*(?:-|–|to)\s*((?:CAD|USD)?\s*\$?\s*[\d,]+(?:\.\d+)?)\s*(?:/?\s*(hour|hr|year|yr|month|week))?",
        t,
    )
    if m:
        a, b, unit_raw = m.group(1), m.group(2), m.group(3)
        currency = "CAD" if "CAD" in (a.upper() + b.upper()) else "USD" if "USD" in (a.upper() + b.upper()) else "CAD"
        unit = _normalize_salary_unit(unit_raw or "year")
        min_amt = _money_to_int(a)
        max_amt = _money_to_int(b)
        salary_display = _format_salary(min_amt, max_amt, currency, unit)
        return salary_display, min_amt, max_amt, currency, unit

    m2 = re.search(
        r"(?i)\b((?:CAD|USD)?\s*\$?\s*[\d,]+(?:\.\d+)?)\s*(?:/?\s*(hour|hr|year|yr|month|week))\b",
        t,
    )
    if m2:
        a, unit_raw = m2.group(1), m2.group(2)
        currency = "CAD" if "CAD" in a.upper() else "USD" if "USD" in a.upper() else "CAD"
        unit = _normalize_salary_unit(unit_raw or "year")
        amt = _money_to_int(a)
        salary_display = _format_salary(amt, amt, currency, unit)
        return salary_display, amt, amt, currency, unit

    return "", None, None, "CAD", "year"


def _is_country_only(loc: str | None) -> bool:
    if not loc:
        return True
    s = loc.strip().lower()
    return s in {"canada", "can", "us", "usa", "united states"}

def _looks_richer(loc: str | None) -> bool:
    if not loc:
        return False
    # commas or pipes usually means a multi location list
    return ("," in loc) or ("|" in loc) or ("•" in loc)


_YC_LOC_RX = re.compile(
    r"\b("
    r"[A-Z][A-Za-z.\-'\s]{1,40}"          # City starts with a capital
    r",\s*"
    r"[A-Za-z.\-'\s]{1,30}"              # Region
    r",\s*"
    r"(?:US|CA|IN|GB|AE|SG|AU|DE|FR|NL|SE|NO|DK|IE|ES|IT|PL)"  # Country code allow list
    r")\b"
)

def _yc_extract_location_from_label(html: str) -> str:
    """
    Extract location from the rendered HTML using the explicit 'Location:' label.
    Works even when soup based regex scanning grabs prose.
    """
    if not html:
        return ""

    soup = BeautifulSoup(html, "html.parser")
    main = soup.select_one("main") or soup.select_one('[role="main"]') or soup.body
    if not main:
        return ""

    # Look for a span that is exactly 'Location:' then take the next sibling span
    for span in main.select("span"):
        if span.get_text(" ", strip=True) == "Location:":
            sib = span.find_next_sibling("span")
            if sib:
                loc = sib.get_text(" ", strip=True).strip()
                return loc

    return ""

def _yc_extract_location_from_soup(soup: BeautifulSoup) -> str:
    """
    Try to extract YC job location from the main job content, not the global nav.
    """
    # 1) Prefer explicit "Location:" label
    for span in soup.select("main span"):
        if span.get_text(" ", strip=True) == "Location:":
            sib = span.find_next_sibling("span")
            if sib:
                txt = sib.get_text(" ", strip=True).strip()
                if txt:
                    return txt

    main = soup.select_one("main") or soup.select_one('[role="main"]') or soup.body
    if not main:
        return ""

    # 2) Narrow scan area: YC job header often contains the location
    header = soup.select_one("main header") or soup.select_one("main")
    txt = header.get_text(" ", strip=True) if header else main.get_text(" ", strip=True)
    if not txt:
        return ""

    # Find candidate strings that look like City, Region, CC
    cands = []
    for m in _YC_LOC_RX.finditer(txt):
        loc = m.group(1).strip()

        # Reject candidates that look like prose, not a location
        if len(loc) > 70:
            continue
        if "." in loc:
            continue

        low = loc.lower()
        if not loc[:1].isupper():
            continue


        # Reject common job description cues
        if any(p in low for p in (
            "minimum qualifications",
            "you can read",
            "about us",
            "requirements",
            "responsibilities",
        )):
            continue

        # Throw away obvious garbage
        if low in {"us", "usa", "canada", "can"}:
            continue
        if "interview guide" in low or "startup jobs" in low:
            continue

        score = 0
        score += loc.count(",") * 2

        if re.search(r"\b(US|CA|IN|GB|AE|SG|AU|DE|FR|NL|SE|NO|DK|IE|ES|IT|PL)\b\s*$", loc):
            score += 3

        # Optional, keep if you like it
        if len(loc) > 60:
            score -= 3

        cands.append((score, loc))

    if not cands:
        return ""

    cands.sort(key=lambda x: x[0], reverse=True)
    return cands[0][1]

def _yc_loc_sandwich(tag: str, details: dict, host: str, url: str) -> None:
    if "ycombinator.com" not in (host or "").lower():
        return
    log_line(tag,
             f"Location={details.get('Location')!r} | "
             f"LocRaw={details.get('Location Raw')!r} | "
             f"LocChips={details.get('Location Chips')!r} | "
             f"AppRegions={details.get('Applicant Regions')!r} | "
             f"CountryChips={details.get('Country Chips')!r} | "
             f"USRule={details.get('US Rule')!r} | "
             f"RemoteRule={details.get('Remote Rule')!r} | "
             f"host={host} | url={url}")

def _yc_is_plausible_location(loc: str | None) -> bool:
    if not loc:
        return False

    s = " ".join(loc.split()).strip()
    if not s:
        return False

    # Too long to be a location
    if len(s) > 70:
        return False

    # If it reads like a sentence, reject
    if "." in s:
        return False

    # Common job description cues
    low = s.lower()
    bad_phrases = [
        "minimum qualifications",
        "you can read",
        "about us",
        "responsibilities",
        "requirements",
        "benefits",
    ]
    if any(p in low for p in bad_phrases):
        return False

    # URL or press list fragments
    if "http" in low or "techcrunch" in low or "business insider" in low:
        return False

    # Location should usually be short and not look like a paragraph
    if s.count(",") > 4:
        return False

    return True


def _prefer_listing_location(details: dict, listing: dict) -> dict:
    """
    If the detail page gives us no location or only a generic collapse like 'Canada',
    but the listing already captured a richer multi-location string, prefer the listing value.
    """
    if not isinstance(details, dict):
        details = {}
    if not isinstance(listing, dict):
        return details

    d_loc = (details.get("Location") or "").strip()
    l_loc = (listing.get("Location") or "").strip()

    # listing candidates that are "richer"
    listing_has_multi = ("|" in l_loc) or (l_loc.count(",") >= 2) or (len(l_loc) > 25)

    # detail is missing or too generic
    detail_is_weak = (not d_loc) or (d_loc.lower() in {"canada", "ca", "can", "us", "usa", "united states"})

    if l_loc and listing_has_multi and detail_is_weak:
        #_yc_trace(details, "YC RULES BEFORE ASSIGN", DOT)
        details["Location"] = l_loc
        raw_from_listing = (listing.get("Location Raw") or "").strip()
        raw_fallback = (l_loc or "").strip()

        # Only write Location Raw if we are improving it, not downgrading it
        if raw_from_listing:
            details["LocationRaw"] = raw_from_listing
        elif raw_fallback and not (details.get("LocationRaw") or "").strip():
            details["LocationRaw"] = raw_fallback


    # also carry over chips or other location signals if they exist
    for k in ["Location Chips", "Locations Text", "Applicant Regions", "Regions"]:
        if listing.get(k) and not details.get(k):
            if k in {"Location Chips", "Applicant Regions"}:
                details[k] = _as_pipe_chips(listing.get(k)) or ""
            else:
                details[k] = listing.get(k)

    return details



def apply_builtin_salary(details: Dict[str, Any]) -> None:
    job_url = details.get("job_url") or details.get("Job URL") or link or ""
    if "builtin" not in job_url:
        return

    # Do not overwrite an existing structured salary
    if details.get("Salary Range") or details.get("Salary Text") or details.get("salary_min") or details.get("salary_max"):
        return

    page_text = details.get("page_text") or ""
    # Built In Vancouver is overwhelmingly CAD, builtin.com is usually USD
    fallback_currency = "CAD" if "builtinvancouver.org" in job_url else "USD"

    page_text = details.get("page_text") or ""
    html_raw = details.get("html_raw") or details.get("html") or ""

    # 1) Try JSON LD from the HTML, not from page_text
    sal = _extract_salary_from_jsonld(html_raw) if html_raw else None

    # 2) If that fails, try visible text
    if not sal:
        sal = _extract_salary_from_visible_text(page_text, fallback_currency=fallback_currency)

    if sal:
        details.update(sal)

        # Bridge to normalized keys if your pipeline uses them elsewhere
        if not details.get("salary_min") and details.get("Salary From") is not None:
            details["salary_min"] = details["Salary From"]
        if not details.get("salary_max") and details.get("Salary To") is not None:
            details["salary_max"] = details["Salary To"]
        if not details.get("salary_raw"):
            details["salary_raw"] = details.get("Salary Range") or details.get("Salary Text") or ""

        def _builtin_tooltip_locations_any(soup0) -> list[str]:
            import html as _html

            # Do NOT require specific tooltip attrs here, because Built In varies:
            # title vs data-bs-title vs data-bs-original-title.
            candidates = list(soup0.select("span[data-bs-toggle='tooltip']"))
            if not candidates:
                candidates = list(soup0.find_all(attrs={"data-bs-toggle": "tooltip"}))

            best: list[str] = []

            for node in candidates:
                label = (node.get_text(" ", strip=True) or "")
                if not re.search(r"\bLocations?\b", label, re.I):
                    continue

                # Built In (US) often uses data-bs-original-title (your probe confirmed this).
                raw = (
                    (node.get("data-bs-original-title") or "").strip()
                    or (node.get("data-bs-title") or "").strip()
                    or (node.get("data-original-title") or "").strip()
                    or (node.get("title") or "").strip()
                )
                if not raw:
                    continue

                log_line("DEBUG", f"[BIVDBG2] raw_len={len(raw)} raw_head={(raw[:60] + '...') if len(raw) > 60 else raw}")
                # Double-unescape to handle nested entities
                import html as _html
                unesc = _html.unescape(_html.unescape(raw))
                inner = BeautifulSoup(unesc, "html.parser")

                # Prefer the common Built In structure first
                locs = [d.get_text(" ", strip=True) for d in inner.select("div.col-lg-6")]
                if not locs:
                    # Fallback: sometimes it is div.text-truncate or just divs
                    locs = [d.get_text(" ", strip=True) for d in inner.select("div.text-truncate")]
                if not locs:
                    locs = [d.get_text(" ", strip=True) for d in inner.select("div")]

                locs = [x for x in locs if x]

                # De-dupe, preserve order
                seen = set()
                uniq: list[str] = []
                for x in locs:
                    k = x.lower()
                    if k not in seen:
                        seen.add(k)
                        uniq.append(x)

                if len(uniq) > len(best):
                    best = uniq

            return best

def _order_location_chips(chips_list: list[str]) -> list[str]:
    # normalize
    raw = []
    for c in chips_list or []:
        c = (c or "").strip().upper()
        if c:
            raw.append(c)

    # de-dupe while preserving first occurrence
    seen = set()
    uniq = []
    for c in raw:
        if c not in seen:
            uniq.append(c)
            seen.add(c)

    # priority order: countries first, then common provinces/states, then rest
    priority = [
        "USA", "CAN",
        "WA", "BC", "ON",
        "CA",  # California state token (intentionally AFTER countries)
    ]
    prio_index = {v: i for i, v in enumerate(priority)}

    def key(c: str):
        return (0, prio_index[c]) if c in prio_index else (1, c)

    return sorted(uniq, key=key)

def set_location_chips(details: dict, value, tag: str = "") -> None:
    prev = details.get("Location Chips")
    details["Location Chips"] = value

    # Only scream when chips go from something to empty
    now = details.get("Location Chips")
    if (prev not in (None, "", [])) and (now in (None, "", [])):
        stack = "".join(traceback.format_stack(limit=8))
        log_line(
            "DEBUG",
            f"[CHIPS WIPE] tag={tag} prev={prev!r} now={now!r} "
            f"Location={details.get('Location')!r} url={details.get('Apply URL') or details.get('Job URL')!r}\n{stack}"
        )

def _finalize_location_chips(chips: list[str]) -> str:
    """
    Truth-only chip cleanup:
    - uppercase
    - normalize synonyms (CANADA->CAN, US->USA, etc)
    - remove REMOTE
    - de-dupe and stable sort
    """
    out: set[str] = set()
    for c in chips or []:
        t = (c or "").strip().upper()
        if not t:
            continue
        t = LOCATION_CHIP_SYNONYMS.get(t, t)
        if t == "REMOTE":
            continue
        out.add(t)
    return "|".join(sorted(out))

def _set_location_chip_source(details: dict, source: str):
    details["_LOCATION_CHIPS_SOURCE"] = source
    details["Location Chips Source"] = source

def canonicalize_location_chips(chips_str: str) -> str:
    """
    Canonical order:
      1) Countries (USA, CAN)
      2) States/provinces (2 letter)
      3) Everything else
    Also de-dupes while preserving canonical order.
    """
    raw = [c.strip().upper() for c in (chips_str or "").split("|") if c.strip()]
    # de-dupe while keeping first occurrence
    seen = set()
    raw = [c for c in raw if not (c in seen or seen.add(c))]

    country_priority = {"USA": 0, "CAN": 1}
    def _key(tok: str):
        if tok in country_priority:
            return (0, country_priority[tok], tok)
        if len(tok) == 2 and tok.isalpha():
            return (1, 0, tok)
        return (2, 0, tok)

    ordered = sorted(raw, key=_key)
    return "|".join(ordered)

def canonicalize_chip_pipe(pipe: str) -> str:
    if not pipe:
        return ""
    toks = [t.strip().upper() for t in str(pipe).split("|") if t.strip()]
    rank = {"CAN": 0, "USA": 1}
    def key(t: str):
        return (rank.get(t, 10), t)
    seen = set()
    out = []
    for t in sorted(toks, key=key):
        if t not in seen:
            seen.add(t)
            out.append(t)
    return "|".join(out)

def extract_job_details(html: str, job_url: str) -> dict:
    """
    Generic page detail parser used by many boards.
    Safe and defensive: never raises, returns a dict with the keys our pipeline expects.
    """

    company_from_header = None
    board_from_header = None
    details: dict = {}
    builtin_meta: dict = {}

    soup = BeautifulSoup(html or "", "html.parser")
    host = (up.urlparse(job_url).netloc or "").lower()
    best: list[str] = []
    _debug_biv_loc(
        "EXTRACT enter",
        details,
        {
            "job_url": job_url,
            "host": host,
            "html_len": len(html or ""),
        },
    )

    vis_loc = ""
    if ("builtin.com" in host) or ("builtinseattle.com" in host) or ("builtinvancouver.org" in host):
        vis_loc = _builtin_visible_location_from_html(html or "")

        if vis_loc:
            cur = (details.get("Location") or "").strip().lower()
            if not cur or cur in {"us", "usa", "united states", "remote"}:
                details["Location"] = vis_loc
                details.setdefault("Location Raw", vis_loc)
                details["Location Source"] = "HTML"

    t_ok = bool((details.get("Title") or "").strip())
    c_ok = bool((details.get("Company") or "").strip())
    if not (t_ok and c_ok) and ("builtin.com" in host or "builtinseattle.com" in host or "builtinvancouver.org" in host):
        _debug_biv_loc(
            "signals snapshot",
            details,
            {
                "html_len": len(html or ""),
                "has_jobpostinit": "Builtin.jobPostInit" in (html or ""),
                "has_jsonld": 'type="application/ld+json"' in (html or ""),
                "has_title_tag": "<title" in (html or ""),
            },
        )

    if "builtin.com" in host or "builtinseattle.com" in host or "builtinvancouver.org" in host:
        _debug_builtin_page_fingerprint(details, host, job_url, html or "")


    card = None

    if "builtin.com" in host or "builtinseattle.com" in host or "builtinvancouver.org" in host:
        _debug_biv_loc("before builtinsignals", details, {"job_url": job_url, "card_found": bool(card)})
        _builtin_fill_title_company_from_builtinsignals(details, soup, html)
        _debug_biv_loc("after builtinsignals", details, {"job_url": job_url, "card_found": bool(card)})
        card = _builtin_job_card_scope(soup, job_url)
        _debug_biv_loc("after card scope", details, {"job_url": job_url, "card_found": bool(card)})


    # --- BIV canonicalize Location using Location Raw when it is more specific ---
    if "builtinvancouver.org" in host:
        loc = (details.get("Location") or "").strip()
        raw = (details.get("LocationRaw") or "").strip()
        _debug_biv_loc("after canonicalize", details, {"Location": details.get("Location"), "Location Raw": details.get("LocationRaw")})

        # If Raw exists and Location looks like a broadened/combined set, prefer Raw.
        # This fixes cases like Location="Canada / United States" while Raw="Canada".
        if raw:
            loc_low = loc.lower()
            raw_low = raw.lower()

            # If Location contains both Canada and US but Raw is clearly one, keep Raw.
            has_can = "canada" in loc_low or loc_low == "can"
            has_us = ("united states" in loc_low) or ("usa" in loc_low) or loc_low == "us"

            raw_is_can = raw_low in {"canada", "can"}
            raw_is_us = raw_low in {"united states", "usa", "us"}

            if (has_can and has_us) and (raw_is_can or raw_is_us):
                if not (is_biv and details.get("_LOCK_LOCATION_FROM_TOOLTIP")):
                    details["Location"] = raw
                details["Location Source"] = (details.get("Location Source") or "") + "|RAW_WINS"
            
    trace_chips(details, "BIV_CHECKPOINT_PRE_NORMALIZE")

    if "builtin.com" in host or "builtinseattle.com" in host or "builtinvancouver.org" in host:
        builtin_meta = {}
        remote_flag = ""
        meta_rule = None
        badge = ""
        badge_rule = None
        if not (details.get("Title") or "").strip() or not (details.get("Company") or "").strip():
            log_line(
                "DEBUG",
                f"[BIVDBG] after builtinsignals url={job_url} "
                f"has_jobpostinit={'Builtin.jobPostInit' in (html or '')} "
                f"has_jsonld={'application/ld+json' in (html or '')} "
                f"title={details.get('Title')} company={details.get('Company')}",
            )

        try:
            builtin_meta = _extract_builtin_job_meta(card) or {}

            # Keep this field if you use it elsewhere
            details["builtin_meta_location"] = (builtin_meta.get("location") or "")

            # -----------------------------------------
            # Built In: header work mode (Tier 1 fact)
            # -----------------------------------------
            try:
                soup_header = BeautifulSoup(html or "", "html.parser")

                # Try to keep this header scoped. If selectors miss, fall back to first chunk of page text.
                header_txt = ""
                try:
                    header_node = (
                        soup_header.select_one("header")
                        or soup_header.select_one("[class*='job']")
                        or soup_header.select_one("[class*='Job']")
                        or soup_header.select_one("[class*='header']")
                        or soup_header.select_one("[class*='Header']")
                        or soup_header.select_one("[class*='hero']")
                        or soup_header.select_one("[class*='Hero']")
                    )
                    if header_node:
                        header_txt = header_node.get_text(" ", strip=True)
                except Exception:
                    header_txt = ""

                if not header_txt:
                    header_txt = soup_header.get_text(" ", strip=True)[:2000]

                header_mode, header_ev = _builtin_extract_header_work_mode(header_txt)
                details["builtin_header_work_mode"] = header_mode
                details["builtin_header_work_mode_evidence"] = header_ev

                # Tier 1: Built In header truth wins
                if header_mode in {"Remote", "Hybrid", "Onsite"}:
                    details["Remote Rule"] = header_mode
                    details["_LOCK_REMOTE_RULE"] = True
                    details["_REMOTE_RULE_SOURCE"] = "BUILTIN_HEADER_WORK_MODE"
                    debug_print(
                        DEBUG_CFG,
                        "gate",
                        f"[REMOTE_RULE_SET_BY_HEADER] mode={header_mode!r} evidence={header_ev!r}",
                    )

            except Exception:
                details["builtin_header_work_mode"] = ""
                details["builtin_header_work_mode_evidence"] = ""

            # -----------------------------------------
            # Built In / BIV header badge (strong signal)
            # -----------------------------------------
            try:
                soup_badge = BeautifulSoup(html or "", "html.parser")
                details["workplace_badge"] = (_builtin_extract_workplace_badge_text(soup_badge) or "").strip()
            except Exception:
                details["workplace_badge"] = ""

            badge = (details.get("workplace_badge") or "").strip()
            badge_low = badge.lower()
            badge_rule = None

            if badge_low == "remote":
                badge_rule = "Remote"
            elif badge_low == "hybrid":
                badge_rule = "Hybrid"
            elif badge_low == "onsite":
                badge_rule = "Onsite"

            # Header badge should win for explicit Hybrid / Onsite / Remote
            if badge_rule:
                details["Remote Rule"] = badge_rule
                details["_LOCK_REMOTE_RULE"] = True
                details["_REMOTE_RULE_SOURCE"] = "BUILTIN_HEADER_BADGE"

            # -----------------------------------------
            # Built In badge (Tier 3, never locks)
            # -----------------------------------------
            badge = ""
            badge_rule = None

            try:
                soup_badge = BeautifulSoup(html or "", "html.parser")
                details["workplace_badge"] = (_builtin_extract_workplace_badge_text(soup_badge) or "").strip()
            except Exception:
                details["workplace_badge"] = ""

            badge = (details.get("workplace_badge") or "").lower()

            if "remote" in badge:
                badge_rule = "Remote"
            elif "hybrid" in badge:
                badge_rule = "Hybrid"
            elif "on-site" in badge or "onsite" in badge or "on site" in badge:
                badge_rule = "Onsite"

            if badge_rule and not details.get("_LOCK_REMOTE_RULE"):
                details["Remote Rule"] = badge_rule
                details["_REMOTE_RULE_SOURCE"] = "BADGE_PROVISIONAL"
                # do not lock

            log_line(
                "DEBUG",
                f"[REMOTE_RULE_SET_BY_BADGE] badge={badge!r} -> Remote Rule={details.get('Remote Rule')!r}",
            )

        except Exception:
            builtin_meta = {}

        # Built In tooltip extraction must happen before any pruning.
        # Use a fresh soup so later mutations cannot break the scan.
        soup_tooltip = BeautifulSoup(html or "", "html.parser")
        
        if "builtin.com" in host or "builtinseattle.com" in host or "builtinvancouver.org" in host:
            soup_pre = BeautifulSoup(html or "", "html.parser")
            pre_nodes = soup_pre.select("[data-bs-toggle='tooltip'], [data-toggle='tooltip']")
            log_line("DEBUG", f"[BIVDBG] pre_prune_tooltip_nodes={len(pre_nodes)}")

            # Always initialize so later "if new_val:" never crashes
            new_val = ""

            tooltip_best = _builtin_tooltip_locations_from_html(html or "")
            log_line(
                "DEBUG",
                f"[BIVDBG] tooltip_extract_result_preprune url={job_url} best_len={len(tooltip_best)} best_sample={(tooltip_best[:3] if tooltip_best else [])}",
            )

            if tooltip_best and len(tooltip_best) > 1:
                details["Location Chips Source"] = "TOOLTIP"
                details["BIV Tooltip Locations"] = tooltip_best
                details["_LOCK_LOCATION_FROM_TOOLTIP"] = True
                details["_LOCK_LOCATION_CHIPS"] = True  # optional but recommended
                details["Location"] = " / ".join(tooltip_best)

                new_val = _chips_pipe_from_location_strings(tooltip_best)

            # Safe now
            if new_val:
                _set_loc_chips(details, new_val, "TOOLTIP chips from tooltip_best", force=True)

                if vis_loc:
                    cur = (details.get("Location") or "").strip().lower()
                    if not cur or cur in {"us", "usa", "united states", "remote"}:
                        #details["Location"] = vis_loc
                        details.setdefault("Location Raw", vis_loc)
                        details["Location Source"] = "HTML"

        # IMPORTANT: remove Similar Jobs etc before any location scraping
        _prune_non_job_sections(soup, host)
        log_line("DEBUG", f"[BIVDBG] post_prune_tooltip_nodes={len(soup.select('[data-bs-toggle=\"tooltip\"], [data-toggle=\"tooltip\"]'))}")

        if "builtin.com" in host:
            _builtin_fill_title_company_from_builtinsignals(details, soup, html)
            _debug_biv_loc(
                "after builtinsignals (second pass)",
                details,
                {
                    "title": details.get("Title"),
                    "company": details.get("Company"),
                    "source": details.get("_builtin_title_company_source"),
                },
            )

        # Built In fallback: single visible location on the right-rail card (no tooltip list)
        if "builtin.com" in host and not best:
            # Scope to the sidebar card to avoid picking up the wrong location-dot icon elsewhere
            panel = (
                soup.select_one("div.col-12.col-lg-3 div.bg-white.rounded-3")
                or soup.select_one("div.col-12.col-lg-3")
            )

            #log_line("DEBUG", f"[BIVDBG] page_loc_scope panel_found={bool(panel)}")

            if panel:
                icon = panel.select_one("i.fa-location-dot")
                log_line("DEBUG", f"[BIVDBG] page_loc_scope icon_found={bool(icon)}")

                if icon:
                    row = icon.find_parent(
                        lambda t: t.name == "div"
                        and "d-flex" in (t.get("class") or [])
                        and "align-items-start" in (t.get("class") or [])
                    )
                    log_line("DEBUG", f"[BIVDBG] page_loc_scope row_found={bool(row)}")

                    span = row.select_one("span") if row else None
                    loc_txt = (span.get_text(" ", strip=True) if span else "").strip()

                    log_line(
                        "DEBUG",
                        f"[BIVDBG] page_loc_probe row_found={bool(row)} span_found={bool(span)} loc={loc_txt!r}",
                    )

                    if loc_txt and loc_txt.lower() not in {"remote", "hybrid"}:
                        cur = (details.get("Location") or "").strip().lower()
                        if not cur or cur in {"us", "usa", "united states", "remote"}:
                            if not (is_biv and details.get("_LOCK_LOCATION_FROM_TOOLTIP")):
                                details["Location"] = loc_txt
                            details.setdefault("Location Raw", loc_txt)
                            details["Location Source"] = "PAGE"
                            details["Location Chips Source"] = details.get("Location Chips Source") or "PAGE"
                            log_line("DEBUG", f"[BIVDBG] page_location_extracted {loc_txt!r}")

        _debug_biv_loc("after builtin_meta", details, {"builtin_meta": builtin_meta})

        if "builtin.com" in host:
            log_line("DEBUG", f"[BIVDBG] tooltip block reached url={job_url}")

        if "builtin.com" in host:
            h = html or ""

            has_nloc = bool(re.search(r"\b\d+\s+Locations\b", h))
            has_tooltip = 'data-bs-toggle="tooltip"' in h
            has_col = "col-lg-6" in h
            has_title_attr = ('title="&lt;div' in h) or ('title="<div' in h)

            log_line(
                "DEBUG",
                "[BIVDBG] html_probe "
                f"len={len(h)} "
                f"has_nloc={has_nloc} "
                f"has_tooltip={has_tooltip} "
                f"has_col={has_col} "
                f"has_austin={'Austin, TX, USA' in h} "
                f"has_title_attr={has_title_attr}"
            )

        # Built In: multi-location list is stored in a tooltip attribute as escaped HTML.
        # Extract this EARLY so later fallbacks do not lock us to one city.
        if ("builtinvancouver.org" in host) and not details.get("BIV Tooltip Locations"):
            try:
                best = _builtin_tooltip_locations(soup)
                if best and len(best) > 1:
                    details["Builtin Tooltip Locations"] = best
                    details["Location"] = " / ".join(best)
                    details["_LOCK_LOCATION_FROM_TOOLTIP"] = True
                    details["_LOCK_LOCATION_CHIPS"] = True
                    details.setdefault("Location Raw", details["Location"])

                    # Do not overwrite locked chips from earlier authoritative sources
                    if details.get("_LOCK_LOCATION_CHIPS") is True:
                        pass
                    else:
                        new_val = _chips_pipe_from_location_strings(best)

                        details["Location Chips Source"] = "TOOLTIP"
                        details["_LOCATION_CHIPS_SOURCE"] = "TOOLTIP"
                        details["_LOCK_LOCATION_CHIPS"] = True

                        if new_val:
                            _set_loc_chips(details, new_val, "TOOLTIP chips from Builtin Tooltip Locations")

                    _debug_biv_loc(
                        "after builtin tooltip locations early extract",
                        details,
                        {"tooltip_count": len(best), "tooltip_locs": best},
                    )
            except Exception as e:
                _debug_biv_loc(
                    "builtin tooltip locations early extract failed",
                    details,
                    {"err": str(e)},
                )

        # Built In fallback: visible single location on the page (no tooltip list or tooltip was useless)
        if "builtin.com" in host and (not best or len(best) <= 1):
            icon = soup.select_one("i.fa-location-dot")
            if icon:
                # Find the nearest row container: <div class="d-flex align-items-start gap-sm">
                row = None
                for parent in icon.parents:
                    if getattr(parent, "name", None) != "div":
                        continue
                    classes = parent.get("class") or []
                    if isinstance(classes, str):
                        classes = classes.split()
                    if "d-flex" in classes and "align-items-start" in classes:
                        row = parent
                        break

                if row:
                    loc_spans = row.select("div.font-barlow span, div.font-barlow, span")
                    texts = [s.get_text(" ", strip=True) for s in loc_spans if s]
                    texts = [re.sub(r"\s+", " ", x).strip() for x in texts]
                    texts = [x for x in texts if x]

                    loc_txt = ""
                    for t in reversed(texts):
                        low = t.lower()
                        if low.startswith("hiring remotely"):
                            continue
                        if low in {"remote", "hybrid", "locations", "job locations"}:
                            continue
                        if _builtin_is_count_label(t):
                            continue
                        if "," in t or low in {"us", "usa", "united states", "canada"}:
                            loc_txt = t
                            break

                    if loc_txt:
                        cur = (details.get("Location") or "").strip().lower()
                        if not cur or cur in {"us", "usa", "united states", "remote"}:
                            if not (is_biv and details.get("_LOCK_LOCATION_FROM_TOOLTIP")):
                                details["Location"] = loc_txt
                            details.setdefault("Location Raw", loc_txt)
                            details["Location Source"] = "PAGE"
                            details["Location Chips Source"] = details.get("Location Chips Source") or "PAGE"
                            log_line("DEBUG", f"[BIVDBG] page_location_extracted {loc_txt!r}")

                else:
                    log_line("DEBUG", "[BIVDBG] page_loc_probe row_found=False span_found=False loc=''")
                
    if "builtinvancouver.org" in host:
        hero_country = _builtin_hero_country(soup)
        existing_loc = (details.get("Location") or "").strip()
        _debug_biv_loc("before hero_country lock", details, {"hero_country ": hero_country, "existing_loc ": existing_loc})

        try:
            h = (details.get("host") or details.get("Host") or "").lower()
        except Exception:
            h = ""

        # --- BIV canonicalize Location using other signals when it is too broad ---
        if "builtinvancouver.org" in host:
            loc = (details.get("Location") or "").strip()
            raw = (details.get("LocationRaw") or "").strip()

            loc_low = loc.lower()
            has_can = "canada" in loc_low or "can" == loc_low
            has_us = ("united states" in loc_low) or ("usa" in loc_low) or ("/ us" in loc_low) or (loc_low.endswith(" us"))

            title_low = (details.get("Title") or "").lower()
            snip_low = (details.get("Description Snippet") or "").lower()

            canada_strong = ("- canada" in title_low) or (" in canada" in snip_low)

            if has_can and has_us and canada_strong:
                details["Location"] = "Canada"
                if not (details.get("LocationRaw") or "").strip():
                    details["LocationRaw"] = "Canada"
                details["Location Source"] = (details.get("Location Source") or "") + "|CANADA_STRONG_WINS"

        yc = ("ycombinator.com" in h)

        if yc:
            _yc_trace(
                "YC RULES TRACE IN",
                f"Loc={details.get('Location')!r} | "
                f"LocChips={details.get('Location Chips')!r} | "
                f"AppRegions={details.get('Applicant Regions')!r} | "
                f"CountryChips={details.get('Country Chips')!r}"
            )

        loc_low = existing_loc.lower()
        if hero_country and loc_low in {"", "canada", "ca", "can"}:
            details["Location"] = hero_country
            details["LocationRaw"] = hero_country
            if not details.get("_DERIVE_LOCATION_RULES_DONE"):
                details = _derive_location_rules(details)
            _debug_biv(details, host, "after hero_country lock + derive")
        else:
            _debug_biv(details, host, "skipped hero_country lock (location already set)")

    # Dice: extract location from header line (Hybrid/Remote/On-site in ...)
    if "dice.com" in host:
        loc_now = (details.get("Location") or "").strip()
        if (not loc_now) or (loc_now.upper() in {"US", "USA", "UNITED STATES"}):
            dice_loc = _dice_extract_location_from_soup(soup)
            if dice_loc:
                details["Location"] = dice_loc
                details["LocationRaw"] = dice_loc



    def _strip_recommendations(soup_obj, page_host: str) -> None:
        """
        Drop right rail / recommended jobs blocks that often contain other salaries.
        Best effort only.
        """
        selectors = [
            "aside",
            ".sidebar",
            '*:div:-soup-contains("Similar jobs")',
            '*:div:-soup-contains("Similar Jobs")',
            '*:div:-soup-contains("You might be interested in")',
            '*:div:-soup-contains("Recommended jobs")',
            '*:div:-soup-contains("People also viewed")',
        ]
        if "edtech.com" in page_host:
            selectors += [
                '*:div:-soup-contains("Here are some similar Product Development jobs")',
            ]
        if "themuse.com" in page_host:
            selectors += ['[role="complementary"]']

        for sel in selectors:
            try:
                for node in soup_obj.select(sel):
                    container = node
                    for _ in range(2):
                        if container.parent and container.parent.name not in ("html", "body"):
                            container = container.parent
                    container.decompose()
            except Exception:
                # selectors are best effort only
                pass

    _strip_recommendations(soup, host)

    main = (
        soup.select_one("main")
        or soup.select_one("article")
        or soup.find("div", role="main")
        or soup.body
    )
    page_text = main.get_text(" ", strip=True) if main else soup.get_text(" ", strip=True)
    details["page_text"] = page_text


        # --- Hard stop on 404 or missing job pages (Built In and others) ---
    pt_l = (page_text or "").lower()
    if any(x in pt_l for x in [
        "404",
        "we can't seem to find the page",
        "page you're looking for",
        "page you’re looking for",
    ]):
        details.update({
            "Title": "",
            "Company": "",
            "Career Board": infer_board_from_url(job_url),
            "Job URL": job_url,
            "job_url": job_url,
            "apply_url": job_url,
            "Apply URL": job_url,
            "Apply URL Note": "Page not found (404)",
            "Location": "",
            "Remote Rule": "",
            "Posting Date": "",
            "Valid Through": "",
        })
        return details


    # Muse: location override
    if "themuse.com" in host:
        muse_loc = _extract_muse_location(soup)
        if muse_loc:
            details["Location"] = muse_loc

    # --- JSON LD helper parse (central place) ---
    try:
        ld_data = parse_jobposting_ldjson(html)
        if ld_data:
            details.update({k: v for k, v in ld_data.items() if v})
    except Exception as e:
        warn("WARN", f"JSON-LD parse failed: {e}")

    # Additional sweep for Posting Date / Valid Through
    import json as _json
    for tag in soup.find_all("script", type="application/ld+json"):
        txt = (tag.string or "").strip()
        if not txt:
            continue
        try:
            node = _json.loads(txt)
        except Exception:
            continue
        objs = node if isinstance(node, list) else [node]
        for obj in objs:
            if not isinstance(obj, dict):
                continue
            if obj.get("@type") not in ("JobPosting", ["JobPosting"]):
                continue
            dp = obj.get("datePosted")
            vt = obj.get("validThrough")
            if dp and not details.get("Posting Date"):
                details["Posting Date"] = parse_date_relaxed(dp)
            if vt and not details.get("Valid Through"):
                details["Valid Through"] = parse_date_relaxed(vt)
        if details.get("Posting Date") and details.get("Valid Through"):
            break

    # ---- Title ----
    title = ""
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        title = h1.get_text(strip=True)
    if not title:
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            title = og["content"].strip()
    if not title and soup.title and soup.title.get_text():
        title = soup.title.get_text(strip=True)
    if builtin_meta.get("title"):
        title = builtin_meta["title"]
    if "builtin.com" in host or "builtinseattle.com" in host or "builtinvancouver.org" in host:
        _builtin_fill_title_company_from_builtinsignals(details, soup, html)
        title = _strip_builtin_brand(title)

    # Workday helper to grab friendly salary text for later parsing
    def _extract_workday_salary_text(soup_obj) -> str:
        import re as _re
        amt_pattern = _re.compile(
            r"\$\s?\d[\d,]{2,}(?:\s*[-–]\s*\$\s?\d[\d,]{2,})?"
        )
        label_re = _re.compile(
            r"(salary|compensation|pay range|pay-rate|pay range minimum|pay range maximum)",
            _re.I,
        )
        for node in soup_obj.find_all(string=label_re):
            try:
                parent = node.parent
                chunk = " ".join(parent.get_text(" ", strip=True).split())
            except Exception:
                chunk = str(node) or ""
            m = amt_pattern.search(chunk)
            if m:
                return m.group(0).strip()
            sib_text = ""
            try:
                for sib in parent.next_siblings:
                    if isinstance(sib, str):
                        sib_text += " " + sib.strip()
                    else:
                        sib_text += " " + sib.get_text(" ", strip=True)
                    if len(sib_text) > 240:
                        break
            except Exception:
                pass
            m = amt_pattern.search(sib_text)
            if m:
                return m.group(0).strip()
        m = amt_pattern.search(soup_obj.get_text(" ", strip=True))
        return m.group(0).strip() if m else ""

    if "dice.com" in host and "/job-detail/" in job_url:
        details["Career Board"] = "Dice"
        details.setdefault("Apply URL", job_url)
        details = enrich_dice_fields(details, html)
        return details

    if "workday" in host:
        sal_txt = _extract_workday_salary_text(soup)
        if sal_txt:
            details.setdefault("Salary Range", sal_txt)
            details.setdefault("Salary Est. (Low-High)", sal_txt)

        # YC: extract title/company/location from page
    if "ycombinator.com" in host:
        yc_loc = None
        yc_src = None

        # 1) Embedded YC job payload
        cand = _yc_extract_location_from_embedded_job_payload(html)
        if _yc_is_plausible_location(cand):
            yc_loc = cand
            yc_src = "embedded_job_payload"

        # 2) JSON-LD
        if not yc_loc:
            cand = _yc_extract_location_from_jsonld(html)
            if _yc_is_plausible_location(cand):
                yc_loc = cand
                yc_src = "jsonld"

        # 3) Label-based / header-based
        if not yc_loc:
            cand = _yc_extract_location_from_label(html)
            if _yc_is_plausible_location(cand):
                yc_loc = cand
                yc_src = "label"

        # 4) Existing soup fallback
        if not yc_loc:
            cand = _yc_extract_location_from_soup(soup)
            if _yc_is_plausible_location(cand):
                yc_loc = cand
                yc_src = "soup"

        # Assign if we found a plausible YC job location.
        # This should override generic sidebar company HQ values like "San Francisco".
        if yc_loc:
            details["Location"] = yc_loc
            details["LocationRaw"] = yc_loc
            details["_YC_LOCATION_SOURCE"] = yc_src
            _yc_loc_sandwich("YC LOC AFTER EXTRACT", details, host, job_url)

        # Title: prefer the page H1
        h1 = soup.select_one("h1.ycdc-section-title")
        if h1:
            t = normalize_text(h1)
            if t:
                details["Title"] = t

        # Company: prefer a visible company link/name on page
        company_text = ""
        try:
            a = soup.select_one('a[href^="/companies/"]')
            if a:
                company_text = normalize_text(a)
        except Exception:
            company_text = ""

        if company_text:
            details["Company"] = company_text
        else:
            # Fallback: derive from URL slug (only if page did not provide it)
            from urllib.parse import urlparse as _yc_urlparse
            try:
                parsed = _yc_urlparse(job_url)
                parts = [p for p in parsed.path.split("/") if p]
            except Exception:
                parts = []

            if "companies" in parts:
                try:
                    idx = parts.index("companies")
                    if idx + 1 < len(parts):
                        comp_slug = parts[idx + 1]
                        comp_name = comp_slug.replace("-", " ").replace("_", " ").strip()
                        if comp_name:
                            details.setdefault("Company", comp_name)
                except Exception:
                    pass
        
            # Location: prefer JSON-LD JobPosting if present
            try:
                for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
                    raw = (s.string or s.get_text() or "").strip()
                    if not raw:
                        continue
                    data = json.loads(raw)

                    # Sometimes JSON-LD is a list
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        if (item.get("@type") or "").lower() != "jobposting":
                            continue

                        jl = item.get("jobLocation")
                        if isinstance(jl, list) and jl:
                            jl = jl[0]
                        if isinstance(jl, dict):
                            addr = jl.get("address") or {}
                            if isinstance(addr, dict):
                                city = addr.get("addressLocality") or ""
                                region = addr.get("addressRegion") or ""
                                country_code = addr.get("addressCountry") or ""
                                parts = [p for p in [city, region, country_code] if str(p).strip()]
                                if parts:
                                    loc_txt = ", ".join(str(p).strip() for p in parts)
                                    details.setdefault("Location", loc_txt)
                                    details.setdefault("Location Raw", loc_txt)
                                    chips_pipe = _chips_pipe_from_location_strings([loc_txt])
                                    if chips_pipe:
                                        details["Location Chips"] = chips_pipe
                                    break
                    if details.get("Location"):
                        break
            except Exception:
                pass


    def _is_bad_title(t: str) -> bool:
        if not t:
            return True
        tl = t.strip().lower()
        if tl.startswith("404"):
            return True
        if "all open positions" in tl:
            return True
        if tl in {"open positions", "jobs"}:
            return True
        return False

    if _is_bad_title(title):
        ogt = soup.find("meta", attrs={"property": "og:title"})
        og_title = ""
        if ogt and ogt.get("content"):
            og_title = ogt.get("content", "").strip()
        if og_title and not _is_bad_title(og_title):
            title = og_title
        else:
            parts = [p for p in up.urlparse(job_url).path.split("/") if p]
            seg = ""
            if "jobs" in parts:
                try:
                    j_idx = parts.index("jobs")
                    if j_idx + 1 < len(parts):
                        seg = parts[j_idx + 1]
                except Exception:
                    seg = ""
            if not seg and parts:
                seg = parts[-1]
            if seg:
                cleaned = seg.replace("-", " ").replace("_", " ").strip()
                if cleaned:
                    title = cleaned.title()

    # Board specific detail extractors
    board_extractors = {
        "remotive.com": parse_remotive,
        "www.remotive.com": parse_remotive,
        "dice.com": parse_dice,
        "www.dice.com": parse_dice,
        "hubspot.com": parse_hubspot_detail,
        "www.hubspot.com": parse_hubspot_detail,
    }

    board_data = {}
    parser = board_extractors.get(host)

    if parser:
        try:
            board_data = parser(soup, job_url) or {}
            details.update({k: v for k, v in board_data.items() if v})
        except Exception as e:
            log_line("WARN", f"board parser failed for {host}: {e}")

        # Generic JSON-LD + HTML date extraction (covers boards like nodesk.co)
    # Only fills when missing, so it will not disrupt board-specific parsers.
    try:
        schema_bits = parse_jobposting_ldjson(str(soup))
        if schema_bits:
            if schema_bits.get("posting_date") and not details.get("posting_date"):
                details["posting_date"] = schema_bits["posting_date"]
            if schema_bits.get("valid_through") and not details.get("valid_through"):
                details["valid_through"] = schema_bits["valid_through"]
            if schema_bits.get("posted") and not details.get("Posted"):
                details["Posted"] = schema_bits["posted"]

        # Extra safety net for weirdly embedded / escaped JSON-LD
        html_dates = _extract_dates_from_html(str(soup))
        if html_dates:
            if html_dates.get("posting_date") and not details.get("posting_date"):
                details["posting_date"] = html_dates["posting_date"]
            if html_dates.get("valid_through") and not details.get("valid_through"):
                details["valid_through"] = html_dates["valid_through"]

    except Exception as e:
        log_line("DEBUG", f"[DATES] generic extraction failed for {host}: {e}")


    # Bridge board-specific date keys into canonical sheet fields (UTC-first)
    raw_posting = (board_data.get("posting_date") if isinstance(board_data, dict) else None) or details.get("posting_date")
    raw_valid   = (board_data.get("valid_through") if isinstance(board_data, dict) else None) or details.get("valid_through")

    if raw_posting and not details.get("Posting Date"):
        bridged = (
            utc_date_from_iso(raw_posting)
            or parse_date_relaxed(raw_posting)
            or str(raw_posting).strip()
        )
        details["Posting Date"] = bridged
        #log_line("DEBUG", f"[DATES] {host} bridged posting_date -> Posting Date: {raw_posting} -> {bridged} ({job_url})")

    if raw_valid and not details.get("Valid Through"):
        bridged = (
            utc_date_from_iso(raw_valid)
            or parse_date_relaxed(raw_valid)
            or str(raw_valid).strip()
        )
        details["Valid Through"] = bridged
        #log_line("DEBUG", f"[DATES] {host} bridged valid_through -> Valid Through: {raw_valid} -> {bridged} ({job_url})")

    # Fallback relative Posted text
    if not details.get("Posted"):
        rel_span = soup.find("span", string=re.compile(r"\d+\s*(day|week|month)s?\s*ago", re.I))
        if rel_span:
            details["Posted"] = rel_span.get_text(strip=True).replace("|", "").strip()

    # Normalize Posted label across boards (remove leading "Job " in "Job Posted ...")
    if details.get("Posted"):
        details["Posted"] = normalize_posted_label(details["Posted"])

    # Ensure keys exist as strings for Sheets
    details.setdefault("Posting Date", "")
    details.setdefault("Valid Through", "")

    # Company basic detection
    company = ""
    a = (
        soup.select_one("a.job-header__jobHeaderCompanyNameProgrammatic")
        or soup.select_one("a.job-header_jobHeaderCompanyNameProgrammatic")
        or soup.select_one("header a[href*='/profiles/']")
    )
    if a:
        company = (a.get_text(" ", strip=True) or "").strip()

    if not company:
        try:
            for script in soup.find_all("script", type="application/ld+json"):
                txt = script.string or ""
                if "hiringOrganization" not in txt:
                    continue
                data = _json.loads(txt)
                objs = data if isinstance(data, list) else [data]
                for obj in objs:
                    org = (obj.get("hiringOrganization") or {})
                    name = (org.get("name") or "").strip()
                    if name:
                        company = name
                        break
                if company:
                    break
        except Exception:
            pass

    if builtin_meta.get("company"):
        company = builtin_meta["company"]
    if not company and "builtinseattle.com" in host:
        cnode = (
            soup.select_one('a[data-id="company-title"]')
            or soup.select_one('h2[data-id="company-title"]')
            or soup.select_one('[data-id="company-title"] span')
        )
        if cnode:
            company = (cnode.get_text(" ", strip=True) or "").strip()

    # Helper that can be reused for missing companies
    def company_from_header_meta(page_host: str, html_text: str) -> str | None:
        s2 = BeautifulSoup(html_text or "", "html.parser")
        og2 = s2.find("meta", attrs={"property": "og:site_name"})
        if og2 and og2.get("content"):
            return og2["content"].strip()
        t2 = s2.find("title")
        if t2:
            txt = t2.get_text(" ", strip=True)
            if " - " in txt:
                maybe = txt.split(" - ")[-1].strip()
                if 2 <= len(maybe) <= 80:
                    return maybe
        h = page_host.lower()
        if "builtinseattle.com" in h:
            # Built In city pages sometimes expose the company only in meta tags
            # even when the visible DOM/header selectors are missing.
            cnode = (
                s2.select_one('a[data-id="company-title"]')
                or s2.select_one('h2[data-id="company-title"]')
                or s2.select_one('[data-id="company-title"] span')
            )
            if cnode:
                ctxt = (cnode.get_text(" ", strip=True) or "").strip()
                if ctxt:
                    return ctxt

            ogt = s2.find("meta", attrs={"property": "og:title"})
            for cand in [
                (ogt.get("content") if ogt and ogt.get("content") else ""),
                (t2.get_text(" ", strip=True) if t2 else ""),
            ]:
                txt = (cand or "").strip()
                if not txt:
                    continue
                if "| Built In" in txt:
                    left = txt.split("| Built In", 1)[0].strip()
                    parts = [p.strip() for p in left.split(" - ") if p.strip()]
                    if len(parts) >= 2:
                        return parts[-1]

            for meta_sel in [
                ("meta", {"name": "description"}),
                ("meta", {"property": "og:description"}),
            ]:
                mtag = s2.find(meta_sel[0], attrs=meta_sel[1])
                txt = (mtag.get("content") or "").strip() if mtag else ""
                if not txt:
                    continue
                m = re.match(r"\s*([A-Za-z0-9&.,'()\\-/ ]{2,120}?)\s+is hiring\b", txt, re.I)
                if m:
                    return m.group(1).strip()
        if "greenhouse.io" in h:
            bc = s2.select_one('[data-mapped="employer_name"], .company-name, .app-title')
            if bc:
                return bc.get_text(" ", strip=True)
        if "builtin.com" in h or "builtinseattle.com" in h:
            c = s2.select_one('a[href*="/company/"], .company__name, [data-test="company-name"]')
            if c:
                return c.get_text(" ", strip=True)
        if "hubspot.com" in h:
            ogt2 = s2.find("meta", attrs={"property": "og:title"})
            if ogt2 and " - " in ogt2.get("content", ""):
                return ogt2["content"].rsplit(" - ", 1)[-1].strip()
        return None

    # ---- Location ----
    loc = ""
    cand = (
        soup.select_one("[class*='location']")
        or soup.select_one("li:has(svg) + li")
        or soup.find("span", string=lambda s: s and "remote" in s.lower())
    )
    if cand:
        loc = cand.get_text(" ", strip=True)

    locs_unique: list[str] = []

    # Built In tooltip locations (central and Vancouver)
    if "builtin.com" in host or "builtinseattle.com" in host or "builtinvancouver.org" in host:
        _builtin_fill_title_company_from_builtinsignals(details, soup, html)
        import html as _html

        def _builtin_locations_from_title() -> list[str]:
            locs: list[str] = []

            # Only look at tooltip nodes that have a title payload
            nodes = list(soup.find_all(attrs={"data-bs-toggle": "tooltip", "title": True}))
            nodes += list(soup.find_all(attrs={"data-bs-toggle": "tooltip", "data-bs-title": True}))

            for node in nodes:
                # BuiltIn uses the HTML payload in the *title* attribute (escaped)
                tattr = (node.get("title") or (node.get("data-bs-original-title") or "").strip())
                if not tattr:
                    continue

                # Only keep the "X Locations" tooltip, not random tooltips
                node_text = (node.get_text(" ", strip=True) or "").lower()
                if "location" not in node_text:
                    continue

                unesc = _html.unescape(tattr)

                # Parse the tooltip HTML, then pull each cell
                if "<div" in unesc:
                    try:
                        inner = BeautifulSoup(unesc, "html.parser")

                        # BuiltIn renders each location in a col div
                        for div in inner.select("div.col-lg-6"):
                            txt = div.get_text(" ", strip=True)
                            if txt:
                                locs.append(txt)

                        # Fallback if class changes
                        if not locs:
                            for div in inner.find_all("div"):
                                txt = div.get_text(" ", strip=True)
                                if txt and "row g-" not in txt.lower():
                                    locs.append(txt)

                    except Exception:
                        continue
                else:
                    # Non HTML tooltip fallback
                    for part in re.split(r"[;|]+", unesc):
                        txt = part.strip()
                        if txt:
                            locs.append(txt)

            # Dedupe Deduplicate while preserving order
            seen = set()
            unique = []
            for x in locs:
                k = x.lower()
                if k not in seen:
                    seen.add(k)
                    unique.append(x)

            return unique

        locs_unique = _builtin_locations_from_title()

        if locs_unique:
            details["Location"] = " / ".join(locs_unique)
            if locs_unique:
                details["Location Chips"] = locs_unique  # optional, but useful
            loc_text = details["Location"]  # prevents later single-location fallback from overwriting
            details["BIV Tooltip Location Count"] = len(locs_unique)  # so your final preservation block actually fires


    # WTTJ special location
    if not loc and "welcometothejungle.com" in host:
        icon = soup.find("i", attrs={"name": "location"})
        if icon:
            span = icon.find_next("span")
            if span:
                loc_text = span.get_text(" ", strip=True)
                if loc_text:
                    loc = loc_text

    # ---- Description ----
    desc = ""
    md = soup.find("meta", attrs={"name": "description"})
    ogd = soup.find("meta", attrs={"property": "og:description"})
    if md and md.get("content"):
        desc = md["content"].strip()
    if not desc and ogd and ogd.get("content"):
        desc = ogd["content"].strip()
    if not desc:
        p = soup.find("p")
        if p:
            desc = p.get_text(" ", strip=True)[:300]
    if not page_text and desc:
        page_text = desc

    if "builtinseattle.com" in host:
        # Preserve any earlier Seattle values from Built In-specific passes.
        title = (title or details.get("Title") or "").strip()
        company = (company or details.get("Company") or "").strip()

        # Seattle fallback from raw HTML main card / page title if generic title stayed empty.
        if not title:
            try:
                s_sea = BeautifulSoup(html or "", "html.parser")
                h1_sea = s_sea.select_one("div[data-id='job-card'] h1") or s_sea.find("h1")
                if h1_sea:
                    title = (h1_sea.get_text(" ", strip=True) or "").strip()
                if not title:
                    t_sea = s_sea.find("title")
                    raw_t = (t_sea.get_text(" ", strip=True) if t_sea else "").strip()
                    if raw_t and "| Built In Seattle" in raw_t:
                        left = raw_t.split("| Built In Seattle", 1)[0].strip()
                        parts = [p.strip() for p in left.split(" - ") if p.strip()]
                        if parts:
                            title = parts[0]
            except Exception:
                pass

    details.update({
        "Title": title,
        "Company": company,
        "Career Board": infer_board_from_url(job_url),
        "Description": desc,
        "Description Snippet": desc,
        "Job URL": job_url,
        "job_url": job_url,
    })

    # Workday location enrich before rules
    if "workday" in host or "myworkday" in host or "myworkdaysite" in host:
        details = _enrich_workday_location(details, html, job_url)

    # Capture full text lower for downstream rules
    # Keep the main-scoped page_text we already set earlier
    page_txt_lower = (details.get("page_text") or "").lower()

    # --- Always initialize Country Chips container for all hosts ---
    country: set[str] = set()

    # Fold in any existing Country Chips if already present
    cc = details.get("Country Chips")
    if isinstance(cc, (list, tuple, set)):
        country.update(str(x).strip().lower() for x in cc if str(x).strip())
    elif isinstance(cc, str) and cc.strip():
        country.update(
            p.strip().lower()
            for p in cc.replace(" / ", "|").replace(",", "|").split("|")
            if p.strip()
        )

    # DEBUG: location state before country and rule derivations
    try:
        if "ycombinator.com" in host and "companies/gromo/jobs" in job_url:
            log_line(
                "YC LOC MID",
                f"pre-country: Location={details.get('Location')!r} | "
                f"LocRaw={details.get('Location Raw')!r} | "
                f"LocChips={details.get('Location Chips')!r} | "
                f"AppRegions={details.get('Applicant Regions')!r} | "
                f"CountryChips={details.get('Country Chips')!r}"
            )
    except Exception:
        pass


    details["html_raw"] = html or ""

    # Optional Muse job id capture
    m = re.search(r"\bJob\s*ID:\s*([A-Za-z0-9_-]+),?\s*(\d+)?", page_txt_lower, re.I)

    if m:
        details["job_id_vendor"] = m.group(1)
        if m.group(2):
            details["job_id_numeric"] = m.group(2)

    # Apply URL generic fallback
    if not details.get("apply_url"):
        try:
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                label = a.get_text(" ", strip=True).lower()
                if "apply" in label or "submit" in label or "apply now" in label:
                    details["apply_url"] = up.urljoin(job_url, href)
                    break
        except Exception:
            pass
    if not details.get("apply_url"):
        details["apply_url"] = job_url
        details["Apply URL Note"] = "Apply link missing or job unavailable"

    # Built In: trust inline meta location only when it makes sense
    builtin_loc = (builtin_meta.get("location") or "").strip()

    if builtin_loc:
        if "builtinvancouver.org" in host:
            # Vancouver site: do not let meta override Canada with US noise
            bl = builtin_loc.lower()
            if any(x in bl for x in ["united states", "u.s.", "usa", "us", "united-states"]):
                # ignore builtin_loc, keep whatever we already set (hero_country / JSON-LD / card)
                pass
            else:
                details["Location"] = builtin_loc
        else:
            # Central Built In is ok to trust more often
            details["Location"] = builtin_loc


    if not details.get("Salary Range") and (builtin_meta.get("salary_text") or builtin_meta.get("salary")):
        details["Salary Range"] = builtin_meta.get("salary_text") or builtin_meta.get("salary")

    # Built In (both builtin.com and builtinvancouver.org):
    # Multi-location list is stored in a tooltip attribute as escaped HTML.
    # Extract EARLY so later fallbacks do not lock us to one city.
    if ("builtin.com" in host or "builtinseattle.com" in host or "builtinvancouver.org" in host) and not details.get("Builtin Tooltip Locations"):
        try:
            import html as _html

            def _builtin_tooltip_locations(soup0) -> list[str]:
                # Typical markup:
                # - builtin.com: <span ... data-bs-title="&lt;div class='text-truncate'&gt;Austin, TX, USA&lt;/div&gt;...">3 Locations</span>
                # - builtinvancouver.org: <span ... title="&lt;div...&gt;&lt;div class='col-lg-6'&gt;...">
                candidates = list(soup0.select("span[data-bs-toggle='tooltip']"))
                if not candidates:
                    candidates = list(soup0.find_all(attrs={"data-bs-toggle": "tooltip"}))

                best: list[str] = []
                for node in candidates:
                    label = (node.get_text(" ", strip=True) or "")
                    if not re.search(r"\bLocations?\b", label, re.I):
                        continue

                    raw = (node.get("data-bs-title") or node.get("title") or "").strip()
                    if not raw:
                        continue

                    import html as _html
                    # Double-unescape to handle nested entities
                    unesc = _html.unescape(_html.unescape(raw))

                    inner = BeautifulSoup(unesc, "html.parser")

                    # Support both tooltip layouts
                    locs = [d.get_text(" ", strip=True) for d in inner.select("div.col-lg-6")]
                    if not locs:
                        locs = [d.get_text(" ", strip=True) for d in inner.select("div.text-truncate")]

                    locs = [x for x in locs if x]

                    # De-dupe, preserve order
                    seen = set()
                    uniq: list[str] = []
                    for x in locs:
                        k = x.lower()
                        if k not in seen:
                            seen.add(k)
                            uniq.append(x)

                    if len(uniq) > len(best):
                        best = uniq

                return best

            best = _builtin_tooltip_locations(soup)
            if best and len(best) > 1:
                details["Builtin Tooltip Locations"] = best
                details["Location"] = " / ".join(best)
                details.setdefault("Location Raw", details["Location"])
                # Keep as a LIST so downstream logic can keep city-level detail
                new_val = _chips_pipe_from_location_strings(best)
                if new_val:
                    details["Location Chips"] = new_val

        except Exception as e:
            # keep quiet unless you want BuiltIn debug noise
            pass


    # Base salary/location enrichment
    details = enrich_salary_fields(details, page_host=host)

    try:
        if "ycombinator.com" in host:
            _yc_trace(
                "YC RULES PRE",
                f"Location={details.get('Location')!r} | "
                f"LocRaw={details.get('Location Raw')!r} | "
                f"LocChips={details.get('Location Chips')!r} | "
                f"AppRegions={details.get('Applicant Regions')!r} | "
                f"CountryChips={details.get('Country Chips')!r}"
            )
    except Exception:
        pass

    if not details.get("_DERIVE_LOCATION_RULES_DONE"):
        details = _derive_location_rules(details)
    _yc_trace(details, "YC RULES POST")

    _debug_biv(details, host, "after base enrich + derive")

    debug(f"[BIV DEBUG]......AFTER enrich_salary_fields: "
          f"Status={details.get('Salary Status')} Placeholder={details.get('Salary Placeholder')} Salary={details.get('Salary')}")


    # Built In: for central site, prefer tooltip locations and add ", USA" when obviously missing.
    # For Built In Vancouver, prefer tooltip but never force USA.
    if "builtin.com" in host or "builtinseattle.com" in host or "builtinvancouver.org" in host:
        _builtin_fill_title_company_from_builtinsignals(details, soup, html)
        try:
            # 0) Scope to the main job header so we do NOT scrape Similar Jobs
            scope = (
                soup.select_one("div.job-header")
                or soup.select_one("header")
                or soup.select_one("main")
                or soup
            )

            loc_text = ""

            # 1) Prefer tooltip locations inside the header scope only
            import html as _html

            def _extract_builtin_tooltip_locations(scope_soup) -> list[str]:
                """Return list of locations embedded in Built In tooltip HTML.

                Built In often stores the multi-location list inside a tooltip attribute
                (title, data-bs-original-title, etc) as HTML that is HTML escaped.
                """
                import html as _html

                # Collect candidate nodes that might carry the tooltip HTML.
                candidates = []
                try:
                    candidates.extend(scope_soup.select("[data-bs-toggle='tooltip']"))
                except Exception:
                    pass

                # Also include any node with a tooltip-like attribute, even if their selector changes.
                try:
                    candidates.extend(scope_soup.find_all(attrs={"title": True}))
                    candidates.extend(scope_soup.find_all(attrs={"data-bs-original-title": True}))
                    candidates.extend(scope_soup.find_all(attrs={"data-original-title": True}))
                    candidates.extend(scope_soup.find_all(attrs={"data-bs-title": True}))
                except Exception:
                    pass

                # De-dupe by object id
                seen_ids = set()
                uniq_candidates = []
                for n in candidates:
                    nid = id(n)
                    if nid in seen_ids:
                        continue
                    seen_ids.add(nid)
                    uniq_candidates.append(n)

                best: list[str] = []

                def _get_tooltip_attr(node) -> str:
                    for k in ("title", "data-bs-original-title", "data-original-title", "data-bs-title"):
                        v = node.get(k)
                        if v:
                            return str(v)
                    return ""

                for node in uniq_candidates:
                    label = (node.get_text(" ", strip=True) or "").lower()

                    # Strong signal: visible label says 'X Locations' or contains 'Locations'
                    if "location" not in label:
                        aria = (node.get("aria-label") or "").lower()
                        if "location" not in aria:
                            continue

                    raw = _get_tooltip_attr(node).strip()
                    if not raw:
                        continue

                    # Built In sometimes double-escapes entities (Montr&amp;#233;al)
                    unesc = _html.unescape(_html.unescape(raw))

                    # If we do not see their grid/cell markers, skip.
                    if ("col-lg-6" not in unesc) and ("row" not in unesc) and ("<div" not in unesc):
                        continue

                    # Parse the embedded tooltip HTML.
                    try:
                        inner = BeautifulSoup(unesc, "html.parser")
                    except Exception:
                        continue

                    # Primary: each location appears in a div.col-lg-6 cell.
                    locs = [d.get_text(" ", strip=True) for d in inner.select("div.col-lg-6")]
                    locs = [x for x in locs if x]

                    # Built In Vancouver sometimes uses .text-truncate without commas (e.g., CA, MO).
                    if not locs and "builtinvancouver.org" in host:
                        locs = [d.get_text(" ", strip=True) for d in inner.select(".text-truncate")]
                        locs = [x for x in locs if x]

                    # Secondary: take any leaf div text that looks like a location
                    if not locs:
                        for div in inner.find_all("div"):
                            txt = div.get_text(" ", strip=True)
                            if txt and ("," in txt or "builtinvancouver.org" in host):
                                locs.append(txt)

                    # Last resort: any stripped string containing commas
                    if not locs:
                        if "builtinvancouver.org" in host:
                            locs = [s.strip() for s in inner.stripped_strings if s]
                        else:
                            locs = [s.strip() for s in inner.stripped_strings if s and "," in s]

                    # De-dupe, preserve order
                    seen = set()
                    uniq: list[str] = []
                    for x in locs:
                        k = x.lower()
                        if k not in seen:
                            seen.add(k)
                            uniq.append(x)

                    # Keep the largest list found (in case multiple tooltips match)
                    if len(uniq) > len(best):
                        best = uniq

                return best


            # If we already extracted the tooltip list earlier (preferred), reuse it.
            if "builtinvancouver.org" in host and isinstance(details.get("BIV Tooltip Locations"), list) and details.get("BIV Tooltip Locations"):
                locs_unique = details["BIV Tooltip Locations"]
            else:
                # Use main as the scope first because div.job-header is often too narrow
                scope = soup.select_one("main") or soup
                locs_unique = _extract_builtin_tooltip_locations(scope)

            # Built In Vancouver fallback: use JSON-LD locations if tooltip extraction misses.
            if "builtinvancouver.org" in host and not locs_unique:
                ld_locs = details.get("locations")
                if isinstance(ld_locs, list) and len(ld_locs) > 1:
                    locs_unique = [x for x in ld_locs if x]


            # DEBUG: show raw tooltip extraction result before setting Location
            if "builtinvancouver.org" in host:
                _debug_biv(
                    {
                        "Location": loc_text,
                        "Location Chips": _as_pipe_location_chips(details.get("Location Chips")) or "",
                        "Canada Rule": details.get("Canada Rule", ""),
                        "US Rule": details.get("US Rule", ""),
                        "WA Rule": details.get("WA Rule", ""),
                        "BC Rule": details.get("BC Rule", ""),
                        "ON Rule": details.get("ON Rule", ""),
                        "Remote Rule": details.get("Remote Rule", ""),
                        "Applicant Regions": _as_pipe_applicant_regions(details.get("Applicant Regions")) or "",
                        "Applicant Regions Source": details.get("Applicant Regions Source", ""),
                    },
                    host,
                    "tooltip extraction raw",
                )

            if locs_unique:
                details["BIV Tooltip Location Count"] = len(locs_unique)
                details["Location"] = " / ".join(locs_unique)
                details.setdefault("Location Raw", details["Location"])

                if details.get("_LOCK_LOCATION_CHIPS") is True:
                    loc_text = details["Location"]
                else:
                    new_val = _chips_pipe_from_location_strings(locs_unique)

                    details["Location Chips Source"] = "TOOLTIP"
                    details["_LOCATION_CHIPS_SOURCE"] = "TOOLTIP"
                    details["_LOCK_LOCATION_CHIPS"] = True

                    if new_val:
                        _set_loc_chips(details, new_val, "TOOLTIP chips from locs_unique (header scope)")

                    loc_text = details["Location"]  # stop later fallbacks from overwriting

                if "builtinvancouver.org" in host:
                    details["BIV Tooltip Location Count"] = len(locs_unique)

            # 2) Built In Vancouver specific: “Hiring Remotely in Canada”
            #    (only check inside the header scope)
            if not loc_text:
                header_text = scope.get_text(" ", strip=True)
                m = re.search(r"Hiring\s+Remotely\s+in\s+([A-Za-z ]+)", header_text, re.I)
                if m:
                    loc_text = m.group(1).strip()
                    details["Remote Rule"] = "Remote"

            # 3) Single location icon fallback INSIDE the header scope only
            if not loc_text:
                loc_icon = scope.select_one("i.fa-location-dot")
                container = None
                if loc_icon:
                    container = loc_icon.find_parent("div", class_=re.compile(r"\bd-flex\b.*align-items-start\b", re.I)) \
                        or loc_icon.find_parent("div", class_=re.compile(r"\bd-flex\b.*gap-sm\b", re.I)) \
                        or loc_icon.find_parent("div", class_=re.compile(r"\bd-flex\b", re.I))

                if container:
                    span_texts = [s.get_text(" ", strip=True) for s in container.find_all("span") if s.get_text(strip=True)]
                    if span_texts:
                        loc_text = span_texts[0]

            if loc_text:
                if not (is_biv and details.get("_LOCK_LOCATION_FROM_TOOLTIP")):
                    details["Location"] = loc_text

            if "builtinvancouver.org" in host and loc_text:
                details["BIV Tooltip Location Count"] = loc_text.count("/") + 1


        except Exception:
            pass

    # Re run location rules after Built In overrides
    if not details.get("_DERIVE_LOCATION_RULES_DONE"):
        details = _derive_location_rules(details)

    # If we extracted full tooltip locations, keep them as the displayed Location
    if details.get("BIV Tooltip Locations"):
        tooltip_locs = [str(x).strip() for x in details["BIV Tooltip Locations"] if str(x).strip()]

        if len(tooltip_locs) > 1:
            details["Location"] = " / ".join(tooltip_locs)
            details.setdefault("Location Raw", details["Location"])

            # Tooltip locations are authoritative. Lock them, but do not recompute if already locked.
            if details.get("_LOCK_LOCATION_CHIPS") is not True:
                new_val = _chips_pipe_from_location_strings(tooltip_locs)

                details["Location Chips Source"] = "TOOLTIP"
                details["_LOCATION_CHIPS_SOURCE"] = "TOOLTIP"
                details["_LOCK_LOCATION_CHIPS"] = True

                if new_val:
                    _set_loc_chips(details, new_val, "TOOLTIP chips from BIV Tooltip Locations (finalize)")

            # after setting Location / Location Raw / Location Chips / Source from tooltip
            details["_DERIVE_LOCATION_RULES_DONE"] = False
            details = _derive_location_rules(details)

        return details

    # Built In Vancouver: normalize dates and treat Canada as remote
    if "builtinvancouver.org" in host:
        try:
            # Walk JobPosting ld+json again in case parse_jobposting_ldjson
            # did not already set the friendly fields.
            for tag in soup.find_all("script", type="application/ld+json"):
                raw = (tag.string or "").strip()
                if not raw:
                    continue

                data = _json.loads(raw)
                objs = data if isinstance(data, list) else [data]

                for obj in objs:
                    if not isinstance(obj, dict):
                        continue
                    if obj.get("@type") != "JobPosting":
                        continue

                    # 1) Dates from structured fields
                    dp = obj.get("datePosted")
                    vt = obj.get("validThrough")
                    if dp and not details.get("Posting Date"):
                        details["Posting Date"] = parse_date_relaxed(dp)
                    if vt and not details.get("Valid Through"):
                        details["Valid Through"] = parse_date_relaxed(vt)

                    # 2) Location from jobLocation.address.addressCountry
                    job_loc = obj.get("jobLocation") or {}
                    if isinstance(job_loc, list):
                        job_loc = job_loc[0] or {}
                    addr = job_loc.get("address") or {}
                    country = addr.get("addressCountry") or addr.get("addressCountryCode")

                    if isinstance(country, dict):
                        country = country.get("name") or country.get("addressCountry")

                    # If the posting is country wide for Canada,
                    # treat it as a remote Canada role
                    if str(country).upper() in {"CAN", "CA"} or str(country).strip().lower() == "canada":
                        existing = (details.get("Location") or "").lower()
                        if existing in {"", "canada", "ca", "can"}:
                            details["Location"] = "Canada"
                            details["Remote Rule"] = "Remote"

                        # Make sure Canada is in Country Chips
                        existing = set()
                        cc = details.get("Country Chips") or []
                        for c in cc:
                            existing.add(str(c).lower())
                        existing.add("canada")
                        details["Country Chips"] = sorted(existing)

                    # We only care about the first JobPosting object
                    break

                # Stop scanning scripts once we have both dates
                if details.get("Posting Date") and details.get("Valid Through"):
                    break
        except Exception:
            # Best effort only, do not break the scraper if Built In changes their JSON
            pass

    # If we have a Posted label but no Posting Date, derive it
    if not (details.get("Posting Date") or "").strip():
        posted = (details.get("Posted") or details.get("posted") or "").strip()
        iso = _posted_label_to_iso_date(posted)
        if iso:
            details["posting_date"] = details.get("posting_date") or iso
            details["Posting Date"] = iso
            #log_line("DEBUG", f"[DATES] bridged Posted -> Posting Date: {posted!r} -> {iso} ({job_url})")


    # Final Built In Vancouver lock, after all other location sources
    if "builtinvancouver.org" in host:
        # Prevent BIV hero lock from running twice for the same job
        if details.get("_BIV_HERO_LOCK_DONE"):
            return details

        hero_country = _builtin_hero_country(soup)
        hero_text = ""
        try:
            # This scope is optional. If you already have a "scope" for hero, use that instead.
            hero_text = soup.get_text(" ", strip=True)
        except Exception:
            hero_text = ""

        hero_is_remote = bool(re.search(r"\bRemote\b", hero_text, flags=re.I)) or bool(
            re.search(r"Hiring\s+Remotely\s+in", hero_text, flags=re.I)
        )

        if hero_is_remote and (details.get("Remote Rule") or "").strip().lower() in {"", "default", "unknown"}:
            details["Remote Rule"] = "Remote"
            details["Eligibility Notes"] = (details.get("Eligibility Notes") or "") + "|BIV_HERO_REMOTE"

        loc = (details.get("Location") or "").lower()
        if hero_country:
            loc_raw = (details.get("LocationRaw") or "").strip().lower()
            loc_now = (details.get("Location") or "").strip().lower()

            # If Raw indicates Canada and hero says CAN, override any polluted multi location display
            raw_is_can = loc_raw in {"can", "canada"}
            polluted_multi = (" / " in loc_now) or ("usa" in loc_now) or ("united states" in loc_now)

            if raw_is_can and polluted_multi:
                details["Location"] = hero_country
                details["LocationRaw"] = hero_country
                _set_loc_chips(details, "CAN", "HERO_LOCK set CAN only")
                details["Location Chips Source"] = "HERO_LOCK"
                details["_LOCATION_CHIPS_SOURCE"] = "HERO_LOCK"
                details["_LOCK_LOCATION_CHIPS"] = True
                details["HERO_LOCK"] = True
                details["Remote Rule"] = "Remote"
                _debug_biv(details, host, "after hero_country lock override (raw_can + polluted)")

            # Keep your original behavior too (covers clean cases)
            elif loc_now in {"", "canada", "ca", "can"}:
                details["Location"] = hero_country
                details["LocationRaw"] = hero_country
                _debug_biv(details, host, "after hero_country lock + derive")
            
            details["_BIV_HERO_LOCK_DONE"] = True


        # Location chips and country chips
        # If HERO_LOCK fired, ensure CAN chip is present.
        if "HERO_LOCK" in (details.get("Location Chips Source") or ""):
            loc_now = (details.get("Location") or "").strip().upper()
            # Built In Vancouver: if hero location resolved to CAN, chips must be CAN only
            if loc_now in ("CAN", "CANADA"):
                # Force overwrite even if chips are already locked
                _set_loc_chips(details, "CAN", "HERO_LOCK set CAN only", force=True)

                # Also make the stored string explicit, in case downstream code reads it directly
                details["Location Chips"] = "CAN"
                details["Location Chips Source"] = "HERO_LOCK"
                details["_LOCK_LOCATION_CHIPS"] = True
                details["_LOCATION_CHIPS_SOURCE"] = "HERO_LOCK"

        debug(f"[NORM PRE] chips_type={type(details.get('Location Chips')).__name__} chips={details.get('Location Chips')!r}")
        _normalize_canada_provinces_in_details(details)
        debug(f"[NORM POST] chips_type={type(details.get('Location Chips')).__name__} chips={details.get('Location Chips')!r}")

        if details.get("BIV Tooltip Locations"):
            tooltip_locs = [str(x).strip() for x in details["BIV Tooltip Locations"] if str(x).strip()]
            if len(tooltip_locs) > 1:
                details["Location"] = " / ".join(tooltip_locs)
                details.setdefault("Location Raw", details["Location"])

                if details.get("_LOCK_LOCATION_CHIPS") is not True:
                    new_val = _chips_pipe_from_location_strings(tooltip_locs)

                    details["Location Chips Source"] = "TOOLTIP"
                    details["_LOCATION_CHIPS_SOURCE"] = "TOOLTIP"
                    details["_LOCK_LOCATION_CHIPS"] = True

                    if new_val:
                        _set_loc_chips(details, new_val, "TOOLTIP chips from BIV Tooltip Locations (post normalize)")

                return details  # keep this return, but only when tooltip list is authoritative

        # Do not normalize locked tooltip chips (commas are part of the location string).
        if details.get("_LOCK_LOCATION_CHIPS"):
            return details

        # Normalize Location Chips to a pipe string (single representation)
        if not details.get("_LOCK_LOCATION_CHIPS"):
            lc = details.get("Location Chips")
            if isinstance(lc, str) and not lc.strip():
                details["Location Chips"] = None
            chips = set()

            if isinstance(lc, (list, tuple, set)):
                chips.update(str(x).strip() for x in lc if str(x).strip())
            elif isinstance(lc, str):
                s = lc.strip()
                if s:
                    # Always split into tokens. Pipes and slashes are valid separators too.
                    parts = s.replace(" / ", "|").replace(",", "|").split("|")
                    chips.update(p.strip() for p in parts if p.strip())

            # Persist chips string deterministically again, but do not overwrite when locked
            if not details.get("_LOCK_LOCATION_CHIPS"):
                def _chip_sort_key(tok: str):
                    t = (tok or "").strip().upper()

                    # Countries first, with explicit order
                    if t == "CAN":
                        return (0, 0, t)
                    if t == "USA":
                        return (0, 1, t)

                    # Then known state and province tokens (alphabetical within)
                    # Keep this aligned with your project stance that "CA" is California, not Canada.
                    STATE_PROV_TOKENS = {"WA", "CA", "DC", "IL", "BC", "ON"}  # extend as needed
                    if t in STATE_PROV_TOKENS:
                        return (1, 0, t)

                    # Everything else last
                    return (2, 0, t)

                uniq = sorted({c.strip().upper() for c in chips_list if str(c).strip()}, key=_chip_sort_key)
                details["Location Chips"] = "|".join(uniq)
            existing_lc = (details.get("Location Chips") or "").strip()
            normalized_lc = "|".join(sorted(chips)) if chips else ""

            if normalized_lc or not existing_lc:
                details["Location Chips"] = normalized_lc

    # --- initialize Country Chips container for all hosts ---
    country: set[str] = set()

    # Fold in any existing Country Chips if already present
    cc = details.get("Country Chips")
    if isinstance(cc, (list, tuple, set)):
        country.update(str(x).strip().lower() for x in cc if str(x).strip())
    elif isinstance(cc, str) and cc.strip():
        country.update(
            p.strip().lower()
            for p in cc.replace(" / ", "|").replace(",", "|").split("|")
            if p.strip()
        )

    # Prefer structured country from Location, not page text
    loc_norm = (details.get("Location") or "").strip().lower()
    if "canada" in loc_norm or loc_norm in {"ca", "can"}:
        country.add("canada")
    elif any(x in loc_norm for x in ["united states", "usa", "u.s."]):
        country.add("us")
    else:
        # fallback to page text only for non Built In pages
        if (
            "builtin.com" not in host
            and "builtinseattle.com" not in host
            and "builtinvancouver.org" not in host
            and "ycombinator.com" not in host
        ):
            if any(t in page_txt_lower for t in ("united states", "u.s.", "usa")):
                country.add("us")
            if "canada" in page_txt_lower:
                country.add("canada")

    details["Country Chips"] = sorted(country)

    try:
        if "ycombinator.com" in host and "companies/gromo/jobs" in job_url:
            log_line(
                "YC LOC END",
                f"post-country: Location={details.get('Location')!r} | "
                f"LocRaw={details.get('Location Raw')!r} | "
                f"LocChips={details.get('Location Chips')!r} | "
                f"AppRegions={details.get('Applicant Regions')!r} | "
                f"CountryChips={details.get('Country Chips')!r}"
            )
    except Exception:
        pass

    details["page_text_lower"] = page_txt_lower

    # Final company cleanup
    existing_company = _normalize_company_name(details.get("Company", ""))
    builtin_company = _strip_builtin_brand(builtin_meta.get("company")) if builtin_meta else ""
    company_source = ""
    for src_name, src_val in [
        ("existing_company", existing_company),
        ("builtin_meta.company", (builtin_meta.get("company") if builtin_meta else "")),
        ("company_from_header", company_from_header),
        ("builtin_meta.company_stripped", builtin_company),
        ("common_selectors", _company_from_common_selectors(soup)),
        ("meta_or_title", _company_from_meta_or_title(host, soup)),
        ("url_fallback", company_from_url_fallback(job_url)),
    ]:
        if _normalize_company_name(src_val):
            company_source = src_name
            break
    company_final = (
        existing_company
        or builtin_meta.get("company")
        or company_from_header
        or builtin_company
        or _company_from_common_selectors(soup)
        or _company_from_meta_or_title(host, soup)
        or company_from_url_fallback(job_url)
        or ""
    )
    company_final = _normalize_company_name(company_final)
    if not company_final:
        fallback = company_from_header_meta(host, html)
        if fallback:
            company_final = fallback
            company_source = company_source or "header_meta"
    if "builtin.com" in host:
        company_final = _strip_builtin_brand(company_final)
    if not details.get("Company"):
        details["Company"] = company_final or "No Company Found"
    if "builtinseattle.com" in host:
        html_dbg = html or ""
        log_line(
            "DEBUG",
            f"[BIVDBG] final_company url={job_url} company={details.get('Company')} source={company_source or 'none'} "
            f"html_len={len(html_dbg)} has_jobpostinit={'Builtin.jobPostInit' in html_dbg} "
            f"has_jsonld={'application/ld+json' in html_dbg} has_title={'<title' in html_dbg.lower()} "
            f"has_meta_desc={'name=\"description\"' in html_dbg.lower()} has_og_desc={'property=\"og:description\"' in html_dbg.lower()}",
        )

    # Final title cleanup based on company
    details["Title"] = normalize_title(details.get("Title"), details.get("Company"))

    # Salary fallback: first try Built In specific parser, then generic text.
    # For Built In Vancouver we still use the Built In extractor to avoid phone numbers.
    if not details.get("salary_min") and not details.get("salary_max"):
        lo = hi = None

        if "builtin.com" in host or "builtinseattle.com" in host or "builtinvancouver.org" in host:
            _builtin_fill_title_company_from_builtinsignals(details, soup, html)
            # Use the Built In specific extractor
            lo, hi = extract_salary_builtin(details.get("html_raw", ""))
        else:
            # All other boards can use the generic text extractor
            lo, hi = extract_salary_from_text(details.get("page_text", ""))

        if lo or hi:
            details["salary_min"] = lo
            details["salary_max"] = hi
            details["salary_raw"] = f"{lo or ''}–{hi or ''}"
    
    # Canonicalize location related fields before returning.
    # Contract:
    # - Location is a string
    # - Location Raw is a string
    # - Location Chips is a pipe string (never a list)
    try:
        #_yc_trace(details, "YC RULES BEFORE ASSIGN")
        details["Location"] = (details.get("Location") or "").strip()
        details["LocationRaw"] = (details.get("LocationRaw") or "").strip()

        # --- Built In Vancouver: final pass to prevent broadened Location ---
        # If Location got polluted (ex: "Canada / United States") but Raw is clean ("Canada"),
        # prefer Raw at the very end so later steps cannot re-expand it.
        if "builtinvancouver.org" in host:
            loc = (details.get("Location") or "").strip()
            raw = (details.get("LocationRaw") or "").strip()

            if raw and loc and (" / " in loc):
                loc_low = loc.lower()
                raw_low = raw.lower()

                has_can = ("canada" in loc_low) or (loc_low == "can")
                has_us = ("united states" in loc_low) or ("usa" in loc_low) or (loc_low == "us")

                raw_is_can = raw_low in {"canada", "can"}
                raw_is_us = raw_low in {"united states", "usa", "us"}

                if (has_can and has_us) and (raw_is_can or raw_is_us):
                    if not (is_biv and details.get("_LOCK_LOCATION_FROM_TOOLTIP")):
                        details["Location"] = raw
                    details["Location Source"] = (details.get("Location Source") or "") + "|RAW_WINS_FINAL"

        log_line("DEBUG", f"[PRE_AS_PIPE] Location Chips before normalize={details.get('Location Chips')!r}")

        # Ensure Location Chips is always a pipe string, but do not wipe truth
        # Normalize Location Chips without wiping an existing truth signal
        lc_existing = details.get("Location Chips")
        lc_norm = _as_pipe_location_chips(lc_existing)
        if lc_norm:
            details["Location Chips"] = lc_norm or ""

        if "Applicant Regions" in details:
            details["Applicant Regions"] = _as_pipe_regions(details.get("Applicant Regions"))

        # Do NOT pipe normalize Applicant Regions Source
        if "Applicant Regions Source" in details:
            details["Applicant Regions Source"] = _as_pipe_source(details.get("Applicant Regions Source"))

        trace_chips(details, "AFTER_AS_PIPE_CHIPS")

        # Optional, but keeps related fields consistent too
        if "Country Chips" in details:
            details["Country Chips"] = _as_pipe_chips(details.get("Country Chips")) or ""
        
        log_line("DEBUG", f"[POST_AS_PIPE] Location Chips after normalize={details.get('Location Chips')!r}")

    except Exception as e:
        log_line("ERROR", f"[PIPE_NORMALIZE_ERROR] {e}")
        raise

    # Finished populating details; return the dict
    return details


def _as_pipe_regions(val) -> str:
    if not val:
        return ""

    if isinstance(val, (list, tuple, set)):
        parts = [str(x) for x in val]
    else:
        s = str(val)
        parts = s.replace(" / ", "|").split("|")

    out = set()
    for p in parts:
        t = p.strip().lower()
        if not t:
            continue

        # Normalize synonyms first
        t = REGION_SYNONYMS.get(t, t)

        # Normalize Canada ambiguity: treat "ca" as "can"
        #if t == "ca":
            #t = "can"

        # Only accept allowed tokens
        if t in ALLOWED_REGION_TOKENS:
            out.add(t)

    return "|".join(sorted(out))

def _as_pipe_location_chips(val) -> str:
    """
    Canonical Location Chips:
    - uppercase tokens
    - must be in ALLOWED_LOCATION_CHIPS
    - pipe-delimited
    """
    if not val:
        return ""

    if isinstance(val, (list, tuple, set)):
        parts = [str(x) for x in val]
    else:
        s = str(val)
        parts = s.replace(" / ", "|").replace(",", "|").split("|")

    out = []
    seen = set()
    for p in parts:
        t = p.strip().upper()
        if not t:
            continue
        if t not in ALLOWED_LOCATION_CHIPS:
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t)

    return "|".join(out)


def _as_pipe_applicant_regions(val) -> str:
    """
    Canonical Applicant Regions:
    - lowercase tokens
    - synonyms applied (REGION_SYNONYMS)
    - must be in ALLOWED_REGION_TOKENS
    - pipe-delimited, sorted
    """
    if not val:
        return ""

    if isinstance(val, (list, tuple, set)):
        parts = [str(x) for x in val]
    else:
        s = str(val)
        parts = s.replace(" / ", "|").replace(",", "|").split("|")

    out = set()
    for p in parts:
        t = p.strip().lower()
        if not t:
            continue
        t = REGION_SYNONYMS.get(t, t)
        if t in ALLOWED_REGION_TOKENS:
            out.add(t)

    return "|".join(sorted(out))

def _biv_detect_countries_from_location(loc_s: str) -> list[str]:
    """
    Detect country chips from a BuiltInVancouver multi-location string.
    Uses:
      - word hints (NON_US_HINTS) for countries spelled out
      - 3-letter country codes common in BIV lists (CAN, PHL, ESP, PRT, USA)
    Returns unique chips preserving order.
    """
    if not loc_s:
        return []

    loc_up = loc_s.upper()
    out: list[str] = []
    seen: set[str] = set()

    def add(chip: str) -> None:
        if chip and chip not in seen:
            seen.add(chip)
            out.append(chip)

    # 1) 3-letter code detection
    for m in BIV_COUNTRY_CODE_RX.finditer(loc_up):
        code = m.group(1)
        chip = _BIV_COUNTRY_3_TO_CHIP.get(code)
        if chip:
            add(chip)

    # 2) word hints for non-US countries (Portugal, Spain, etc)
    # We only add the ones you already support in your country maps.
    loc_low = loc_s.lower()
    if NON_US_HINTS.search(loc_low):
        # Only add supported ones to keep chips consistent
        for cc, word in _COUNTRY_CODE_TO_WORD.items():
            if word and word in loc_low:
                # use 2-letter cc as chip if you want, but you currently use CAN, USA, etc.
                # For EU countries, use their ISO2 as chip (ES, etc) OR map to 3-letter if you prefer.
                add(cc)

    # 3) US word hints if present
    if US_HINTS.search(loc_s):
        add("USA")

    return out

def _canonical_country_words_from_location(loc: str) -> set[str]:
    """
    Country words for internal debug or internal logic.
    Returns: {"canada"} or {"united states"} or both.
    Never returns abbreviations like 'ca'.
    """
    s = (loc or "").strip().lower()
    out: set[str] = set()

    if "canada" in s or s in {"can", "can / remote", "canada / remote"}:
        out.add("canada")

    if "united states" in s or "usa" in s or "u.s." in s or s in {"us", "us / remote"}:
        out.add("united states")

    return out

def _as_pipe_chips(val) -> str:
    if not val:
        return ""

    if isinstance(val, (list, tuple, set)):
        parts = [str(x) for x in val]
    else:
        s = str(val)
        parts = s.replace(" / ", "|").split("|")

    out = []
    seen = set()
    dropped = []

    for p in parts:
        t = p.strip().upper()
        if not t:
            continue

        t = LOCATION_CHIP_SYNONYMS.get(t, t)

        if t not in ALLOWED_LOCATION_CHIPS:
            dropped.append(t)
            continue
        if t in seen:
            continue

        seen.add(t)
        out.append(t)

    if dropped:
        try:
            from logging_utils import debug
            debug(f"[AS_PIPE_CHIPS] dropped={sorted(set(dropped))} allowed_sample={sorted(list(ALLOWED_LOCATION_CHIPS))[:20]}")
        except Exception:
            pass

    return "|".join(out)

def _as_pipe_source(val) -> str:
    """
    Pipe normalize a Source field (not regions).
    Keeps arbitrary tokens like TEXT, BIV_RULES, CHIPS_BACKFILL, LDJSON.
    """
    if not val:
        return ""
    if isinstance(val, (list, tuple, set)):
        parts = [str(x) for x in val]
    else:
        parts = str(val).replace(" / ", "|").replace(",", "|").split("|")

    out = []
    seen = set()
    for p in parts:
        t = p.strip().upper()
        if not t:
            continue
        if t not in seen:
            seen.add(t)
            out.append(t)
    return "|".join(out)

def _chips_pipe_from_location_strings(locs: list[str]) -> str:
    """
    Convert a list of location strings into a pipe-delimited chip string.

    Examples:
      ["Chicago, IL, USA", "Los Angeles, CA, USA", "Washington, DC, USA"]
        -> "USA|CA|IL|DC"

      ["US"]
        -> "USA"
    """
    chips: set[str] = set()

    def add(tok: str) -> None:
        t = (tok or "").strip().upper()
        if t:
            chips.add(t)

    # Normalize each incoming location string using geo_constants synonyms first
    normalized_parts: list[str] = []
    for raw in (locs or []):
        s = (raw or "").strip()
        if not s:
            continue

        s_up = s.upper()
        if s_up in LOCATION_CHIP_SYNONYMS:
            normalized_parts.append(LOCATION_CHIP_SYNONYMS[s_up])
        else:
            normalized_parts.append(s)

    for raw in normalized_parts:
        s = (raw or "").strip()
        if not s:
            continue

        s_up = s.upper()
        low = s.lower()

        # If the whole token is already a canonical macro chip
        if s_up in {"USA", "CAN"}:
            add(s_up)
            continue

        # Handle bare two letter tokens like "CA" or "ON"
        if re.fullmatch(r"[A-Za-z]{2}", s):
            ab = s.lower()

            if ab in US_STATE_ABBRS:
                add("USA")
                add(ab.upper())
                continue

            if s_up in CAN_PROV_ABBRS:
                add("CAN")
                add(s_up)
                continue

        # Country detection inside longer strings
        if ("usa" in low) or ("united states" in low) or re.search(r"\bus\b", s, flags=re.I):
            add("USA")
        if ("canada" in low) or re.search(r"\bcan\b", s, flags=re.I):
            add("CAN")

        # Extract state or province codes like ", IL," or ", ON,"
        for abbr in re.findall(r",\s*([A-Za-z]{2})\s*,", s):
            ab = abbr.lower()
            ab_up = abbr.upper()

            if ab in US_STATE_ABBRS:
                add("USA")
                add(ab_up)
            elif ab_up in CAN_PROV_ABBRS:
                add("CAN")
                add(ab_up)

    # Ordering: CAN then provinces, then USA then states, then anything else
    ordered: list[str] = []

    if "CAN" in chips:
        ordered.append("CAN")
        provs = sorted([c for c in chips if c in CAN_PROV_ABBRS])
        ordered.extend(provs)

    if "USA" in chips:
        ordered.append("USA")
        states = sorted([c for c in chips if (c not in {"CAN", "USA"} and c not in CAN_PROV_ABBRS)])
        # That list includes US states plus any unknown chips. If you want only US state chips here,
        # we can tighten it, but this keeps behavior permissive.
        ordered.extend(states)

    # Remove duplicates while preserving order
    seen: set[str] = set()
    out: list[str] = []
    for c in ordered:
        if c not in seen:
            out.append(c)
            seen.add(c)

    return "|".join(out)

def _looks_like_id(s: str | None) -> bool:
    return bool(s and ID_LIKE_RX.fullmatch(s.strip()))

def _title_from_url(job_url: str) -> str:
    p = up.urlparse(job_url)
    segs = [s for s in p.path.split("/") if s]
    cand = ""
    if segs:
        last = segs[-1]
        if _looks_like_id(last) and len(segs) >= 2:
            cand = segs[-2]
        else:
            cand = last
    cand = cand.replace("-", " ").replace("_", " ").strip()
    return cand

BUILTIN_JOB_INIT_RX = re.compile(r"Builtin\.jobPostInit\((\{.*?\})\);?", re.S)
BUILTIN_BRAND_RX   = re.compile(r"\s*\|\s*Built In\s*$", re.I)

def _builtin_hero_country(soup: BeautifulSoup) -> str | None:
    """
    Built In pages often show:
      <span>Hiring Remotely in </span><span>Canada</span>

    We want the second span (Canada) and we want it from the hero area,
    not from Similar Jobs.
    """
    if not soup:
        return None

    # Find the hero phrase anywhere in the main content
    node = soup.find(string=re.compile(r"Hiring\s+Remotely\s+in", re.I))
    if not node:
        return None

    # node is usually a NavigableString inside a <span>
    span = node.parent if getattr(node, "parent", None) else None
    if not span:
        return None

    # Next span after "Hiring Remotely in" is typically the country
    nxt = span.find_next("span")
    if not nxt:
        return None

    country = nxt.get_text(" ", strip=True)
    if not country:
        return None

    # Safety: avoid accidentally re-grabbing the phrase
    if re.search(r"Hiring\s+Remotely\s+in", country, re.I):
        return None

    return country


def _builtin_job_card_scope(soup, job_url: str):
    """
    Return the BeautifulSoup node for the main job card, so we don't
    accidentally parse Similar Jobs / Recommended Jobs blocks.
    """
    m = re.search(r"/(\d+)(?:[/?#]|$)", job_url or "")
    if m:
        card = soup.find(id=f"job-card-{m.group(1)}")
        if card:
            return card

    # Fallback: Built In pages generally mark the main card with data-id="job-card"
    card = soup.select_one('div[data-id="job-card"]')
    return card or soup


def _builtin_extract_location_from_card(card) -> str | None:
    """
    Extract location from the hero card only.

    Handles:
      - "Hiring Remotely in Canada"
      - multi-location popovers (best effort)
    """
    if not card:
        return None

    text = card.get_text(" ", strip=True) if hasattr(card, "get_text") else ""
    text = re.sub(r"\s+", " ", text).strip()

    if DEBUG_LOCATION:
        raw = str(card).replace("\n", " ")
        raw = re.sub(r"\s+", " ", raw).strip()
        #log_line("BIV DEBUG", f"{DOTL}..card_html_len: {len(raw)}")
        #log_line("BIV DEBUG", f"{DOTL}..card_has_location_word: {('location' in raw.lower())}")
        #log_line("BIV DEBUG", f"{DOTL}..card_has_data_bs: {('data-bs' in raw.lower())}")
        #log_line("BIV DEBUG", f"{DOTL}..card_has_svg: {('<svg' in raw.lower())}")
        if len(raw) > 600:
            raw = raw[:600] + "...(trunc)"
        #log_line("BIV DEBUG", f"{DOTL}..card_html_head: {raw}")


    # 1) Best case: exact phrase on Built In Vancouver page
    m = re.search(r"Hiring\s+Remotely\s+in\s+([A-Za-z][A-Za-z\s]+)", text, re.I)
    if m:
        return m.group(1).strip()

    # 1b) Multi-location list stored in a Bootstrap tooltip.
    # Example: <span data-bs-toggle="tooltip" title="&lt;div ...&gt;...<div class='col-lg-6'>Toronto, ON, CAN</div>...">
    try:
        candidates = card.select("span[data-bs-toggle='tooltip'][title]")
        best: list[str] = []

        for sp in candidates:
            label_raw = (sp.get_text(" ", strip=True) or "").strip()
            # Only accept the exact widget text like "3 Locations" (or "1 Location")
            if not re.search(r"^\d+\s+locations?$", label_raw, re.I):
                continue

            raw = (sp.get("title") or "").strip()
            if not raw:
                continue

            unesc = _html.unescape(_html.unescape(raw))
            if ("col-lg-6" not in unesc) and ("row" not in unesc):
                continue

            inner = BeautifulSoup(unesc, "html.parser")
            locs = [d.get_text(" ", strip=True) for d in inner.select("div.col-lg-6")]
            locs = [x for x in locs if x]

            if not locs:
                locs = [d.get_text(" ", strip=True) for d in inner.select("div.text-truncate")]
            if not locs:
                locs = [d.get_text(" ", strip=True) for d in inner.select("div")]
            locs = [x for x in locs if x]

            if len(locs) > len(best):
                best = locs

        if best:
            return " / ".join(best)
    except Exception:
        pass

    # 2) icon based fallback (kept as a last resort)
    # 2) Multi-location popover
    # Built In often uses SVG icons, not <i class="fa-location-dot">
    loc_icon = card.select_one("i.fa-location-dot")
    if loc_icon:
        container = (
            loc_icon.find_parent("div", class_=re.compile(r"\bd-flex\b.*align-items-start\b", re.I))
            or loc_icon.find_parent("div", class_=re.compile(r"\bd-flex\b.*gap-sm\b", re.I))
            or loc_icon.find_parent("div", class_=re.compile(r"\bd-flex\b", re.I))
        )
        if container:
            spans = [s.get_text(" ", strip=True) for s in container.find_all("span") if s.get_text(strip=True)]
            if spans:
                return spans[0]

    icon = (
        card.select_one("i.fa-location-dot")
        or card.select_one('svg[data-icon="location-dot"]')
        or card.select_one('svg[aria-label*="location" i]')
    )

    # Find the most likely container near the icon, but do not depend on it
    search_root = None
    if icon:
        search_root = icon.find_parent("div") or icon.parent
    if not search_root:
        search_root = card

    # Popover content can live on a sibling/ancestor, or directly on a button/span
    pop = (
        search_root.find(attrs={"data-bs-content": True})
        or search_root.find(attrs={"data-bs-title": True})
        or search_root.find(attrs={"data-bs-original-title": True})
        or card.find(attrs={"data-bs-content": True})
        or card.find(attrs={"data-bs-title": True})
        or card.find(attrs={"data-bs-original-title": True})
    )

    #if DEBUG_LOCATION:
        #has_any = bool(card.find(attrs={"data-bs-content": True}) or card.find(attrs={"data-bs-title": True}) or card.find(attrs={"data-bs-original-title": True}))
        #log_line("BIV DEBUG", f"{DOTL}..loc_icon_found        : {bool(icon)}")
        #log_line("BIV DEBUG", f"{DOTL}..has_any_popover_attrs : {has_any}")
        #log_line("BIV DEBUG", f"{DOTL}..popover_node_found    : {bool(pop)}")

    if not pop:
        return None

    # Combine possible attribute fields
    blob = " ".join(
        [
            pop.get("data-bs-content", "") or "",
            pop.get("data-bs-title", "") or "",
            pop.get("data-bs-original-title", "") or "",
        ]
    ).strip()

    if not blob:
        return None

    # Attributes often contain escaped HTML like &lt;br&gt;
    try:
        import html as _html
        blob = _html.unescape(blob)
    except Exception:
        pass

    # Convert HTML-ish separators into newlines, then strip tags
    blob = re.sub(r"(?i)<br\s*/?>", "\n", blob)
    blob = re.sub(r"(?i)</(div|li|p|span|tr|td)>", "\n", blob)
    blob = re.sub(r"<[^>]+>", " ", blob)

    # Preserve newlines, normalize whitespace within lines
    blob = blob.replace("\r", "\n")
    blob = re.sub(r"[ \t]+", " ", blob)
    blob = re.sub(r"\n{2,}", "\n", blob).strip()

    # Split into candidates
    candidates = re.split(r"\n|\s*\|\s*|•|;", blob)
    candidates = [c.strip(" ,") for c in candidates if c.strip()]

    locs = []
    for c in candidates:
        if not re.search(r"\bLocations?\b", c, re.I) and "job locations" not in c.lower():
                continue
        if re.search(r"\b(remote|hybrid)\b", c, re.I):
            continue

        # Strong pattern: "City, Region, CountryCode"
        if c.count(",") >= 2 and re.search(r"\b(CAN|CA|USA|US|UK)\b", c, re.I):
            locs.append(c)
            continue

        # Useful: "City, Province" or similar, but avoid junk
        if c.count(",") >= 1 and len(c) >= 5:
            locs.append(c)

    # De-dupe, preserve order
    deduped = []
    seen = set()
    for l in locs:
        key = l.lower()
        if key not in seen:
            seen.add(key)
            deduped.append(l)

    if deduped:
        return " | ".join(deduped)

    # Country collapse fallback
    if re.search(r"\bcanada\b|\bCAN\b", blob, re.I):
        return "Canada"
    if re.search(r"\bunited states\b|\bUSA\b|\bUS\b", blob, re.I):
        return "US"

    return blob or None




def _builtin_extract_remote_rule_from_card(card) -> str | None:
    icon = card.select_one("i.fa-house-building")
    if not icon:
        return None
    container = icon.find_parent("div")
    if not container:
        return None

    label = container.get_text(" ", strip=True)
    mode = classify_work_mode(label)

    return None if mode == "Unknown" else mode



def _extract_builtin_job_meta(soup: BeautifulSoup) -> dict[str, str]:
    """
    Parse Built In's inline JS payload so we can recover title/company even when
    the visible DOM is missing easy selectors.
    """
    if not soup:
        return {}
    node = soup.find("script", string=lambda s: s and "Builtin.jobPostInit" in s)
    if not node:
        return {}
    raw = (node.string or node.get_text()) or ""
    m = BUILTIN_JOB_INIT_RX.search(raw)
    if not m:
        return {}
    try:
        data = json.loads(m.group(1))
    except Exception:
        return {}
    job = data.get("job") or {}
    return {
        "title": (job.get("title") or "").strip(),
        "company": (job.get("companyName") or job.get("company_name") or "").strip(),
        "location": (job.get("city_state") or job.get("location") or "").strip(),
        "remote": str(job.get("remote")) if job.get("remote") is not None else "",
        "salary": (job.get("compensation_max") or "").strip(),
        "salary_text": (job.get("compensation_display") or "").strip(),
    }

def _strip_builtin_brand(value: str | None) -> str:
    """Remove trailing '| Built In' noise from titles or company names."""
    if not value:
        return ""
    return BUILTIN_BRAND_RX.sub("", value).strip()

def _best_title(soup: BeautifulSoup, job_url: str, current: str | None) -> str:
    """Choose the best human title from several sources; avoid numeric IDs."""
    t = (current or "").strip()

    # If title looks like ATS boilerplate, fall back to URL slug
    bad_ats_titles = ("greenhouse", "my greenhouse", "job board", "lever", "ashby", "workday")
    if t and any(b in t.lower() for b in bad_ats_titles):
        t = ""

    # 1) og:title
    if not t:
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            t = og["content"].strip()

    # 2) common H1s
    if not t:
        h1 = soup.find("h1") or soup.select_one("[data-testid='title'], .app-title, .opening h2, .posting-headline h2")
        if h1:
            cand = h1.get_text(" ", strip=True)
            # avoid numeric-only H1s like "7416947"
            looks_numeric = _looks_like_id(cand.lower().replace(" ", ""))
            if not looks_numeric:
                t = cand

    # 3) <title>
    if not t:
        page_t = soup.find("title")
        if page_t:
            t = page_t.get_text(" ", strip=True)

    # 4) URL segment fallback
    if not t or _looks_like_id(t.lower().replace(" ", "")):
        t = _title_from_url(job_url)

    # Final cleanup
    return normalize_title(t)


def _refine_remotive_snippet(host: str, soup, fallback: str, title_text: str = "") -> str:
    """
    For remotive.com: prefer the first strong “lead-in” sentence from the job body,
    e.g., “In this role…” or “This is a…”. Fall back to the caller-provided snippet.
    """
    if "remotive.com" not in host:
        return fallback

    # Try to grab article-ish content; then normalize to one big string.
    # We favor <article>, then any content container remotive uses.
    article = soup.select_one("article") or soup.select_one(".content") or soup.select_one(".job-description")
    if article:
        blocks = [el.get_text(" ", strip=True) for el in article.select("p, li")]
        text = " ".join(blocks)
    else:
        text = soup.get_text(" ", strip=True)

    if not text:
        return fallback

    # Strip common labels / headers that appear before the real body.
    text = re.sub(r"\b(Title|Location|About(?:\s+this\s+role)?|Summary of Role)\b\s*[:\-–]*\s*", "", text, flags=re.I)

    # If the title itself appears at the start, drop it.
    if title_text:
        text = re.sub(re.escape(title_text) + r"\s*[:\-–]*\s*", "", text, flags=re.I)

    # Find a strong, job-body lead sentence.
    m = re.search(
        r"(In this role[^.?!]*[.?!])|(This is a [^.?!]*[.?!])|(You will [^.?!]*[.?!])",
        text,
        flags=re.I,
    )
    if m:
        # return the first non-None capture group
        return next(g for g in m.groups() if g)

    return fallback


def _is_target_role(d: dict) -> bool:
    """
    True only when:
      A) Title or lead text is an explicit match we want
         OR
      B) Title/lead is a fuzzy neighbor AND responsibilities look like PO/PM/BA/BSA/Scrum work
    Titles on the exclude list always fail, even if responsibilities match.
    """
    title = normalize_title(d.get("Title") or "").lower()
    snippet = (d.get("Description Snippet") or "").lower()
    full = f"{title} {snippet}"

    # pass on clear titles first
    if any(re.search(p, title, re.I) for p in INCLUDE_TITLES_EXACT):
        return True

    # NEW: if the title is generic but the snippet clearly says it's one of our target roles
    if any(re.search(p, snippet, re.I) for p in INCLUDE_TITLES_EXACT):
        return True

    # hard blocks next
    if any(re.search(p, title, re.I) for p in EXCLUDE_TITLES):
        return False

    # fuzzy titles need responsibility corroboration
    fuzzy_hit = any(re.search(re.escape(p), title, re.I) for p in INCLUDE_TITLES_FUZZY)
    if fuzzy_hit:
        resp_hits = len(set(m.group(0) for m in RESP_SIG_RX.finditer(full)))
        return resp_hits >= 2

    return False

def _is_remote(d: dict) -> bool:
    rr  = str(d.get("Remote Rule") or "")
    loc = str(d.get("Location") or "")
    chips_val = d.get("Location Chips")
    chips = " ".join(map(str, chips_val)) if isinstance(chips_val, (list, tuple)) else str(chips_val or "")

    text = " ".join([rr, loc, chips]).strip()
    mode = classify_work_mode(text).lower()
    return mode in ("remote", "hybrid")

def _matches_locality(region_key: str, loc: str, chips: list[str], url: str = "") -> bool:
    cfg = LOCALITY_HINTS.get(region_key, {})

    any_terms = [t.lower().strip() for t in cfg.get("any", []) if str(t).strip()]
    tokens_cfg = [t.lower().strip() for t in cfg.get("tokens", []) if str(t).strip()]

    low_loc = (loc or "").lower()
    low_url = (url or "").lower()

    # Chips are structured, so treat them as exact tokens
    chips_tokens = {c.strip().upper() for c in (chips or []) if str(c).strip()}

    # Tokenize location and url into words (prevents "on" matching "washington")
    # We keep it simple and deterministic.
    norm_loc = low_loc.replace("d.c.", "dc").replace("d. c.", "dc")
    norm_url = low_url.replace("d.c.", "dc").replace("d. c.", "dc")
    loc_words = set(re.findall(r"[a-z]{2,}", norm_loc))
    url_words = set(re.findall(r"[a-z]{2,}", norm_url))
    word_set = loc_words | url_words

    # Strong match first: phrase style hints (substring is OK here)
    chips_text = "|".join(sorted({c.lower() for c in chips_tokens}))
    haystacks = [low_loc, chips_text, low_url]
    for term in any_terms:
        if term and any(term in h for h in haystacks):
            return True

    # Weak match: token style hints must be safe
    for tok in tokens_cfg:
        if not tok:
            continue

        # 2-letter province/state style tokens must match as whole token
        # - ON should only match "on" as a token or as a chip, not inside "washington"
        if len(tok) == 2:
            if tok.upper() in chips_tokens:
                return True
            if tok in word_set:
                return True
            continue

        # Longer tokens like "ontario" or "british columbia" can be checked safely
        # Prefer whole-word membership when possible, otherwise phrase match.
        if tok in word_set:
            return True

        # Allow multi word tokens via substring, but only for 3+ chars to reduce noise
        if len(tok) >= 3 and (tok in low_loc or tok in low_url):
            return True

    return False

def _is_us_canada_eligible(d: dict) -> bool:
    """
    Liberal when unknown, stricter when explicit non-NA regions appear.
    Accept any strong US/Canada mentions. Reject on strong non-NA signals.
    """
    # Country chips (rarely set, but keep the logic)
    country_chips = d.get("Country Chips")
    countries = set()
    if isinstance(country_chips, (list, tuple)):
        countries.update(str(x).lower() for x in country_chips if x)
    else:
        for t in str(country_chips or "").lower().split(","):
            t = t.strip()
            if t:
                countries.add(t)

    # Location chips
    loc_chips = d.get("Location Chips")
    if isinstance(loc_chips, (list, tuple)):
        loc_chips_txt = " ".join(map(str, loc_chips)).lower()
    else:
        loc_chips_txt = str(loc_chips or "").lower()

    # Pull out all the text we care about
    location_txt = str(d.get("Location") or "").lower()
    snippet_txt  = str(d.get("Description Snippet") or "").lower()
    page_txt     = str(d.get("page_text") or "").lower()

    txt = " ".join([location_txt, snippet_txt, loc_chips_txt, page_txt])

    # If we see any explicit US/USA/United States mention anywhere in the combined text,
    # treat as eligible even if other regions are also present.
    if re.search(r"\b(us|usa|u\.s\.|united states)\b", txt):
        return True

    # 1) Strong positives – clearly US / Canada
    CANON_COUNTRIES = {"canada", "united states"}

    if {c.strip().lower() for c in countries} & CANON_COUNTRIES:
        return True

    # Treat Remote/Nationwide US variants as hard positives
    if re.search(r"remote\s*/?\s*nationwide.*\b(us|usa|united states)\b", txt):
        return True

    # General US / Canada positives
    if any(p in txt for p in [
        "us only", "usa only",
        "anywhere in the us",
        "anywhere in the united states",
        "eligible to work in the us",
        "remote - us", "remote in the us", "remote, us", "remote within the us",
        "nationwide, us",
        "nationwide in the us",
        "canada",
        "north america", "us or canada",
    ]):
        return True

    # 2) Strong non-NA negatives – *only* when they appear in a region/location context
    # (REGION_CONTEXT and NON_US_HINTS are already defined above in your file)
    if REGION_CONTEXT.search(txt) and NON_US_HINTS.search(txt):
        return False

    # 3) Default allow if we can't tell
    return True

def build_rule_reason(d: dict) -> str:
    reasons = []
    if not _is_target_role(d):
        reasons.append("Not target role")
    if not (_is_remote(d) or _is_commutable_local(d)):
        reasons.append("Not remote or local-commutable")
    if not _is_us_canada_eligible(d):
        reasons.append("Not US/Canada-eligible")
    wa_rule = (d.get("WA Rule") or "").strip().lower()
    if wa_rule and wa_rule not in ("pass", "default"):
        reasons.append("Not WA-eligible")

    # salary gating: treat 'below_floor' as a hard skip
    status = str(d.get("Salary Status") or "").lower()
    if status == "below_floor":
        reasons.append("Salary below floor")

    # If no rules failed, return "" and let technical_fallback speak
    return ", ".join(reasons)

AUTO_SKIP_IF_APPLIED = False  # change to True if you want auto-skip on applied
def choose_skip_reason(d: dict, technical_fallback: str) -> str:
    if AUTO_SKIP_IF_APPLIED:
        applied = (d.get("Applied?") or "").strip()
        if applied:
            return f"Already applied on {applied}"

    # Main rule coming from your ruleset
    rule = build_rule_reason(d)

    # Extra safety: if no rule, check US / Canada eligibility
    # so obvious non-eligible jobs (e.g. "Remote jobs in Australia")
    # do not just say "Filtered by rules".
    try:
        if not rule and not _is_us_canada_eligible(d):
            rule = "Not US/Canada-eligible"
    except Exception:
        # Any unexpected issue with eligibility should not break the run.
        # Fall back to the technical / generic reason instead.
        pass

    return rule or technical_fallback


# --- URL helpers used throughout ---
#from urllib.parse import urlparse, urljoin, parse_qs, unquote

_NO_LONGER_AVAILABLE_PATTERNS = [
    "no longer available",
    "this job is no longer available",
    "position has been filled",
    "we know this isn't what you were hoping for",
    "start a new search to view all remote jobs",
    # New, covers Remotive and similar wordings
    "this job listing is archived",
    "job listing is archived",
    "job is archived",
    "job archived",
    "this job has expired",
    "job has expired",
    "applications are closed",
    "application closed",
]

def _detect_dead_post(domain: str, soup, raw_text: str) -> str:
    """
    Return a human-readable reason if the page clearly indicates the job is gone.
    Otherwise return "".
    """
    lt = (raw_text or "").lower()

    # NoDesk: red alert banner
    if "nodesk.co" in domain:
        banner = soup.select_one('[role="alert"], .bg-red-100, .text-red-700')
        if banner:
            t = banner.get_text(" ", strip=True).lower()
            if any(p in t for p in _NO_LONGER_AVAILABLE_PATTERNS):
                return "Posting removed on NoDesk"

    # Remotive: page shows "This job listing is archived"
    if "remotive.com" in domain:
        # Prefer a quick full-page text check since Remotive wording is consistent
        if "this job listing is archived" in lt:
            return "Posting archived on Remotive"
        # Light DOM heuristic just in case the copy moves into a badge/button
        badge = soup.select_one(".alert, .notice, .badge, button, .job-status")
        if badge:
            t = badge.get_text(" ", strip=True).lower()
            if any(p in t for p in _NO_LONGER_AVAILABLE_PATTERNS):
                return "Posting archived on Remotive"

    # Generic fallback: works for other boards that use standard copy
    if any(p in lt for p in _NO_LONGER_AVAILABLE_PATTERNS):
        site = domain.replace("www.", "")
        return f"Posting closed or removed on {site}"

# constants you already have; keep your own values if different
SALARY_TARGET_MIN  = 120_000
SALARY_NEAR_DELTA  = 15_000
TRIGGER_PHRASES = (
    "competitive base", "competitive salary", "competitive compensation",
    "very competitive", "highly competitive", "market competitive",
    "salary commensurate", "commensurate with experience",
    "market rate", "salary doe", "doe", "compensation package",
)


SALARY_GOOD_WORDS = [
    "salary", "compensation", "pay", "pay range", "payrate",
    "per year", "annual", "base", "base pay", "hour", "hourly",
    "ote", "on-target earnings", "earnings", "hiring range"
]

SALARY_BAD_WORDS = [
    "technologists", "employees", "employee",
    "people", "customers", "consumers", "clients", "users",
    "transactions", "accounts",
    "revenue", "sales", "turnover", "profit",
    "assets under management", "assets under mgmt", "aum",
    "market cap", "valuation",
    "budget", "spend", "tech spend", "investment",
    "trillion", "billion", "million",
]

_PLAIN_BIG_NUMBER_RE = re.compile(r"\b\d{2,3}(?:,\d{3})\b")


def _find_plain_salary_candidates(page_text: str) -> list[int]:
    """Find 5- or 6-digit numbers that actually look like salaries."""
    nums: list[int] = []

    for m in _PLAIN_BIG_NUMBER_RE.finditer(page_text):
        start, end = m.span()
        if not _is_salary_context(page_text, start, end):
            # Example: "50,000 technologists globally" will be skipped
            continue

        raw = m.group(0)
        n = _to_int(raw)  # you already have _to_int right below
        if n is None:
            continue

        nums.append(n)

    return nums



# catches “competitive … base … salary” even with words in between
SALARY_SIGNAL_RX = re.compile(r"\b(very|highly)?\s*competitive\b.*\bbase\b.*\bsalary\b", re.I | re.S)

_SALARY_RANGE_RE = re.compile(
    r"""\$?\s*
        (?P<a>\d{2,3}(?:,\d{3})?)(?P<ak>[kK])?
        \s*[-–]\s*
        \$?\s*(?P<b>\d{2,3}(?:,\d{3})?)(?P<bk>[kK])?
        (?!\s*(users?|user|alumni|students?|views?|employees?))
    """,
    re.VERBOSE,
)

_SALARY_ONE_RE = re.compile(
    r"""\$?\s*(?P<a>\d{2,3}(?:,\d{3})?)(?P<ak>[kK])?
        \s*(?:per\s*year|/year|yr|annual|annually|salary)
        (?!\s*(users?|user|alumni|students?|views?|employees?))
    """,
    re.VERBOSE | re.IGNORECASE,
)


def _is_salary_context(snippet: str) -> bool:
    """Return True if this text snippet looks like salary context."""
    text = snippet.lower()

    # 1) First, block obvious non-salary uses
    if any(bad in text for bad in SALARY_BAD_WORDS):
        return False

    # 2) Then, treat as salary only if we see strong positive signals
    if any(good in text for good in SALARY_GOOD_WORDS) or "$" in snippet:
        return True

    # 3) Otherwise, default to "not salary"
    return False


def _salary_number_has_good_context(page_text: str, n: int) -> bool:
    """
    Given the full page_text and a chosen salary number n,
    return True only if that number appears in valid salary context.

    Be generous: block only when the context clearly looks non-salary.
    """
    if not page_text:
        # If we cannot inspect context, do not throw away the salary.
        return True

    text = page_text

    formatted = f"{n:,}"

    # 1) Look for the nicely formatted version, e.g. "120,000"
    for m in re.finditer(re.escape(formatted), text):
        start, end = m.span()
        ctx_start = max(0, start - 60)
        ctx_end = min(len(text), end + 60)
        snippet = text[ctx_start:ctx_end]
        if _is_salary_context(snippet):
            return True

    # 2) Also look for the raw digits (e.g. "120000") in case commas were stripped
    raw = str(n)
    for m in re.finditer(re.escape(raw), text):
        start, end = m.span()
        ctx_start = max(0, start - 60)
        ctx_end = min(len(text), end + 60)
        snippet = text[ctx_start:ctx_end]
        if _is_salary_context(snippet):
            return True

    return False

def _to_int(n, kflag: str | None = None) -> int | None:
    """Convert a number (optionally with a 'k' flag) into an int."""
    if n in (None, ""):
        return None

    s = str(n).replace(",", "").strip()
    # Drop trailing decimals like ".00"
    if "." in s:
        s = s.split(".", 1)[0]

    if not s:
        return None

    try:
        x = int(s)
    except ValueError:
        return None

    if kflag and kflag.lower() == "k":
        x *= 1000

    return x

#  Original
def _to_int_token(t: str) -> int | None:
    t = (t or "").lower() \
        .replace("\u00a0", " ") \
        .replace(" ", "") \
        .replace(",", "")
    if not t:
        return None

    # accept 123, 123k, 1m
    kflag = None
    if t.endswith("k"):
        kflag = "k"
        t = t[:-1]
    elif t.endswith("m"):
        kflag = "m"
        t = t[:-1]

    if not t.isdigit():
        return None

    val = int(t)
    if kflag == "k":
        val *= 1000
    elif kflag == "m":
        val *= 1_000_000
    return val

def extract_salary_builtin(html: str) -> tuple[int | None, int | None]:
    """
    Improved BuiltIn salary extractor.
    Avoids false positives like company employee count (e.g., '289,097 employees').
    Extracts only realistic salary values.
    """

    import re

    # 1. Narrow search space: salary usually appears near "$" or "Annually"
    #    Extract only text that looks like a salary section
    salary_section_re = re.compile(
        r"(?:\$\s*\d[\d,]*\s*[kK]?)|(?:\d+\s*[kK]\s*(?:–|-|to)\s*\d+\s*[kK])",
        re.IGNORECASE
    )

    salary_candidates = salary_section_re.findall(html)
    if not salary_candidates:
        return None, None

    # 2. Hard guard against massive numbers (company size, downloads, etc.)
    def looks_like_salary(val: int) -> bool:
        return 30_000 <= val <= 400_000

    # 3. Normalize helpers
    def parse_number(txt: str) -> int | None:
        txt = txt.replace(",", "").lower().strip()
        if txt.endswith("k"):
            base = txt[:-1]
            if base.isdigit():
                return int(base) * 1000
        if txt.isdigit():
            return int(txt)
        return None

    # 4. Regex for ranges: “170k – 240k” or “150K to 200K”
    range_re = re.compile(
        r"(\d[\d,]*\s*[kK]?)\s*(?:–|-|to)\s*(\d[\d,]*\s*[kK]?)",
        re.IGNORECASE
    )

    # 5. Regex for single salary “170k” or “180,000”
    single_re = re.compile(r"(\d[\d,]*\s*[kK]?)", re.IGNORECASE)

    # Try ranges first
    for cand in salary_candidates:
        m = range_re.search(cand)
        if m:
            lo = parse_number(m.group(1))
            hi = parse_number(m.group(2))
            if lo and hi and looks_like_salary(lo) and looks_like_salary(hi):
                return lo, hi

    # Fallback: single values
    for cand in salary_candidates:
        m = single_re.search(cand)
        if m:
            val = parse_number(m.group(1))
            if val and looks_like_salary(val):
                return val, val

    return None, None

def extract_salary_from_text(txt: str) -> tuple[int | None, int | None]:
    # Make sure we always have a string to search
    text = txt or ""

    # Case 1: a range like "120k–150k" or "90,000 - 110,000"
    m = _SALARY_RANGE_RE.search(text)
    if m:
        lo = _to_int(m.group("a"), m.group("ak"))
        hi = _to_int(m.group("b"), m.group("bk"))
        return lo, hi

    # Case 2: a single value like "120k" or "95,000"
    m = _SALARY_ONE_RE.search(text)
    if m:
        val = _to_int(m.group("a"), m.group("ak"))
        return val, val

    # No salary detected
    return None, None



def _contains_any(text: str, phrases: tuple[str, ...]) -> bool:
    t = (text or "").lower()
    return any(p in t for p in phrases)


from bs4 import BeautifulSoup  # you already import bs4 earlier; safe to use here


# ── Terminal-only display helpers ─────────────────────────────────────────────

def _safe_disp(val: str | None, fallback: str) -> str:
    val = (val or "").strip()
    return val if val else fallback



    # OLD: --- Salary line formatting for terminal output ---
    """
    Terminal-only line:
      [💲 SALARY $90,000 ]...Near min
      [💲 SALARY $90,000 ]...At or above min
      [💲 SALARY $xxx,xxx ]...Estimated
      [💲 SALARY ]...Missing or Unknown
    """
def _fmt_salary_line(row: dict) -> str:
    """
    Turn the salary-related columns on a keep_row into a short human string.

    It is intentionally forgiving: it will show *something* if we have any of:
    - an estimated range
    - a human salary text (e.g. "Competitive salary")
    - a detected max value
    - a status / note
    - a placeholder (e.g. "Competitive salary")
    """
    # Raw values from the row
    status      = (row.get("Salary Status") or "").strip()
    placeholder = (row.get("Salary Placeholder") or "").strip()
    est_str     = (row.get("Salary Est. (Low-High)") or "").strip()
    salary_txt  = (row.get("Salary") or "").strip()     # <-- NEW

    max_raw  = row.get("Salary Max Detected") or ""
    near_min = bool(row.get("Salary Near Min"))

    parts: list[str] = []

    # 1) Preferred: human-friendly estimated range, if present
    if est_str:
        parts.append(est_str)

    # 2) Next best: any explicit Salary text ("Competitive salary", etc.)
    if not parts and salary_txt:
        parts.append(salary_txt)

    # 3) Fallback: just show the max we saw
    if not parts and max_raw:
        try:
            max_int = int(str(max_raw).replace(",", "").strip())
            parts.append(f"max ≈ ${max_int:,}")
        except ValueError:
            # Couldn't parse as int – just show whatever text we have
            parts.append(str(max_raw))

    # 4) If we’re near your configured floor, add a warning tag
    if near_min:
        parts.append("⚠ near floor")

    # 5) Status / note can give extra context (e.g. “estimated”, “signal_only”)
    if status:
        parts.append(f"({status})")

    # 6) Placeholder is things like “Competitive salary”, when no numbers exist
    if placeholder:
        parts.append(placeholder)

    if status == "signal_only":
        parts.append("salary mentioned")

    # If we assembled anything at all, join with separators
    if parts:
        return " · ".join(parts)

    # Absolute fallback – nothing detected anywhere
    return "Missing or Unknown"


def _company_and_board_for_terminal(row: dict) -> tuple[str, str]:
    company = (row.get("Company") or "").strip()
    board   = (row.get("Career Board") or row.get("career_board") or "").strip()
    if not company:
        company = "No Company Found"
    if not board:
        board = "No Board Found"
    return company, board

import re
#from urllib.parse import urlparse

def _derive_company_and_title(d: dict, html: str | None = None) -> dict:
    """
    Return a copy of d with a cleaned Title and a filled Company when the board
    embeds it in Title or URL. Handles patterns like 'IT Business Analyst at Hitachi Energy'
    or 'Hitachi Energy – IT Business Analyst', and Muse-style URLs /jobs/<company>/...
    """
    out = dict(d)
    t = (out.get("Title") or "").strip()
    company = (out.get("Company") or "").strip()
    url = (out.get("Job URL") or out.get("job_url") or "").strip()

    # Title patterns
    m = re.match(r"^(?P<title>.+?)\s+at\s+(?P<company>.+)$", t, flags=re.I)

    if not m:
        # Only split on hyphen if the left side is NOT a role and the right side IS a role
        m2 = re.match(r"^(?P<left>.+?)\s*[-–|]\s*(?P<right>.+)$", t)
        if m2:
            left  = m2.group("left").strip()
            right = m2.group("right").strip()
            ROLE_RX = re.compile(
                r"\b("
                r"product|program|project|business|data|software|system|systems|platform|growth|marketing|ops|operations"
                r")\b.*\b("
                r"analyst|owner|manager|specialist|engineer|designer|lead|director|administrator|architect"
                r")\b",
                re.I,
            )
            # only treat left as company when left!=role and right==role
            if not company and not ROLE_RX.search(left) and ROLE_RX.search(right):
                company = left
                t = right
            # else: leave t as-is

    if m:
        # Prefer explicit company from title if we do not already have one
        if not company:
            company = m.group("company").strip()
        t = m.group("title").strip()

            # Prefer the on-page company name (e.g., The Muse header / JSON-LD)
        if not company and html:
            try:
                soup = BeautifulSoup(html, "html.parser")

                # The Muse — header company name (multiple class variants seen)
                a = (
                    soup.select_one("a.job-header__jobHeaderCompanyNameProgrammatic")
                    or soup.select_one("a.job-header_jobHeaderCompanyNameProgrammatic")
                    or soup.select_one("header a[href*='/profiles/']")
                )
                if a:
                    name = (a.get_text(" ", strip=True) or "").strip()
                    if name:
                        company = name

                # Fallback: JSON-LD hiringOrganization.name
                if not company:
                    for s in soup.select('script[type="application/ld+json"]'):
                        try:
                            data = json.loads(s.string or "")  # may raise
                            # normalize to list to iterate
                            candidates = data if isinstance(data, list) else [data]
                            for node in candidates:
                                org = (
                                    node.get("hiringOrganization")
                                    or node.get("organization")
                                    or {}
                                )
                                name = (org.get("name") or "").strip()
                                if name:
                                    company = name
                                    break
                            if company:
                                break
                        except Exception:
                            pass
            except Exception:
                pass

        # …after title-pattern checks…
        if not company and html:
            try:
                soup = BeautifulSoup(html, "html.parser")
                # The Muse – company link in the job header
                a = soup.select_one("a.job-header__jobHeaderCompanyNameProgrammatic")
                if a:
                    name = (a.get_text(" ", strip=True) or "").strip()
                    if name:
                        company = name
            except Exception:
                pass


    out["Title"] = t
    if company:
        out["Company"] = company
    return out




PLAYWRIGHT_DOMAINS = {
    "edtech.com", "www.edtech.com",
    "edtechjobs.io/", "www.edtechjobs.io",
    "builtin.com", "www.builtin.com",
    "builtinseattle.com", "www.builtinseattle.com",
    "remoteok.com", "www.remoteok.com",
    "builtinvancouver.org", "www.builtinvancouver.org",
    "wellfound.com", "www.wellfound.com",
    "welcometothejungle.com", "www.welcometothejungle.com",
    "app.welcometothejungle.com", "us.welcometothejungle.com",
    "workingnomads.com", "www.workingnomads.com",
    # JS heavy boards that need Playwright
    "dice.com", "www.dice.com",
    "myworkdayjobs.com", "wd1.myworkdayjobs.com", "myworkdaysite.com",
    "wd5.myworkdaysite.com", "ashbyhq.com", "jobs.ashbyhq.com",
}


PLAYWRIGHT_DOMAINS.update({
    "www.hubspot.com",
    "hubspot.com",
    "www.ycombinator.com",
    "ycombinator.com",
    "about.gitlab.com",
    "zapier.com",
})

try:
    from playwright.sync_api import sync_playwright
except Exception:
    sync_playwright = None  # run without Playwright if not available

KNOWN = {
    # …keep existing…
    "remoteok.com": "Remote OK",
    "builtin.com": "Built In",
    "www.builtin.com": "Built In",
    "builtinseattle.com": "Built In Seattle",
    "www.builtinseattle.com": "Built In Seattle",
    "wellfound.com": "Wellfound",
    "builtinvancouver.org": "Built In Vancouver",
    "www.builtinvancouver.org": "Built In Vancouver",
    "welcometothejungle.com": "Welcome to the Jungle",
}


import os

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")  # local time
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT_CSV = os.path.join(OUTPUT_DIR, f"product_owner_jobs_{RUN_TS}.csv")
SKIPPED_CSV = os.path.join(OUTPUT_DIR, f"skipped_jobs_{RUN_TS}.csv")


# ---- Config ----
USER_AGENT = "AngeJobScraper/1.0 (+https://linkedin.com/in/angespring)"
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}

REQUEST_TIMEOUT = 20          # seconds
MIN_DELAY = 2.0               # seconds
MAX_DELAY = 5.0               # seconds
ROBOTS_TIMEOUT = 6            # max seconds to read robots.txt
PW_GOTO_TIMEOUT = 20000       # Playwright page.goto in ms
PW_WAIT_TIMEOUT = 7000        # Playwright wait_for_selector in ms
MAX_SECONDS_PER_SITE = 60     # hard cap per listing page

# === Google Sheets push ===
GS_KEY_PATH = "/Users/ange/job-scraper/service_account.json"   # absolute path to your JSON key
GS_SHEET_URL = "https://docs.google.com/spreadsheets/d/1UloVHEsBxvMJ3WeQ8XkHvtIrL1cQ2CiyD50bsOb-Up8/edit?gid=1531552984#gid=1531552984"              # full URL to “product jobs scraper”
GS_TAB_NAME = "Table1"  # or "Sheet1" — match the tab name shown in the bottom-left of the sheet
# GitHub push control (toggle: "off" | "ask" | "auto")
#PUSH_MODE = "ask"   # Set to "auto" for automatic pushes, or "off" to disable

SKIPPED_KEYS = [
    "Date Scraped",
    "Title",
    "Job ID (Vendor)",
    "Job ID (Numeric)",
    "Job Key",
    "Company",
    "Career Board",
    "Location",
    "Posted",
    "Posting Date",
    "Valid Through",
    "Job URL",
    "Reason Skipped",
    "WA Rule",
    "BC Rule",
    "ON Rule",
    "Remote Rule",
    "US Rule",
    "Canada Rule",
    "Salary Max Detected",
    "Salary Rule",
    "Location Chips",
    "Applicant Regions",
]

def is_blocked_url(url: str) -> bool:
    return any(url.startswith(prefix) for prefix in BLOCKED_URL_PREFIXES)


# URLs that should never be crawled or processed
BLOCKED_URL_PREFIXES = [
    "https://www.washington.edu/jobs",
    "https://washington.edu/jobs",  # just in case
    "https://nodesk.co/remote-jobs/us",
    "https://nodesk.co/remote-jobs/other",
    "https://nodesk.co/remote-jobs/asia",
    "https://nodesk.co/remote-jobs/operations",
    "https://nodesk.co/remote-jobs/full-time",
    "https://nodesk.co/remote-jobs/uk",
    "https://nodesk.co/remote-jobs/product-marketing",
    "https://nodesk.co/remote-jobs/sql",
    "https://nodesk.co/remote-jobs/customer-support",
    "https://nodesk.co/remote-jobs/new",
    "https://nodesk.co/remote-jobs/europe",
    "https://nodesk.co/remote-jobs/part-time",
    "https://nodesk.co/remote-jobs/product-manager",
    "https://nodesk.co/remote-jobs/data/",
    "https://nodesk.co/remote-jobs/ai/",
    "https://weworkremotely.com/remote-jobs/new",
    "https://weworkremotely.com/remote-jobs/new?utm_content=post-job-cta&utm_source=wwr-accounts-nav-mobile",
    "https://dhigroupinc.com/careers/default.aspx",
    "https://main.hercjobs.org/jobs/saved",
    "https://main.hercjobs.org/jobs/dualsearch",
    "https://edtechjobs.io/jobs/product-management",
    "https://edtechjobs.io/jobs/contract",
    "https://edtechjobs.io/jobs/higher-ed",
    "https://edtechjobs.io/jobs/leadership",
    "https://edtechjobs.io/jobs/technical-leadership",
    "https://edtechjobs.io/jobs/remote-position"
    "https://edtechjobs.io/jobs/ai-driven-products",
    "https://edtechjobs.io/jobs/educational-innovation"
    "https://edtechjobs.io/jobs/technology-leadership",
    "https://edtechjobs.io/jobs/digital-transformation",
    "https://edtechjobs.io/jobs/artificial-intelligence",
    "https://edtechjobs.io/jobs/design-leadership",
    "https://edtechjobs.io/jobs/early-childhood-education",
    "https://edtechjobs.io/jobs/saas-leadership",
    "https://edtechjobs.io/jobs/stakeholder-management",

]

STARTING_PAGES = [

    # Preferred SMOKE target: The Muse (keeps lightweight, server-rendered HTML)
    #"https://www.themuse.com/jobs?categories=product&location=remote",



    "https://main.hercjobs.org/jobs?keywords=Business+Analyst&place=canada%2Cnationwide",

    # Product Manager / Product Owner
    "https://www.themuse.com/search/location/remote-flexible/keyword/product+manager",
    "https://www.themuse.com/jobs?categories=information-technology&location=remote&query=product%20manager",
    "https://www.builtin.com/jobs?search=product%20manager&remote=true",
    "https://www.builtin.com/jobs?search=associate%20product%20manager&remote=true",
    "https://www.builtin.com/jobs?search=product%20owner&remote=true",
    "https://www.builtinseattle.com/jobs?search=product%20manager&remote=true",
    "https://www.builtinseattle.com/jobs?search=associate%20product%20manager&remote=true",
    "https://www.builtinseattle.com/jobs?search=product%20owner&remote=true",
    "https://builtinvancouver.org/jobs?search=Product+Manager",
    "https://builtinvancouver.org/jobs?search=associate%20product%20manager&remote=true",
    "https://builtinvancouver.org/jobs?search=Product+Owner",
    "https://www.simplyhired.com/search?q=product+manager&l=remote",
    "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=product+manager",
    "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=product+owner",
    "https://builtinvancouver.org/jobs/product-management/product-manager",
    "https://builtinvancouver.org/jobs/product-management/product-owner",
    "https://jobright.ai/jobs/search?value=product+owner",
    "https://jobright.ai/jobs/search?value=product+manager",




    # HubSpot (server-rendered listings; crawl like a board)
    #"https://www.hubspot.com/careers/jobs?q=product&;page=1",
    # If you want a tighter filter and HubSpot supports it, you can also try:
    # "https://www.hubspot.com/careers/jobs?page=1&functions=product&location=Remote%20-%20USA",


    # University of Washington (Workday) — focused keyword searches
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=business%20analyst",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=systems%20analyst",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=business%20systems%20analyst",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=product%20manager",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=product%20owner",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=scrum%20master",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=release%20train%20engineer",



    # University of British Columbia (Workday) — focused keyword searches
    #"https://ubc.wd10.myworkdayjobs.com/en-US/ubcstaffjobs/jobs?q=business+analyst",


    # Business Analyst / Systems Analyst
    "https://www.themuse.com/search/location/remote-flexible/keyword/business-analyst",
    "https://www.themuse.com/jobs?categories=information-technology&location=remote&query=business%20analyst",
    "https://www.themuse.com/jobs?categories=information-technology&location=remote&query=systems%20analyst",
    "https://www.builtin.com/jobs?search=business%20analyst&remote=true",
    "https://www.builtin.com/jobs?search=systems%20analyst&remote=true",
    "https://www.builtin.com/jobs?search=agile&remote=true",
    "https://www.builtinseattle.com/jobs?search=agile&remote=true",
    "https://www.builtinseattle.com/jobs?search=business%20analyst&remote=true",
    "https://www.builtinseattle.com/jobs?search=systems%20analyst&remote=true",
    "https://builtinvancouver.org/jobs?search=Business+Analyst",
    "https://builtinvancouver.org/jobs?search=Systems+Analyst",
    "https://www.simplyhired.com/search?q=systems+analyst&l=remote",
    "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=systems+analyst",
    "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=business+analyst",
    "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=business+systems+analyst",



    # Scrum Master / RTE
    "https://remotive.com/remote-jobs/product?search=scrum%20master",
    "https://www.builtin.com/jobs?search=scrum%20master&remote=true",
    "https://www.builtinseattle.com/jobs?search=scrum%20master&remote=true",
    "https://builtinvancouver.org/jobs?search=Scrum+Master",
    "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=scrum+master",
    "https://jobright.ai/jobs/search?value=agile",
    "https://jobright.ai/jobs/search?value=scrum+master",
    "https://jobright.ai/jobs/search?value=release+train+engineer",





    # Remote-friendly product boards that serve real HTML
    "https://remotive.com/remote-jobs/product?locations=Canada%2BUSA",
    "https://weworkremotely.com/categories/remote-product-jobs",
    "https://nodesk.co/remote-jobs/product/",
    "https://www.workingnomads.com/jobs?tag=product",
    "https://www.workingnomads.com/remote-product-jobs",
    "https://www.simplyhired.com/search?q=product+owner&l=remote",
    "https://www.edtech.com/jobs/fully-remote-jobs?Cat=Product%20Development",
    "https://www.edtech.com/jobs/fully-remote-jobs?Cat=Information%20Technology",
    "https://www.edtech.com/jobs/fully-remote-jobs?Cat=Operations",
    "https://edtechjobs.io/jobs/product-management?location=Remote",
    "https://edtechjobs.io/jobs/business-analysis?location=Remote",


    # Ascensus (Workday tenant) — focused role searches
    # "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=product%20manager",                   # 20251227- removed to lesson the amount of jobs scraped can add back if desired
    # "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=product%20owner",                     # 20251227- removed to lesson the amount of jobs scraped can add back if desired
    # "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=business%20analyst",                  # 20251227- removed to lesson the amount of jobs scraped can add back if desired
    # "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=systems%20analyst",                   # 20251227- removed to lesson the amount of jobs scraped can add back if desired
    # "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=scrum%20master",                      # 20251227- removed to lesson the amount of jobs scraped can add back if desired
    # optional: release train engineer / RTE
    # "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers/search?q=release%20train%20engineer",   # 20251227- removed to lesson the amount of jobs scraped can add back if desired

    "https://jobs.ashbyhq.com/zapier",

    # The Muse works well (canonical filtered URL lives at the top of STARTING_PAGES)
    "https://www.themuse.com/jobs?categories=product&location=remote",
    "https://www.themuse.com/jobs?categories=management&location=remote",

    # YC jobs (Playwright-friendly)
    "https://www.ycombinator.com/jobs/role/product-manager",
    "https://www.workatastartup.com/companies?role=product",
    "https://www.workatastartup.com/companies?role=agile",

    # Remote OK
    "https://remoteok.com/?location=CA,US,region_NA",

    # Built In (JS-heavy → Playwright)
    "https://www.builtin.com/jobs?search=product%20manager&remote=true",
    "https://www.builtin.com/jobs?search=product%20owner&remote=true",
    "https://builtinvancouver.org/jobs?search=Product+Manager",
    "https://builtinvancouver.org/jobs?search=Product+Owner",

    # HubSpot (server-rendered listings; crawl like a board)
    "https://www.hubspot.com/careers/jobs?q=product&;page=1",
    # If you want a tighter filter and HubSpot supports it, you can also try:
    # "https://www.hubspot.com/careers/jobs?page=1&functions=product&location=Remote%20-%20USA",


    # Wellfound (AngelList Talent) (JS-heavy → Playwright)
    "https://wellfound.com/role/r/product-manager",

    # Welcome to the Jungle (JS-heavy → Playwright)
    "https://www.welcometothejungle.com/en/jobs?query=product%20manager&remote=true",
    "https://app.welcometothejungle.com/companies/12Twenty#jobs-section"
    "https://app.welcometothejungle.com/companies/Microsoft#jobs-section"
    "https://app.welcometothejungle.com/companies/Google#jobs-section"
    "https://app.welcometothejungle.com/companies/Adobe#jobs-section"
    "https://app.welcometothejungle.com/companies/Asana#jobs-section"
    "https://app.welcometothejungle.com/companies/Amazon#jobs-section"
    "https://app.welcometothejungle.com/companies/Airtable#jobs-section"
    "https://app.welcometothejungle.com/companies/Beam-Benefits#jobs-section"
    "https://app.welcometothejungle.com/companies/Chime-Bank#jobs-section"
    "https://app.welcometothejungle.com/companies/Clari#jobs-section"
    "https://app.welcometothejungle.com/companies/Confluent#jobs-section"
    "https://app.welcometothejungle.com/companies/DataDog#jobs-section"
    "https://app.welcometothejungle.com/companies/Dataminr#jobs-section"
    "https://app.welcometothejungle.com/companies/Expensify#jobs-section"
    "https://app.welcometothejungle.com/companies/Figma#jobs-section"
    "https://app.welcometothejungle.com/companies/Gong-io#jobs-section"
    "https://app.welcometothejungle.com/companies/HashiCorp#jobs-section"
    "https://app.welcometothejungle.com/companies/HubSpot#jobs-section"
    "https://app.welcometothejungle.com/companies/Looker#jobs-section"
    "https://app.welcometothejungle.com/companies/MaintainX#jobs-section"
    "https://app.welcometothejungle.com/companies/Notion#jobs-section"
    "https://app.welcometothejungle.com/companies/Outreach#jobs-section"
    "https://app.welcometothejungle.com/companies/PagerDuty#jobs-section"
    "https://app.welcometothejungle.com/companies/Segment#jobs-section"
    "https://app.welcometothejungle.com/companies/Smartsheet#jobs-section"
    "https://app.welcometothejungle.com/companies/Stripe#jobs-section"
    "https://app.welcometothejungle.com/companies/Top-Hat#jobs-section"
    "https://app.welcometothejungle.com/companies/TripActions#jobs-section"
    "https://app.welcometothejungle.com/companies/UiPath#jobs-section"
    "https://app.welcometothejungle.com/companies/Vetcove#jobs-section"
    "https://app.welcometothejungle.com/companies/Zoom#jobs-section"
    "https://app.welcometothejungle.com/companies/Metabase#jobs-section"
    "https://app.welcometothejungle.com/api/jobs?query=product%20owner&locations=remote",
    "https://app.welcometothejungle.com/api/jobs?query=product",
]

assert all(u.startswith("http") for u in STARTING_PAGES), "A STARTING_PAGES entry is missing a comma."

# -------------------------------------------------------------------
# LEGACY TEXT BASED GEO HEURISTICS (DO NOT EXTEND IN THIS SOW)
#
# Purpose:
# - Historical helper logic used by older boards and earlier iterations.
#
# Current strategy:
# - BuiltIn, BuiltIn Vancouver, and Y Combinator are the only boards considered "complete."
# - The canonical geo pipeline for complete boards is:
#     Location + Location Chips -> _extract_location_signals -> gates (classification_rules)
#
# Policy:
# - Do NOT add new geo behavior here.
# - Do NOT add new countries, states, or equivalence rules here.
# - If a complete board needs new geo behavior, update geo_constants and/or the gate pipeline instead.
#
# Future refactor SOW:
# - Convert any still needed pieces into "chip inference only" utilities, or delete.
# -------------------------------------------------------------------

# Company careers pages to scan for ATS boards (PUBLIC sources)
# Notes:
# - The helper expand_career_sources() will scrape these pages once,
#   discover their Greenhouse/Lever/Ashby board URLs, and return those
#   listing pages (which are HTML and safe to crawl).
CAREER_PAGES = [
    "https://about.gitlab.com/jobs/all-jobs/",                                      # Greenhouse → boards.greenhouse.io/gitlab
    "https://zapier.com/jobs#job-openings",                                         # Lever → jobs.lever.co/zapier
    # add more any time
]


# ---- Filters ----
REQUIRE_US = True

US_HINTS = re.compile(
    r"\b(united states|u\.?s\.?a\.?|u\.?s\.?|us only|authorized to work in the us|"
    r"remote\s*us|remote\s*usa|timezone:? (?:pt|pst|pdt|mt|mst|mdt|ct|cst|cdt|et|est|edt)|"
    r"(?:^|[^A-Z])(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|"
    r"MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)(?:[^A-Z]|$)"
    r")\b",
    re.I,
)


# Detect a real "allowed states" list context
STATE_LIST_CONTEXT = re.compile(
    r"(eligible in|must (?:reside|live|be based) in|open to candidates in|"
    r"(?:hire|hiring) in|only in|restricted to|within (?:these|the) states|"
    r"the following states|available in|states?:)",
    re.I,
)

# Detect explicit WA exclusion (works even without a full list)
WA_EXCLUDE_REGEX = re.compile(
    r"\b(excluding|except|not\s+eligible|not\s+available|cannot\s+(?:hire|employ)|"
    r"not\s+allowed|no\s+hires?\s+in)\s+(washington|wa)\b",
    re.I,
)



# Require WA if job lists specific states
STATE_LIST_REGEX = re.compile(
    r"\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|"
    r"MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b",
    re.I,
)
WA_REGEX = re.compile(r"\b(Washington|WA)\b", re.I)

# Role keywords
ROLE_REGEX = re.compile(r"\b(agile\s+product\s+owner|technical\s+product\s+owner|product\s+owner|product-owner|productowner)\b", re.I)
PM_REGEX   = re.compile(r"\b(product\s+manager|product\s+management)\b", re.I)

# BIV location lists often contain 3-letter country codes like CAN, PHL, ESP, PRT.
BIV_COUNTRY_CODE_RX = re.compile(r"(?:^|[,\s/])([A-Z]{3})(?:$|[,\s/])")

# Map 3-letter codes we care about into the chip tokens used downstream.
_BIV_COUNTRY_3_TO_CHIP = {
    "CAN": "CAN",
    "USA": "USA",
    "PHL": "PHL",
    "ESP": "ESP",
    "PRT": "PRT",
}

# Soft non-NA hints (used only for the soft rule on remote boards)
NON_US_HINTS = re.compile(
    r"\b(europe|emea|eu|united kingdom|uk|england|ireland|scotland|wales|germany|france|spain|italy|"
    r"netherlands|belgium|sweden|norway|finland|denmark|switzerland|austria|poland|czech|portugal|"
    r"romania|bulgaria|greece|hungary|slovakia|baltics|australia|new zealand|india|singapore|mexico|"
    r"ukraine|brazil|argentina|colombia|apac|costa rica|latam)\b",
    re.I,
)


# Strong non-US signals in a location or timezone context
REGION_CONTEXT = re.compile(
    r"(location|remote location|time\s*zone|timezone|candidates must be|must (?:reside|live|be based) in|"
    r"only in|restricted to|open to candidates in|within (?:these|the) states|available in|states?:)",
    re.I,
)

EU_STRONG = re.compile(
    r"\b(europe|eu|emea|european\s+union|schengen|uk|united\s+kingdom|britain|gb|ireland)\b",
    re.I,
)


# Keep your existing US_HINTS and NON_US_HINTS, but this one is a stronger blocker
NON_US_STRONG = re.compile(
    r"(location\s*[:\-]?\s*(canada|australia|new zealand|apac|latam)\b|"
    r"\b(canada[- ]only|apac[- ]only|latam[- ]only)\b)",
    re.I,
)


def _extract_excluded_states(text: str) -> set[str]:
    """
    Find 'remote except these states' patterns and return canonical state names.
    Works for lines like:
      'with the exception of the following states: ...'
      'remote in US except ...'
    Returns empty set if no exclusion context is detected.
    """
    t = " ".join((text or "").lower().split())

    # quick gate: only run if text hints at an exclusion list
    if not any(w in t for w in ("except", "exception", "excluding", "not available in")):
        return set()

    # pick up state names and abbreviations that appear near the exclusion cue
    tokens = set(m.group(1).lower() for m in _STATES_RX.finditer(t))
    out: set[str] = set()
    for tok in tokens:
        out.add(_US_ABBR.get(tok, tok))

    # normalize D.C.
    if "district of columbia" in out or "washington d.c." in out or "dc" in tokens:
        out.discard("district of columbia")
        out.add("washington d.c.")

    return out


# Strong non-North-America countries/regions (if these appear, reject even on remote boards)
NON_NA_STRONG = re.compile(
    r"\b("
    r"india|philippines|sri\s*lanka|pakistan|bangladesh|nepal|indonesia|vietnam|thailand|malaysia|singapore|"
    r"china|taiwan|japan|korea|hk|hong\s*kong|"
    r"mexico|brazil|argentina|colombia|chile|peru|uruguay|paraguay|bolivia|ecuador|"
    r"latam|apac|asean|south\s*asia|south\s*east\s*asia"
    r")\b",
    re.I,
)

REMOTE_REGEX = re.compile(
    r"\b(remote|work from anywhere|wfh|distributed|telecommute)\b"
    r"|usa\s*only|u\.s\.a?\s*only|us\s*only", re.I
)

# ---- Helpers ----
# Exact column order for "keep" CSV (Columns A–AD)
KEEP_FIELDS = [
    "Applied?",
    "Reason",
    "Date Scraped",
    "Title",
    "Job ID (Vendor)",
    "Job ID (Numeric)",
    "Job Key",
    "Company",
    "Career Board",
    "Location",
    "Posted",
    "Posting Date",
    "Valid Through",
    "Job URL",
    "Apply URL",
    "Apply URL Note",
    "Description Snippet",
    "WA Rule",
    "BC Rule",
    "ON Rule",
    "Remote Rule",
    "US Rule",
    "Canada Rule",
    "Salary Max Detected",
    "Salary Rule",
    "Salary Near Min",
    "Salary Status",
    "Salary Note",
    "Salary Est. (Low-High)",
    "Location Chips",
    "Applicant Regions",
    "Visibility Status",
    "Confidence Score",
    "Confidence Mark",
]


# Exact column order for "skip" CSV (Columns A–T)
SKIP_FIELDS = [
    "Date Scraped",
    "Title",
    "Job ID (Vendor)",
    "Job ID (Numeric)",
    "Job Key",
    "Company",
    "Career Board",
    "Location",
    "Posted",
    "Posting Date",
    "Valid Through",
    "Job URL",
    "Reason Skipped",
    "WA Rule",
    "BC Rule",
    "ON Rule",
    "Remote Rule",
    "US Rule",
    "Canada Rule",
    "Salary Max Detected",
    "Salary Rule",
    "Location Chips",
    "Applicant Regions",
]

import json
import requests
from urllib.parse import urlparse, urljoin

WORKDAY_LIMIT = 30

# ======================================
# WORKDAY JSON API PAGINATION COLLECTOR
# ======================================
import json
import requests
from urllib.parse import urlparse, urljoin


def fetch_workday_json_jobs(api_base, payload):
    """
    Wrapper around the Workday CXS endpoint.

    Returns {} on any failure or non-JSON response so the caller can fall back to HTML.
    """
    try:
        resp = requests.post(
            api_base,
            headers=HEADERS,
            data=json.dumps(payload),
            timeout=30,
        )
    except Exception as e:
        log_line("WARN", f".[WORKDAY] Request failed (api={api_base}, error={type(e).__name__}: {e})")
        return {}

    return _safe_resp_json(resp, context=f".[WORKDAY] api json (api={api_base})") or {}




def _safe_resp_json(resp, context: str = "") -> dict:
    """
    Safely parse JSON from a requests response.
    Returns {} if response is not JSON or cannot be parsed.
    """
    if resp is None:
        log_line("WARN", f"{context} No response object; returning empty JSON")
        return {}

    # Pull body once so logs and heuristics are consistent
    body = (getattr(resp, "text", "") or "")
    preview = body.strip().replace("\n", " ")[:200]

    try:
        resp.raise_for_status()
    except Exception as e:
        log_line(
            "WARN",
            f"{context} HTTP failed (status={getattr(resp,'status_code','n/a')}, "
            f"url={getattr(resp,'url','')}, error={e}, preview='{preview}')"
        )
        return {}

    ctype = (resp.headers.get("content-type") or "").lower()
    looks_like_json = body.lstrip().startswith(("{", "["))

    # Prefer header, but allow content sniffing fallback
    if ("json" not in ctype) and (not looks_like_json):
        log_line(
            "WARN",
            f"{context} Non-JSON response (status={resp.status_code}, url={getattr(resp,'url','')}, "
            f"content-type='{ctype}', preview='{preview}')"
        )
        return {}

    try:
        return resp.json() or {}
    except Exception as e:
        log_line(
            "WARN",
            f"{context} JSON parse failed (status={resp.status_code}, url={getattr(resp,'url','')}, "
            f"error={e}, preview='{preview}')"
        )
        return {}


def collect_workday_jobs(listing_url: str, max_links: int | None = None) -> list[str]:
    """
    Robust JSON-based Workday collector.
    Handles pagination via limit/offset.
    """
    parsed = urlparse(listing_url)
    host = parsed.netloc.lower()

    # Workday API lives on myworkday.com even if the UI host is myworkdaysite.com
    api_host = host.replace("myworkdaysite.com", "myworkday.com")

    parts = [p for p in parsed.path.split("/") if p]
    tenant = site = None
    if parts and parts[0].lower() == "recruiting" and len(parts) >= 3:
        tenant, site = parts[1], parts[2]
    elif len(parts) >= 2:
        tenant, site = parts[0], parts[1]

    if not (tenant and site):
        log_line("WARN", f".[WORKDAY] Could not infer tenant/site from {listing_url}; skipping JSON API.")
        return []



    api_base = f"https://{api_host}/wday/cxs/{tenant}/{site}/jobs"

    # Extract `keywords=` from listing URL if present
    # Example: ...search?q=business+analyst
    query = parsed.query
    keywords = None
    if "q=" in query:
        try:
            from urllib.parse import parse_qs
            qs = parse_qs(query)
            raw = qs.get("q", [""])[0]
            keywords = raw.replace("+", " ")
        except Exception:
            pass

    all_links = []
    offset = 0

    while True:
        payload = {
            "limit": WORKDAY_LIMIT,
            "offset": offset,
        }
        if keywords:
            # Workday usually uses searchText in the JSON body
            payload["searchText"] = keywords

        data = fetch_workday_json_jobs(api_base, payload)
        if not data:
            # JSON endpoint not available; fall back to HTML parsing for this listing page
            html = get_html(listing_url)
            if html:
                links = find_job_links(html, listing_url)
                return links
            break

        # Different tenants sometimes use jobPostings vs jobs – be defensive
        jobs = data.get("jobPostings", []) or data.get("jobs", [])

        for job in jobs:
            ext = job.get("externalPath")
            if not ext:
                continue

            detail_url = urljoin(f"https://{host}/", ext)

            if detail_url not in all_links:
                all_links.append(detail_url)
                if max_links and len(all_links) >= max_links:
                    return all_links

        if len(jobs) < WORKDAY_LIMIT:
            break

        offset += WORKDAY_LIMIT

    return all_links

from urllib.parse import urljoin
from bs4 import BeautifulSoup
import requests   # or use your existing fetch_html helper if you prefer

def extract_wttj_company_jobs(html: str, company_url: str) -> list[str]:
    """
    Given the HTML for a company page on app.welcometothejungle.com,
    return a list of absolute job URLs found on that page.
    """
    soup = BeautifulSoup(html, "html.parser")

    links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/jobs/" not in href:
            continue

        full_url = urljoin(company_url, href)
        if full_url not in links:
            links.append(full_url)

    return links


def collect_wttj_company_links(company_url: str) -> list[str]:
    """
    Fetch a WTTJ company page and return job URLs listed on it.
    Example:
        collect_wttj_company_links(
            "https://app.welcometothejungle.com/companies/Beam-Benefits"
        )
    """
    resp = requests.get(company_url, timeout=20)
    resp.raise_for_status()
    html = resp.text

    return extract_wttj_company_jobs(html, company_url)



from dataclasses import dataclass
from typing import Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, NavigableString, Tag


REMOTE_ROCKETSHIP_PM_US = (
    "https://www.remoterocketship.com/country/united-states/jobs/product-manager/"
)


@dataclass
class RemoteRocketshipJob:
    source: str = "remote_rocketship"
    title: str = ""
    company: str = ""
    location_raw: Optional[str] = None
    salary_text: Optional[str] = None
    posted_text: Optional[str] = None
    tags_raw: Optional[str] = None
    listing_url: Optional[str] = None      # Remote Rocketship "View Job"
    apply_url: Optional[str] = None        # External ATS link

    def to_row(self) -> dict:
        """
        Map into your generic row structure.
        Adjust keys here to align with base_row_from_listing or your CSV schema.
        """
        return {
            "source": self.source,
            "title": self.title,
            "company": self.company,
            "location_raw": self.location_raw,
            "salary_text": self.salary_text,
            "posted_text": self.posted_text,
            "tags_raw": self.tags_raw,
            "listing_url": self.listing_url,
            "apply_url": self.apply_url,
        }


def fetch_remote_rocketship_html(
    session: requests.Session,
    url: str = REMOTE_ROCKETSHIP_PM_US,
) -> str:
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def _next_text_containing(start_node: Tag, substring: str) -> Optional[str]:
    node = start_node
    while node is not None:
        node = node.find_next(string=True)
        if node is None:
            return None
        if isinstance(node, NavigableString) and substring in node:
            text = node.strip()
            if text:
                return text
    return None


def _first_salary_after(anchor: Tag) -> Optional[str]:
    for sib in anchor.next_siblings:
        if isinstance(sib, NavigableString):
            text = sib.strip()
            if "$" in text:
                return text
        elif isinstance(sib, Tag):
            # stop if we hit a new heading or a new card
            if sib.name in {"h3", "h4"}:
                break
    return None


def parse_remote_rocketship_jobs(html: str, page_url: str) -> List[RemoteRocketshipJob]:
    soup = BeautifulSoup(html, "html.parser")
    jobs: List[RemoteRocketshipJob] = []

    # Each job starts at an h3 that has a "View Job" and "Apply" further down
    for h3 in soup.find_all("h3"):
        title_link = h3.find("a")
        if not title_link:
            continue

        # Skip section headers such as "People also searched for"
        if "People also searched for" in title_link.get_text(strip=True):
            continue

        view_link = h3.find_next("a", string=lambda s: s and "View Job" in s)
        apply_link = h3.find_next("a", string=lambda s: s and s.strip() == "Apply")
        if not view_link or not apply_link:
            # Not a full job card
            continue

        # Make sure these belong to this card and not a later one
        # Basic guard: if the next h3 comes before the apply link, skip
        next_h3 = h3.find_next("h3")
        if next_h3 and next_h3 is not h3:
            for node in h3.next_elements:
                if node is next_h3:
                    # we reached the next card before seeing "Apply"
                    if node is not apply_link and node is not view_link:
                        apply_link = None
                    break
        if not apply_link:
            continue

        title = title_link.get_text(strip=True)

        # Company is usually the next h4
        company = ""
        company_h4 = h3.find_next("h4")
        if company_h4:
            company_a = company_h4.find("a")
            if company_a:
                company = company_a.get_text(strip=True)
            else:
                company = company_h4.get_text(strip=True)

        # Posted text such as "6 minutes ago", "9 hours ago"
        posted_text = _next_text_containing(h3, "ago")

        # Location anchor contains "Remote"
        loc_anchor = h3.find_next("a", string=lambda s: s and "Remote" in s)
        location_raw = loc_anchor.get_text(strip=True) if loc_anchor else None

        # Salary if present
        salary_text = _first_salary_after(loc_anchor) if loc_anchor else None

        # Tags between location line and Apply button
        tags: List[str] = []
        if loc_anchor:
            for tag_a in loc_anchor.find_all_next("a"):
                if tag_a is apply_link or tag_a is view_link:
                    break
                text = tag_a.get_text(strip=True)
                # Keep only meaningful job tags
                if any(
                    key in text
                    for key in [
                        "Full Time",
                        "Part Time",
                        "Contract",
                        "Junior",
                        "Mid-level",
                        "Senior",
                        "Lead",
                        "Product Manager",
                        "H1B",
                    ]
                ):
                    tags.append(text)

        job = RemoteRocketshipJob(
            title=title,
            company=company,
            location_raw=location_raw,
            salary_text=salary_text,
            posted_text=posted_text,
            tags_raw=", ".join(dict.fromkeys(tags)),  # dedupe while preserving order
            listing_url=urljoin(page_url, title_link.get("href", "")),
            apply_url=apply_link.get("href", None),
        )
        jobs.append(job)

    return jobs


def scrape_remote_rocketship_pm_us(session: Optional[requests.Session] = None) -> List[dict]:
    """
    One shot helper that returns rows ready for CSV or your base_row_from_listing.
    """
    if session is None:
        session = requests.Session()

    html = fetch_remote_rocketship_html(session, REMOTE_ROCKETSHIP_PM_US)
    jobs = parse_remote_rocketship_jobs(html, REMOTE_ROCKETSHIP_PM_US)
    return [job.to_row() for job in jobs]




def can_fetch(url):
    """Check robots.txt with a timeout so it never hangs."""
    parsed = up.urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = robotparser.RobotFileParser()
    try:
        r = requests.get(robots_url, headers=HEADERS, timeout=ROBOTS_TIMEOUT)
        if r.status_code >= 400 or not r.text:
            rp.parse([])  # treat as empty robots
        else:
            rp.parse(r.text.splitlines())
    except Exception:
        rp.parse([])

    allowed = rp.can_fetch(USER_AGENT, url)
    if not allowed:
        return False
    delay = rp.crawl_delay(USER_AGENT)
    return float(delay) if delay is not None else None


def polite_get(url, retries=2):
    backoff = 1.5
    req_host = up.urlparse(url).netloc.lower()

    def _http_fallback(u: str) -> str | None:
        """Return http:// variant to dodge TLS/525 handshake issues."""
        p = up.urlparse(u)
        if p.scheme.lower() != "https":
            return None
        return up.urlunparse(("http", p.netloc, p.path, p.params, p.query, p.fragment))

    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            final_host = up.urlparse(resp.url).netloc.lower()
            # If the request was redirected off-site to a blocked or aggregator domain, treat as not-fetchable
            REDIRECT_BLOCKERS = {"talent.com", "de.talent.com", "in.talent.com"}
            if final_host != req_host and (final_host in BLOCKED_DOMAINS or final_host in REDIRECT_BLOCKERS):
                raise requests.HTTPError(f"Cross-domain redirect to {final_host} blocked")

            resp.raise_for_status()
            return resp
        except Exception as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            is_ssl = isinstance(e, requests.exceptions.SSLError) or status_code == 525

            # One-shot HTTP fallback for TLS/525 handshake issues (e.g., Cloudflare edge/origin)
            if is_ssl:
                fallback_url = _http_fallback(url)
                if fallback_url and fallback_url != url:
                    try:
                        resp = requests.get(
                            fallback_url,
                            headers=HEADERS,
                            timeout=REQUEST_TIMEOUT,
                            allow_redirects=True,
                        )
                        resp.raise_for_status()
                        return resp
                    except Exception:
                        pass

            if attempt == retries:
                for ln in f"{DOT3}{DOTW} Warning: Failed to GET listing page: {url}\n{e}".splitlines():
                    log_print(f"{_box('WARN ')}{DOT3}{ln} {RESET}")

                return None
            time.sleep(backoff * (attempt + 1))


def career_board_name(url: str) -> str:
    """Return a human-friendly name for the site that hosts the job page."""
    host = up.urlparse(url).netloc.lower().replace("www.", "")
    # Friendly names for common hosts
    KNOWN = {
        "nodesk.co": "NoDesk",
        "careerbuilder.com": "CareerBuilder",
        "weworkremotely.com": "We Work Remotely",
        "remoteok.com": "Remote OK",
        "remotive.com": "Remotive",
        "simplyhired.com": "SimplyHired",
        "themuse.com": "The Muse",
        "ycombinator.com": "Y Combinator",
        "jobs.lever.co": "Lever",
        "boards.greenhouse.io": "Greenhouse",
        "glassdoor.com": "Glassdoor",
        "builtin.com": "Built In",
        "www.builtin.com": "Built In",
        "builtinseattle.com": "Built In Seattle",
        "www.builtinseattle.com": "Built In Seattle",
        "builtinvancouver.org": "Built In Vancouver",
        "www.builtinvancouver.org": "Built In Vancouver",
        "wellfound.com": "Wellfound (Wellfound)",
        "welcometothejungle.com": "Welcome to the Jungle",
        "app.welcometothejungle.com": "Welcome to the Jungle",
        "ashbyhq.com": "Ashby",
        "jobs.ashbyhq.com": "Ashby",
        # in career_board_name(url) or similar mapping
        "ascensushr.wd1.myworkdayjobs.com": "Ascensus (Workday)",
        #"wd5.myworkdaysite.com/recruiting/uw/": "University of WA (Workday)",
        "myworkdayjobs.com": "Workday",
        "myworkdaysite.com": "Workday",
        "workday.com": "Workday",
        "myworkdaysite.com": "Workday",
        "wd5.myworkdaysite.com": "Workday",
        "edtech.com": "EdTech",
        "edtechjobs.io": "EdTech Jobs",
        "gitlab.com": "GitLab",
        "jobright.ai": "JobRight",

    }
    for k, v in KNOWN.items():
        if host.endswith(k):
            return v
    # Generic fallback: take the registrable label and prettify
    parts = host.split(".")
    core = parts[-2] if len(parts) >= 2 else host
    return core.replace("-", " ").title()


def random_delay(base_min=MIN_DELAY, base_max=MAX_DELAY):
    time.sleep(random.uniform(base_min, base_max))

def is_job_detail_url(u: str) -> bool:
    p = up.urlparse(u)
    host = p.netloc.lower()
    path = p.path
    q = p.query.lower()

    # Ashby
    if host.endswith("ashbyhq.com") or host.endswith("jobs.ashbyhq.com"):
        # detail pages look like /{company}/{job-slug-or-id}
        # examples: /zapier/senior-product-manager-abc123
        return bool(re.match(r"^/[^/]+/[^/]+/?$", path)) and "departmentid=" not in q

     # Glassdoor
    if "glassdoor.com" in host:
        # Real job pages look like: /job-listing/<slug>-<id>.htm
        # Search pages look like:    /Job/<query>... (we should skip these)
        return path.startswith("/job-listing/")


    # We Work Remotely
    if "weworkremotely.com" in host:
        return path.startswith("/remote-jobs/") and "top-remote-companies" not in path and "categories" not in path

    # remote.co
    if "remote.co" in host:
        return "/remote-jobs/" in path and "categories" not in path and "top-remote-companies" not in path and not path.endswith("/")

    # Remotive
    if "remotive.com" in host:
        return "/remote-jobs/" in path and path.count("/") >= 3 and not path.endswith("/product")

    # NoDesk
    if "nodesk.co" in host:
        return "/remote-jobs/" in path and path.count("/") >= 3 and not path.endswith("/product/")

    # Working Nomads
    if "workingnomads.com" in host:
            # old: return "/jobs/" in path and path.count("/") >= 3 and "category" not in path
        return "/jobs/" in path and path.count("/") >= 2 and "category" not in path

    # edtech
    if "edtech.com" in host:
        return path.startswith("/jobs/") and not path.endswith("-jobs") and path.count("/") >= 2

    # edtechjobs
    if "edtechjobs.io" in host:
        return path.startswith("/jobs/") and not path.endswith("-jobs") and path.count("/") >= 2

    # Built In (main site) and Built In Vancouver
    if "builtin.com" in host or "builtinseattle.com" in host or "builtinvancouver.org" in host:
        _builtin_fill_title_company_from_builtinsignals(details, soup, html)
        # Real job pages look like: /job/<slug>/<numeric-id>
        # Examples:
        #   /job/business-analyst-oms/7890832
        #   /job/senior-product-manager-core-sync/7790870
        return bool(re.match(r"^/job/[^/]+/\d+/?$", path))


    # Wellfound (AngelList Talent)
    if "wellfound.com" in host:
        # They use several patterns; accept /jobs/<id-or-slug> and /l/<slug>
        return (path.startswith("/jobs/") and path.count("/") >= 2) or path.startswith("/l/")

    # Welcome to the Jungle (web + app)
    if "welcometothejungle.com" in host or "app.welcometothejungle.com" in host:
        # e.g., /en/companies/<company>/jobs/<slug> or /en/jobs/<id>
        return "/jobs/" in path and not path.endswith("/jobs") and path.count("/") >= 3

    # Dice
    if "dice.com" in host:
        # Real job pages look like /job-detail/...; search pages use /jobs
        return "/job-detail/" in path


    # Common ATS providers (good yield)
    # Greenhouse
    if "boards.greenhouse.io" in host:
        return "/jobs/" in path and re.search(r"/jobs/\d+", path)

    # Lever
    if "jobs.lever.co" in host:
        # /<company>/<slug-or-id>
        return path.count("/") >= 2 and not path.endswith("/")

    # Ashby
    if "jobs.ashbyhq.com" in host:
        # /<company>/jobs/<id-or-slug>
        return "/jobs/" in path and path.count("/") >= 3

   # Y Combinator
    if "ycombinator.com" in host:
        return path.startswith("/companies/") and "/jobs/" in path and path.count("/") >= 3

    # Workday (incl. myworkdaysite)
    if "workday.com" in host or "myworkdaysite.com" in host:
        # Real job pages look like .../job/...; search/list pages are .../search
        return "/job/" in path and "/search" not in path



    # Default heuristic
    return bool(re.search(r"/(job|jobs|position|opening|careers?)/", path, re.I))



def should_skip_url(u: str) -> bool:
    """Return True for URLs we never want to treat as job details."""
    if not u:
        return True

    # Global hard blocklist (e.g., UW marketing pages, benefits pages)
    if is_blocked_url(u):
        return True

    p = up.urlparse(u)
    host = p.netloc.lower()
    path = p.path.lower()

    # Glassdoor: only /job-listing/* are real job pages.
    if "glassdoor.com" in host:
        if path.startswith("/job-listing/"):
            return False
        # everything else (e.g., /Job/..., /Jobs/..., search pages) -> skip
        return True

    # Add other future filters here if needed
    return False


# --- replace your current normalize_text with this ---
def normalize_text(node_or_text, strip=True) -> str:
    """Return a single-spaced string from either a Soup node or a plain string."""
    if node_or_text is None:
        return ""
    # If it's a soup node (Tag/NavigableString) use get_text; otherwise treat as str
    if hasattr(node_or_text, "get_text"):
        s = node_or_text.get_text(" ", strip=strip)
    else:
        s = str(node_or_text)
        if strip:
            s = s.strip()
    # collapse all whitespace
    return " ".join(s.split())

def find_job_links(listing_html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(listing_html, "html.parser")
    anchors = soup.find_all("a", href=True)
    links: set[str] = set()

    base_host = up.urlparse(base_url).netloc.lower()
    is_workday = (
        "myworkdayjobs.com" in base_host
        or "myworkdaysite.com" in base_host
        or "workday.com" in base_host
    )

    # Built In (US), Built In Seattle, and Built In Vancouver listing pages
    if "builtin.com" in base_host or "builtinseattle.com" in base_host or "builtinvancouver.org" in base_host:
        for a in anchors:
            href = a["href"].strip()
            if not href:
                continue

            # Skip "Top ___ Jobs ..." style links
            text = a.get_text(" ", strip=True)
            text_lower = text.lower()
            if text_lower.startswith("top ") and " jobs" in text_lower:
                continue

            full_url = up.urljoin(base_url, href)
            if is_blocked_url(full_url):
                continue

            p = up.urlparse(full_url)
            path = p.path.lower()

            # Only keep real job detail pages like /job/<slug>/<numeric-id>
            if not re.match(r"^/job/[^/]+/\d+/?$", path):
                continue

            links.add(full_url)

        return list(links)


    cap = 120 if ("ashbyhq.com" in base_host or is_workday) else None
    # Ashby special case
    if "ashbyhq.com" in base_host:
        for a in anchors:
            href = a["href"].strip()
            full_url = up.urljoin(base_url, href)

            if is_blocked_url(full_url):
                continue

            p = up.urlparse(full_url)
            if p.netloc.lower().endswith("ashbyhq.com"):
                if re.fullmatch(r"/[^/]+/[1-9]\d*/", p.path) and "departmentid=" not in p.query.lower():
                    links.add(full_url)
                elif re.fullmatch(r"/[^/]+/jobs/[^/]+/", p.path):
                    links.add(full_url)

                if cap and sum(1 for L in links if base_host in L) >= cap:
                    break

        return list(links)

    # default path (everything else)
    for a in anchors:
        href = a["href"].strip()
        full_url = up.urljoin(base_url, href)

        # 1) global blocklist
        if is_blocked_url(full_url):
            continue

        p = up.urlparse(full_url)

        # 2) for Workday, stay on the Workday host only
        if is_workday and p.netloc.lower() != base_host:
            continue

        if is_job_detail_url(full_url):
            links.add(full_url)

        if cap and sum(1 for L in links if base_host in L) >= cap:
            break

    return list(links)


def _clean_bits(bits):
    out, seen = [], set()
    for b in (bits or []):
        t = " ".join(str(b).split())
        if not t:
            continue
        t = t.replace("–", "-")
        if t.lower() in ("•", "|"):
            continue
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

def _as_text(v):
    return " ".join(map(str, v)) if isinstance(v, (list, tuple)) else str(v or "")

def gather_location_chips(job_url: str, soup: BeautifulSoup, ld: dict, existing: list[str] | None = None) -> str:
    """Pull chips/badges that hint remote/region/type from common boards + ldjson."""
    host = up.urlparse(job_url).netloc.replace("www.", "")
    bits: list[str] = list(existing or [])

    # 1) JSON-LD signals
    if ld:
        for r in ld.get("applicant_regions") or []:
            bits.append(str(r))
        for loc in ld.get("locations") or []:
            bits.append(str(loc))
        jlt = (ld.get("job_location_type") or "").upper()
        if "TELECOMMUTE" in jlt or "REMOTE" in jlt:
            bits.append("Remote")

    # 2) Board-specific selectors / heuristics
    if host.endswith("nodesk.co"):
        aside = soup.find("aside")
        if aside:
            for sel in ["a", "li", "span", "div"]:
                for t in aside.select(sel):
                    txt = t.get_text(" ", strip=True)
                    if txt:
                        bits.append(txt)

    elif host.endswith("remotive.com"):
        # Badges/labels near the right card (varies by page)
        for sel in [".job-tags", ".badge", ".tag", "[class*=badge]", "[class*=tag]"]:
            for t in soup.select(sel):
                txt = t.get_text(" ", strip=True)
                if txt:
                    bits.append(txt)

        # Optional explicit chip for pages that render "USA Only" outside the tag list
        page_text = soup.get_text(" ", strip=True)
        if re.search(r"\busa\s*only\b", page_text, re.I):
            bits.append("USA Only")

        # Region callouts (already in your code)
        full = soup.get_text(" ", strip=True)
        for needle in ["USA Only", "US Only", "Canada", "North America", "Europe", "EU", "EMEA", "UK"]:
            if re.search(rf"\b({re.escape(needle)})\b", full, re.I):
                bits.append(needle)

        # Region callouts (very consistent copy on Remotive)
        full = soup.get_text(" ", strip=True)
        for needle in ["USA Only", "US Only", "Canada", "North America", "Europe", "EU", "EMEA", "UK"]:
            if re.search(rf"\b{re.escape(needle)}\b", full, re.I):
                bits.append(needle)

        # Explicit “REMOTE LOCATION” card label → pull the next words
        hit = soup.find(string=re.compile(r"REMOTE\s+LOCATION", re.I))
        if hit:
            seg = hit.parent.get_text(" ", strip=True) if hit.parent else ""
            # seg often looks like "REMOTE LOCATION Europe"
            m = re.search(r"REMOTE\s+LOCATION\s+(.+)", seg, re.I)
            if m:
                bits.append(m.group(1).strip())

    elif host.endswith("weworkremotely.com"):
        for sel in [".region", ".listing-header-container .region", ".company-card .region"]:
            for t in soup.select(sel):
                bits.append(t.get_text(" ", strip=True))
        for t in soup.select(".listing-header-container .listing-tag, .job-type, .job-category"):
            bits.append(t.get_text(" ", strip=True))

    elif host.endswith("simplyhired.com"):
        for sel in ["[data-testid=viewJobHeader]", "[data-testid=jobDescription]", ".job-details", ".viewjob-jobDescription"]:
            for t in soup.select(sel):
                for a in t.find_all(["span","div","li","a"]):
                    txt = a.get_text(" ", strip=True)
                    if any(k in txt.lower() for k in ["remote", "usa", "united states", "canada", "europe", "full-time", "contract", "part-time"]):
                        bits.append(txt)

    elif host.endswith("builtin.com") or host.endswith("www.builtin.com") or host.endswith("builtinseattle.com") or host.endswith("www.builtinseattle.com") or host.endswith("builtinvancouver.org") or host.endswith("www.builtinvancouver.org"):
        # Try common metadata containers; harmless if missing
        for el in soup.select('[data-testid="job-metadata"] li, .job-metadata li, .metadata li'):
            bits.append(el.get_text(" ", strip=True))


    # 3) Header title line sometimes carries a “Remote” chip
    h1 = soup.find("h1")
    if h1:
        txt = h1.get_text(" ", strip=True)
        if "remote" in txt.lower():
            bits.append("Remote")

    return " | ".join(_clean_bits(bits))[:300]

import html as _html

_EMOJI_RX = re.compile(r"[\U0001F1E0-\U0001FAD6\U00002700-\U000027BF\U0001F300-\U0001FAFF]+")
_TAG_PREFIX_RX = re.compile(
    r"""^\s*
        (?:\[(?:hiring|remote|urgent|new|hot|apply\s*now)\]   # [Hiring] [Remote] ...
         |(?:hiring|now\s*hiring|we\s*are\s*hiring)           # Hiring: Now hiring:
        )
        [\s:|,-]*                                             # separators
    """,
    re.I | re.X,
)
# replace the old two "tail" regexes with these
_AT_TAIL_RX = re.compile(r"\s*@\s*[\w.&'()\- ]+\s*$", re.I)
_TRAILING_COMPANY_RX = re.compile(r"\s*[–—\-|:]\s*@?\s*[\w.&'()\- ]+\s*$", re.I)

REMOTE_SUFFIX_RX = re.compile(
    r"""
    \s*
    (?:
        # " - Remote", "– Remote", "/ Remote"
        (?:[-–—/]\s*)?remote
      | \(\s*remote\s*\)           # "(Remote)"
    )
    \s*$
    """,
    re.I | re.X,
)

# strip gender/schedule/formatting suffixes often seen in EU postings
_GENDER_MARKER_RX   = re.compile(r"\(\s*(?:m\s*/\s*f\s*/\s*d|m\s*/\s*w\s*/\s*d|w\s*/\s*m\s*/\s*d)\s*\)", re.I)
_GENDER_WORDS_RX    = re.compile(r"\b(mwd|m/w/d|w/m/d)\b", re.I)
_SCHEDULE_SUFFIX_RX = re.compile(r"\b(full[-\s]?time|part[-\s]?time|contract|permanent|temporary|vollzeit|teilzeit)\b", re.I)
_PARENS_TRAILER_RX  = re.compile(r"\s*\([^)]*\)\s*$")  # generic trailing (…) cleaner

# Put near your other regex helpers
HYPHEN_CHARS_RX   = r"[\-\u2013\u2014]"  # -, – , —
COMPANY_SUFFIX_RX = r"(?:,?\s*(?:inc\.?|llc|ltd|limited|corp(?:oration)?))?"

def _norm_company_for_compare(s: str) -> str:
    import re
    s = re.sub(COMPANY_SUFFIX_RX + r"$", "", str(s or ""), flags=re.I).strip()
    return re.sub(r"[^a-z0-9]", "", s.lower())

def normalize_title(t: str, company: str | None = None) -> str:
    """Normalize title without chopping legitimate role parts after hyphens."""
    import re, html
    if not t:
        return ""

    # Basic unescape + strip
    t = _html.unescape(str(t)).strip()

    # 1) Strip leading taggy noise like "[Hiring]" / "Hiring:" etc.
    t = _TAG_PREFIX_RX.sub("", t)

    # 2) Strip common emoji noise
    t = _EMOJI_RX.sub("", t)

    # 3) Strip gender / schedule suffixes and trailing Remote markers
    t = _GENDER_MARKER_RX.sub("", t)
    t = _GENDER_WORDS_RX.sub("", t)
    t = _SCHEDULE_SUFFIX_RX.sub("", t)
    t = REMOTE_SUFFIX_RX.sub("", t)

    # 4) Strip generic trailing "(…)" blobs
    t = _PARENS_TRAILER_RX.sub("", t)

    # 5) Strip trailing " @Company" or " – Company" regardless of whether
    #    we got an explicit company name from the board.
    t = _AT_TAIL_RX.sub("", t)

    # Normalize whitespace after all the chopping
    t = re.sub(r"\s+", " ", t).strip()

    # If we *don't* have a trusted company string, stop here.
    if not company:
        return t

    comp_norm = _norm_company_for_compare(company)
    if not comp_norm:
        return t

    # Now, with a known company, we can also strip patterns like
    # "Company – Title" or "Title – Company" using the hyphen logic.

    parts = re.split(rf"\s*{HYPHEN_CHARS_RX}\s*", t)
    if len(parts) >= 2:
        tail_norm = _norm_company_for_compare(parts[-1])
        if tail_norm == comp_norm:
            # keep everything before the last hyphen; preserve original spacing minimally
            return " - ".join(parts[:-1]).strip()

    # Try strip leading “Company – …”
    head_norm = _norm_company_for_compare(parts[0]) if parts else ""
    if head_norm == comp_norm and len(parts) >= 2:
        return " - ".join(parts[1:]).strip()

    return t


def _countries_in(values: list[str]) -> set[str]:
    out: set[str] = set()
    for v in values or []:
        if not v:
            continue
        s = v.strip()

        # Canada (do NOT treat "CA" as Canada)
        if re.search(r"\bcanada\b", s, re.I) or re.search(r"\bCAN\b", s):
            out.add("CANADA")

        # United States
        if re.search(r"\bunited states\b", s, re.I) or re.search(r"\bUSA\b", s) or re.search(r"\bUS\b", s):
            out.add("US")

    return out

def infer_default_country(job_url: str) -> str:
    host = (urlparse(job_url).netloc or "").lower()

    # Built In Vancouver is Canada
    if "builtinvancouver" in host:
        return "Canada"

    # Dice is US default
    if host.endswith("dice.com"):
        return "US"

    # Optional: .ca domains default to Canada
    if host.endswith(".ca"):
        return "Canada"

    # Safe default
    return "US"

def best_location_for_display(ld: dict, chips: str, scraped_loc: str, default_country: str = "US") -> str:
    def _all_canadian_locs(values: list[str]) -> bool:
        """Return True when we have multiple locations and they are all Canada-coded."""
        vals = [v for v in values if v]
        if len(vals) < 2:
            return False
        return all(
            re.search(r"\bcan(?:ada)?\b", v, re.I) or re.search(r"\bCAN\b", v)
            for v in vals
        )

    def _looks_specific(loc: str) -> bool:
        """Heuristic: prefer city/state strings over generic region tokens."""
        if not loc:
            return False
        low = loc.lower()
        if "remote" in low or "global" in low or "anywhere" in low:
            return False
        # commas or multiple words suggest city/state
        if "," in loc:
            return True
        if len(loc.split()) >= 2:
            return True
        return False

    locs_from_ld = ld.get("locations") or []
    scraped_parts = [p.strip() for p in re.split(r"[;/|]", scraped_loc) if p.strip()] if scraped_loc else []

    # If scraped location is explicitly Canada, do not let chip noise override it
    if scraped_loc and re.search(r"\bcan(?:ada)?\b", scraped_loc, re.I):
        return scraped_loc if _looks_specific(scraped_loc) else "Canada"


    # Collapse multi-province Canada lists down to a country-level label
    if _all_canadian_locs(locs_from_ld):
        return "Canada"
    if scraped_parts and _all_canadian_locs(scraped_parts):
        return "Canada"

    # 1) Prefer explicit region tokens in chips (skip pure "Remote")
    if chips:
        tokens = [p.strip() for p in re.split(r"[|/;,]", chips) if p.strip()]
        #tokens = [p.strip() for p in chips.split("|") if p.strip()]        Original line
        tokens = [t for t in tokens if t.lower() not in ("remote", "global", "anywhere")]

        countries = _countries_in(tokens + ([scraped_loc] if scraped_loc else []))

        # Only collapse to Canada if Canada is the only detected country
        if countries == {"CANADA"}:
            return scraped_loc if _looks_specific(scraped_loc) else "Canada"

        # If multiple countries exist, prefer the more specific scraped_loc
        if _looks_specific(scraped_loc):
            return scraped_loc

        # Otherwise return the most informative token (or the first)
        if tokens:
            return tokens[0]


    # 2) LD-JSON locations
    locs = ld.get("locations") or []
    if locs:
        nice = [l for l in locs if l and l.lower() not in ("remote", "global", "anywhere")]
        if nice:
            job_url = (
                (ld.get("job_url") or ld.get("Job URL") or ld.get("url") or ld.get("URL") or "")
            )
            if "builtinvancouver.org" in str(job_url).lower():
                return ", ".join(nice)        # keep all locations for Built In Vancouver
            return ", ".join(nice[:2])        # keep short version elsewhere

    # 3) Scraped location unless it was the crawler placeholder
    if scraped_loc and not PLACEHOLDER_RX.search(scraped_loc):
        return scraped_loc

    # 4) Last resort: site default country
    return default_country

import re

# Optional: tidy up ALL-CAPS or stray spaces before punctuation
def _normalize_company_name(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s).strip()
    if s.isupper() and not re.search(r"[A-Z]\.", s):  # turn “HITACHI ENERGY” into “Hitachi Energy”
        s = s.title()
    return s.replace(" ,", ",").replace(" .", ".")

#def _safe_node_text(node) -> str:
#    try:
#        # BeautifulSoup Tag path
#        return node.get_text(" ", strip=True)
#    except AttributeError:
#        # None or plain string
#        return str(node or "")

def _safe_node_text(node):
    return " ".join(node.get_text(" ", strip=True).split()) if node else ""

def _company_from_meta_or_title(host: str, soup) -> str:
    # Try common meta/title patterns first (works well on The Muse)
    # 1) og:title often looks like "Business Analyst at Experian | The Muse"
    meta = soup.find("meta", attrs={"property": "og:title"})
    if meta and meta.get("content"):
        m = re.search(r"\bat\s+([^|–-]+)", meta["content"], flags=re.I)
        if m:
            return m.group(1).strip()

    # 2) <title> fallback with same pattern
    t = (soup.title.string or "").strip() if soup.title and soup.title.string else ""
    if t:
        m = re.search(r"\bat\s+([^|–-]+)", t, flags=re.I)
        if m:
            return m.group(1).strip()

    return ""

def _company_from_common_selectors(soup) -> str:
    # Try a few likely places; harmless if they don't exist
    cand = (
        soup.select_one("[data-qa='company-name']")
        or soup.select_one("[data-testid='company-name']")
        or soup.select_one("a.company, span.company, header .company")
    )
    if cand:
        return cand.get_text(strip=True)
    return ""

# Legacy wrapper. Kept temporarily to avoid breaking any missed call sites.
# Remove once all references to _tokenize_location_chips are eliminated.
def _tokenize_location_chips(loc: str, page_text: str) -> list[str]:
    return tokenize_location_chips(loc, page_text)

def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _builtin_extract_header_work_mode(text: str) -> Tuple[str, str]:
    """
    Extracts strong work mode signals that appear in the job header area.
    Returns: (mode, evidence_text)
      mode in {"Remote", "Hybrid", "Onsite", ""}

    Notes:
    - This is text based on purpose so it is resilient across small DOM changes.
    - It is designed to prefer phrases like "Hiring Remotely in US" which are strong.
    - Evidence is returned for debug logs.
    """
    t = _norm_space(text).lower()
    if not t:
        return "", ""

    # Strong remote phrases seen on Built In
    strong_remote_patterns = [
        r"\bhiring remotely\b",
        r"\bhiring remote\b",
        r"\bremote\b",
        r"\bremote (in|within)\b",
        r"\bwork remotely\b",
        r"\bremotely\b",
        r"\bfully remote\b",
    ]

    # Strong hybrid phrases
    strong_hybrid_patterns = [
        r"\bhybrid\b",
        r"\bflexible hybrid\b",
        r"\bhybrid work\b",
    ]

    # Strong onsite phrases
    strong_onsite_patterns = [
        r"\bon[- ]?site\b",
        r"\bonsite\b",
        r"\bin[- ]office\b",
        r"\bin office\b",
    ]

    # Order matters: remote first, then hybrid, then onsite
    for pat in strong_remote_patterns:
        if re.search(pat, t):
            # capture a short evidence snippet
            return "Remote", _evidence_snippet(text, pat)

    for pat in strong_hybrid_patterns:
        if re.search(pat, t):
            return "Hybrid", _evidence_snippet(text, pat)

    for pat in strong_onsite_patterns:
        if re.search(pat, t):
            return "Onsite", _evidence_snippet(text, pat)

    return "", ""

def _evidence_snippet(original_text: str, pattern: str, window: int = 60) -> str:
    """
    Returns a small snippet around the first match for logging.
    """
    s = _norm_space(original_text)
    if not s:
        return ""
    try:
        m = re.search(pattern, s, flags=re.IGNORECASE)
        if not m:
            return ""
        start = max(0, m.start() - window)
        end = min(len(s), m.end() + window)
        return s[start:end]
    except re.error:
        return ""

def _builtin_get_header_text(soup) -> str:
    """
    Attempts to extract text from the job header region only.
    Falls back safely if selectors do not match.
    """
    if soup is None:
        return ""

    selectors = [
        # These are intentionally broad. You can tighten after you see logs.
        "header",
        "[data-testid*='job']",
        "[class*='job']",
        "[class*='Job']",
        "[class*='header']",
        "[class*='Header']",
        "[class*='hero']",
        "[class*='Hero']",
    ]

    for sel in selectors:
        try:
            node = soup.select_one(sel)
            if node:
                txt = node.get_text(" ", strip=True)
                if txt and len(txt) > 30:
                    return txt
        except Exception:
            continue

    # fallback: only use the first chunk of page text
    page_txt = soup.get_text(" ", strip=True)
    return page_txt[:2000] if page_txt else ""

def _builtin_extract_workplace_badge_text(soup: "BeautifulSoup") -> str:
    """
    Extract Built In / BIV work mode text from the job header area only.

    Returns one of:
      "Remote", "Hybrid", "Onsite", "Mixed", or ""

    Notes:
    - Scoped to header/title containers only
    - Avoids office/location modules
    - Does not do a full page scan
    """
    if not soup:
        return ""

    CANON = {
        "remote": "Remote",
        "remote (us)": "Remote",
        "remote(us)": "Remote",
        "remote us": "Remote",

        "hybrid": "Hybrid",

        "in office": "Onsite",
        "in-office": "Onsite",
        "in office only": "Onsite",
        "on-site": "Onsite",
        "onsite": "Onsite",

        "in-office or remote": "Mixed",
        "in office or remote": "Mixed",
        "onsite or remote": "Mixed",
        "on-site or remote": "Mixed",
        "hybrid or remote": "Mixed",
    }

    PRIORITY = {
        "Mixed": 4,
        "Remote": 3,
        "Hybrid": 2,
        "Onsite": 1,
    }

    def _clean(txt: str) -> str:
        t = " ".join((txt or "").strip().split())
        t = t.replace("–", "-").replace("—", "-")
        return t

    def _normalize(txt: str) -> str:
        low = _clean(txt).lower()

        if low in CANON:
            return CANON[low]

        # small flexible patterns
        if low.startswith("remote (") or low.startswith("remote("):
            return "Remote"

        if "in-office or remote" in low or "in office or remote" in low:
            return "Mixed"

        if "hybrid or remote" in low:
            return "Mixed"

        if low == "remote":
            return "Remote"
        if low == "hybrid":
            return "Hybrid"
        if low in {"in office", "in-office", "onsite", "on-site"}:
            return "Onsite"

        return ""

    def _is_offices_context(node) -> bool:
        if not node:
            return False

        cur = node
        for _ in range(10):
            if not cur:
                break

            attrs = getattr(cur, "attrs", {}) or {}
            idv = (attrs.get("id") or "")
            clz = " ".join(attrs.get("class") or [])
            aria = (attrs.get("aria-label") or "")

            blob = f"{idv} {clz} {aria}".lower()
            if any(k in blob for k in ("office-locations", "job-location", "locations-list", "map")):
                return True

            heading = cur.find(["h2", "h3", "h4"])
            if heading:
                htxt = (heading.get_text(" ", strip=True) or "").lower()
                if any(k in htxt for k in ("offices", "office locations", "locations")):
                    return True

            cur = cur.parent

        return False

    def _candidate_texts(container) -> list[str]:
        if not container:
            return []

        out = []

        # First pass: existing badge-ish nodes
        nodes = container.select(
            "[data-testid*='workplace'], [data-testid*='job-workplace'], [data-testid*='job-type'], "
            "span.badge, div.badge, span[class*='badge'], div[class*='badge'], "
            "span[class*='pill'], div[class*='pill']"
        )

        for node in nodes:
            if _is_offices_context(node):
                continue
            txt = _clean(node.get_text(" ", strip=True))
            if txt:
                out.append(txt)

        # Second pass: scan only small text nodes in the scoped header area
        # This catches cases where the label is plain text next to an icon.
        for node in container.find_all(["span", "div", "p"], limit=120):
            if _is_offices_context(node):
                continue

            txt = _clean(node.get_text(" ", strip=True))
            if not txt or len(txt) > 40:
                continue

            norm = _normalize(txt)
            if norm:
                out.append(txt)

        return out

    def _pick_best(texts: list[str]) -> str:
        best = ""
        best_score = 0

        for txt in texts or []:
            norm = _normalize(txt)
            if not norm:
                continue
            score = PRIORITY.get(norm, 0)
            if score > best_score:
                best = norm
                best_score = score

        return best

    # Scope to header area only
    top_containers = []
    main = soup.find("main") or soup.find("article") or soup
    top_containers.append(main)

    h1 = soup.find("h1")
    if h1:
        title_scope = h1.find_parent(["header", "section", "article", "div"]) or h1.parent
        if title_scope:
            top_containers.insert(0, title_scope)

    for sel in (
        "[data-testid='job-header']",
        "[data-testid*='job-header']",
        "header",
    ):
        node = soup.select_one(sel)
        if node:
            top_containers.insert(0, node)

    seen = set()
    scoped = []
    for c in top_containers:
        if not c:
            continue
        cid = id(c)
        if cid in seen:
            continue
        seen.add(cid)
        scoped.append(c)

    # Strong remote banner in header
    for c in scoped[:3]:
        t = _clean(c.get_text(" ", strip=True)).lower()
        if "hiring remotely" in t:
            return "Remote"

    texts = []
    for c in scoped[:3]:
        texts.extend(_candidate_texts(c))

    best = _pick_best(texts)
    return best or ""

def _clamp_applicant_regions_using_loc_chips(applicant_regions: list[str], loc_chips_raw: str) -> list[str]:
    chips = [c.strip().upper() for c in (loc_chips_raw or "").split("|") if c.strip()]
    if not chips:
        return applicant_regions

    has_usa = "USA" in chips
    has_can = "CAN" in chips or "BC" in chips or "ON" in chips

    # Pure US geography
    if has_usa and not has_can:
        applicant_regions = [r for r in applicant_regions if r != "can"]
        if not applicant_regions:
            applicant_regions = ["us"]

    # Pure Canada geography
    elif has_can and not has_usa:
        applicant_regions = [r for r in applicant_regions if r != "us"]
        if not applicant_regions:
            applicant_regions = ["can"]

    return applicant_regions

def _detect_applicant_regions(text: str) -> list[str]:
    t = text or ""
    t_low = t.lower()
    regions: list[str] = []

    # California context means "CA" should not imply Canada.
    # Only treat as California when it looks like a location signal.
    is_california_context = bool(
        re.search(r"\bcalifornia\b", t_low)
        or re.search(r"\bcalif\.?\b", t_low)
        or re.search(r",\s*ca\b", t_low)                # "San Jose, CA"
        or re.search(r"\bca\s*,\s*(?:us|usa)\b", t_low) # "CA, USA"
    )

    # US
    if re.search(r"\b(united states|u\.s\.|usa|us)\b", t_low):
        regions.append("us")

    if "canada" in t_low or re.search(r"(?:^|[,\s])can(?:$|[,\s])", t_low):
        i = t_low.find("canada")
        if i != -1:
            debug(f"[APPREGIONS] canada_context={t[i-60:i+80]!r}")

    # Canada (conservative)
    if not is_california_context:
        if re.search(r"\bcanada\b", t_low) or re.search(r"(?:^|[,\s])can(?:$|[,\s])", t_low):
            regions.append("can")

    # Special CA handling: only allow CA to imply Canada when explicitly framed as eligibility.
    if (not is_california_context) and re.search(r"\bCA\b", t, flags=re.IGNORECASE):
        california_location_pattern = bool(
            re.search(r",\s*CA\s*,\s*(USA|US)\b", t, flags=re.IGNORECASE)
            or re.search(r"\bCalifornia\b", t, flags=re.IGNORECASE)
            or re.search(r"\bCalif\.?\b", t, flags=re.IGNORECASE)
            or re.search(r"\bCA\s*,\s*(USA|US)\b", t, flags=re.IGNORECASE)
        )

        # Special CA handling (CA is ambiguous: California vs Canada)
        # Only infer Canada from "CA" when we are NOT in a California context.
        if not is_california_context:
            eligibility_framed = bool(
                re.search(
                    r"\b(eligible|eligibility|applicants?|candidates?|residents?|hiring|work authorization|authorized)\b"
                    r".{0,80}?"
                    r"\b(US|USA|United States)\s*(/|and|&)\s*CA\b",
                    text or "",
                    flags=re.IGNORECASE | re.DOTALL,
                )
                or re.search(
                    r"\b(eligible|eligibility|applicants?|candidates?|residents?|hiring|work authorization|authorized)\b"
                    r".{0,80}?"
                    r"\bCA\s*(/|and|&)\s*(US|USA|United States)\b",
                    text or "",
                    flags=re.IGNORECASE | re.DOTALL,
                )
                or re.search(
                    r"\b(eligible|eligibility|applicants?|candidates?|residents?|hiring|work authorization|authorized)\b"
                    r".{0,80}?"
                    r"\bCA\s+only\b",
                    text or "",
                    flags=re.IGNORECASE | re.DOTALL,
                )
            )

            if eligibility_framed:
                regions.append("can")
                debug(f"[APPREGIONS] added 'can' via CA eligibility framed text; t_sample={(text or '')[:160]!r}")

    # Dedupe preserve order
    seen: set[str] = set()
    out: list[str] = []
    for r in regions:
        if r not in seen:
            seen.add(r)
            out.append(r)
    return out

def _apply_path_location_hint(details: dict, url: str | None = None) -> None:
    """
    For Workday (and similar) URLs, derive a city/campus from the path when the current
    location is generic (empty/US/Remote). Adds US/WA chips accordingly.
    """
    try:
        loc_cur_raw = (details.get("Location") or "").strip()
        loc_cur_cmp = loc_cur_raw.casefold()

        target_url = url or details.get("Job URL") or details.get("job_url") or ""
        path_cmp = (up.urlparse(str(target_url)).path or "").casefold()

        # Only apply when current location is generic
        if loc_cur_cmp not in ("", "us", "united states", "remote"):
            return

        for token, nice in items():
            tok = (token or "").strip()
            if not tok:
                continue

            if tok.casefold() in path_cmp:
                details["Location"] = nice

                chips = details.get("Location Chips") or ""
                if isinstance(chips, str):
                    chips_set = {c.strip() for c in chips.split("|") if c.strip()}
                else:
                    chips_set = {str(c).strip() for c in (chips or []) if str(c).strip()}

                chips_set.update({"US", "WA", nice})
                details["Location Chips"] = "|".join(sorted(chips_set))

                if not (details.get("US Rule") or "").strip():
                    details["US Rule"] = "Pass"

                if not (details.get("WA Rule") or "").strip():
                    details["WA Rule"] = "Pass" if "wa" in nice.casefold() else "Fail"

                break

    except Exception:
        pass


def _has_can_province_signal(text: str) -> bool:
    """
    True when text contains a Canadian province or territory, either as:
    - abbreviation (BC, ON, QC, etc) or
    - full name (british columbia, ontario, etc)
    """
    t = (text or "").lower()

    if CAN_PROV_RX.search(t):
        return True

    for name in CAN_PROV_MAP.keys():
        if name in t:
            return True

    return False


def _fold_ascii(value: str) -> str:
    """Lowercase and strip diacritics for robust province matching."""
    import unicodedata
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(ch)
    ).lower()

def _normalize_canada_province_tokens(loc: str) -> str:
    tokens = [t.strip() for t in (loc or "").split(",") if t.strip()]
    if not tokens:
        return (loc or "").strip()
    changed = False
    out: list[str] = []
    for tok in tokens:
        abbr = CAN_PROV_MAP.get(_fold_ascii(tok))
        if abbr:
            out.append(abbr)
            changed = True
        else:
            out.append(tok)
    if not changed:
        return (loc or "").strip()
    return ", ".join(out)

def _normalize_canada_provinces_value(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        return value

    parts = re.split(r"\s*/\s*", value.strip())
    out = []

    for p in parts:
        p = p.strip()
        if not p:
            continue
        np = _normalize_canada_province_tokens(p)

        from logging_utils import debug
        debug(f"[NORM CHECK] token='CAN' -> {_normalize_canada_province_tokens('CAN')!r}")

        # If normalization returns empty, keep the original token
        if not isinstance(np, str) or not np.strip():
            out.append(p)
        else:
            out.append(np.strip())

    if not out:
        return value

    return " / ".join(out) if len(parts) > 1 else out[0]

def _normalize_canada_provinces_in_details(details: dict) -> None:
    lock_chips = bool(details.get("_LOCK_LOCATION_CHIPS"))

    for key in ("Location", "Location Raw"):
        if isinstance(details.get(key), str):
            details[key] = _normalize_canada_provinces_value(details[key])

    for list_key in ("BIV Tooltip Locations", "locations"):
        vals = details.get(list_key)
        if isinstance(vals, list):
            details[list_key] = [_normalize_canada_provinces_value(v) for v in vals if v]

    if lock_chips:
        return

    chips = details.get("Location Chips")
    if isinstance(chips, list):
        norm = []
        for v in chips:
            if not v:
                continue
            nv = _normalize_canada_provinces_value(v)
            nv = str(nv).strip()
            if nv:
                norm.append(nv)

        # Preserve order, do not sort
        seen = set()
        out = []
        for v in norm:
            if v in seen:
                continue
            seen.add(v)
            out.append(v)

        details["Location Chips"] = "|".join(out)

    elif isinstance(chips, str) and chips.strip():
        parts = [p.strip() for p in chips.split("|") if p.strip()]
        parts = [_normalize_canada_provinces_value(p) for p in parts]
        details["Location Chips"] = "|".join([p for p in (str(x).strip() for x in parts) if p])

def _normalize_biv_location_value(s: str) -> str:
    """
    Normalize BuiltIn Vancouver display location to a consistent vocabulary.

    Rules:
    - CAN for Canada
    - USA for United States
    - remove any '/ Remote' fragments
    - collapse whitespace
    """
    if not s:
        return ""
    x = str(s).strip()

    # Remove Remote from display locations
    x = re.sub(r"\s*/\s*remote\b", "", x, flags=re.I)
    x = re.sub(r"\bremote\b", "", x, flags=re.I).strip()

    # Normalize country words to CAN / USA
    x = re.sub(r"\bcanada\b", "CAN", x, flags=re.I)
    x = re.sub(r"\bunited states\b", "USA", x, flags=re.I)

    # Normalize bare US variants to USA (only as a standalone token)
    x = re.sub(r"\bU\.S\.\b", "USA", x, flags=re.I)
    x = re.sub(r"\bUSA\b", "USA", x, flags=re.I)
    x = re.sub(r"\bUS\b", "USA", x, flags=re.I)

    # Normalize CA -> CAN only when context suggests Canada
    if re.search(r"\bcanada\b", x, flags=re.I) or _has_can_province_signal(x.lower()):
        x = re.sub(r"\bCA\b", "CAN", x)

    # Clean separators
    x = re.sub(r"\s*/\s*", " / ", x).strip()
    x = re.sub(r"\s{2,}", " ", x).strip()

    # Final safety: never allow Canada to become CA (California ambiguity).
    # If we intended Canada, represent as CAN.
    if x.strip().upper() == "CA":
        x = "CAN"

    return x

def _canon_work_mode(v: str) -> str:
    v = (v or "").strip().lower()
    if v == "remote":
        return "Remote"
    if v == "hybrid":
        return "Hybrid"
    if v in {"onsite", "on-site", "on site", "in-office", "in office"}:
        return "Onsite"
    return "Unknown"

def _set_location_chips_source(details: dict, source: str) -> None:
    source = (source or "").strip()
    details["_LOCATION_CHIPS_SOURCE"] = source
    details["Location Chips Source"] = source


def _set_loc_chips(details: dict, value: str, source: str, force: bool = False) -> None:
    """
    Single place to set Location Chips consistently and log the change.

    value can be:
      - pipe string "USA|WA"
      - list ["USA", "WA"]
      - set/tuple
      - single token "CAN"
    """
    from logging_utils import debug  # local import avoids cycles

    # If locked and not forcing, skip
    if details.get("_LOCK_LOCATION_CHIPS") and not force:
        debug(f"[LOC_CHIPS_SET] SKIP (locked) {source} | value={value!r}")
        return

    before = details.get("Location Chips")

    # Normalize to pipe string
    normalized = _as_pipe_location_chips(value)

    # Filter to allowed chips only
    chips_list = [c for c in normalized.split("|") if c] if normalized else []
    chips_list = [c for c in chips_list if c in ALLOWED_LOCATION_CHIPS]
    normalized = "|".join(chips_list)

    details["Location Chips"] = normalized
    details["Location Chips Source"] = source
    details["_LOCATION_CHIPS_SOURCE"] = source

    debug(f"[LOC_CHIPS_SET] {source} | before={before!r} -> after={normalized!r} | force={force}")

    # Normalize into canonical pipe string
    try:
        normalized = _as_pipe_chips(value)
    except Exception:
        # last resort
        if isinstance(value, (list, tuple, set)):
            normalized = "|".join([str(x).strip().upper() for x in value if str(x).strip()])
        else:
            normalized = str(value or "").strip().upper()

    details["Location Chips"] = normalized

    # Preserve or backfill a source if caller did not set one yet
    if not (details.get("Location Chips Source") or "").strip():
        details["Location Chips Source"] = (
            details.get("_LOCATION_CHIPS_SOURCE")
            or ""
        )

    debug(f"[LOC_CHIPS_SET] {source} | before={before!r} -> after={normalized!r} | force={force}")

def _derive_location_rules(details: dict, html: str = "") -> dict:
    # ----------------------------
    # Stage 0: Inputs and host type
    # ----------------------------
    job_url = details.get("Job URL") or ""
    job_url_low = job_url.lower()

    # --- Safety defaults to prevent NameError and preserve intent ---
    ca_rule_raw = details.get("Canada Rule Raw") or details.get("Canada Rule") or "unknown"
    us_rule_raw = details.get("US Rule Raw") or details.get("US Rule") or "unknown"

    # Normalize in case you store "Pass"/"Fail" sometimes
    ca_rule_raw = str(ca_rule_raw).strip().lower()
    us_rule_raw = str(us_rule_raw).strip().lower()

    if details.get("Source") == "Built In Vancouver" or details.get("Board") == "Built In Vancouver":
        debug(f"[RULE_RAW_DEBUG] ca_rule_raw={ca_rule_raw!r} us_rule_raw={us_rule_raw!r} keys={list(details.keys())[:30]}")
    
    # Fallback: retrieve html from details if not passed as parameter
    if not html:
        html = details.get("html_raw", "")

    # Normalize LocationRaw key. Support both legacy "Location Raw" and newer "LocationRaw".
    if not (details.get("LocationRaw") or "").strip():
        legacy = (details.get("Location Raw") or "").strip()
        if legacy:
            details["LocationRaw"] = legacy

    # Optional: if you want the output column to always be populated too
    if not (details.get("Location Raw") or "").strip():
        normalized = (details.get("LocationRaw") or "").strip()
        if normalized:
            details["Location Raw"] = normalized

    # 1) If we do not know the URL yet, do nothing
    if not job_url_low.strip():
        return details

    # 2) Run once per job record
    if details.get("_DERIVE_LOCATION_RULES_DONE"):
        return details

    is_biv = "builtinvancouver.org" in job_url_low
    is_builtin = ("builtin.com" in job_url_low) or is_biv

    # --- BIV: prefer hero LocationRaw if specific ---
    if is_biv:
        loc_raw = (details.get("LocationRaw") or "").strip()
        if loc_raw and loc_raw.upper() not in {"CAN", "CANADA"} and "," in loc_raw:
            details["Location"] = loc_raw
            details["_LOCK_LOCATION_FROM_HERO"] = True

            if not (details.get("Applicant Regions Source") or "").strip():
                details["Applicant Regions Source"] = "BIV_LOCATION_HINT"

    # Use the same location precedence you already use
    loc = details.get("Location", "") or details.get("display_location", "") or ""
    loc = loc if isinstance(loc, str) else str(loc)
    loc_s = loc.strip()
    loc_low = loc_s.lower()

    # Build once, reused later
    text_for_rules = ""

    # BIV: Prefer explicit "Hiring Remotely in ..." over tooltip lists.
    # Tooltip can be noisy (often includes US + Canadian provinces).
    if is_biv:
        try:
            page_html = html or ""  # use the real page HTML string

            # -------------------------------------------------
            # Tier 1: "Hiring Remotely in CAN" (authoritative)
            # -------------------------------------------------
            hiring_loc = _extract_biv_hiring_remotely_location(page_html)
            if hiring_loc:
                details["Location"] = hiring_loc
                details["Location Raw"] = hiring_loc

                # Chips: lock to country level (CAN, or USA, etc.)
                country_chips = _extract_biv_hiring_remotely_country_chips(page_html)

                # Normalize chips ordering deterministically and safely
                if country_chips:
                    # Keep preferred order for common pairs
                    preferred = ["USA", "CAN"]
                    ordered = [c for c in preferred if c in country_chips] + sorted(
                        [c for c in country_chips if c not in set(preferred)]
                    )
                    details["Location Chips"] = "|".join(ordered)
                else:
                    # Your Lumos case lands here
                    details["Location Chips"] = "CAN"

                details["_LOCK_LOCATION_FROM_HIRING_REMOTELY"] = True
                details["_LOCK_LOCATION_FROM_TOOLTIP"] = False
                details["_LOCK_LOCATION_CHIPS"] = True

                log_line("DEBUG", f"[BIVDBG] hiring_remotely_lock_success loc='{hiring_loc}' chips='{details.get('Location Chips')}'")

            # -------------------------------------------------
            # Tier 3: Tooltip locations (fallback only)
            # -------------------------------------------------
            if not details.get("_LOCK_LOCATION_CHIPS"):
                tooltip_locs = _builtin_primary_locations_from_html(page_html)
                if not tooltip_locs:
                    tooltip_locs = _builtin_tooltip_locations_from_html(page_html)  # fallback

                if tooltip_locs:
                    loc_joined = " / ".join(tooltip_locs)

                    details["BIV Tooltip Locations"] = tooltip_locs

                    # Set every location field variant your pipeline might read later
                    details["Location"] = loc_joined
                    details["Location Raw"] = loc_joined
                    details["LocationRaw"] = loc_joined   # this is the missing one that bit you

                    # Locks
                    details["_LOCK_LOCATION_FROM_TOOLTIP"] = True
                    details["_LOCK_LOCATION"] = True
                    details["_LOCK_LOCATION_CHIPS"] = True

                    log_line("DEBUG", f"[BIVDBG] tooltip_lock_success locs={tooltip_locs}")
                
                if details.get("_LOCK_LOCATION_FROM_TOOLTIP"):
                    log_line("DEBUG", "[BIVDBG] location overwrite blocked (tooltip lock)")
                    # If this is inside a function, do this:
                    # return details
                    # If you cannot return here, at least prevent later logic from running:
                    # (use a flag)
                    pass

        except Exception as e:
            log_line("DEBUG", f"[BIVDBG] biv_lock_failed err={e}")

    if is_biv:
        log_line(
            "BIV INPUTS",
            " | ".join(
                [
                    f"url={job_url}",
                    f"Location={details.get('Location')!r}",
                    f"Location Raw={details.get('Location Raw')!r}",
                    f"LocationRaw={details.get('LocationRaw')!r}",
                    f"LocationsText={details.get('Locations Text')!r}",
                    f"workplace_type={details.get('workplace_type')!r}",
                    f"work_mode={details.get('work_mode')!r}",
                    f"workplace_badge={details.get('workplace_badge')!r}",
                    f"display_location={details.get('display_location')!r}",
                ]
            ),
        )

    # ---------------------------------------------------------
    # Stage 0b: HERO_LOCK (BIV only) can set chips and lock them
    # ---------------------------------------------------------
    if is_biv:
        hero = (
            details.get("Hero Location")
            or details.get("Hero Country")
            or details.get("hero_location")
            or details.get("hero_country")
            or ""
        )
        hero_up = str(hero).strip().upper()

        if hero_up in ("CAN", "CANADA") and not details.get("_LOCK_LOCATION_FROM_HERO"):
            details["HERO_LOCK"] = True

            # Lock location display to Canada
            details["Location"] = "CAN"
            details["LocationRaw"] = "CAN"

            # Lock chips to CAN only, overriding anything earlier
            details["_LOCK_LOCATION_CHIPS"] = True
            details["_LOCATION_CHIPS_SOURCE"] = "HERO_LOCK"
            details["Location Chips Source"] = "HERO_LOCK"
            _set_loc_chips(details, "CAN", "HERO_LOCK set CAN only", force=True)

    # -----------------------------------------
    # Stage 1: Chips from structured location only
    # -----------------------------------------
    # Rule:
    # - Built In and BIV chips come ONLY from the structured location string.
    # - Non Built In sources can use page scan tokenization.

    if details.get("_LOCK_LOCATION_CHIPS"):
        lc = (details.get("Location Chips") or "").strip()
        if not lc:
            tooltip_locs = details.get("BIV Tooltip Locations") or []
            loc_text = " / ".join(tooltip_locs) if tooltip_locs else (details.get("Location") or "")

            # Prefer tooltip locations as structured inputs
            if tooltip_locs:
                loc_parts = [p.strip() for p in tooltip_locs if str(p).strip()]
            else:
                loc_parts = [p.strip() for p in str(loc_text).split("/") if p.strip()]

            lc_new = _chips_pipe_from_location_strings(loc_parts)

            if lc_new:
                details["Location Chips"] = lc_new

                # Keep a more-specific source if it already exists, otherwise mark fallback
                details["Location Chips Source"] = (details.get("Location Chips Source") or "LOCKED_FALLBACK")
                debug_print(
                    DEBUG_CFG,
                    "chips",
                    f"[CHIPS_LOCK_FALLBACK] filled Location Chips={lc_new!r} from loc_text={loc_text!r}",
                )
            else:
                # Could not derive chips. Prefer to keep lock if tooltip was the authority,
                # but mark the error so it is explainable.
                if tooltip_locs:
                    details["_LOCK_LOCATION_CHIPS"] = True
                    details["_BIV_CHIPS_ERROR"] = True
                    details["Location Chips Source"] = "BIV_TOOLTIP_ERROR_EMPTY"
                else:
                    # No tooltip authority: allow downstream normal flow instead of crashing.
                    details["_LOCK_LOCATION_CHIPS"] = False
                    if not (details.get("Location Chips Source") or "").strip():
                        details["Location Chips Source"] = "UNLOCKED_FALLBACK_EMPTY"

                debug_print(
                    DEBUG_CFG,
                    "chips",
                    f"[CHIPS_LOCK_FALLBACK] Could not derive chips from locked signals. tooltip={bool(tooltip_locs)} loc_text={loc_text!r}",
                )

    existing_src = (details.get("Location Chips Source") or "").upper()

    if is_builtin:
        if ("HERO_LOCK" in existing_src) or ("TOOLTIP" in existing_src) or (details.get("_LOCK_LOCATION_CHIPS") is True):
            if not (details.get("Location Chips") or "").strip():
                raise RuntimeError(
                    "LOCKED chips but Location Chips is empty. Expected Location Chips to be set before lock."
                )
            trace_chips(details, "AFTER_STAGE1_CHIPS")
        else:
            loc_chips: list[str] = []

            # BIV: multi-location strings with multiple countries must produce country-only chips.
            if is_biv and " / " in loc_s:
                countries = _biv_detect_countries_from_location(loc_s)
                # treat as multi-country only if we have 2+ distinct countries
                if len(countries) >= 2:
                    details["_BIV_MULTI_COUNTRY"] = True
                    _set_loc_chips(details, "|".join(countries), "BIV multi-country country-only", force=True)
                    details["_LOCK_LOCATION_CHIPS"] = True
                    details["Location Chips Source"] = "BIV_MULTI_COUNTRY_COUNTRY_ONLY"
                    trace_chips(details, "AFTER_STAGE1_CHIPS")
                    # Skip parsing provinces and cities for this record
                    goto_stage3 = True
                else:
                    goto_stage3 = False
            else:
                goto_stage3 = False

            if not goto_stage3:
                # Built In Vancouver: if we somehow still have a multi-location string, do not extract chips from it.
                # Multi-location parsing is a known source of false positives for BIV.
                if is_biv and " / " in loc_s:
                    loc_for_parse = loc_s.split(" / ")[0]
                else:
                    loc_for_parse = loc_s

                state_hits = re.findall(r",\s*([A-Z]{2})\b", loc_for_parse)

                for st in state_hits:
                    st_up = st.upper()

                    # Strict 2 letter validation, includes "dc" in US_STATE_ABBRS
                    if st_up.lower() in US_STATE_ABBRS:
                        if "USA" not in loc_chips:
                            loc_chips.append("USA")
                        if st_up not in loc_chips:
                            loc_chips.append(st_up)

                # Canada signals from structured location and Raw hints
                raw_low = (details.get("LocationRaw") or "").strip().lower()
                if loc_s.strip().upper() == "CAN" or loc_low in {"can", "canada"}:
                    loc_chips.append("CAN")
                elif raw_low in {"can", "canada", "ca"}:
                    loc_chips.append("CAN")
                elif CAN_PROV_RX.search(loc_low):
                    loc_chips.append("CAN")

                # Finalize chips list for Built In
                chips = []
                if loc_low != "remote":
                    chips = sorted(set([c for c in loc_chips if c]))

                # --- BIV: if hero LocationRaw is specific (e.g., "Vancouver, BC"), force CAN|<PROV> ---
                if is_biv and (not chips or chips == ["CAN"]):
                    m = re.search(r",\s*([A-Z]{2})\b", (loc_s or "").strip(), flags=re.IGNORECASE)
                    if m:
                        prov = m.group(1).upper()
                        if prov.upper() in CAN_PROV_ABBRS:
                            chips = ["CAN", prov.upper()]
                            details["Location Chips Source"] = (
                                (details.get("Location Chips Source") or "") + "|BIV_HERO_LOC_RAW"
                            )
                            details["_LOCK_LOCATION_CHIPS"] = True

                src_up = (details.get("Location Chips Source") or "").upper()

                if src_up in {"TOOLTIP", "HERO_LOCK", "BIV_MULTI_COUNTRY_COUNTRY_ONLY"}:
                    # Do not overwrite chips
                    if src_up == "TOOLTIP":
                        details["_LOCK_LOCATION_CHIPS"] = True
                    trace_chips(details, "AFTER_STAGE1_CHIPS")
                else:
                    # --- Built In: country-only fallback for structured loc_s like "US" ---
                    chips_str = "|".join(chips) if chips else ""

                    if not chips_str:
                        canon = LOCATION_CHIP_SYNONYMS.get((loc_s or "").strip().upper(), "")
                        if canon in {"USA", "CAN"}:
                            chips_str = canon

                    # Only set chips if we have something meaningful.
                    # Do NOT force-set an empty string, because it blocks downstream fixes.
                    if chips_str:
                        _set_loc_chips(
                            details,
                            chips_str,
                            "STAGE1 BUILTIN_LOCATION_ONLY derived from loc_s",
                            force=True,
                        )
                    else:
                        # Leave Location Chips unset here.
                        # Downstream stages or locks can still populate it.
                        pass

                    trace_chips(details, "AFTER_STAGE1_CHIPS")

            else:
                # multi-country BIV already set chips and locked them
                # Do not overwrite source, do not recompute, do not re-trace if you do not want duplicate logs
                pass

    elif "ycombinator.com" in (details.get("Job URL") or "").lower():
        # YC: use structured/header location only.
        # Do not page-scan description text because it picks up HQ, visa text, and other noise.
        details["_LOCK_LOCATION_CHIPS"] = False
        details["_LOCATION_CHIPS_SOURCE"] = "YC_LOCATION_ONLY"
        details["Location Chips Source"] = "YC_LOCATION_ONLY"

        loc_hint = " / ".join(
            [
                str(details.get("LocationRaw") or "").strip(),
                str(details.get("Location") or "").strip(),
            ]
        ).strip(" /")

        chips = tokenize_location_chips(loc_hint, "")

        _set_loc_chips(
            details,
            "|".join(chips) if chips else "",
            "STAGE1 YC_LOCATION_ONLY tokenize_location_chips (loc_hint only)",
            force=True,
        )

        trace_chips(details, "AFTER_STAGE1_CHIPS")

    else:
        # Not Built In: allow page scan tokenizer
        details["_LOCK_LOCATION_CHIPS"] = False
        details["_LOCATION_CHIPS_SOURCE"] = "PAGE_SCAN"
        details["Location Chips Source"] = "PAGE_SCAN"

        loc_hint = " ".join(
            [
                str(details.get("LocationRaw") or ""),
                str(details.get("Location") or ""),
                str(details.get("Locations Text") or ""),
                str(details.get("display_location") or ""),
            ]
        ).strip()

        page_text = " ".join(
            [
                details.get("page_text") or "",
                details.get("Description") or "",
                details.get("Description Snippet") or "",
            ]
        ).strip()

        chips = tokenize_location_chips(loc_hint, page_text)

        _set_loc_chips(
            details,
            "|".join(chips) if chips else "",
            "STAGE1 PAGE_SCAN tokenize_location_chips (loc_hint + page_text)",
            force=True,
        )

        trace_chips(details, "AFTER_STAGE1_CHIPS")

    # -----------------------------------------
    # Stage 2: Build text_for_rules once
    # -----------------------------------------
    loc_for_rules = " ".join(
        [
            str(details.get("LocationRaw") or ""),
            str(details.get("Location") or ""),
            str(details.get("Locations Text") or ""),
        ]
    ).strip()
    if loc_for_rules:
        text_for_rules = f"{text_for_rules} {loc_for_rules}".strip()

    text = " ".join(
        [
            details.get("page_text") or "",
            details.get("Description") or "",
            details.get("Description Snippet") or "",
        ]
    ).strip()
    if text:
        text_for_rules = f"{text_for_rules} {text}".strip()

    # -----------------------------------------
    # Stage 3: Applicant Regions list, never None
    # -----------------------------------------
    applicant_regions: list[str] = _detect_applicant_regions(text_for_rules) or []

    # Normalize to lowercase tokens early
    applicant_regions = [str(r).strip().lower() for r in applicant_regions if str(r).strip()]

    # Dedupe while preserving order
    seen = set()
    applicant_regions = [r for r in applicant_regions if not (r in seen or seen.add(r))]

    # Normalize to lowercase tokens early
    applicant_regions = [str(r).strip().lower() for r in applicant_regions if str(r).strip()]

    # Dedupe while preserving order
    seen = set()
    applicant_regions = [r for r in applicant_regions if not (r in seen or seen.add(r))]

    # If detector found something and no source is set yet, mark it
    if applicant_regions and not (details.get("Applicant Regions Source") or "").strip():
        details["Applicant Regions Source"] = "TEXT"

    # If detector found something and no source is set yet, mark it
    if applicant_regions and not (details.get("Applicant Regions Source") or "").strip():
        details["Applicant Regions Source"] = "TEXT"

    # Backfill from chips if detector found nothing
    if not applicant_regions:
        chips_now = {c.strip().upper() for c in (details.get("Location Chips") or "").split("|") if c.strip()}
        if "CAN" in chips_now:
            applicant_regions = ["can"]
            if not (details.get("Applicant Regions Source") or "").strip():
                details["Applicant Regions Source"] = "CHIPS_BACKFILL"
        elif "USA" in chips_now:
            applicant_regions = ["us"]
            if not (details.get("Applicant Regions Source") or "").strip():
                details["Applicant Regions Source"] = "CHIPS_BACKFILL"

    details["_APPLICANT_REGIONS_LIST"] = list(applicant_regions)

    # -----------------------------------------
    # Stage 4: Apply BIV overrides once
    # -----------------------------------------
    chips_list = [c.strip().upper() for c in _as_listish(details.get("Location Chips")) if c.strip()]

    chips_list, applicant_regions = _apply_builtinvancouver_overrides(
        details=details,
        job_url=job_url,
        loc_low=loc_low,
        chips=chips_list,
        applicant_regions=applicant_regions,
    )

    # Persist chips back into the row in your canonical representation
    details["Location Chips"] = "|".join(chips_list)

    # Persist chips deterministically again, but do not overwrite when locked
    if not details.get("_LOCK_LOCATION_CHIPS"):
        ordered = _order_location_chips([c for c in chips_list if c])
        details["Location Chips"] = "|".join(ordered)
        raw = [c for c in chips_list if c]
        uniq = []
        seen = set()
        for c in raw:
            if c not in seen:
                uniq.append(c)
                seen.add(c)

        preferred = ["USA", "CAN"]  # add if needed
        def sort_key(token: str):
            t = token.upper()
            if t in preferred:
                return (0, preferred.index(t))
            if len(t) == 2:  # likely state or province code
                return (1, t)
            return (2, t)

        details["Location Chips"] = "|".join(sorted(uniq, key=sort_key))

    trace_chips(details, "AFTER_STAGE4_OVERRIDES")

    # -----------------------------------------
    # Stage 5: Determine remote_rule once
    # -----------------------------------------
    remote_rule = _canon_work_mode(details.get("Remote Rule") or details.get("remote_flag") or "Unknown")
    remote_locked = remote_rule in {"Remote", "Hybrid", "Onsite"}

    badge_text = " ".join(
        [
            details.get("workplace_type") or "",
            details.get("work_mode") or "",
            details.get("workplace_badge") or "",
        ]
    ).strip()

    snip_low = (details.get("Description Snippet") or "").lower()
    job_url_low = (details.get("Job URL") or "").lower()
    is_yc_job = "ycombinator.com" in job_url_low

    if is_biv:
        says_remote = ("remote" in loc_low) or ("remote" in snip_low)
        if says_remote and remote_rule not in {"Remote", "Hybrid"}:
            remote_rule = _canon_work_mode("Remote")
            details["Remote Rule Source"] = (details.get("Remote Rule Source") or "") + "|BIV_REMOTE_FALLBACK"
            remote_locked = True

    if not remote_locked:
        if is_biv:
            remote_rule = _canon_work_mode(classify_work_mode(badge_text) if badge_text else "Unknown")
        else:
            # YC / non-BIV:
            # Prefer explicit work mode labels before generic full-text classification.
            bt_low = badge_text.lower().strip()
            txt_low = (text or "").lower()

            explicit_mode = ""

            # Strong explicit remote labels like "Remote (US)"
            if bt_low.startswith("remote"):
                explicit_mode = "Remote"
            elif "remote (" in txt_low or "remote(" in txt_low:
                explicit_mode = "Remote"
            elif bt_low == "remote":
                explicit_mode = "Remote"

            # Explicit hybrid
            elif bt_low == "hybrid":
                explicit_mode = "Hybrid"

            # Explicit onsite
            elif bt_low in {"in office", "in-office", "onsite", "on-site"}:
                explicit_mode = "Onsite"

            if explicit_mode:
                remote_rule = _canon_work_mode(explicit_mode)
                details["Remote Rule Source"] = (details.get("Remote Rule Source") or "") + "|EXPLICIT_WORK_MODE"
            else:
                if is_yc_job:
                    remote_rule = "Unknown"
                    details["Remote Rule Source"] = (details.get("Remote Rule Source") or "") + "|YC_NO_EXPLICIT_WORK_MODE"
                else:
                    remote_rule = _canon_work_mode(classify_work_mode(f"{txt_low} {bt_low}".lower()))
                    details["Remote Rule Source"] = (details.get("Remote Rule Source") or "") + "|CLASSIFIED_WORK_MODE"
            
            debug(f"[YC REMOTE DEBUG] explicit_mode={explicit_mode!r} | is_yc_job={is_yc_job!r}")

    # -----------------------------------------
    # Stage 5.5 exists because Stage 6 remote parity needs a Canada-only vs US-only signal,
    # but final US/Canada rules are computed later in Stage 7. These are provisional "raw" rules
    # derived from chips + loc text to prevent NameError and to avoid appending "us" for BIV
    # listings that are clearly Canada-only remote.
    # Stage 5.5: Provisional CA and US rules for parity gating
    # -----------------------------------------
    chips_now = {c.strip().upper() for c in chips_list if c and c.strip()}

    # Provisional US rule
    us_rule_raw = "fail"
    if "USA" in chips_now:
        us_rule_raw = "pass"
    else:
        if any(tok in loc_low for tok in ("united states", "usa", "u.s.")) or any(s in loc_low for s in US_STATE_ABBRS):
            us_rule_raw = "pass"

    # Provisional Canada rule
    ca_rule_raw = "fail"
    has_can_chip_now = "CAN" in chips_now
    has_can_token_now = bool(re.search(r"(?:^|[,\s])can(?:$|[,\s])", loc_low))
    has_canada_word_now = ("canada" in loc_low)
    has_can_prov_now = bool(CAN_PROV_RX.search(loc_low)) if "CAN_PROV_RX" in globals() else False

    if has_can_chip_now or has_can_token_now or has_canada_word_now or has_can_prov_now:
        ca_rule_raw = "pass"

    debug(
        f"[PARITY_RULES_PROVISIONAL] is_biv={is_biv} ca_rule_raw={ca_rule_raw} us_rule_raw={us_rule_raw} "
        f"chips_now={sorted(chips_now)} loc_low_sample={loc_low[:80]!r}"
    )

    # -----------------------------------------
    # Stage 6: Remote parity once, late
    # -----------------------------------------
    is_remote_like = (str(remote_rule).lower() == "remote")
    if is_remote_like:
        parts = list(applicant_regions)
        parts_low = {p.lower() for p in parts}

        # Parity only if can present and us absent
        if "can" in parts_low and "us" not in parts_low:
            if not (is_biv and ca_rule_raw == "pass" and us_rule_raw == "fail"):
                parts.append("us")

        applicant_regions = parts

    # -----------------------------------------
    # Stage 7: Compute rules, finalize, and exit
    # -----------------------------------------
    chips_final_str = details.get("Location Chips") or ""
    chips_list = [c.strip().upper() for c in chips_final_str.split("|") if c.strip()]
    #chips_set = set(chips_list)
    chips_set = {c.strip().upper() for c in chips_final_str.split("|") if c.strip()}
    ar_set = {r.strip().lower() for r in applicant_regions if r.strip()}

    # US Rule
    us_rule = (details.get("US Rule") or "").strip()
    if not us_rule or us_rule.lower() == "default":
        if ("USA" in chips_set) or (not loc_s and ("us" in ar_set or "na" in ar_set)):
            us_rule = "Pass"
        elif any(x in chips_set for x in {"EU", "UK", "EMEA", "APAC", "LATAM"}) or any(
            x in ar_set for x in {"eu", "uk", "emea", "apac", "latam"}
        ):
            us_rule = "Fail"
        else:
            if any(tok in loc_low for tok in ("united states", "usa", "u.s.")) or any(
                s in loc_low for s in US_STATE_ABBRS
            ):
                us_rule = "Pass"
            else:
                us_rule = "Fail"

    has_canada_word = False
    has_can_token = False
    has_can_chip = False

    debug(
        f"[CAN_RULE] has_canada_word={has_canada_word} "
        f"has_can_token={has_can_token} has_can_chip={has_can_chip} "
        f"prov={bool(CAN_PROV_RX.search(loc_low))} loc_low_sample={loc_low[:120]!r}"
        )

    # Canada Rule
    canada_rule = (details.get("Canada Rule") or "").strip()
    if not canada_rule or canada_rule.lower() == "default":
        has_can_chip = "CAN" in chips_set
        has_can_token = bool(re.search(r"(?:^|[,\s])can(?:$|[,\s])", loc_low))
        has_canada_word = ("canada" in loc_low)

        if has_canada_word or CAN_PROV_RX.search(loc_low) or has_can_chip or has_can_token:
            canada_rule = "Pass"
        else:
            canada_rule = "Fail"

    chips_list = _as_listish(chips_final_str)
    chips_set = {c.strip().upper() for c in chips_list if c.strip()}

    # WA Rule
    wa_rule = (details.get("WA Rule") or "").strip()
    if not wa_rule or wa_rule.lower() == "default":
        wa_rule = "Fail"

        # Primary signal: WA explicitly present
        if _matches_locality("WA", loc=loc_s, chips=chips_list, url=job_url):
            wa_rule = "Pass"
        else:
            # Remote fallback must respect explicit state restrictions
            is_remote = (remote_rule or "").strip().lower() == "remote"
            is_us_pass = (us_rule or "").strip().lower() == "pass"

            if is_remote and is_us_pass:
                us_state_hits = {c for c in chips_set if c in US_STATE_CHIPS}  # includes DC

                if us_state_hits and "WA" not in us_state_hits:
                    wa_rule = "Fail"
                else:
                    wa_rule = "Pass"

    # BC Rule
    bc_rule = (details.get("BC Rule") or "").strip()
    if not bc_rule or bc_rule.lower() == "default":
        bc_rule = "Pass" if _matches_locality("BC", loc=loc_s, chips=chips_list, url=job_url) else "Fail"

    # ON Rule
    on_rule = (details.get("ON Rule") or "").strip()
    loc_tokens = set(_as_listish(loc_s))
    chip_tokens = {c.upper() for c in chips_list}

    has_on_token = ("ontario" in loc_tokens) or ("toronto" in loc_tokens) or ("ottawa" in loc_tokens)
    has_on_chip = ("ON" in chip_tokens)

    if not on_rule or on_rule.lower() == "default":
        if has_on_chip or has_on_token:
            on_rule = "Pass" if _matches_locality("ON", loc=loc_s, chips=chips_list, url=job_url) else "Fail"
        else:
            on_rule = "Fail"

    # Province scoped fallback for Canada
    if (canada_rule or "").strip().lower() == "fail" and (bc_rule == "Pass" or on_rule == "Pass"):
        loc_low = (loc_s or "").lower()

        has_can_chip = "CAN" in chips_set
        has_can_token = bool(re.search(r"\bcan\b", loc_low))
        has_canada_word = ("canada" in loc_low)
        has_can_prov = bool(CAN_PROV_RX.search(loc_low)) if "CAN_PROV_RX" in globals() else False

        if has_can_chip or has_can_token or has_canada_word or has_can_prov:
            canada_rule = "Pass"

    # -----------------------------------------
    # YC / non-target country guard
    # If location is explicitly outside US/CAN, do not default to NA regions.
    # This is especially important when structured YC location is known
    # but Location Chips are blank because the chip tokenizer is NA-focused.
    # -----------------------------------------
    loc_low = (loc_s or "").strip().lower()
    chips_blank = not chips_set

    has_non_target_code = bool(NON_TARGET_COUNTRY_CODE_RX.search(loc_low))
    has_non_target_word = bool(NON_TARGET_COUNTRY_WORD_RX.search(loc_low))

    has_us_signal = (
        "USA" in chips_set
        or "WA" in chips_set
        or bool(TARGET_US_RX.search(loc_low))
    )

    has_can_signal = (
        "CAN" in chips_set
        or "BC" in chips_set
        or "ON" in chips_set
        or bool(TARGET_CAN_RX.search(loc_low))
        or bool(CAN_PROV_RX.search(loc_low))
    )

    if chips_blank and (has_non_target_code or has_non_target_word) and not has_us_signal and not has_can_signal:
        us_rule = "Fail"
        canada_rule = "Fail"
        wa_rule = "Fail"
        bc_rule = "Fail"
        on_rule = "Fail"
        applicant_regions = []
        details["Applicant Regions Source"] = "YC_NON_TARGET_LOCATION"

    def _canon_rule(x: str) -> str:
        x = (x or "").strip().lower()
        if x == "pass":
            return "Pass"
        if x == "fail":
            return "Fail"
        if x == "remote":
            return "Remote"
        if x == "hybrid":
            return "Hybrid"
        if x == "onsite":
            return "Onsite"
        if x in {"", "unknown"}:
            return "Unknown"
        return x.capitalize()

    applicant_regions = _clamp_applicant_regions_using_loc_chips(
        applicant_regions,
        details.get("Location Chips") or ""
    )

    chips_final_str = details.get("Location Chips") or ""
    chips_final = {c.strip().upper() for c in chips_final_str.split("|") if c.strip()}

    has_us_chip = "USA" in chips_final
    has_can_chip = "CAN" in chips_final or "BC" in chips_final or "ON" in chips_final

    # Final geography truth guardrail
    if has_can_chip and not has_us_chip:
        applicant_regions = ["can"]
        details["Applicant Regions Source"] = "LOC_CHIPS_CAN_GUARD"
        us_rule = "Fail"
        canada_rule = "Pass"
        wa_rule = "Fail"
        bc_rule = "Pass" if "BC" in chips_final else "Fail"
        on_rule = "Pass" if "ON" in chips_final else "Fail"

    elif has_us_chip and not has_can_chip:
        applicant_regions = ["us"]
        details["Applicant Regions Source"] = "LOC_CHIPS_US_GUARD"
        us_rule = "Pass"
        canada_rule = "Fail"
        wa_rule = "Pass" if "WA" in chips_final else "Fail"

    details["US Rule"] = _canon_rule(us_rule)
    details["Canada Rule"] = _canon_rule(canada_rule)
    details["Remote Rule"] = _canon_rule(remote_rule)
    details["WA Rule"] = _canon_rule(wa_rule)
    details["BC Rule"] = _canon_rule(bc_rule)
    details["ON Rule"] = _canon_rule(on_rule)

    # Finalize Applicant Regions once, pipe format
    details["Applicant Regions"] = _as_pipe_regions(applicant_regions)
    debug(f"[APPREGIONS FINALIZE] applicant_regions={applicant_regions!r} | chips={details.get('Location Chips')!r} | loc={details.get('Location')!r}")

    if "Applicant Regions Source" in details:
        details["Applicant Regions Source"] = _as_pipe_source(details.get("Applicant Regions Source"))

    # Location Chips normalization
    # Important: do not normalize locked chips. Some normalizers drop tokens like DC or IL.
    if details.get("_LOCK_LOCATION_CHIPS"):
        details["Location Chips"] = (details.get("Location Chips") or "").strip()
    else:
        details["Location Chips"] = _as_pipe_chips(details.get("Location Chips") or "")

    # Respect multiple location display
    if isinstance(details.get("Location"), str) and " / " in details["Location"]:
        _yc_trace(details, "YC RULES EXIT", DOTL)
        details["_DERIVE_LOCATION_RULES_DONE"] = True
        return details

    # Update display location if not locked
    if not details.get("_LOCK_LOCATION_CHIPS"):
        try:
            details["Location"] = best_location_for_display(details, details.get("Location Chips", ""), loc_s)
        except Exception:
            pass

    _yc_trace(details, "YC RULES EXIT", DOTL)
    details["_DERIVE_LOCATION_RULES_DONE"] = True

    if not (details.get("Applicant Regions Source") or "").strip():
        details["Applicant Regions Source"] = "UNKNOWN"

    return details

from bs4 import BeautifulSoup  # keep this where it already is

def enrich_salary_fields(d: dict, page_host: str | None = None) -> dict:
    """
    Populate our derived salary columns from any mix of fields that might exist.

    - Detects numeric salaries anywhere in the text.
    - Ignores obvious non-salary numbers such as Job IDs and 401k/403k.
    - Special-cases The Muse & Remotive so footer ZIP codes / job-count banners
      (e.g. "Unlock 69,133 Remote Jobs") do not look like pay.
    - Classifies vs SALARY_FLOOR / SOFT_SALARY_FLOOR.
    - If there are no usable numbers but we see salary trigger phrases,
      sets a Salary Placeholder and marks the row as signal-only.
    """
    import re
    global SALARY_FLOOR, SOFT_SALARY_FLOOR
    workday_min: int | None = None
    workday_max: int | None = None


    # 1) Normalize Salary Range to plain text
    sr = d.get("Salary Range", "")
    if isinstance(sr, dict):
        sr_text = str(sr.get("Text", ""))
    else:
        sr_text = str(sr or "")

    # 2) Build a text blob we can search for salary hints
    full_desc = d.get("Description") or ""
    snippet   = d.get("Description Snippet") or ""
    title     = d.get("Title", "")
    page_text = str(d.get("page_text") or "")
    phost     = (page_host or "").lower()

    def _fmt_est(cur: str | None, lo: int | None, hi: int | None) -> str:
        cur = (cur or "").upper().strip()
        if lo and hi:
            # Matches your existing "CAD 69,450 - 119,450" style
            return f"{cur} {lo:,} - {hi:,}" if cur else f"{lo:,} - {hi:,}"
        if hi:
            return f"{cur} ≈ {hi:,}" if cur else f"≈ {hi:,}"
        return ""

    def _salary_rule_from_status(status: str, source: str = "") -> str:
        # One label you can filter on without re parsing status text
        source = (source or "").lower()
        if "builtin" in source:
            return "builtin_range"
        if status in ("at_or_above", "near_min", "below_floor"):
            return "floor_check"
        if status == "signal_only":
            return "signal_only"
        return "missing_salary"

    def _salary_variants(n: int) -> list[str]:
        # Variants you will commonly see in HTML
        return [
            str(n),                       # 96400
            f"{n:,}",                     # 96,400
            f"{n:,.2f}",                  # 96,400.00
            f"${n}",                      # $96400
            f"${n:,}",                    # $96,400
            f"${n:,.2f}",                 # $96,400.00
        ]



    # --- Built In (incl. city sites like Built In Vancouver) ---
    # If Built In did NOT give us a structured salary, be conservative:
    # don't keep tiny or obviously bogus dollar amounts scraped from the text.
    if page_host and ("builtin.com" in page_host or "builtinseattle.com" in page_host or "builtinvancouver.org" in page_host):
        have_structured = bool(d.get("Salary Range") or d.get("Salary Text"))
        if not have_structured:
            # If a later pass guessed a salary but it's tiny (e.g. $12)
            # and there's no "hour" / "hr" context, treat it as noise and drop it.
            s_from = d.get("Salary From")
            s_text = (d.get("Salary Text") or "").lower()
            if (
                isinstance(s_from, (int, float))
                and s_from < 1000
                and "hour" not in s_text
                and "/hr" not in s_text
                and "per hour" not in s_text
            ):
                for k in ["Salary From", "Salary To", "Salary Range", "Salary Text", "Salary Source"]:
                    d.pop(k, None)


    # EdTech specific: page_text includes "Similar Jobs" with other jobs and salaries.
    # For this host we trust the main description fields and drop page_text.
    if "edtech.com" in phost:
        page_text = ""

    blob_parts = [
        d.get("page_text") or "",
        d.get("Description") or "",
        d.get("Description Snippet") or "",
    ]

    # Built In: include raw HTML because salary ranges are often split across spans
    if page_host and ("builtin.com" in page_host or "builtinseattle.com" in page_host or "builtinvancouver.org" in page_host):
        blob_parts.append(d.get("html_raw") or "")

    blob = " ".join(str(x or "") for x in blob_parts)
    blob_lower = blob.lower()

    # DEBUG: confirm what salary text we are scanning
    if page_host and "builtinvancouver.org" in page_host:

        range_rx = re.compile(
            r"\$\s*(?P<min>[\d,]+(?:\.\d+)?)\s*"
            r"(?:-|–|—|to|\u2014|\u2013)\s*"
            r"\$\s*(?P<max>[\d,]+(?:\.\d+)?)\s*"
            r"(?P<cur>cad|usd)?",
            re.IGNORECASE
        )

        flat = blob
        flat = re.sub(r"</span>\s*<span[^>]*>", "", flat, flags=re.I)
        flat = re.sub(r"<[^>]+>", " ", flat)
        flat = re.sub(r"\s+", " ", flat)
        flat_lower = flat.lower()

        m = range_rx.search(flat)
        if m:
            lo = int(float(m.group("min").replace(",", "")))
            hi = int(float(m.group("max").replace(",", "")))

            window = flat_lower[max(0, m.start() - 160): min(len(flat_lower), m.end() + 160)]

            if any(w in window for w in ("compensation", "pay range", "salary", "base salary", "annual")):

                cur = (m.group("cur") or "").upper()
                if not cur:
                    cur = "CAD" if ("cad" in window or "cad" in blob_lower) else "USD"

                d["Salary From"] = lo
                d["Salary To"] = hi
                d["salary_min"] = lo
                d["salary_max"] = hi
                d["Salary Currency"] = cur
                d["Salary Unit"] = "YEAR"

                d["Salary Range"] = f"{cur} {lo:,} - {hi:,}"
                d["Salary Text"] = d["Salary Range"]
                d["Salary Source"] = d.get("Salary Source") or "builtin_range"
                d["Salary Placeholder"] = ""

                d["Salary Max Detected"] = hi
                d["Salary Status"] = "detected_range"
                d["Salary Note"] = "Built In Vancouver pay range detected"

                # Fill the extra columns you want in Sheets
                d["Salary Rule"] = d.get("Salary Rule") or "builtin_range"

                if SALARY_FLOOR and hi < SALARY_FLOOR and (not SOFT_SALARY_FLOOR or hi >= SOFT_SALARY_FLOOR):
                    d["Salary Near Min"] = hi
                else:
                    d["Salary Near Min"] = ""

                d["Salary Est. (Low-High)"] = d.get("Salary Est. (Low-High)") or _fmt_est(cur, lo, hi)

                #try:
                    #log_line("BIV DEBUG", f"{DOT6}✅ Promoted range: {d['Salary Range']}")
                #except Exception:
                    #pass

                return d

            #else:
                #try:
                    #log_line("BIV DEBUG", f"⚠️ Range matched but context gate failed: {window!r}")
                #except Exception:
                    #pass

        else:
            # No range matched at all
            first_dollar = flat_lower.find("$")
            if first_dollar != -1:
                preview = flat_lower[max(0, first_dollar - 80) : first_dollar + 180]
            else:
                preview = flat_lower[:220]

            log_line(
                "BIV DEBUG",
                f"⚠️ No range match in flat blob preview={preview!r}"
            )


        # ==== WORKDAY salary extraction (simple and robust) ====
        #workday_min = None
        #workday_max = None
        workday_text = page_text  # you already defined page_text above

        if "Pay Range Minimum" in workday_text or "Pay Range Maximum" in workday_text:
            m1 = re.search(r"Pay Range Minimum.*?([\d,]+)", workday_text, re.I | re.S)
            m2 = re.search(r"Pay Range Maximum.*?([\d,]+)", workday_text, re.I | re.S)

            def _to_int(x: str | None) -> int | None:
                if not x:
                    return None
                try:
                    return int(x.replace(",", ""))
                except Exception:
                    return None

            workday_min = _to_int(m1.group(1) if m1 else None)
            workday_max = _to_int(m2.group(1) if m2 else None)

            if workday_max is not None:
                # Feed Workday into the generic path consistently
                d["Salary Max Detected"] = workday_max
                d["Salary Near Min"] = workday_min or ""
                d["Salary Status"] = "at_or_above"
                d["Salary Note"] = "Workday min/max detected"
                d["Salary Rule"] = d.get("Salary Rule") or "workday_range"

                cur_for_est = d.get("Salary Currency") or ("CAD" if "cad" in blob_lower else "")
                d["Salary Est. (Low-High)"] = d.get("Salary Est. (Low-High)") or _fmt_est(cur_for_est, workday_min, workday_max)

                return d



    def _looks_like_job_count(span: tuple[int, int]) -> bool:
        """Heuristic: filter out numbers that sit next to 'remote jobs' banner counts."""
        start, end = span
        ctx = blob_lower[max(0, start - 12): min(len(blob_lower), end + 16)]

        return any(
            phrase in ctx
            for phrase in (
                " remote jobs",
                " remote job",
                "remote-jobs",
                " employees",        # catches "289,097 employees"
                " employee count",
            )
        )

    # 3) Salary-language signal, even if there are no numbers
    signal_blob = blob_lower
    has_signal = any(
        phrase in signal_blob
        for phrase in (
            "competitive base salary",
            "competitive salary",
            "highly competitive base salary",
            "very competitive base salary",
            "base salary",
            "salary range",
            "pay range",
            "compensation",
        )
    )

    # If we have salary language but no numbers, preserve the signal
    has_numeric = bool(
        isinstance(d.get("Salary From"), (int, float))
        or isinstance(d.get("Salary To"), (int, float))
        or d.get("salary_min")
        or d.get("salary_max")
        or d.get("Salary Max Detected")
    )


    if has_signal and not has_numeric:
        # Do not overwrite a better existing text value
        if not (d.get("Salary Text") or "").strip():
            # Pick a friendly label based on what we saw
            if "competitive" in signal_blob:
                d["Salary Text"] = "Competitive salary (no range listed)"
                d["Salary Range"] = "Competitive salary"
            elif "pay range" in signal_blob or "salary range" in signal_blob:
                d["Salary Text"] = "Salary range mentioned (no numbers listed)"
                d["Salary Range"] = "Salary range mentioned"
            else:
                d["Salary Text"] = "Compensation mentioned (no numbers listed)"
                d["Salary Range"] = "Compensation mentioned"

            d["Salary Source"] = d.get("Salary Source") or "signal_only"


    # Collect known job-id numbers so we can filter them out of salary candidates.
    job_id_numbers: set[int] = set()
    job_id_raw = d.get("job_id_numeric")
    try:
        job_id_val = int(str(job_id_raw).strip()) if job_id_raw not in (None, "") else None
    except Exception:
        job_id_val = None
    if job_id_val:
        job_id_numbers.add(job_id_val)

    job_id_pattern = re.compile(r"(?i)\b(?:req|requisition|job id|jobid|job req)[^\d]{0,8}(\d{4,})")
    for match in job_id_pattern.finditer(blob):
        try:
            job_id_numbers.add(int(match.group(1).lstrip("0") or "0"))
        except ValueError:
            continue

    # 4) Numeric candidates
    dollar_candidates: list[int] = []
    k_candidates: list[int] = []


    # Seed candidates with any explicit Workday min / max if present.
    # Some runs hit an UnboundLocalError on workday_min/workday_max due to scoping,
    # so guard access and fall back to None.
    try:
        wm = workday_min
    except UnboundLocalError:
        wm = None

    try:
        wx = workday_max
    except UnboundLocalError:
        wx = None

    if isinstance(wm, int) and wm > 0:
        candidates.append(wm)
    if isinstance(wx, int) and wx > 0:
        candidates.append(wx)


    # Pattern like "$120,000" or "120,000".
    # On The Muse *and* Remotive we require an actual "$" so footer ZIPs
    # and "Unlock 69,133 Remote Jobs" do not get picked up.
    force_dollar = (
        ("themuse.com" in phost)
        or ("remotive.com" in phost)
        or ("builtin.com" in phost)
        or ("builtinseattle.com" in phost)
        or ("builtinvancouver.org" in phost)
    )

    if force_dollar:
        dollar_pattern = r"\$\s*([\d][\d,]{2,})"   # require '$' for Muse + Remotive
    else:
        dollar_pattern = r"\$?\s*([\d][\d,]{2,})"  # keep old behavior elsewhere

    for m in re.finditer(dollar_pattern, blob):
        try:
            val = int(m.group(1).replace(",", ""))
            # Keep plausible annual salaries only: 20k–1M
            if _looks_like_job_count(m.span()):
                continue
            if 20_000 <= val <= 1_000_000 and val not in job_id_numbers:
                dollar_candidates.append(val)
        except Exception:
            continue

    # Pattern like "120k", "120 k", "120k+"
    for m in re.finditer(r"(\d{2,3})\s*k\b", blob_lower):
        try:
            digits = int(m.group(1))
            # Ignore classic retirement-plan references like 401k, 403k, 404k, 457k
            if digits in (401, 403, 404, 457):
                continue
            val = digits * 1_000
            # Again: plausible annual salaries only
            if _looks_like_job_count(m.span()):
                continue
            if 20_000 <= val <= 1_000_000 and val not in job_id_numbers:
                k_candidates.append(val)
        except Exception:
            continue

    # If we saw any dollar based salaries, trust those and ignore plain "120k" style counts
    if dollar_candidates:
        candidates = dollar_candidates
    else:
        candidates = k_candidates

    # Drop known Job ID numbers from the candidate list
    if job_id_numbers:
        candidates = [c for c in candidates if c not in job_id_numbers]

    # Fallback: if no direct numeric candidates, try generic range/single extractor on the full blob,
    # then on raw HTML if available.
    max_detected = max(candidates) if candidates else None
    if max_detected is None and blob:
        lo, hi = extract_salary_from_text(blob)
        if hi:
            max_detected = hi
    if max_detected is None:
        html_raw = d.get("html_raw", "")
        if html_raw:
            lo, hi = extract_salary_from_text(html_raw)
            if hi:
                max_detected = hi

    # 5) Derive flags/columns
    status       = ""
    note         = ""
    near_min_val = ""
    placeholder  = (d.get("Salary Placeholder") or "").strip()

    if max_detected is not None:
        # We have a plausible numeric salary
        if SOFT_SALARY_FLOOR and max_detected < SOFT_SALARY_FLOOR:
            status = "below_floor"
            note = f"Detected max ${max_detected:,} below soft floor"
        elif SALARY_FLOOR and max_detected < SALARY_FLOOR:
            status = "near_min"
            near_min_val = max_detected
            note = f"Detected max ${max_detected:,} between soft floor and floor"
        else:
            status = "at_or_above"
            note = f"Detected max ${max_detected:,} at or above floor"

        page_text = d.get("page_text") or ""

        # Final sanity check so we do not treat values like "50,000 technologists"
        # as salary. Only keep max_detected if it appears in valid salary context.
        if max_detected:
            try:
                variants = _salary_variants(int(max_detected))

                if not any(_salary_number_has_good_context(blob, v) for v in variants):
                    max_detected = None
                    near_min_val = ""
                    status = "missing"
                    note = "No valid salary context found"
            except Exception:
                # If context checking crashes, fall back safely
                max_detected = None
                near_min_val = ""
                status = "missing"
                note = "Salary context check failed"

        # Only finalize numeric salary fields if we still have a numeric salary
        if max_detected:
            d["Salary Max Detected"] = max_detected or ""
            d["Salary Near Min"]     = near_min_val
            d["Salary Status"]       = status
            d["Salary Note"]         = note
            d["Salary Placeholder"]  = ""  # real number beats placeholder

            # Fill the extra columns you want in Sheets
            d["Salary Rule"] = d.get("Salary Rule") or _salary_rule_from_status(status, d.get("Salary Source") or "")

            cur_for_est = d.get("Salary Currency") or ("CAD" if "cad" in blob_lower else "")
            d["Salary Est. (Low-High)"] = d.get("Salary Est. (Low-High)") or _fmt_est(cur_for_est, None, int(max_detected))

            return d


    # 6) No usable numeric salary: fall back to signal-only / missing
    already_numeric = bool(
        d.get("Salary Max Detected")
        or d.get("salary_max")
        or d.get("salary_min")
        or d.get("Salary To")
        or d.get("Salary From")
    )

    if has_signal and not already_numeric:
        status      = "signal_only"
        placeholder = placeholder or "Competitive salary"
        note        = "Salary language present but no concrete numbers"

        # NEW: treat signal as a real salary artifact for UI + BuiltIn checks
        d["Salary"]        = d.get("Salary") or placeholder
        d["Salary Text"]   = d.get("Salary Text") or placeholder
        d["Salary Range"]  = d.get("Salary Range") or placeholder
        d["Salary Source"] = d.get("Salary Source") or "signal_only"
    else:
        status      = "missing"
        placeholder = ""
        note        = "No salary information detected"

    if status in ("missing", "signal_only"):
        d["Salary Max Detected"] = ""
        d["Salary Near Min"]     = ""
        d["Salary Status"]       = status
        d["Salary Note"]         = note
        d["Salary Placeholder"]  = placeholder

    return d


# --- Auto-scroll helper for infinite-scroll listings (EdTech etc.) ---
def _autoscroll_listing(page,
                        link_css='a[href^="/jobs/"]:not([href$="-jobs"])',
                        max_loops=1100,
                        idle_ms=2000):
    last_count = 0
    stable_rounds = 0
    last_height = 0

    for _ in range(max_loops):
        # count current links
        count = len(page.query_selector_all(link_css))

        # scroll down
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(idle_ms)

        # wait until page height grows or times out
        try:
            page.wait_for_function(
                "document.body.scrollHeight > __last",
                arg=page.evaluate("document.body.scrollHeight"),
                timeout=1000
            )
        except Exception:
            pass  # okay if it didn’t grow this round

        # recompute count
        new_count = len(page.query_selector_all(link_css))

        if new_count == count:
            stable_rounds += 1
        else:
            stable_rounds = 0

        if stable_rounds >= 2:
            break

        last_count = new_count

    return len(page.query_selector_all(link_css))


from playwright.sync_api import sync_playwright
from urllib import parse as up
import re
import time

TRANSIENT_NET_MARKERS = (
    "ERR_INTERNET_DISCONNECTED",
    "ERR_CONNECTION_RESET",
    "ERR_CONNECTION_CLOSED",
    "ERR_TIMED_OUT",
)

PW_SUCCESS = 0
PW_FAIL = 0
REQ_FALLBACK = 0


def fetch_html_with_playwright(url, user_agent=USER_AGENT, engine="chromium"):
    """
    Fetch HTML for a URL using Playwright, with:
      - Transient network retries on page.goto
      - Host specific waits and scrolling
      - Optional stats counters (PW_SUCCESS, PW_FAIL, REQ_FALLBACK) if defined
    """
    if sync_playwright is None:
        return None

    # network errors we treat as transient and worth retrying
    TRANSIENT_NET_MARKERS = (
        "ERR_INTERNET_DISCONNECTED",
        "ERR_CONNECTION_RESET",
        "ERR_CONNECTION_CLOSED",
        "ERR_TIMED_OUT",
    )

    try:
        with sync_playwright() as p:
            browser_type = getattr(p, engine)  # "chromium" | "firefox" | "webkit"
            browser = browser_type.launch(headless=True, args=["--no-sandbox"])
            context = browser.new_context(user_agent=user_agent)
            page = context.new_page()

            try:
                # ---------------------------
                # 1. Robust page.goto with retry
                # ---------------------------
                last_exc = None
                for attempt in range(1, 4):  # up to 3 attempts
                    try:
                        resp = page.goto(
                            url,
                            timeout=PW_GOTO_TIMEOUT,
                            wait_until="domcontentloaded",
                        )

                        """ removed 20261215 - this was just for debugging YC routing and block detection, but it was too noisy in the logs
                        # DEBUG: YC routing and block detection (runs only when goto succeeded)
                        if "ycombinator.com" in (up.urlparse(url).netloc or "").lower():
                            status = resp.status if resp else None
                            log_event("DEBUG", f"YC goto status={status} requested={url} final={page.url}")
                            try:
                                log_event("DEBUG", f"YC page title: {page.title()}")
                            except Exception:
                                pass

                        last_exc = None
                        break
                        """

                    except Exception as e:
                        last_exc = e
                        msg = str(e)
                        is_transient = any(m in msg for m in TRANSIENT_NET_MARKERS)
                        if is_transient and attempt < 3:
                            log_event(
                                "WARN",
                                (
                                    f"Playwright network issue on attempt {attempt} for "
                                    f"{url} ({e.__class__.__name__}): {e}. Retrying..."
                                ),
                            )
                            time.sleep(3 * attempt)
                            continue

                        # non transient or last attempt
                        raise

                # if all attempts failed, re raise last exception
                if last_exc is not None:
                    raise last_exc

                # ---------------------------
                # 2. Host and path info
                # ---------------------------
                try:
                    parsed = up.urlparse(url)
                    host = parsed.netloc.lower()
                    path = parsed.path or "/"
                    #log_event("DEBUG", f"Playwright host={host} path={path} url={url}")    ignored 20251215
                except Exception:
                    host = ""
                    path = "/"

                """ removed 20260108 - this was just for debugging YC routing and block detection, but it was too noisy in the logs
                if "ycombinator.com" in host:
                    log_event("DEBUG", f"PW YC fetch active: {url}")
                 """


                # ---------------------------
                # 3. Host specific behavior
                # ---------------------------
                try:
                    if host.endswith("jobs.ashbyhq.com"):
                        # Wait until job cards or links render
                        page.wait_for_selector(
                            "a[href*='/jobs/']:not([href$='/jobs'])",
                            timeout=PW_WAIT_TIMEOUT * 2,
                        )
                        # Gentle scroll to trigger lazy loads
                        page.mouse.wheel(0, 2500)
                        page.wait_for_timeout(800)

                    elif host.endswith("myworkdayjobs.com") or host.endswith("myworkdaysite.com"):
                        # Workday often needs a bit of extra time
                        page.wait_for_timeout(1200)

                    elif host.endswith("ycombinator.com") or host.endswith("www.ycombinator.com"):
                        # YC is React. We need to wait for the rendered job header.
                        page.wait_for_selector(
                            "h1.ycdc-section-title",
                            timeout=PW_WAIT_TIMEOUT * 2,
                        )
                        # Optional: small pause to let adjacent fields render consistently
                        page.wait_for_timeout(300)


                    elif host.endswith("wellfound.com"):
                        page.wait_for_selector(
                            "a[href^='/jobs/'], a[href^='/l/']",
                            timeout=PW_WAIT_TIMEOUT * 2,
                        )
                        page.mouse.wheel(0, 3000)
                        page.wait_for_timeout(800)

                    elif host.endswith("dice.com") or host.endswith("www.dice.com"):
                        # Wait for job cards rendered by JS
                        page.wait_for_selector(
                            "a[href*='/job-detail/']",
                            timeout=PW_WAIT_TIMEOUT * 2,
                        )
                        page.mouse.wheel(0, 4000)
                        page.wait_for_timeout(800)

                    elif host.endswith("welcometothejungle.com"):
                        # Wait for the main content
                        page.wait_for_selector(
                            "main, [data-testid='job-offer']",
                            timeout=PW_WAIT_TIMEOUT * 2,
                        )

                        # Click visible "View more" expanders so hidden sections load
                        try:
                            # Role-based locator first
                            buttons = page.get_by_role(
                                "button",
                                name=re.compile(r"view more", re.I),
                            )
                            count = buttons.count()
                            if count:
                                for i in range(min(count, 4)):
                                    try:
                                        buttons.nth(i).click()
                                        page.wait_for_timeout(300)
                                    except Exception:
                                        pass

                            # Fallback to common WTTJ expanders
                            for sel in [
                                "button:has-text('View more')",
                                "[role='button']:has-text('View more')",
                                "button[data-testid='show-more']",
                            ]:
                                els = page.locator(sel)
                                n = els.count()
                                if n:
                                    for i in range(min(n, 4)):
                                        try:
                                            els.nth(i).click()
                                            page.wait_for_timeout(300)
                                        except Exception:
                                            pass

                            # Let the DOM settle
                            page.wait_for_timeout(500)
                        except Exception:
                            pass

                except Exception:
                    # do not fail the run on host specific tweaks
                    pass

                # ---------------------------
                # 4. EdTech listing autoscroll
                # ---------------------------
                try:
                    needs_autoscroll = (
                        host in {"edtech.com", "www.edtech.com"}
                        and "/jobs/" in path
                        and path.endswith("-jobs")
                    )

                    if needs_autoscroll:
                        # click "More" buttons first if present
                        try:
                            for _ in range(50):
                                btn = page.query_selector(
                                    "button:has-text('More'), "
                                    "a:has-text('More'), "
                                    "button:has-text('Load'), "
                                    "a:has-text('Load')"
                                )
                                if not btn:
                                    break
                                btn.click()
                                page.wait_for_timeout(900)
                        except Exception:
                            pass

                        # then deep autoscroll until stable
                        _autoscroll_listing(
                            page,
                            link_css='a[href^="/jobs/"]:not([href$="-jobs"])',
                            max_loops=1100,
                            idle_ms=2000,
                        )
                except Exception:
                    # do not fail run if autoscroll logic hiccups
                    pass

                # ---------------------------
                # 5. Generic waits for common job structures
                # ---------------------------
                try:
                    page.wait_for_selector(
                        "a[href^='/job/'], "
                        "a[href*='/jobs/'], "
                        "a[href*='/remote-jobs/'], "
                        "a:has(h2), "
                        "a:has(h3), "
                        "main article",
                        timeout=PW_WAIT_TIMEOUT,
                    )
                except Exception:
                    pass

                # wait for common embedded boards if present
                try:
                    page.wait_for_selector(
                        "script[src*='greenhouse.io/embed/job_board'], "
                        "iframe[src*='greenhouse'], "
                        "a[href*='jobs.lever.co'], "
                        "a[href*='ashbyhq.com']",
                        timeout=PW_WAIT_TIMEOUT,
                    )
                except Exception:
                    # fine to fall back to whatever is loaded
                    pass

                # Built In is JS heavy. In bulk runs we can capture the pre hydration shell.
                # Wait briefly for any post hydration signal before reading page.content().
                try:
                    cur_url = page.url or ""
                except Exception:
                    cur_url = ""

                if ("builtin.com/job/" in cur_url) or ("builtinseattle.com/job/" in cur_url) or ("builtinvancouver.org/job/" in cur_url) or ("builtin.com" in (cur_url or "")) or ("builtinseattle.com" in (cur_url or "")) or ("builtinvancouver.org" in (cur_url or "")):
                    try:
                        page.wait_for_function(
                            """() => {
                                const html = document.documentElement && document.documentElement.innerHTML ? document.documentElement.innerHTML : "";
                                if (html.includes("Builtin.jobPostInit")) return true;
                                if (html.includes('type="application/ld+json"')) return true;
                                if (document.querySelector("span[data-bs-toggle='tooltip']")) return true;
                                return false;
                            }""",
                            timeout=15000,
                        )
                    except Exception:
                        pass
                    # Seattle pages sometimes hydrate later but expose stable DOM markers first.
                    if "builtinseattle.com/job/" in (cur_url or ""):
                        try:
                            page.wait_for_selector(
                                "[data-id='company-title'], div[data-id='job-card'] h1",
                                timeout=15000,
                            )
                        except Exception:
                            pass

                # ---------------------------
                # 6. Capture HTML and bump counters
                # ---------------------------
                html = page.content()

                # Built In Seattle can intermittently return a partial shell first
                # (title present, but no JSON-LD / jobPostInit / company node yet).
                if "builtinseattle.com/job/" in (cur_url or ""):
                    def _sea_has_strong_job_signals(h: str) -> bool:
                        h_low = (h or "").lower()
                        return (
                            ("Builtin.jobPostInit" in (h or ""))
                            or ("hiringOrganization" in (h or ""))
                            or ('data-id="company-title"' in (h or ""))
                            or ("job-post-body-" in h_low)
                            or ('<meta name="description"' in h_low)
                        )

                    if not _sea_has_strong_job_signals(html):
                        for _ in range(3):  # additive wait budget ~9s
                            try:
                                page.wait_for_timeout(3000)
                            except Exception:
                                break
                            html = page.content()
                            if _sea_has_strong_job_signals(html):
                                break

                # If we still got a tiny shell, try one reload once.
                if (("builtin.com/job/" in cur_url) or ("builtinseattle.com/job/" in cur_url)) and (not html or len(html) < 50000):
                    try:
                        page.reload(wait_until="domcontentloaded")
                        try:
                            page.wait_for_function(
                                """() => {
                                    const html = document.documentElement && document.documentElement.innerHTML ? document.documentElement.innerHTML : "";
                                    if (html.includes("Builtin.jobPostInit")) return true;
                                    if (html.includes('type="application/ld+json"')) return true;
                                    if (document.querySelector("span[data-bs-toggle='tooltip']")) return true;
                                    return false;
                                }""",
                                timeout=15000,
                            )
                        except Exception:
                            pass
                        if "builtinseattle.com/job/" in (cur_url or ""):
                            try:
                                page.wait_for_selector(
                                    "[data-id='company-title'], div[data-id='job-card'] h1",
                                    timeout=15000,
                                )
                            except Exception:
                                pass
                        html = page.content()
                        if "builtinseattle.com/job/" in (cur_url or ""):
                            # One more short poll after reload for slower Seattle hydration.
                            for _ in range(2):
                                html_low = (html or "").lower()
                                if (
                                    "Builtin.jobPostInit" in (html or "")
                                    or "hiringOrganization" in (html or "")
                                    or 'data-id="company-title"' in (html or "")
                                    or "job-post-body-" in html_low
                                    or '<meta name="description"' in html_low
                                ):
                                    break
                                try:
                                    page.wait_for_timeout(2500)
                                except Exception:
                                    break
                                html = page.content()
                    except Exception:
                        pass


                # optional counters if you define them globally
                try:
                    g = globals()
                    if "PW_SUCCESS" in g:
                        g["PW_SUCCESS"] += 1
                except Exception:
                    pass

                #log_event("DEBUG", f"Playwright returned HTML for {url}")       ignored 20251215
                return html

            finally:
                # always clean up Playwright resources
                try:
                    context.close()
                except Exception:
                    pass
                try:
                    browser.close()
                except Exception:
                    pass

    except Exception as e:
        # optional failure / fallback counters
        try:
            g = globals()
            if "PW_FAIL" in g:
                g["PW_FAIL"] += 1
            if "REQ_FALLBACK" in g:
                g["REQ_FALLBACK"] += 1
        except Exception:
            pass

        msg = (
            f"Playwright failed on {url} ({e.__class__.__name__}):\n"
            f"{e}\n"
            "Falling back to requests."
        )
        for ln in msg.splitlines():
            log_event("WARN", ln)
        return None


def _is_partial_builtinseattle_job_shell(url: str, html: str | None) -> bool:
    try:
        u = str(url or "")
        h = str(html or "")
        if "builtinseattle.com/job/" not in u.lower():
            return False
        if not h:
            return False
        h_low = h.lower()
        has_title = "<title" in h_low
        has_jobpostinit = "Builtin.jobPostInit" in h
        has_jsonld = ('type="application/ld+json"' in h) or ("hiringOrganization" in h)
        has_company_node = 'data-id="company-title"' in h
        has_meta_desc = '<meta name="description"' in h_low
        has_job_body = "job-post-body-" in h_low
        # The problematic shell consistently has title but lacks real job signals.
        return has_title and not any([has_jobpostinit, has_jsonld, has_company_node, has_meta_desc, has_job_body])
    except Exception:
        return False


def get_html(url):
    domain = up.urlparse(url).netloc.lower()
    if domain in PLAYWRIGHT_DOMAINS:
        html = fetch_html_with_playwright(url)
        if _is_partial_builtinseattle_job_shell(url, html):
            try:
                log_line("DEBUG", f"[BIVDBG] Seattle partial shell detected, retrying Playwright once: {url}")
            except Exception:
                pass
            html_retry = fetch_html_with_playwright(url)
            if html_retry and not _is_partial_builtinseattle_job_shell(url, html_retry):
                html = html_retry
            else:
                # Seattle-only additive fallback: some pages are server-rendered well enough via requests.
                try:
                    resp = polite_get(url)
                except Exception:
                    resp = None
                html_req = resp.text if resp else None
                if html_req and not _is_partial_builtinseattle_job_shell(url, html_req):
                    try:
                        log_line("DEBUG", f"[BIVDBG] Seattle partial shell resolved via requests fallback: {url}")
                    except Exception:
                        pass
                    html = html_req
                else:
                    # One more PW try for intermittent Seattle pages (kept Seattle-only).
                    html_retry2 = fetch_html_with_playwright(url)
                    if html_retry2 and not _is_partial_builtinseattle_job_shell(url, html_retry2):
                        html = html_retry2
        return html  # do not attempt requests() fallback for PW-only sites
    resp = polite_get(url)
    return resp.text if resp else None

def _find_ats_boards(html: str) -> list[str]:
    boards = []

    # Greenhouse: both JS and embed variants
    for m in re.findall(r'https?://boards\.greenhouse\.io/(?:embed/job_board|embed/job_board/js|job_board/js)\?for=([a-z0-9\-]+)', html, flags=re.I):
        boards.append(f"https://boards.greenhouse.io/embed/job_board?for={m}")

    # Also catch direct /<company> style if present (rare on GH)
    for m in re.findall(r'https?://boards\.greenhouse\.io/([a-z0-9\-]+)/?', html, flags=re.I):
        boards.append(f"https://boards.greenhouse.io/embed/job_board?for={m}")

    # Lever: jobs.lever.co/<company>
    for m in re.findall(r'https?://jobs\.lever\.co/([a-z0-9\-]+)', html, flags=re.I):
        boards.append(f"https://jobs.lever.co/{m}")

    # De-dupe
    return sorted(set(boards))

def expand_career_sources():
    """Return a list of ATS job board URLs discovered on company careers pages."""
    pages = []
    for url in CAREER_PAGES:
        progress_clear_if_needed()
        _bk_log_wrap("[CAREERS", f" ]{DOT3}Probing {url}")
        html = get_html(url)
        if not html:
            #log_print("[WARN", f" ]{DOT3}{DOTW}Warning: Failed to GET listing page: {url}")

            progress_clear_if_needed()
            _bk_log_wrap("[WARN", f" ]{DOT3}{DOTW}Could not fetch: {url}")
            continue


        soup = BeautifulSoup(html, "html.parser")
        found = set()

        # Scan visible links
        for a in soup.find_all("a", href=True):
            full = up.urljoin(url, a["href"])
            host = up.urlparse(full).netloc.lower()
            path = up.urlparse(full).path.lower()

            # 1) Greenhouse embedded via <iframe>
            for iframe in soup.find_all("iframe", src=True):
                src = iframe["src"]
                if "greenhouse" in src and "for=" in src:
                    qs = up.parse_qs(up.urlparse(src).query)
                    slug = qs.get("for", [None])[0]
                    if slug:
                        found.add(f"https://job-boards.greenhouse.io/embed/job_board?for={slug}")

            # 2) Plain text fallback (some sites inline the URL in data blobs)
            raw = html or ""
            for m in re.findall(r"https?://(?:boards|job-boards)\.greenhouse\.io/[^\"'<> ]+", raw, flags=re.I):
                # normalize any /<company>/jobs style to embed if needed
                g = re.search(r"(?:boards|job-boards)\.greenhouse\.io/([^/]+)/", m)
                if g:
                    found.add(f"https://job-boards.greenhouse.io/embed/job_board?for={g.group(1)}")
            for m in re.findall(r"https?://jobs\.lever\.co/([a-z0-9\-]+)", raw, flags=re.I):
                found.add(f"https://jobs.lever.co/{m}")
            for m in re.findall(r"https?://jobs\.ashbyhq\.com/([a-z0-9\-]+)/?", raw, flags=re.I):
                found.add(f"https://jobs.ashbyhq.com/{m}")

            # Greenhouse
            if "boards.greenhouse.io" in host or "job-boards.greenhouse.io" in host:
                m = re.search(r"(?:boards|job-boards)\.greenhouse\.io/([^/]+)/jobs", full)
                if m:
                    slug = m.group(1)
                    found.add(f"https://job-boards.greenhouse.io/embed/job_board?for={slug}")
                elif "embed/job_board" in path and "for=" in full:
                    found.add(full)

            # Lever
            elif "jobs.lever.co" in host:
                m = re.search(r"jobs\.lever\.co/([^/]+)/?", full)
                if m:
                    found.add(f"https://jobs.lever.co/{m.group(1)}")

            # Ashby
            elif "jobs.ashbyhq.com" in host:
                m = re.search(r"jobs\.ashbyhq\.com/([^/]+)/", full)
                if m:
                    found.add(f"https://jobs.ashbyhq.com/{m.group(1)}")

        # Greenhouse embed <script src="...embed/job_board/js?for=<slug>">
        for s in soup.find_all("script", src=True):
            src = s["src"]
            if "greenhouse.io/embed/job_board" in src and "for=" in src:
                qs = up.parse_qs(up.urlparse(src).query)
                slug = qs.get("for", [None])[0]
                if slug:
                    found.add(f"https://job-boards.greenhouse.io/embed/job_board?for={slug}")

        if found:
            progress_clear_if_needed()
            _bk_log_wrap("[CAREERS", f" ]{DOT6}{len(found)} board(s) found on {url}")
            pages.extend(sorted(found))
        else:
            progress_clear_if_needed()
            _bk_log_wrap("[CAREERS", f" ]{DOT6}No ATS links found on {url}")

    return pages

import json
from datetime import datetime, timedelta, timezone

def now_ts() -> str:
    # ISO-like stamp sortable to the second
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _coerce_list(x): return x if isinstance(x, list) else ([] if x is None else [x])

def _short(s, width=100):
    s = " ".join((s or "").split())
    return (s[: width - 1] + "…") if len(s) > width else s

_POSTED_REL_RX = re.compile(
    r"^\s*(?:about\s+|approximately\s+)?(\d+)\s*(minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)\s*(?:ago)?\s*$",
    re.I,
)

def _posted_label_to_iso_date(posted: str, now: datetime | None = None) -> str | None:
    """
    Convert labels like:
      - "Posted 5 days ago"
      - "5 days ago"
      - "Today" / "Posted Today"
      - "Yesterday" / "Posted Yesterday"
      - "Jan 3, 2026"
    into "YYYY-MM-DD".
    """
    if not posted:
        return None

    s = " ".join(str(posted).strip().split())
    if not s:
        return None

    # Normalize common prefixes
    low = s.lower()
    if low.startswith("posted "):
        s = s[7:].strip()
        low = s.lower()

    now = now or datetime.now()

    if low in ("today", "just now"):
        return now.strftime("%Y-%m-%d")

    if low == "yesterday":
        return (now - timedelta(days=1)).strftime("%Y-%m-%d")

    m = _POSTED_REL_RX.match(low)
    if m:
        n = int(m.group(1))
        unit = m.group(2).lower()

        if "minute" in unit or "hour" in unit:
            return now.strftime("%Y-%m-%d")
        if "day" in unit:
            return (now - timedelta(days=n)).strftime("%Y-%m-%d")
        if "week" in unit:
            return (now - timedelta(days=7 * n)).strftime("%Y-%m-%d")
        if "month" in unit:
            return (now - timedelta(days=30 * n)).strftime("%Y-%m-%d")
        if "year" in unit:
            return (now - timedelta(days=365 * n)).strftime("%Y-%m-%d")

    # Absolute dates fallback
    try:
        dt = dateparser.parse(s, fuzzy=True)
        if dt:
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    return None


def parse_jobposting_ldjson(html: str) -> dict:
    """Parse schema.org JobPosting JSON-LD and pull out title, company, locations, dates."""
    soup = BeautifulSoup(html, "html.parser")

    def _set_if(d: dict, key: str, val):
        if val not in (None, "", []):
            d[key] = val

    out: dict = {}

    for s in soup.find_all("script", type="application/ld+json"):
        raw = s.string or s.text or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        # Handle list and @graph wrappers (The Muse style)
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            graph = data.get("@graph") or data.get("graph")
            if isinstance(graph, list):
                items = graph
            else:
                items = [data]
        else:
            continue

        for it in items:
            if not isinstance(it, dict):
                continue

            t = it.get("@type") or it.get("@type".lower())
            if isinstance(t, list):
                t = t[0] if t else None
            if not t or "jobposting" not in str(t).lower():
                continue

            # ----- Title -----
            raw_title = it.get("title") or it.get("Title")
            if raw_title:
                _set_if(out, "Title", normalize_title(raw_title))

            # ----- Company -----
            org = it.get("hiringOrganization") or it.get("hiringorganization")
            if isinstance(org, dict):
                _set_if(out, "Company", org.get("name"))

            # ----- Job location type -----
            _set_if(
                out,
                "job_location_type",
                it.get("jobLocationType") or it.get("joblocationtype") or "",
            )

            # ----- Locations -----
            locs: list[str] = []
            jl = it.get("jobLocation") or it.get("joblocation")
            if isinstance(jl, list):
                src = jl
            elif isinstance(jl, dict):
                src = [jl]
            else:
                src = []

            for loc in src:
                addr = loc.get("address") if isinstance(loc, dict) else None
                if isinstance(addr, dict):
                    city = addr.get("addressLocality") or addr.get("addresslocality")
                    region = addr.get("addressRegion") or addr.get("addressregion")
                    country = addr.get("addressCountry") or addr.get("addresscountry")
                    locs.append(", ".join([x for x in [city, region, country] if x]))
                elif isinstance(addr, str) and addr.strip():
                    locs.append(addr.strip())

            if (
                it.get("jobLocationType")
                in ("TELECOMMUTE", "https://schema.org/Telecommute")
                and "Remote" not in locs
            ):
                locs.append("Remote")

            if locs:
                _set_if(out, "locations", [l for l in locs if l])

            # ----- Applicant location requirements -> applicant_regions -----
            apps = (
                it.get("applicantLocationRequirements")
                or it.get("applicantlocationrequirements")
            )
            regions: list[str] = []

            def _as_list(x):
                return x if isinstance(x, list) else ([x] if x else [])

            for a in _as_list(apps):
                if isinstance(a, dict):
                    v = (
                        a.get("name")
                        or a.get("addressRegion")
                        or a.get("addressregion")
                        or a.get("addressCountry")
                        or a.get("addresscountry")
                    )
                    if isinstance(v, str) and v.strip():
                        regions.append(v.strip())
                elif isinstance(a, str) and a.strip():
                    regions.append(a.strip())

            if regions:
                _set_if(out, "applicant_regions", regions)

            # ====== DATES: this is the part that was missing ======

            # Example Muse / Remotive JSON-LD:
            # "datePosted": "2025-10-31T19:04:01"
            # "validThrough": "2025-12-30T19:04:01.000Z"
            dp = it.get("datePosted") or it.get("dateposted")
            if isinstance(dp, str) and dp.strip():
                # keep just the date part
                iso_date = dp.split("T")[0]
                _set_if(out, "date_posted", iso_date)
                _set_if(out, "posting_date", iso_date)

            vt = it.get("validThrough") or it.get("validthrough")
            if isinstance(vt, str) and vt.strip():
                iso_valid = vt.split("T")[0]
                _set_if(out, "valid_through", iso_valid)

            # Some boards include a relative text like "1 week ago"
            rel = it.get("postedAge") or it.get("posted_age")
            if isinstance(rel, str) and rel.strip():
                _set_if(out, "posted", rel.strip())

    return out


MUSE_DATE_POSTED_RX = re.compile(r'"datePosted"\s*:\s*"([^"]+)"')
MUSE_VALID_THROUGH_RX = re.compile(r'"validThrough"\s*:\s*"([^"]+)"')

def _extract_muse_dates_from_html(html: str) -> dict:
    """Pull datePosted / validThrough from The Muse job pages using regex."""
    out: dict = {}
    if not html:
        return out

    def _norm(raw: str) -> str:
        raw = str(raw)
        # date is always at the front of the timestamp: "YYYY-MM-DD..."
        m = re.match(r"(\d{4}-\d{2}-\d{2})", raw)
        return m.group(1) if m else raw

    m = MUSE_DATE_POSTED_RX.search(html)
    if m:
        iso = _norm(m.group(1))
        out["date_posted"] = iso
        out["posting_date"] = iso

    m = MUSE_VALID_THROUGH_RX.search(html)
    if m:
        iso = _norm(m.group(1))
        out["valid_through"] = iso

    return out

DATEPOSTED_HTML_RE = re.compile(r'"datePosted"\s*:\s*"([^"]+)"', re.IGNORECASE)
VALIDTHROUGH_HTML_RE = re.compile(r'"validThrough"\s*:\s*"([^"]+)"', re.IGNORECASE)

def _extract_dates_from_html(html: str) -> dict:
    """Fallback parser that pulls datePosted / validThrough out of raw HTML.

    This works even when the JobPosting JSON-LD is embedded as an escaped string
    inside another JSON blob (the pattern The Muse uses). It returns simple
    YYYY-MM-DD strings.
    """
    out: dict = {}
    if not html:
        return out

    # Handle pages where JSON is doubly-escaped with \" sequences
    text = html.replace('\\"', '"')

    def _pick_date(raw: str) -> str:
        # Grab the first YYYY-MM-DD we see
        m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
        return m.group(1) if m else raw.strip()

    m = DATEPOSTED_HTML_RE.search(text)
    if m:
        out["posting_date"] = _pick_date(m.group(1))

    m = VALIDTHROUGH_HTML_RE.search(text)
    if m:
        out["valid_through"] = _pick_date(m.group(1))

    return out

from urllib.parse import urljoin
from bs4 import BeautifulSoup

def extract_wttj_company_jobs(html: str, company_url: str) -> list[str]:
    """
    Given the HTML for a company page on app.welcometothejungle.com,
    return a list of absolute job URLs found on that page.
    """
    soup = BeautifulSoup(html, "html.parser")

    links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]

        # Only keep job detail links
        if "/jobs/" not in href:
            continue

        full_url = urljoin(company_url, href)
        if full_url not in links:
            links.append(full_url)

    return links



# --- Role taxonomy: allow ONLY these families (title-first), plus close neighbors by responsibility ---

# exact title hits we want
INCLUDE_TITLES_EXACT = [
    r"\bproduct owner\b",
    r"\bproduct manager\b",
    r"\bgroup product manager\b", r"\bstaff product manager\b", r"\bprincipal product manager\b",
    r"\bdirector of product management\b",
    r"\bproduct leader\b",
    r"\bbusiness analyst\b",
    r"\bsystems analyst\b",
    r"\bbusiness systems analyst\b",
    r"\bbusiness systems engineer\b",
    r"\bproduct business analyst\b",
    #r"\bbusiness technology analyst\b",
    r"\bscrum master\b",
    r"\brelease train engineer\b", r"\brte\b",

]

# looser title hits we will allow only if responsibilities also match
INCLUDE_TITLES_FUZZY = [
    r"\bproduct\s+operations?\b",
    r"\bprod\s*ops\b",
    r"\bproduct\s+analyst\b",
    r"\bbusiness\s+analyst\b",
    r"\bbusiness\s+system\s+analyst\b",
    r"\brequirements?\s+(?:analyst|engineer)\b",
    r"\bsolutions?\s+analyst\b",
    r"\bbusiness?\s+technology\s+analyst\b",
    r"\bimplementation\s+analyst\b",
    r"\btechnical\s+program\s+manager\b",       # only with responsibilities (see _is_target_role)
    r"\bbusiness\s+systems?\s+analyst\b",       # Business System(s) Analyst
    r"\bsystem?\s+analyst\b",                   # System(s) Analyst
    r"\boperations?\s+business\s+analyst\b",    # Operations Business Analyst
    r"(senior\s+)?business\s+(system|systems|intelligence)?\s*analyst",
    r"operations\s+business\s+analyst",
    r"(product\s+leader|product\s+specialist|product\s+consultant)\b",
]
#
# Potential allied/adjacent titles that are not exact matches but should
# contribute some score if present in the listing text.
#POTENTIAL_ALLIED_TITLES = [
#    r"\bproduct\s+operations?\b",
#    r"\bprod\s*ops\b",
#    r"\bproduct\s+analyst\b",
#    r"\bimplementation\s+analyst\b",
#    r"\bsolutions?\s+analyst\b",
#    r"\btechnical\s+program\s+manager\b",
#]

# titles we explicitly do NOT want
EXCLUDE_TITLES = [
    # Allow growth if paired with product roles; block growth-only or growth marketing
    r"\b(product\s+marketing|brand|demand\s+gen)\b",
    r"\bgrowth\b(?!.*product)",
    r"\b(project\s+manager)\b(?!.*\bproduct\b)",
    r"\b(data|financial|research|credit)\s+analyst\b",
    r"\bdata\s+(scientist|engineer)\b",
    r"\b(?:ml|ai)\s+(engineer|scientist)\b",
    r"\b(dev|backend|frontend|full[-\s]?stack|software|platform|sre|qa|test)\s+engineer\b",
    r"\bdesigner|ux|ui|visual\s+design|graphic\s+design\b",
    r"\b(sales|account\s+manager|customer\s+success|support)\b(?!.*\bproduct\b)",
    r"\bhr|recruiter|talent|people\s+ops\b",
    r"\b(finance|payroll|bookkeep|accountant)\b(?!.*\bproduct\b)",
    r"\boperations?\b(?!.*\bproduct\b)",
    r"\bintern\b",
]

# responsibility signals that look like PO/PM/BA/BSA/Scrum Master work
RESPONSIBILITY_SIGNALS = [
    r"\b(backlog|product\s+backlog)\b",
    r"\buser\s+stor(?:y|ies)\b", r"\bacceptance\s+criteria\b",
    r"\bprioriti[sz]e\b", r"\broadmap\b", r"\bdiscovery\b",
    r"\bstakeholders?\b", r"\bfacilitat(e|ing)\b", r"\balign(ment)?\b",
    r"\brequirements?\b.*\b(analysis|definition|elicitation|specification)\b",
    r"\bsprint(s)?\b", r"\bscrum\b", r"\bagile\b",
    r"\bimpact\b.*\bmetrics?\b|\bexperiments?\b|\bmvp\b",
]
RESP_SIG_RX = re.compile("|".join(RESPONSIBILITY_SIGNALS), re.I)

POS_SIGNALS = [
    r"\b(backlog|roadmap|prioriti[sz]e|discovery|user stor(y|ies)|acceptance criteria)\b",
    r"\b(stakeholder|cross[-\s]?functional|trade[-\s]?off|launch|mvp|experiments?)\b",
    r"\b(requirements?|compliance|regulatory|Title iv|fafsa|fisap|doe)\b",
]
NEG_SIGNALS = [
    r"\b(product marketing|brand|demand gen|campaign)\b",
    r"\bfinancial analyst\b", r"\bsalesforce\b", r"\baccounts? payable\b",
    r"\bfacilit(y|ies) operations?\b", r"\blaborator(y|ies)\b",
]

REMOTE_KEYWORDS = [
    r"\bremote\b", r"remote[-\s]?first", r"\b(us|u\.s\.)\s*remote\b",
    r"\banywhere\b.*\b(us|u\.s\.)\b", r"\bwork from home\b",
]
ONSITE_BLOCKERS = [r"\bon[-\s]?site\b", r"\boffice[-\s]?based\b"]

SINGLE_CITY_PATTERNS = [
    r"\bnew york( city)?\b|\bnyc\b", r"\bsan francisco\b|\bsf\b",
    r"\bseattle\b", r"\baustin\b", r"\blondon\b", r"\bparis\b",
    # add more as needed
]

# Helpers used by extract_job_details() to label remote/hybrid/onsite quickly
HYBRID_REGEX   = re.compile(r"\bhybrid\b", re.I)
ONSITE_REGEX   = re.compile(r"\b(on[-\s]?site|in[-\s]?office|office[-\s]?based)\b", re.I)
REMOTE_BADGE   = re.compile(r"\b(remote|telecommute|work[-\s]?from[-\s]?home|wfh)\b", re.I)



# --- Visibility / Confidence helpers ---
import re, datetime as dt

def has_recent(text_or_json: str, days=30) -> bool:
    if not text_or_json:
        return False
    m = re.search(r"(20\d{2}-\d{2}-\d{2})", text_or_json)
    if not m:
        return False
    try:
        d = dt.date.fromisoformat(m.group(1))
        return (dt.date.today() - d).days <= days
    except Exception:
        return False

# --- Simplified visibility/confidence computation ---
def compute_visibility_and_confidence(details):
    """
    Returns (visibility_status, confidence_score, confidence_mark)
    visibility_status: 'public' or 'quiet'
    confidence_score: 0-100
    confidence_mark: '🟢' (>=75), '🟠' (40-74), '🔴' (<40)
    """
    url      = (details.get("Job URL") or "").lower()
    company  = (details.get("Company") or "")
    posting  = (details.get("Posting Date") or "")
    salary   = details.get("Salary Max Detected")
    desc     = (details.get("Description Snippet") or "").lower()

    # --- visibility ---
    PUBLIC_HINTS = (
        "   ", "boards.greenhouse.io", "lever.co", "myworkdayjobs.com",
        "icims.com", "jobvite.com", "smartrecruiters.com", "adp.com"
    )
    QUIET_HINTS  = (
        "remotive", "remoteok", "nodesk", "weworkremotely", "ycombinator.com/jobs",
        "builtin.com/job", "/jobs/", "/remote-jobs/"
    )

    is_public = any(h in url for h in PUBLIC_HINTS)
    is_quiet  = any(h in url for h in QUIET_HINTS) and not is_public

    visibility = "public" if is_public else ("quiet" if is_quiet else "quiet")

    # --- base score by visibility ---
    score = 60 if visibility == "public" else 40

    # --- additive signals ---
    from datetime import datetime
    try:
        d = str(posting).split()[0]
        age_days = (datetime.today() - datetime.strptime(d, "%Y-%m-%d")).days
        if age_days <= 7:
            score += 12
        elif age_days <= 14:
            score += 8
        elif age_days <= 30:
            score += 4
    except Exception:
        pass

    if salary not in (None, "", "missing"):
        score += 6

    Title = (details.get("Title") or "").lower()
    role_hits = 0
    for kw in ("product manager", "product owner", "pm",
            "business analyst", "systems analyst", "business systems analyst", "system analyst",
            "scrum master", "release train engineer", "rte"):
        if kw in Title:
            role_hits += 1

    # Responsibility presence gives a small bump even if title is unfamiliar
    if RESP_SIG_RX.search((details.get("Description Snippet") or "").lower()):
        score += 5

    if (details.get("remote_rule") or "") == "default" and "remote" in (details.get("Location") or "").lower():
        score += 4
    if (details.get("us_rule") or "") == "default" and "us" in (details.get("Location") or "").lower():
        score += 3

    if any(s in Title for s in ("staff", "principal", "director", "vp")):
        score -= 3

    score = max(0, min(100, score))
    mark = "🟢" if score >= 75 else ("🟠" if score >= 40 else "🔴")
    return visibility, score, mark


# unique tracking to avoid double-appends across the run
_seen_kept_urls: set[str] = set()
_seen_skip_urls: set[str] = set()
kept_count: int = 0
skip_count: int = 0
# Track jobs we have already recorded (KEEP or SKIP) by a stable key
_seen_job_keys: set[str] = set()


def _log_keep_to_terminal(row: dict) -> None:
    """
    Render KEEP rows using the unified log_event layout so spacing/wrapping
    stays aligned with SKIP blocks.
    """
    title = _title_for_log(row, row.get("Job URL", ""))
    log_event("KEEP", title, right=row)


def _host(u: str) -> str:
    try:
        return up.urlparse(u).netloc.replace("www.", "")
    except Exception:
        return u

def log_info_processing(url: str):
    progress_clear_if_needed()
    c = LEVEL_COLOR.get("INFO", RESET)
    msg = "Processing listing page: " + url
    for ln in _wrap_lines(msg, width=120):
        log_print(f"{c}{_info_box()}.{ln}{RESET}")



def log_info_found(n: int, url: str, elapsed_s: float):
    progress_clear_if_needed()
    c = LEVEL_COLOR.get("INFO", RESET)
    host = _host(url)
    # Example: [🔎 FOUND 60           ].candidate job links on edtech.com in 71.7s
    log_print(f"{c}{_found_box(n)}.candidate job links on {host} in {elapsed_s:.1f}s{RESET}")
    # repaint the progress line if one is active
    #refresh_progress()


def log_info_done():
    progress_clear_if_needed()
    c = LEVEL_COLOR.get("INFO", RESET)
    # Example: [✔ DONE                ].
    log_print(f"{c}{_done_box()}" + f"{RESET}")
    # repaint the progress line if one is active
    #refresh_progress()


def _info_box() -> str:
    return _box("INFO")

def _found_box(n: int) -> str:
    # right-align the count to 3 spaces: 0..999
    return _box(f"🔎 FOUND {n:>3}")

def _done_box() -> str:
    return _box("✔ DONE")




# =====================================================================
# 🔍 Remote / Onsite detection + confidence constants
# =====================================================================


KEEP_REASON_FIELD = "Reason"           # lives only in Table1
SKIP_REASON_FIELD = "Reason Skipped"   # lives only in SKIPPED tab


SIMPLYHIRED_BASE = "https://www.simplyhired.com"
# Map job detail URL → title text from the SimplyHired listing page
SIMPLYHIRED_TITLES: dict[str, str] = {}


# Whitelists (always treat as remote)
SOURCE_WHITELIST_REMOTE = ["weworkremotely.com", "remoteok.com"]
COMPANY_ALWAYS_REMOTE = {"Automattic", "GitLab", "Zapier"}

def role_fit_score(Title: str, desc: str) -> int:
    t = (Title or "")
    full = (t + " " + (desc or "")[:2000]).lower()
    score = 0

    if any(re.search(p, t, re.I) for p in INCLUDE_TITLES_EXACT): score += 55
    if any(re.search(p, full, re.I) for p in INCLUDE_TITLES_FUZZY): score += 30
    if any(re.search(p, full, re.I) for p in POTENTIAL_ALLIED_TITLES): score += 20

    # Responsibilities boost
    resp_hits = len(set(m.group(0) for m in RESP_SIG_RX.finditer(full)))
    score += min(10 * resp_hits, 30)

    if any(re.search(p, full, re.I) for p in EXCLUDE_TITLES): score -= 40
    return max(0, min(100, score))


import datetime as dt

def careers_links_contains_job(careers_html: str, posting_url: str, Title: str) -> bool:
    if not careers_html: return False
    t = (Title or "").lower()
    return (t in careers_html.lower()) or (posting_url.split("?")[0] in careers_html)

def label_visibility(ats_status_200: bool, listed_on_careers: bool,
                     in_org_feed: bool, has_recent_date: bool,
                     last_seen_days: int, cache_only: bool):
    score = 0
    if ats_status_200: score += 40
    if listed_on_careers: score += 20
    if in_org_feed: score += 15
    if has_recent_date: score += 15
    if last_seen_days > 7: score -= min(20, 10)
    if cache_only: score -= 20
    score = max(0, min(100, score))
    if ats_status_200 and (listed_on_careers or in_org_feed):
        return "public", score, "🟢"
    elif ats_status_200:
        return "quiet", score, "🟠"
    else:
        return "expired", score, "🔴"

def _public_sanity_checks(keep_row: dict) -> tuple[str, int, str]:
    """
    Light-weight checks to decide if a job looks PUBLIC and boost confidence:
      - 200 on ATS/job URL
      - listed on company careers page we scraped (if present)
      - any recent ISO date on the page
    Returns (visibility, score, mark) using label_visibility()
    """
    import requests
    url = (keep_row.get("Job URL") or "").strip()
    if not url:
        return "quiet", 40, "🟠"

    ats_ok = False
    soft_ok = False  # treat bot-blocked/429 as quiet
    text   = ""
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        ats_ok = (r.status_code == 200)
        if r.status_code in (403, 429):
            soft_ok = True
        text   = r.text or ""
    except Exception:
        pass

    # If we previously fetched the company careers page HTML, you can pass it in later;
    # for now we just use the URL host as a proxy (Lever/Greenhouse/Ashby usually = public)
    host = up.urlparse(url).netloc.lower()
    listed_on_careers = any(h in host for h in ("greenhouse", "lever", "ashbyhq", "workday", "icims", "smartrecruiters"))
    BOT_BLOCKED_HOSTS = ("builtin.com", "jobs.builtin.com", "www.builtin.com")
    if any(host.endswith(h) for h in BOT_BLOCKED_HOSTS):
        soft_ok = soft_ok or not ats_ok
    has_recent_date = has_recent(text, days=45)  # you already have has_recent()

    # Reuse your compact scorer → (visibility, score, mark)
    return label_visibility(
        ats_status_200 = ats_ok or soft_ok,
        listed_on_careers = listed_on_careers,
        in_org_feed = False,
        has_recent_date = has_recent_date,
        last_seen_days = 0,
        cache_only = False,
    )

import urllib.parse as up

BOARD_MAP = {
    "themuse.com":            "The Muse",
    "boards.greenhouse.io":   "Greenhouse",
    "greenhouse.io":          "Greenhouse",
    "jobs.lever.co":          "Lever",
    "lever.co":               "Lever",
    "workday.com":            "Workday",
    "ashbyhq.com":            "Ashby",
    "myworkdayjobs.com":      "Workday",
    "icims.com":              "iCIMS",
    "smartrecruiters.com":    "SmartRecruiters",
    "remotive.com":           "Remotive",
    # add others you care about…
}

def _as_str(x) -> str:
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", "ignore")
        except Exception:
            return str(x)
    return str(x or "")


def infer_board_from_url(url: str) -> str:
    return career_board_name(url or "")
    host = up.urlparse(url or "").netloc.lower()
    for key, name in BOARD_MAP.items():
        if host.endswith(key):
            return name
    return ""

def company_from_url_fallback(url):
    url = _as_str(url)
    p = up.urlparse(url)

    # make sure path/netloc are str
    path  = _as_str(p.path)
    host  = _as_str(p.netloc).lower()
    parts = [x for x in path.strip("/").split("/") if x]

    if "jobs.lever.co" in host and parts: return parts[0]
    if "boards.greenhouse.io" in host and parts: return parts[0]
    if "job-boards.greenhouse.io" in host:
        q = up.parse_qs(p.query)
        return (q.get("for") or [None])[0]
    if "themuse.com" in host and len(parts) >= 2 and parts[0] == "jobs":
        return parts[1]
    return None

REL_POSTED_RE = re.compile(
    r"\b(\d+)\s*(minute|min|hour|hr|day|d|week|wk|month|mo)s?\b\s*ago\b",
    re.I,
)


def compute_posting_date_from_relative(rel_text: str, anchor_date=None) -> str | None:
    """
    Convert a relative label like:
      "Posted 6 days ago", "6 days ago", "2 weeks ago", "1 month ago", "12 hours ago"
    into YYYY-MM-DD, anchored to anchor_date.
    If anchor_date is None, defaults to LOCAL today (not UTC) to avoid date surprises.
    """
    m = REL_POSTED_RE.search(rel_text or "")
    if not m:
        return None

    n = int(m.group(1))
    unit = m.group(2).lower()

    base = anchor_date or datetime.now().date()

    if unit in ("day", "d"):
        return (base - timedelta(days=n)).isoformat()
    if unit in ("hour", "hr"):
        return base.isoformat()
    if unit in ("minute", "min"):
        return base.isoformat()
    if unit in ("week", "wk"):
        return (base - timedelta(days=7 * n)).isoformat()
    if unit in ("month", "mo"):
        return (base - timedelta(days=30 * n)).isoformat()

    return None


# --- Salary helpers (replace the old versions with these) ---
def _money_to_number(num_str, has_k=False):
    s = num_str.replace(",", "").strip()
    val = float(s)
    if has_k:
        val *= 1_000
    return val

def _annualize(amount, unit, hours_per_week=40, weeks_per_year=52):
    if not unit:
        return amount  # assume yearly if unit missing
    u = unit.lower()
    if u in ("hour", "hr"):
        return amount * hours_per_week * weeks_per_year   # default 2080 hours/year
    if u in ("day", "daily"):
        return amount * 5 * weeks_per_year                # 5 days/week
    if u in ("week", "wk", "weekly"):
        return amount * weeks_per_year
    if u in ("month", "mo", "monthly"):
        return amount * 12
    if u in ("year", "yr", "annum", "annual"):
        return amount
    return amount  # fallback

# ===== Salary thresholds (tune these anytime) =====
SALARY_TARGET_MIN = 110_000         # your “happy” minimum
SALARY_NEAR_DELTA = 15_000          # within this of target → KEEP with warning badge
SALARY_HARD_FLOOR = 90_000          # below this → SKIP

def _salary_status(max_detected: int | None, est_low: int | None, est_high: int | None):
    """
    Decide how to treat salary:
      - at_or_above: >= target → normal KEEP
      - near_min: within delta below target → KEEP + badge
      - below_floor: below hard floor → SKIP
      - unknown: no signal → treat elsewhere (no salary gating)
    Returns: (status:str, badge_text:str)
    """
    # pick a single comparable number
    val = max_detected or (est_high if est_high else None)
    if val is None:
        return "unknown", ""
    if val >= SALARY_TARGET_MIN:
        return "at_or_above", f"${val:,}+"
    if val >= SALARY_TARGET_MIN - SALARY_NEAR_DELTA:
        return "near_min", f"${val:,}"
    if val < SALARY_HARD_FLOOR:
        return "below_floor", f"${val:,}"
    # between floor and near band → keep but note it
    return "low", f"${val:,}"


def detect_salary_max(text):
    """
    Find the highest annualized salary mentioned in text.
    Supports ranges/single values and units: hour/hr, day, week, month, year/yr.
    Examples: $90-$170/hr, $150k–$200k, 120k/year, 120,000 annually, $85/hour, $12k/mo
    """
    if not text:
        return None

    t = text.replace("\u2013", "-").replace("\u2014", "-")  # en/em dashes -> '-'

    pattern = re.compile(
        r"""
        \$?\s*([0-9][\d,]*(?:\.\d+)?)\s*([kK])?                       # first number + optional k
        (?:\s*(?:-|to)\s*\$?\s*([0-9][\d,]*(?:\.\d+)?)\s*([kK])?)?    # optional range end + k
        (?:\s*(?:/|\bper\b|\ba\b)\s*(hour|hr|day|week|wk|month|mo|year|yr|annual|annum))? # optional unit
        """,
        re.I | re.X,
    )

    def _bare_yearish(num_str, has_k, unit):
        if unit or has_k:             # unit or 'k' means it's a pay figure
            return False
        s = num_str.replace(",", "").split(".")[0]
        if not s.isdigit():
            return False
        n = int(s)
        # treat 4-digit years and other small bare integers as not-a-salary
        if 1900 <= n <= 2100:
            return True
        if n < 10_000:                # e.g., 2026, 500, 8000 (without unit/$/k)
            return True
        return False

    annual_max = None
    for m in pattern.finditer(t):
        n1, k1, n2, k2, unit = m.groups()

        # skip matches that are just '2025', '2026', etc., without unit/$/k
        if _bare_yearish(n1, bool(k1), unit) and (not n2 or _bare_yearish(n2, bool(k2), unit)):
            continue

        # Context gate for bare numbers: require currency or pay words nearby
        surround = t[max(0, m.start()-20): m.end()+20].lower()
        has_currency = "$" in surround or " usd" in surround
        has_pay_word = any(w in surround for w in ("salary", "compensation", "pay", "per year", "yr", "annual", "base", "ote"))
        if not has_currency and not has_pay_word and not (k1 or k2 or unit):
            continue

                # Skip obvious non salary counts like "289,097 employees"
        # even if there is money talk nearby, unless this
        # snippet is clearly about salary or compensation.
        if any(w in surround for w in ("organizations", "users", "employees", "customers")) \
           and "salary" not in surround and "compensation" not in surround:
            continue

        a1 = _annualize(_money_to_number(n1, bool(k1)), unit)
        vals = [a1]
        if n2:
            a2 = _annualize(_money_to_number(n2, bool(k2)), unit)
            vals.append(a2)
        candidate = max(vals)
        if annual_max is None or candidate > annual_max:
            annual_max = candidate

    return int(round(annual_max)) if annual_max is not None else None



def eval_salary(text: str):
    """
    Returns (annual_max, should_skip).
    annual_max is an int or None.
    should_skip is True only when we detect a salary outside bounds.
    """
    annual_max = detect_salary_max(text)  # your existing parser
    if annual_max is None:
        return annual_max, (not KEEP_UNKNOWN_SALARY)
    if SALARY_FLOOR and annual_max < SALARY_FLOOR:
        return annual_max, True
    if SALARY_CEIL and annual_max > SALARY_CEIL:
        return annual_max, True
        # If we didn't parse anything, honor the global toggle
    return annual_max, (not KEEP_UNKNOWN_SALARY)
# ---------- end salary helper ----------

def is_remote_friendly(text: str):
    t = text.lower()
    for p in ONSITE_BLOCKERS:
        if re.search(p, t):
            return False, "onsite_blocker"
    for p in REMOTE_KEYWORDS:
        if re.search(p, t):
            return True, "remote_keyword"
    for p in SINGLE_CITY_PATTERNS:
        if re.search(p, t):
            return False, "single_city_no_remote"
    return False, "no_remote_signal"

def _is_commutable_local(d: dict) -> bool:
    """
    Treat nearby hybrid / on-site roles as acceptable even if not marked remote.
    Right now this is tuned for Seattle area.
    """
    loc  = str(d.get("Location") or "").lower()
    chips_val = d.get("Location Chips")
    chips = " ".join(map(str, chips_val)).lower() if isinstance(chips_val, (list, tuple)) else str(chips_val or "").lower()
    locs_val = d.get("locations")  # from JSON-LD addressLocality/addressRegion extraction
    locs_text = " ".join(map(str, locs_val)).lower() if isinstance(locs_val, (list, tuple)) else str(locs_val or "").lower()
    page_txt = str(d.get("page_text") or "").lower()

    text = " ".join([loc, chips, locs_text, page_txt])

    # Accept any explicit Washington state signal (but avoid Washington DC)
    if re.search(r"\bwashington\b", text) and "dc" not in text:
        return True
    if re.search(r"\bwa\b", text) and "dc" not in text:
        return True

    if any(term in text for term in SEATTLE_TERMS):
        return True

    return False


def _enrich_workday_location(details: dict, html: str, job_url: str = "") -> dict:
    """
    Pull location info from Workday job detail page JSON blobs and the visible
    badges (remoteType, locations).
    """
    try:
        html_lower = (html or "").lower()
        soup = BeautifulSoup(html or "", "html.parser")

        # 1. Visible "Remote type" badge, for tenants like UW
        try:
            rt_dd = soup.select_one("[data-automation-id='remoteType'] dd") or \
                    soup.select_one("[data-automation-id='remoteType']")
            if rt_dd:
                rt_txt = rt_dd.get_text(" ", strip=True)
                if rt_txt:
                    low = rt_txt.lower()
                    if "hybrid" in low or "telework" in low:
                        details["Remote Rule"] = "Hybrid"
                        details["remote_flag"] = "Hybrid"
                    elif "remote" in low:
                        details["Remote Rule"] = "Remote"
                        details["remote_flag"] = "Remote"
                    else:
                        # only set Onsite if nothing was set yet
                        details.setdefault("Remote Rule", "Onsite")
        except Exception:
            pass

        # 2. Visible "Location" badge, usually city and campus
        try:
            loc_dd = soup.select_one("[data-automation-id='locations'] dd") or \
                     soup.select_one("[data-automation-id='locations']")
            if loc_dd:
                loc_txt = loc_dd.get_text(" ", strip=True)
                if loc_txt:
                    cur = (details.get("Location") or "").strip().lower()
                    # overwrite only when current value is generic
                    if cur in ("", "us", "united states", "remote"):
                        if not details.get("_LOCK_LOCATION_FROM_TOOLTIP"):
                            details["Location"] = loc_txt
        except Exception:
            pass

        # 3. Pre detect hybrid/telework signal from full HTML as a fallback
        if ("hybrid" in html_lower) or ("telework" in html_lower):
            details.setdefault("Remote Rule", "Hybrid")
            details.setdefault("remote_flag", "Hybrid")

        # existing JSON parsing code follows
        script_txt = ""
        for s in soup.find_all("script"):
            raw = s.string or s.get_text() or ""
            if not raw:
                continue
            if "mosaic.providerData" in raw or "INITIAL_STATE" in raw:
                script_txt = raw
                break
        if not script_txt:
            return details

        for s in soup.find_all("script"):
            raw = s.string or s.get_text() or ""
            if not raw:
                continue
            if "mosaic.providerData" in raw or "INITIAL_STATE" in raw:
                script_txt = raw
                break
        if not script_txt:
            return details

        # Extract JSON payload (Workday often embeds window.mosaic.providerData or INITIAL_STATE = {...})
        payload = None
        for pattern in [
            r"mosaic\\.providerData\\s*=\\s*({.*?})\\s*;",
            r"INITIAL_STATE\\s*=\\s*({.*})\\s*;",
            r"providerData\\s*=\\s*({.*?})\\s*;",
        ]:
            m = re.search(pattern, script_txt, re.S)
            if m:
                try:
                    payload = json.loads(m.group(1))
                    break
                except Exception:
                    continue

        if not payload:
            return details

        job = payload.get("jobPostingInfo") or {}
        # Some tenants nest under "jobPostingInfo" -> "jobLocations"
        job_locations = job.get("jobLocations") or job.get("joblocations") or []
        locs: list[str] = []
        chips: set[str] = set()

        # Remote/Hybrid hint embedded in the Workday JSON payload (robust even when the page
        # text lacks the visible badge we want). This gives Hybrid priority over Remote.
        job_text = ""
        try:
            job_text = json.dumps(job).lower()
        except Exception:
            job_text = ""
        if job_text:
            if ("hybrid" in job_text) or ("telework" in job_text):
                details["Remote Rule"] = "Hybrid"
                details["remote_flag"] = "Hybrid"
            elif "remote" in job_text and not details.get("Remote Rule"):
                details["Remote Rule"] = "Remote"
                details.setdefault("remote_flag", "Remote")

        # Remote type from visible HTML rows (fallback when JSON lacks it)
        if not details.get("Remote Rule"):
            try:
                import re as _re
                rt_label = soup.find(string=_re.compile(r"remote\\s*type", _re.I))
                if rt_label:
                    dd = rt_label.find_next("dd")
                    if dd and dd.get_text(strip=True):
                        details["Remote Rule"] = dd.get_text(strip=True)
                        details.setdefault("remote_flag", details["Remote Rule"])
            except Exception:
                pass

        for loc in job_locations:
            if not isinstance(loc, dict):
                continue
            city = loc.get("city") or ""
            state = loc.get("state") or loc.get("region") or ""
            country = loc.get("country") or ""
            parts = [p for p in [city, state, country] if p]
            if parts:
                locs.append(", ".join(parts))
            for chip in (city, state):
                if chip:
                    chips.add(str(chip))

        # Fallback: single jobLocation into address dict
        jl = job.get("jobLocation") or job.get("joblocation") or {}
        if isinstance(jl, dict):
            city = jl.get("city") or jl.get("addressLocality") or ""
            state = jl.get("state") or jl.get("addressRegion") or ""
            country = jl.get("country") or jl.get("addressCountry") or ""
            parts = [p for p in [city, state, country] if p]
            if parts:
                locs.append(", ".join(parts))
            for chip in (city, state):
                if chip:
                    chips.add(str(chip))

        if locs and not details.get("locations"):
            details["locations"] = locs
        if chips:
            existing = details.get("Location Chips")
            if isinstance(existing, (list, tuple, set)):
                chips.update(str(x) for x in existing if str(x).strip())
            elif existing and str(existing).strip():
                # existing may be a pipe string, a single token, or something messy
                chips.update(p.strip() for p in str(existing).replace(" / ", "|").replace(",", "|").split("|") if p.strip())


        # Set primary Location if missing
        if not details.get("Location") and locs:
            details["Location"] = locs[0]

        # If the Workday page text mentions Seattle-area terms, add to chips and prefer that location
        page_txt = str(details.get("page_text") or "").lower()
        if page_txt:
            for term in SEATTLE_TERMS:
                if term in page_txt:
                    chips.add(term)
            if "seattle campus" in page_txt and ("remote" in str(details.get("Location","")).lower() or not details.get("Location")):
                details["Location"] = "Seattle Campus"

        # Path-derived location hints (e.g., seattle-campus, tacoma-campus, harborview)
        path_lower = (urlparse(details.get("Job URL") or job_url or "").path or "").lower()
        for token, nice in items():
            if token in path_lower:
                loc_cur = (details.get("Location") or "").lower()
                if (not loc_cur or loc_cur in {"", "us", "united states"} or "remote" in loc_cur):
                    details["Location"] = nice
                chips.append("US")
                chips.append("WA")
                chips.append(nice)
                break
        
        # Final write for Location Chips should happen after ALL chip sources are merged,
        # including path derived hints.
        if chips:
            details["Location Chips"] = "|".join(
                sorted({str(c).strip().lower() for c in chips if str(c).strip()})
            )

    except Exception:
        pass
        
    # Dedupe Deduplicate while preserving order
    seen = set()
    out = []
    for c in chips:
        c = str(c).strip().upper()
        if not c or c in seen:
            continue
        seen.add(c)
        out.append(c)
    return out




PRIOR_DECISIONS_CACHE: dict[str, tuple[str, str]] = {}

def _apply_prior_decisions(row: dict, prior: dict[str, tuple[str, str]] | None) -> None:
    if not row or not prior:
        return
    url = (row.get("Job URL") or row.get("job_url") or "").strip()
    if not url:
        return
    applied_prev, reason_prev = prior.get(url, ("", ""))
    if applied_prev and not row.get("Applied?"):
        row["Applied?"] = applied_prev
    if reason_prev:
        if not row.get("Reason"):
            row["Reason"] = reason_prev
        if not row.get("Reason Skipped"):
            row["Reason Skipped"] = reason_prev


import re, time, sys


def _ansi_strip(s: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", s or "")


LEVEL_WIDTH = 22


def _box(label: str) -> str:
    raw = (label or "")[:LEVEL_WIDTH]
    vis = sum(max(0, wcswidth(ch)) for ch in raw)
    pad = max(0, LEVEL_WIDTH - vis)
    return f"[{raw}{' ' * pad}]"


def _center_fit(label: str, width: int) -> str:
    """Truncate by display width and center-pad to a fixed width."""
    s = (label or "")
    out, used = [], 0
    for ch in s:
        w = wcswidth(ch)
        if w < 0:
            w = 1
        if used + w > width:
            break
        out.append(ch)
        used += w
    pad = max(0, width - used)
    left = pad // 2
    right = pad - left
    return (" " * left) + "".join(out) + (" " * right)


def _progress_print(msg: str) -> None:
    """Draw/overwrite the single progress line in-place."""
    sys.stdout.write("\r\033[2K" + msg)
    sys.stdout.flush()

import atexit

atexit.register(lambda: progress_clear_if_needed(permanent=True))
atexit.register(_spinner_stop_thread)


def stop_spinner(final_msg: str | None = None) -> None:
    """Erase spinner line; optionally print a final 'DONE' line."""
    progress_clear_if_needed(permanent=True)
    if final_msg:
        log_print(f"{now_ts()} {_box('DONE ')} {final_msg}")


def refresh_progress() -> None:
    """Legacy compatibility wrapper."""
    progress_refresh_after_log(force=True)


CURRENT_SOURCE = ""  # global tag used by log_event

def set_source_tag(url: str):
    """Set a short source tag like 'remotive.com' or 'simplyhired.com'."""
    global CURRENT_SOURCE
    host = up.urlparse(url).netloc.replace("www.", "")
    CURRENT_SOURCE = host


def _normalize_job_defaults(d: dict) -> dict:
    """
    Normalize field names and basic defaults so downstream code
    can treat every board the same.

    Also normalizes date fields:
      - posting_date / Posting Date / date_posted
      - valid_through / Valid Through

    Produces:
      - d["posting_date"], d["valid_through"] as YYYY-MM-DD when possible
      - d["Posting Date"], d["Valid Through"] for CSV
      - d["Posted"] as a relative label when possible
    """

    #if DEBUG_LOCATION:
        #log_line("DEBUG", "[TRACE] entered _normalize_job_defaults")

    d = dict(d or {})

    from datetime import datetime

    d = dict(d or {})

    # Basic fields used everywhere
    d.setdefault("Applied?", "")
    d.setdefault("Reason", "")
    d.setdefault("Date Scraped", now_ts())

    d["Title"]            = d.get("Title") or d.get("title") or ""
    d["Job ID (Vendor)"]  = d.get("Job ID (Vendor)") or d.get("job_id_vendor") or ""
    d["Job ID (Numeric)"] = d.get("Job ID (Numeric)") or d.get("job_id_numeric") or ""
    d["Job Key"]          = d.get("Job Key") or d.get("job_key") or ""

    d["Company"]      = d.get("Company") or d.get("company") or ""
    d["Career Board"] = d.get("Career Board") or d.get("career_board") or ""
    d["Location"]     = d.get("Location") or d.get("display_location") or d.get("location") or ""

    # ---------- dates ----------
    posting_date_raw = d.get("posting_date") or d.get("Posting Date") or ""
    posting_date_raw = str(posting_date_raw).strip()  # JSON-LD ISO string if available
    valid_through_raw = d.get("valid_through") or d.get("Valid Through") or ""

    # Prefer explicit JSON-LD ISO date_posted when present
    if posting_date_raw.lower().startswith("posted "):
        posting_date_raw = ""

    def _strip_time(s: str) -> str:
        if not s:
            return ""
        s = str(s).strip()
        for sep in ("T", " "):
            if sep in s:
                return s.split(sep, 1)[0].strip()
        return s

    posting_date_str = _strip_time(posting_date_raw)
    valid_through_str = _strip_time(valid_through_raw)

    # ---------- Posted (relative label) ----------
    posted_label = d.get("Posted") or d.get("posted_label") or d.get("posted") or ""
    if not posted_label and d.get("Period"):
        posted_label = d.get("Period") or ""

    # Normalize Posted label early (so date math sees clean input)
    if posted_label:
        posted_label = str(posted_label).strip()
        posted_label = re.sub(r"^\s*job\s+", "", posted_label, flags=re.I)
        posted_label = re.sub(r"^\s*posted\s+", "Posted ", posted_label, flags=re.I)
        posted_label = posted_label.replace("|", " ").strip()
        posted_label = re.sub(r"\s{2,}", " ", posted_label).strip()

    # If still missing, try to compute Posting Date from a relative Posted label (anchored to Date Scraped)
    if not posting_date_str:
        rel = str(posted_label or "").strip()

        # Anchor to Date Scraped when available
        ds = str(d.get("Date Scraped") or "").strip()
        anchor = None
        if ds:
            ds_iso = parse_date_relaxed(ds)
            if ds_iso:
                anchor = ds_iso[:10]

        try:
            anchor_date = datetime.fromisoformat(anchor).date() if anchor else None
        except Exception:
            anchor_date = None

        computed = compute_posting_date_from_relative(rel, anchor_date=anchor_date)
        if computed:
            posting_date_str = computed

    # Store internal canonical keys
    if posting_date_str:
        d["posting_date"] = posting_date_str
    if valid_through_str:
        d["valid_through"] = valid_through_str

    # Store CSV keys
    d["Posting Date"] = posting_date_str or ""
    d["Valid Through"] = valid_through_str or ""

    # If Posted is still missing, derive it from posting_date_str
    if not posted_label and posting_date_str:
        try:
            if len(posting_date_str) == 10 and posting_date_str[4] == "-" and posting_date_str[7] == "-":
                dt_posted = datetime.fromisoformat(posting_date_str)
                days = (datetime.now().date() - dt_posted.date()).days

                if days <= 0:
                    posted_label = "Today"
                elif days == 1:
                    posted_label = "1 day ago"
                elif days < 7:
                    posted_label = f"{days} days ago"
                elif days < 30:
                    weeks = days // 7
                    posted_label = f"{weeks} week{'s' if weeks != 1 else ''} ago"
                else:
                    months = days // 30
                    posted_label = f"{months} month{'s' if months != 1 else ''} ago"
        except Exception:
            posted_label = posted_label or ""

    d["Posted"] = posted_label or ""


    posting_date_str = _strip_time(posting_date_raw)
    valid_through_str = _strip_time(valid_through_raw)

    # If still missing, try to compute from a relative Posted label (anchored to Date Scraped)
    if not posting_date_str:
        rel = str(posted_label or "").strip()

        # Anchor to Date Scraped when available
        ds = str(d.get("Date Scraped") or "").strip()
        anchor = None
        if ds:
            # parse_date_relaxed returns ISO YYYY-MM-DD when possible
            ds_iso = parse_date_relaxed(ds)
            if ds_iso:
                anchor = ds_iso[:10]

        try:
            anchor_date = datetime.fromisoformat(anchor).date() if anchor else None
        except Exception:
            anchor_date = None

        computed = compute_posting_date_from_relative(rel, anchor_date=anchor_date)
        if computed:
            posting_date_str = computed

    # Store internal canonical keys
    if posting_date_str:
        d["posting_date"] = posting_date_str
    if valid_through_str:
        d["valid_through"] = valid_through_str

    # Store CSV keys
    d["Posting Date"] = posting_date_str or ""
    d["Valid Through"] = valid_through_str or ""


    # URLs
    d["Job URL"] = d.get("Job URL") or d.get("job_url") or ""
    if not d.get("Career Board") and d["Job URL"]:
        d["Career Board"] = infer_board_from_url(d["Job URL"])
    d["Apply URL"] = d.get("Apply URL") or d.get("apply_url") or d.get("Job URL") or ""
    d["Apply URL Note"] = d.get("Apply URL Note") or ""

    # Text and rules
    d["Description Snippet"] = d.get("Description Snippet") or d.get("description_snippet") or ""
    d["WA Rule"]             = d.get("WA Rule") or d.get("wa_rule") or "Default"
    d["BC Rule"]             = d.get("BC Rule") or d.get("bc_rule") or "Default"
    d["ON Rule"]             = d.get("ON Rule") or d.get("on_rule") or "Default"
    d["Remote Rule"]         = d.get("Remote Rule") or d.get("remote_rule") or "Default"
    d["US Rule"]             = d.get("US Rule") or d.get("us_rule") or "Default"
    d["Canada Rule"]         = d.get("Canada Rule") or d.get("canada_rule") or "Default"

    return d




def link_key(u: str) -> str:
    """
    Stable key so duplicate listing and detail URLs collapse across boards.
    - lowercases host
    - lowercases path for most boards
    - preserves path casing for case-sensitive boards (YC)
    - strips 'www.' and trailing slashes
    - drops tracking or paging query params
    - ignores fragments
    """
    p = urlparse(str(u or ""))
    host = (p.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]

    path_raw = (p.path or "").rstrip("/") or "/"
    path = path_raw if "ycombinator.com" in host else path_raw.lower()

    drop = {
        "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
        "ref", "source", "src", "page", "p", "start"
    }

    kept = []
    for k, v in parse_qsl(p.query or "", keep_blank_values=False):
        if (k or "").lower() in drop:
            continue
        kept.append(((k or "").lower(), v))

    q = urlencode(kept, doseq=True)
    return f"{host}{path}?{q}" if q else f"{host}{path}"


def _title_for_log(d: dict, link: str) -> str:
    # normalize first so trailing " @Company" / "[Hiring]" etc. are removed
    t = normalize_title((d.get("Title") or "").strip())

    # ensure link is a string for urlparse
    link = _as_str(link)

    # normalize first so trailing " @Company" / "[Hiring]" etc. are removed
    t = normalize_title(
        (d.get("Title") or "").strip(),
        (d.get("Company") or "") or company_from_url_fallback(d.get("Job URL"))
    )

    # filter out obvious non-titles that sometimes get scraped
    bad = ("remotive", "rss feeds", "public api", "my account", "log in",
           "boost your career", "job search tips", "employers", "find remote jobs")
    if t:
        low = t.lower()
        if not any(b in low for b in bad):
            return t

    # strong slug fallback (kept as-is)
    p =up.urlparse( link)
    segs = [s for s in p.path.split("/") if s]
    if segs:
        slug = segs[-1].split("?")[0]
        slug = re.sub(r"-?\d{5,}$", "", slug)
        slug = slug.replace("-", " ").strip()
        if slug:
            return slug.title()

    return "Assumed Title: Product Owner"

# ---- Console formatting helpers (fixed widths) ----------------------
from wcwidth import wcswidth

_COL_W      = 13                  # legacy use elsewhere; keep as-is if referenced
_STATUS_W   = 22                  # exactly 22 visible cells inside [ … ]

def _vis_pad(s: str, width: int) -> str:
    """
    Pad string s to a *visual* width using wcwidth, so emoji count correctly.
    Never truncates; only pads with spaces to reach `width`.
    """
    s = s or ""
    diff = width - wcswidth(s)
    if diff > 0:
        return s + (" " * diff)
    return s

from wcwidth import wcwidth  # add beside your existing wcswidth import

def _vis_fit(s: str, width: int) -> str:
    """Trim or pad to exactly `width` visible cells, counting emoji correctly."""
    s = (s or "").strip()
    out, used = [], 0
    for ch in s:
        cw = wcwidth(ch)
        if cw < 0:
            cw = 0
        if used + cw > width:
            break
        out.append(ch)
        used += cw
    if used < width:
        out.append(" " * (width - used))
    return "".join(out)


def _status_box(label: str) -> str:
    """Bracketed label padded to exactly _STATUS_W cells, emoji-safe."""
    inner = _vis_pad(label.strip(), _STATUS_W - 2)  # -2 for '[' and ']'
    return f"[{inner}]"


def _conf_box(visibility: str | None, score: int | str | None, mark: str | None = None) -> str:
    vis = (visibility or "").upper()
    icon = {"PUBLIC": "🟢", "QUIET": "🟠", "UNKNOWN": "⚪"}.get(vis, "⚪")

    # Always reserve 3 visual cells for score: "  5", " 55", "100" or blanks
    sc = "   "
    try:
        if score is not None and str(score).strip() != "":
            sc = f"{int(score):>3}"
    except Exception:
        pass

    # Pad the label so “PUBLIC” and “QUIET” align; leave one space before score block
    label = f"{icon} {vis:<6}{sc}"
    return _box(label)


import re  # make sure this is available at top-of-file
# ===== console helpers (place just above def log_event) =====
import textwrap


def _wrap_lines(s: str, width: int = 100) -> list[str]:
    s = " ".join((s or "").split())
    if not s:
        return [""]
    return [s[i:i+width] for i in range(0, len(s), width)]


def _salary_payload_22(d: dict) -> str:
    """
    Terminal payload text, padded or trimmed to exactly 22 visible cells.

    Handles:
      - Real numeric salaries
      - Estimate strings
      - “Signal only” text like “competitive salary”
      - True missing
    """
    status      = (d.get("Salary Status") or "").strip().lower()
    note        = (d.get("Salary Note") or "").strip()
    max_det_raw = d.get("Salary Max Detected")
    placeholder = (d.get("Salary Placeholder") or "").strip()
    est_str     = (d.get("Salary Est. (Low-High)") or "").strip()

    # --- 1) Numeric salary present -----------------------------------------
    max_val = None
    try:
        if max_det_raw not in (None, ""):
            max_val = int(max_det_raw)
    except Exception:
        max_val = None

    if max_val is not None:
        core = f"${max_val:,.0f}"

        if status == "near_min":
            core += " · near min"
        elif status == "below_floor":
            core += " · below floor"
        elif status == "at_or_above":
            core += " · at/above floor"

        if "est" in note.lower():
            core += " (est.)"

        return _vis_fit(core, 50)

    # --- 2) Range/estimate string ------------------------------------------
    if est_str:
        return _vis_fit(f"{est_str} (est.)", 50)

    # --- 3) Signal-only textual salary -------------------------------------
    if status == "signal_only" and placeholder:
        # e.g. “Competitive salary · signal_only”
        return _vis_fit(f"{placeholder} · signal_only", 50)

    # --- 4) Plain placeholder without explicit status ----------------------
    if placeholder:
        return _vis_fit(placeholder, 50)

    # --- 5) Truly missing --------------------------------------------------
    return _vis_fit("Missing or Unknown", 22)


# ============================================================


def log_event(level: str,
              left: str = "",
              right=None,
              *, job=None, url: str | None = None,
              width: int = 120,
              reason: str | None = None,
              **_):

    # NEW: make sure the spinner/progress row is erased before we print real rows
    progress_clear_if_needed()

    lvl   = (level or "").upper()
    color = LEVEL_COLOR.get(lvl, RESET)
    tag   = _box(lvl)

    # ---- normalize inputs ----
    # Prefer job= as the right-side dict if given
    if job is not None and right is None:
        right = job

    job_dict = right if isinstance(right, dict) else {}

    # Derive URL in a tolerant order
    url = (
        (url or "")
        or (right.strip() if isinstance(right, str) else "")
        or job_dict.get("Job URL")
        or job_dict.get("job_url")
        or job_dict.get("Apply URL")
        or ""
    )

    company      = job_dict.get("Company", "") if job_dict else ""
    career_board = job_dict.get("Career Board", "") if job_dict else ""
    vis          = job_dict.get("Visibility Status", "") if job_dict else ""
    score        = job_dict.get("Confidence Score", "") if job_dict else ""
    mark         = job_dict.get("Confidence Mark", "") if job_dict else ""

    # Helper to wrap and print one block
    def _emit(txt: str):
        for ln in _wrap_lines(str(txt), width=width):
            log_print(f"{color}{tag}.{ln}{RESET}")

    # Build left text once
    left_txt = " ".join((left or "").split())
    if not left_txt and job_dict:
        # Fall back to your existing title helper if no explicit left text
        try:
            left_txt = _title_for_log(job_dict, url or job_dict.get("Job URL", ""))
        except Exception:
            left_txt = job_dict.get("Title", "") or ""

    # Simple levels: INFO, WARN, DONE, etc.
    if lvl not in {"KEEP", "SKIP"}:
        if left_txt:
            _emit(left_txt)
        if url:
            for ln in _wrap_lines(url, width=width):
                log_print(f"{color}{tag}{DOT4}{ln}{RESET}")
        #refresh_progress()
        return

    # Detailed KEEP / SKIP layout
    title = left_txt or job_dict.get("Title", "") or ""

    if lvl == "KEEP":
        progress_clear_if_needed()
        keep_tag = _box("KEEP ")
        keep_body_color = None  # no body highlighting requested

        # --- Title (first line) ---
        title_txt = (job_dict.get("Title") or "").strip()
        if title_txt:
            for ln in _wrap_lines(title_txt, width=width):
                log_print(f"{keep_tag}.{ln}", color=KEEP_TITLE_COLOR, color_prefix=True)

    #        wrapped = _wrap_lines(title_txt, width=width)
    #        for i, ln in enumerate(wrapped):
    #            if i == 0:
    #                log_print(f"{_box('KEEP ')}.{ln}", color=KEEP_TITLE_COLOR)
    #            else:
    #                log_print_plain(f"{_box('KEEP ')}.{ln}", color=KEEP_TITLE_COLOR)

        company = (job_dict.get("Company") or "").strip() or "Missing Company"
        board   = ((job_dict.get("Career Board") or job_dict.get("career_board") or "").strip()
                or "Missing Board")

        # --- Company / Board (friendly fallbacks) ---
        #company = (job_dict.get("Company") or "Missing Company").strip()
        #board   = (job_dict.get("Career Board") or "Missing Board").strip()
        progress_clear_if_needed()
        for ln in _wrap_lines(f"{color}{board}{DOT6}{company}{RESET}", width=width):
            log_print(f"{color}{keep_tag}{DOT3}{ln}", color=keep_body_color)

        # --- URL line ---
        if url:
            for ln in _wrap_lines(url, width=width):
                progress_clear_if_needed()
                log_print(f"{color}{keep_tag}{DOT3}{ln}", color=keep_body_color)
        loc = (job_dict.get("Location") or "").strip()
        if loc:
            remote_term = (
                job_dict.get("Remote Rule")
                or job_dict.get("remote_rule")
                or job_dict.get("Remote Flag")
                or job_dict.get("remote_flag")
                or "Remote"
            )
            loc_line = f"{color}{remote_term} Location: {loc}."
            for ln in _wrap_lines(loc_line, width=width):
                log_print(f"{color}{keep_tag}{DOT3}{ln}", color=keep_body_color)
        apply_note = job_dict.get("Apply URL Note", "")
        if apply_note:
            for ln in _wrap_lines(f"Apply note: {apply_note}", width=width):
                log_print(f"{color}{keep_tag}{DOT3}{ln}", color=keep_body_color)

        # --- Always show Salary line with fixed 22-cell width ---
        salary_str = _fmt_salary_line(job_dict) if isinstance(job_dict, dict) else ""
        log_print(f"{color}{_box('SALARY ')}{DOT3}{_salary_payload_22(job_dict)}{RESET}")
        #log_line("SALARY", {DOT3}_vis_fit(salary_str or "Missing or Unknown", 22))



        #if vis or score or mark:
        #    log_print(f"{color}{_conf_box(vis, score, mark)}.{RESET}")

        _print_debug_rows_for(job_dict, color=keep_body_color)
        log_print(f"{color}{_box('DONE ')}.✅ {RESET}")
        return

    if lvl == "SKIP":
        progress_clear_if_needed()
        if title:
            wrapped = _wrap_lines(title, width=width)
            for i, ln in enumerate(wrapped):
                if i == 0:
                    log_print(f"{_box('SKIP ')}.{ln}", color=SKIP_TITLE_COLOR, color_prefix=True)
                else:
                    log_print(f"{_box('SKIP ')}.{ln}", color=SKIP_TITLE_COLOR)

        if career_board or company:
            progress_clear_if_needed()
            log_print(f"{color}{_box('SKIP ')}{DOT3}{career_board}{DOT6}{company}{RESET}")

        if url:
            for ln in _wrap_lines(url, width=width):
                progress_clear_if_needed()
                log_print(f"{color}{_box('SKIP ')}{DOT3}{ln}{RESET}")
        loc = (job_dict.get("Location") or "").strip()
        if loc:
            remote_term = (
                job_dict.get("Remote Rule")
                or job_dict.get("remote_rule")
                or job_dict.get("Remote Flag")
                or job_dict.get("remote_flag")
                or "Remote"
            )
            loc_line = f"{remote_term} Location: {loc}."
            for ln in _wrap_lines(loc_line, width=width):
                log_print(f"{color}{_box('SKIP ')}{DOT3}{ln}{RESET}")

        # --- Always show Salary line with fixed 22-cell width ---
        salary_str = _fmt_salary_line(job_dict) if isinstance(job_dict, dict) else ""
        log_print(f"{color}{_box('SALARY ')}{DOT3}{_salary_payload_22(job_dict)}{RESET}")
        #log_line("SALARY", {DOT3} _vis_fit(salary_str or "Missing or Unknown", 22))



        # use the explicit reason parameter if provided, otherwise the dict
        # prefer explicit reason argument, fall back to dict field
        reason_text = (reason or "").strip() or job_dict.get("Reason Skipped") or ""
        if reason_text:
            for ln in _wrap_lines(reason_text, width=width):
                progress_clear_if_needed()
                log_print(f"{color}{_box('SKIP ')}...{ln}{RESET}")

        #if vis or score or mark:
        #    log_print(f"{color}{_conf_box(vis, score, mark)}{RESET}")
        progress_clear_if_needed()
        _print_debug_rows_for(job_dict, color=color)
        log_print(f"{color}{_box('DONE ')}.🚫{RESET}")
        return


def _rule(details: dict, key: str, default="default"):
    # allows both snake_case and Title Case with space
    return details.get(key) or details.get(key.replace("_", " ").title()) or default

def _make_skip_row(link: str, reason: str, details: dict | None = None) -> dict:
    """Build a normalized 'skip' row from whatever we know."""
    d = details or {}
    # ensure fields like chips/regions/salary are present if we have snippet/title
    if d:
        d = _normalize_job_defaults(d)
        d = enrich_salary_fields(d)

    base = {
        "Job URL": details.get("job_url", link),
        "Career Board": career_board_name(link),
        "Reason Skipped": reason or "Filtered by rules",
        "Title": normalize_title(details.get("Title", ""), details.get("Company", "")),
    }

    # copy nice-to-have fields if present
    for k in (
        "Company","Location","Posted","Posting Date","Valid Through",
        "Apply URL","Description Snippet","Location Chips","Applicant Regions",
        "Salary Max Detected","Salary Rule","Salary Status","Salary Note","Salary Est. (Low-High)"
    ):
        if d.get(k):
            base[k] = d[k]

    return _normalize_skip_defaults(base)

# alias so older call sites without the underscore still work
# keep the public alias consistent
def log_and_record_skip(link: str, rule_reason: str | None = None, keep_row: dict | None = None) -> None:
    # Legacy alias: keep the old entry point but route to the shared helper
    # _record_skip(link, None, row)  # buggy call kept for reference
    _log_and_record_skip(link, rule_reason, keep_row)


PRIOR_DECISIONS_CACHE: dict[str, tuple[str, str]] = {}


def _log_and_record_skip(link: str, rule_reason: str | None = None, row: dict | None = None) -> None:
    """
    Helper used by both the main loop and the rule engine.
    Logs a SKIP line and records the skip row once.
    """
    row = row or {}
    reason = (row.get("Reason Skipped") or rule_reason or "").strip() or "filtered by rules"

    # Log a structured SKIP block with the row details and a plain-text reason
    log_event("SKIP", _title_for_log(row, link), right=row, reason=reason)

    # Actually record the skip
    _record_skip(row, reason=reason)



def _normalize_skip_defaults(row: dict) -> dict:
    base = {
        "Date Scraped": now_ts(),
        "Title": row.get("Title", ""),
        "Job ID (Vendor)": row.get("Job ID (Vendor)", row.get("job_id_vendor", "")),
        "Job ID (Numeric)": row.get("Job ID (Numeric)", row.get("job_id_numeric", "")),
        "Job Key": row.get("Job Key", ""),
        "Company": row.get("Company", ""),
        "Career Board": row.get("Career Board", ""),
        "Location": row.get("Location", ""),
        "Posted": row.get("Posted", ""),
        "Posting Date": row.get("Posting Date", row.get("posting_date", "")),
        "Valid Through": row.get("Valid Through", row.get("valid_through", "")),
        "Job URL": row.get("Job URL", ""),
        "Reason Skipped": row.get("Reason Skipped", row.get("Reason", "")),
        "WA Rule": row.get("WA Rule", ""),
        "BC Rule": row.get("BC Rule", ""),
        "ON Rule": row.get("ON Rule", ""),
        "Remote Rule": row.get("Remote Rule", ""),
        "US Rule": row.get("US Rule", ""),
        "Canada Rule": row.get("Canada Rule", ""),
        "Salary Max Detected": row.get("Salary Max Detected", ""),
        "Salary Rule": row.get("Salary Rule", ""),
        "Location Chips": row.get("Location Chips", ""),
        "Applicant Regions": row.get("Applicant Regions", ""),
        "Applicant Regions Source": row.get("Applicant Regions Source", ""),
    }
    base.update(row)

    # Workday location fallback: derive city/campus from URL when current location is generic
    try:
        loc_cur = (base.get("Location") or "").strip().lower()
        url = base.get("Job URL") or ""
        path_lower = (up.urlparse(url).path or "").lower()
        for token, nice in items():
            if token in path_lower and loc_cur in ("", "us", "united states", "remote"):
                base["Location"] = nice
                chips = base.get("Location Chips") or ""
                chips_set = set(chips.split("|")) if isinstance(chips, str) else set(chips or [])
                chips_set.update({"US", "WA", nice})
                base["Location Chips"] = "|".join(sorted({c.strip() for c in chips_set if c}))
                if not base.get("US Rule"):
                    base["US Rule"] = "Pass"
                if not base.get("WA Rule"):
                    base["WA Rule"] = "Pass" if "wa" in nice.lower() else "Fail"
                break
    except Exception:
        pass

    return base



def _record_keep(row: dict) -> bool:
    global kept_count

    # 1) Merge in any prior decisions from Google Sheets (Reason, Applied?, etc.)
    try:
        _apply_prior_decisions(row, PRIOR_DECISIONS_CACHE)
    except NameError:
        # Safety: if cache not defined for some reason, just continue
        pass

    # 2) Track seen URLs for de-dupe
    url = (row.get("Job URL") or row.get("job_url") or "").strip()
    if url:
        _seen_kept_urls.add(url)

    # 3) Ensure the Reason column always has whatever decision we know
    if not row.get("Reason"):
        row["Reason"] = row.get("Reason Skipped", "")

    kept_rows.append(to_keep_sheet_row(row))
    kept_count += 1
    progress_clear_if_needed()
    _progress_after_decision()
    return True



def _record_skip(
    row_or_link: dict | str,
    reason: str | None = None,
    row: dict | None = None,
) -> None:
    """
    Record a skipped job. Accepts both the legacy signature
    `_record_skip(row, reason)` and the newer form `_record_skip(link, reason, row)`.
    """
    global skip_count, _seen_job_keys

    # Normalize inputs so downstream logic always has a dict row
    if isinstance(row_or_link, dict):
        row = row_or_link or {}
    else:
        row = row or {}
        row.setdefault("Job URL", row_or_link)

    # Preserve the skip reason on the row for later consumers
    if reason and not row.get("Reason Skipped"):
        row["Reason Skipped"] = reason

    url = (row.get("Job URL") or row.get("job_url") or "").strip()
    job_key = (row.get("Job Key") or url).strip()

    # Hard de-dupe: if we have already recorded this job once, stop here
    if job_key:
        if job_key in _seen_job_keys:
            # We have already recorded a KEEP or SKIP for this job
            return
        _seen_job_keys.add(job_key)

    # Keep URL-level stats for debugging
    if url:
        _seen_skip_urls.add(url)

    skipped_rows.append(to_skipped_sheet_row(row))
    skip_count += 1
    progress_clear_if_needed()
    _progress_after_decision()


def _progress_after_decision() -> None:
    """Advance the spinner counters after each keep/skip decision."""
    try:
        processed = kept_count + skip_count
        if processed < 0:
            processed = 0
        progress_tick(i=processed, kept=kept_count, skip=skip_count)
    except NameError:
        pass



def _debug_single_url(url: str):
    """Fetch and parse a single job URL, then pretty-print the details."""
    html = get_html(url)
    #if not html:
        #_bk_log_wrap("ERROR", ".Failed to fetch {url}")
    if not html or "we can't seem to find the page" in html.lower() or "<title>404" in html.lower():
        log_line("WARN", "Test URL returned a 404 page. Check the job URL.")
        return



    # Standard pipeline: extract → enrich salary → normalize
    details = extract_job_details(html, url)
    details = enrich_salary_fields(details, page_host=up.urlparse(url).netloc)
    details = _normalize_job_defaults(details)

    import json
    pretty = json.dumps(details, indent=2, sort_keys=True)
    #log_print(f"❖ DEBUG single-url …Parsed details:\n{pretty}")



##########################################################
##########################################################
##########################################################


# ---- Main ----
kept_rows = []       # the “keep” rows in internal-key form
skipped_rows = []    # the “skip” rows in internal-key form

#start_ts = None  # define at module level

def log_env_sanity_check(log_line):
    import os
    import sys
    import site

    py = sys.executable
    ver = sys.version.split()[0]
    venv = os.environ.get("VIRTUAL_ENV")
    prefix = sys.prefix
    base_prefix = getattr(sys, "base_prefix", None)

    in_venv = bool(venv) or (base_prefix and prefix != base_prefix)

    log_line("ENV", f"Python executable: {py}")
    log_line("ENV", f"Python version: {ver}")
    log_line("ENV", f"sys.prefix: {prefix}")
    if base_prefix:
        log_line("ENV", f"sys.base_prefix: {base_prefix}")
    log_line("ENV", f"VIRTUAL_ENV: {venv or ''}")
    log_line("ENV", f"In virtualenv: {in_venv}")

    if not in_venv:
        log_line("WARN", "Not running inside a virtual environment. Dependency installs may go to the wrong Python.")

    # Optional but helpful
    try:
        sp = site.getsitepackages()
        log_line("ENV", f"site-packages: {sp[-1] if sp else ''}")
    except Exception:
        pass


def require_optional_package(pkg_name: str, log_line, extra_hint: str = "") -> bool:
    try:
        __import__(pkg_name)
        return True
    except Exception as e:
        log_line("WARN", f"Optional dependency missing: {pkg_name} ({e.__class__.__name__})")
        log_line("WARN", f"Install with: {sys.executable} -m pip install {pkg_name}")
        if extra_hint:
            log_line("WARN", extra_hint)
        return False

def main(args: argparse.Namespace | None = None) -> None:
#   #global raw_print
#   global kept_count, skip_count, _seen_job_keys        #, start_ts  # add start_ts to globals
#   start_ts = datetime.now()
    if args is None:
        args = _parse_args()

    skip_row = None

    # If user passed a single test URL, handle it and exit early
    if getattr(args, "test_url", ""):
        _debug_single_url(args.test_url)
        return

    # Optional SMOKE presets
#    # make these globals visible outside main()
    global SMOKE, PAGE_CAP, LINK_CAP, LIST_LINKS, SALARY_FLOOR, SOFT_SALARY_FLOOR, SALARY_CEIL
    SMOKE = args.smoke or os.getenv("SMOKE") == "1"
    PAGE_CAP = args.limit_pages or (1 if SMOKE else 0)       # SMOKE: only hit the first listing site
    LINK_CAP = args.limit_links or (30 if SMOKE else 0)      # SMOKE: visit ≤ 20 job links
    LIST_LINKS = bool(getattr(args, "list_links", False))
    ONLY_KEYS = [s.strip().lower() for s in args.only.split(",") if s.strip()]

    SALARY_FLOOR = args.floor
    SOFT_SALARY_FLOOR = args.soft_floor or 0
    SALARY_CEIL  = args.ceil or None

#    PW_SUCCESS = 0
#    PW_FAIL = 0
#    REQ_FALLBACK = 0

    global kept_count, skip_count
    _seen_job_keys = set()
    start_ts = datetime.now()
    progress_clear_if_needed()
    info(f".Starting run")

    # Carry-forward map from Google Sheets: url -> (Applied?, Reason)
    global PRIOR_DECISIONS_CACHE
    prior_decisions: dict[str, tuple[str, str]] = {}

    prior_decisions = {}
    try:
        prior_decisions = fetch_prior_decisions(
            GS_SHEET_URL,
            key_path=GS_KEY_PATH,
            tab_name=GS_TAB_NAME,
        )
        info(f".Loaded {len(prior_decisions)} prior decisions for carry-forward.")
    except Exception as e:
        warn(f"[GS] No prior decisions loaded ({e}). Continuing without carry-forward.")
        prior_decisions = {}

    PRIOR_DECISIONS_CACHE = prior_decisions
    CLASSIFIER_CONFIG = ClassificationConfig(
        mode="review",              # you want all reasons for now
        allow_missing_salary=True,
        allow_near_min_salary=True,
        strict_age_policy=False,
        # you can tweak allowed cities/states/countries later if needed
    )

    seen_keys_this_run: set[str] = set()

    from urllib.parse import urlparse

    # 1) Build the final set of listing pages
    pages = list(STARTING_PAGES)

    # Temporarily disable HubSpot listings until title parsing is fixed
    pages = [p for p in pages if "hubspot.com/careers/jobs" not in p]

    # --- only-url override (single detail page run) ---
    only_url = (args.only_url or "").strip()

    all_detail_links: list[str] = []
    listing_ctx_by_url: dict[str, dict] = {}

    if only_url:
        all_detail_links = [only_url]
        log_line("INFO", f".Using only-url override (1 link): {only_url}")

    else:
        # Full run behavior stays the same
        if not (SMOKE and not args.limit_pages):
            pages += expand_career_sources()

        if ONLY_KEYS:
            pages = [u for u in pages if any(k in u.lower() for k in ONLY_KEYS)]

        if PAGE_CAP:
            pages = pages[:PAGE_CAP]

        total_pages = len(pages)

    # 1) Gather job detail links from each listing page
    if not only_url:
        for i, listing_url in enumerate(pages, start=1):
            t0 = time.time()
            if "hubspot.com/careers/jobs" not in listing_url:
                progress_clear_if_needed()
            set_source_tag(listing_url)
            html = get_html(listing_url)
            if not html:
                log_print(f"{_box('WARN')} {DOT3}{DOTW} Failed to fetch listing page: {listing_url}")
                continue

            # derive host safely from the listing URL
            p = up.urlparse(listing_url if isinstance(listing_url, str) else str(listing_url))
            host = p.netloc.lower().replace("www.", "")

            # HubSpot listing → handle pagination here and continue
            if "hubspot.com" in host and "/careers/jobs" in listing_url:
                links = collect_hubspot_links(listing_url, max_pages=25)
                all_detail_links.extend(links)
                elapsed = time.time() - t0
                progress_clear_if_needed()
                continue

            if "dice.com" in host and "/jobs" in up.urlparse(listing_url).path:
                links = collect_dice_links(listing_url, max_pages=25)
                all_detail_links.extend(links)
                continue

            # Workday listing → detail expansion
            if host.endswith("myworkdayjobs.com") or host.endswith("myworkdaysite.com"):
                wd_detail_links = collect_workday_jobs(
                    listing_url,
                    max_links=(LINK_CAP or None),
                )
                if not wd_detail_links:
                    wd_detail_links = workday_links_from_listing(listing_url, max_results=250)
                if wd_detail_links:
                    all_detail_links.extend(wd_detail_links)
                    elapsed = time.time() - t0
                    progress_clear_if_needed()
                    continue

            else:
                if "hubspot.com/careers/jobs" in listing_url:
                    links = collect_hubspot_links(listing_url, max_pages=25)
                elif "simplyhired.com/search" in listing_url:
                    links = collect_simplyhired_links(listing_url)
                else:
                    links = find_job_links(html, listing_url)

                if "dice.com/jobs" in listing_url:
                    links = collect_dice_links(listing_url, max_pages=25)
                elif "hubspot.com/careers/jobs" in listing_url:
                    links = collect_hubspot_links(listing_url, max_pages=25)

            # HubSpot pagination
            if "hubspot.com" in host and "/careers/jobs" in listing_url:
                page_num = 1
                hubspot_links = []
                while True:
                    page_url = (
                        re.sub(r"page=\d+", f"page={page_num}", listing_url)
                        if "page=" in listing_url
                        else (listing_url + ("&" if "?" in listing_url else "?") + f"page={page_num}")
                    )
                    progress_clear_if_needed()
                    set_source_tag(listing_url)
                    html = get_html(page_url)
                    links = parse_hubspot_list_page(html or "", base="https://www.hubspot.com")
                    if not links:
                        break
                    hubspot_links.extend(links)
                    page_num += 1
                    if page_num > 20:
                        break

                info(f".Found {len(hubspot_links)}.candidate job links on hubspot.com")
                all_detail_links.extend(hubspot_links)
                progress_clear_if_needed()
                continue

            # Generic collector
            if "simplyhired.com/search" in listing_url:
                links = collect_simplyhired_links(listing_url)
            else:
                links = find_job_links(html, listing_url)

            progress_clear_if_needed()
            all_detail_links.extend(links)


    def _norm_url(u: str) -> str:
        from urllib.parse import urlparse, urlunparse

        if not u:
            return ""

        u = str(u).strip().replace(" ", "")
        p = urlparse(u)
        host = (p.netloc or "").lower()

        path_raw = (p.path or "/").strip().rstrip("/") or "/"
        clean_path = path_raw if "ycombinator.com" in host else path_raw.lower()

        return urlunparse((
            (p.scheme or "https").lower(),
            host,
            clean_path,
            "", "", ""
        ))

    from urllib.parse import urlparse, parse_qsl, urlencode  # (at top of file if not already imported)

    # De-duplicate by normalized link key
    _seen = set()
    deduped_links = []
    for u in all_detail_links:
        k = link_key(u)
        if not k or k in _seen:
            continue
        _seen.add(k)
        deduped_links.append(u)

    all_detail_links = deduped_links
    #processed_keys: set[str] = set()

    # Optional caps (leave both; LINK_CAP takes precedence if you set it)
    # SMOKE keeps the shorter cap if you’re using it
    # --- Final normalization & deduplication ---
    import re
    from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

    def _normalize_link(url: str) -> str:
        """
        Normalize links for dedupe while preserving case-sensitive paths (YC).
        - Lowercase scheme and host
        - Preserve path casing for ycombinator.com, lowercase path elsewhere
        - Strip obvious tracking params
        - Sort kept query params for stability
        """
        if not url:
            return ""

        u = str(url).strip().replace(" ", "")
        p = urlparse(u)

        scheme = (p.scheme or "https").lower()
        host = (p.netloc or "").lower()

        path_raw = (p.path or "/").rstrip("/") or "/"
        path = path_raw if "ycombinator.com" in host else path_raw.lower()

        drop = {
            "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
            "ref", "referrer", "source", "_hsmi", "_hsenc", "gh_src", "page", "p", "start",
        }

        kept = []
        for k, v in parse_qsl(p.query or "", keep_blank_values=True):
            k_norm = (k or "").lower()
            if k_norm in drop:
                continue
            kept.append((k_norm, v))

        kept.sort()
        q = urlencode(kept, doseq=True)

        return urlunparse((scheme, host, path, "", q, ""))

    # === HARD DEDUPE AFTER EXPANSIONS, BEFORE PROGRESS TOTALS ===
    from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

    def _norm_url(u: str) -> str:
        """Lowercase host, strip tracking params and trailing slash, keep stable path."""
        p = urlparse(u)
        host = p.netloc.lower()
        # strip common trackers everywhere
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
            if k.lower() not in {"utm_source","utm_medium","utm_campaign","utm_term","utm_content",
                                "ref","source","_hsmi","_hsenc"}]
        if "workday" in host:
            q = [(k, v) for k, v in q if k.lower() not in {"q","t","timeType","locations","location","jobFamily"}]
        clean = urlunparse((
            p.scheme,
            p.netloc.lower(),
            p.path.rstrip("/"),
            "",  # params
            urlencode(q, doseq=True),
            ""   # fragment
        ))
        return clean

    # before you start dedupe and deduping
    before_total = len(all_detail_links)

    deduped = []
    _seen = set()

    # normalize each individual link, then de-dupe
    for raw_link in all_detail_links:
        link = _normalize_link(raw_link)

        if link in _seen:
            continue

        _seen.add(link)
        deduped.append(link)

    after_unique = len(deduped)
    #log_line("DE-DUPE", f"{DOT3}{DOTR} Reduced {before_total} → {after_unique} unique URLs")   ignored 20251215

    # use the cleaned list from here on
    all_detail_links = deduped



    # =============================================================

    # apply any explicit LINK_CAP after dedupe so duplicates don't shrink totals
    if LINK_CAP:
        all_detail_links = all_detail_links[:LINK_CAP]

    # now set your totals AFTER dedupe (and AFTER any smoke cap)
    if SMOKE:
        smoke_cap = args.limit_links or 30
        before_cap = len(all_detail_links)
        all_detail_links = all_detail_links[:smoke_cap]  # final cap for smoke runs
        if before_cap < smoke_cap:
            info(
                f".SMOKE run requested {smoke_cap} links but only "
                f"{before_cap} unique detail link{'s' if before_cap != 1 else ''} were discovered."
            )
        else:
            info(
                f".SMOKE run: limiting to {smoke_cap} job detail link{'s' if smoke_cap != 1 else ''}."
            )

    if LIST_LINKS:
        info(f".LIST links: {len(all_detail_links)} after dedupe/cap (showing below)")
        for idx, url in enumerate(all_detail_links, start=1):
            log_print(f"{_box('LIST')} {idx:02d}. {url}")
        return

    total = len(all_detail_links)
    info(f".Processing {total} detail link{'s' if total != 1 else ''} after dedupe/cap.")

    # progress setup
    kept_count = 0
    skip_count = 0
    progress_start(len(all_detail_links))

    try:
        for j, link in enumerate(all_detail_links, start=1):
            # ensure details is always defined, even if extract_job_details blows up
            details: dict = {}
            try:
                source_url = link

                # If you track listing context per detail URL, prefer it when present
                ctx = listing_ctx_by_url.get(link) if isinstance(listing_ctx_by_url, dict) else None
                if ctx:
                    source_url = ctx.get("listing_url") or ctx.get("source_url") or link

                set_source_tag(source_url)

                html = get_html(link)

                # A) Could not fetch detail page → record a minimal SKIP and continue
                if not html:
                    default_reason = "Failed to fetch job detail page"
                    board = career_board_name(link)

                    # If this is a SimplyHired job we saw on the listing page,
                    # reuse the captured title; otherwise fall back to URL-based guess.
                    meta_title = SIMPLYHIRED_TITLES.get(link, "")
                    title_for_log = _title_for_log({"Title": meta_title}, link)

                    skip_row = _normalize_skip_defaults({
                        "Job URL": link,
                        "Title": title_for_log,
                        "Company": board or "Missing Company",
                        "Career Board": board or "Missing Board",
                        "Reason Skipped": default_reason,
                        "WA Rule": "",
                        "BC Rule": "",
                        "ON Rule": "",
                        "Remote Rule": "",
                        "US Rule": "",
                        "Canada Rule": "",
                        "Salary Max Detected": "",
                        "Salary Rule": "",
                        "Location Chips": "",
                        "Applicant Regions": "",
                        "Applicant Regions Source": "",
                    })

                    _log_and_record_skip(link, default_reason, skip_row or {"Job URL": link})
                    continue

                if "ycombinator.com" in link:
                    html_now = html or ""
                    has_h1 = "ycdc-section-title" in html_now
                    #log_event("YC HTML", f"len={len(html_now)} | has_ycdc_h1={has_h1}")        #removed 20260108- activate if you want to log the length of the HTML and whether it contains the expected H1 element for YC job pages.

                    # One time dump to inspect the returned HTML
                    try:
                        with open("yc_debug.html", "w", encoding="utf-8") as f:
                            f.write(html_now)
                    except Exception:
                        pass

                # B) we have HTML -> parse details and enrich salary
                details = extract_job_details(html, link)

                # DEBUG one-off: YC location correctness
                try:
                    if "ycombinator.com" in (up.urlparse(link).netloc or "").lower() and "companies/gromo/jobs" in link:
                        host_dbg = (up.urlparse(link).netloc or "").lower()
                        log_line(
                            "YC CHECK",
                            f"Location={details.get('Location')!r} | Location Raw={details.get('Location Raw')!r} | "
                            f"Location Chips={details.get('Location Chips')!r} | Applicant Regions={details.get('Applicant Regions')!r} | "
                            f"Country Chips={details.get('Country Chips')!r} | host={host_dbg} | url={link}"
                        )
                except Exception:
                    pass

                if "ycombinator.com" in link:
                    log_line("YC TAP", f"Company={details.get('Company')!r} | Title={details.get('Title')!r} | job_url={details.get('job_url')!r}")        #removed 20260108- activate if you want to log the company, title, and job URL for each YC job page.

                listing = listing_ctx_by_url.get(link, {})
                details = _prefer_listing_location(details, listing)

                if DEBUG_LOCATION and "builtinvancouver.org" in link:
                    host = (urlparse(link).netloc or "").lower()
                    #log_line("BIV DEBUG", f"{DOTL}..prefer_listing: detail_loc={details.get('Location')!r} listing_loc={listing.get('Location')!r}")



                # 1) Try to pull structured JobPosting data (datePosted, validThrough, etc.)
                schema_bits = parse_jobposting_ldjson(html)
                if schema_bits:
                    # Only copy fields we care about; avoid overwriting with None
                    for key in ("Title", "Company", "posting_date", "valid_through", "posted"):
                        val = schema_bits.get(key)
                        if val:
                            details[key] = val

                # 2) Fallback: regex-based extraction from raw HTML (Muse, Remotive, etc.)
                html_dates = _extract_dates_from_html(html)
                if html_dates:
                    for k, v in html_dates.items():
                        if v:
                            details[k] = v

                # 3) Enrich salary and board
                # ... keep ALL your existing logic here unchanged ...
                # through keep_row construction, filters, salary gate, etc.

                with capture_debug_rows(details):
                    if "themuse.com" in link:
                        msg = (
                            "👀 DEBUG Muse dates "
                            f"Posting Date='{details.get('Posting Date', '')}' "
                            f"| Valid Through='{details.get('Valid Through', '')}' "
                            f"| Posted='{details.get('Posted', '')}'"
                        )
                        _append_debug_row(details, msg)

                    # Assign Job Key (URL dedupe already happened upstream)
                    jk = _job_key(details, link)
                    details["Job Key"] = jk

                                    # compute derived fields once
                details["WA Rule"] = details.get("WA Rule", "default")
                details["BC Rule"] = details.get("BC Rule", "default")
                details["ON Rule"] = details.get("ON Rule", "default")
                details["Remote Rule"] = details.get("Remote Rule", "default")
                details["US Rule"] = details.get("US Rule", "default") 
                details["Canada Rule"] = details.get("Canada Rule", "default")
                details["Reason"] = details.get("Reason","")  # leave as-is unless you set it
                vis, score, mark = compute_visibility_and_confidence(details)
                details["Visibility Status"] = vis
                details["Confidence Score"]  = score
                details["Confidence Mark"]   = mark

                # C) If extractor itself marked as removed/archived → SKIP with details
                if details.get("removed_flag"):
                    rm_reason = details.get("Reason") or "Marked removed on detail page"
                    skip_row = _normalize_skip_defaults({
                        "Job URL":              details.get("job_url", link),
                        "Apply URL":            details.get("apply_url", link),
                        "Title":                details.get("Title", ""),
                        "Job ID (Vendor)":      details.get("job_id_vendor",""),
                        "Job ID (Numeric)":     details.get("job_id_numeric",""),
                        "Job Key":              jk,
                        "Company":              details.get("Company",""),
                        "Career Board":         details.get("Career Board",""),
                        "Location":             details.get("Location",""),
                        "Posted":               details.get("Posted",""),
                        "Posting Date":         details.get("Posting Date",""),
                        "Valid Through":        details.get("Valid Through",""),
                        "Reason Skipped":       rm_reason,
                        "WA Rule":              details.get("WA Rule","default"),
                        "BC Rule":              details.get("BC Rule","default"),
                        "ON Rule":              details.get("ON Rule","default"),
                        "Remote Rule":          details.get("Remote Rule","default"),
                        "US Rule":              details.get("US Rule","default"),
                        "Canada Rule":          details.get("Canada Rule","default"),
                        "Salary Max Detected":  details.get("Salary Max Detected",""),
                        "Salary Rule":          details.get("Salary Rule",""),
                        "Salary Status":        details.get("Salary Status",""),
                        "Salary Note":          details.get("Salary Note",""),
                        "Salary Near Min":      details.get("Salary Near Min",""),
                        "Location Chips":       details.get("Location Chips",""),
                        "Applicant Regions":    details.get("Applicant Regions",""),
                        "Applicant Regions Source": details.get("Applicant Regions Source", ""),
                    })
                    _inherit_debug_rows(skip_row, details)
                    _log_and_record_skip(link, rm_reason, skip_row or {"Job URL": link})
                    continue

                # Build a normalized “keep” row
                keep_row = {
                    "Applied?": "",
                    "Reason": "",
                    "Date Scraped": now_ts(),
                    "Title": normalize_title(details.get("Title", ""), details.get("Company", "")),
                    "Job ID (Vendor)": details.get("job_id_vendor",""),
                    "Job ID (Numeric)": details.get("job_id_numeric",""),
                    "Job Key": details.get("Job Key", ""),
                    "Company": details.get("Company", "") or "Missing Company",
                    "Career Board": details.get("Career Board", "") or "Missing Board",
                    "Location": details.get("Location", ""),
                    "Posted": details.get("Posted", ""),
                    "Posting Date": details.get("Posting Date", ""),
                    "Valid Through": details.get("Valid Through", ""),
                    "Job URL": details.get("job_url", link),
                    "Apply URL": details.get("apply_url", link),
                    "Description Snippet": details.get("Description Snippet", ""),
                    "WA Rule": details.get("WA Rule", "default"),
                    "BC Rule": details.get("BC Rule", "default"),
                    "ON Rule": details.get("ON Rule", "default"),
                    "Remote Rule": details.get("Remote Rule") or details.get("is_remote_flag", "unknown_or_onsite"),
                    "US Rule": details.get("US Rule", "default"),
                    "Canada Rule": details.get("Canada Rule", "default"),
                    "Salary Max Detected": details.get("Salary Max Detected", ""),
                    "Salary Rule": details.get("Salary Rule", ""),
                    "Salary Status": details.get("Salary Status", ""),
                    "Salary Note": details.get("Salary Note", ""),
                    "Salary Near Min": details.get("Salary Near Min", ""),
                    "Salary Est. (Low-High)": details.get("Salary Est. (Low-High)", ""),
                    "Location Chips": _as_pipe_chips(details.get("Location Chips")) or "",
                    "Applicant Regions": _as_pipe_regions(details.get("Applicant Regions")) or "",
                    "Applicant Regions Source": (details.get("Applicant Regions Source") or "").strip().upper(),
                    "Visibility Status": details.get("Visibility Status", ""),
                    "Confidence Score": details.get("Confidence Score", ""),
                    "Confidence Mark": details.get("Confidence Mark", ""),
                }

                log_line("DEBUG", f"[AS_PIPE_CHIPS] pre_normalize Location Chips raw={details.get('Location Chips')!r}")
                log_line("DEBUG", f"[AS_PIPE_CHIPS] pre_normalize Location Chips norm={_as_pipe_chips(details.get('Location Chips'))!r}")
                # Basic remote/US rules (lightweight; you can expand later)
                remote_flag = details.get("is_remote_flag", "unknown_or_onsite")
                remote_rule = "default" if remote_flag == "remote" else "no_remote_signal"
                us_rule = "default"  # placeholder until you add stricter US checks
                wa_rule = "default"  # placeholder for WA logic

                # Normalize company and title before we build keep_row
                keep_row["Company"] = _normalize_company_name(keep_row["Company"])
                keep_row["Title"]   = normalize_title(keep_row["Title"], keep_row["Company"])

                # Derive location/remote/US rules now that details are populated
                keep_row_normalized = keep_row

                keep_row.update({
                    "Location": keep_row_normalized.get("Location", keep_row.get("Location", "")),
                    "Location Chips": keep_row_normalized.get("Location Chips", keep_row.get("Location Chips", "")),
                    "Applicant Regions": keep_row_normalized.get("Applicant Regions", keep_row.get("Applicant Regions", "")),
                    "Applicant Regions Source": keep_row_normalized.get(
                        "Applicant Regions Source",
                        keep_row.get("Applicant Regions Source", "")
                    ),
                })

                remote_rule = keep_row_normalized.get("Remote Rule", remote_rule)
                us_rule = keep_row_normalized.get("US Rule", us_rule)
                wa_rule = keep_row_normalized.get("WA Rule", wa_rule)
                keep_row["Remote Rule"] = remote_rule
                keep_row["US Rule"] = us_rule
                keep_row["WA Rule"] = wa_rule

                # Preserve explicit remote flag fields for downstream consumers/logs
                keep_row["remote_flag"] = details.get("remote_flag") or details.get("is_remote_flag") or remote_rule
                keep_row["Remote Flag"] = keep_row["remote_flag"]

                # IMPORTANT: carry Canada Rule into the row used by the classifier
                keep_row["Canada Rule"] = keep_row_normalized.get("Canada Rule") or details.get("Canada Rule", "")

                # YC fallback: ensure core fields are present for classification when Title is missing.
                if not (keep_row.get("Title") or "").strip():
                    job_url = (keep_row.get("Job URL") or "").strip()
                    inferred_title = _infer_yc_title_from_job_url(job_url)
                    if inferred_title:
                        keep_row["Title"] = inferred_title

                # Classification via the new rules helper
                row_for_classification = {
                    **keep_row,
                    "Salary Status": details.get("Salary Status", ""),
                    "Canada Rule": keep_row.get("Canada Rule") or details.get("Canada Rule", ""),
                }

                is_biv = "builtinvancouver.org" in (keep_row.get("Job URL") or "").lower()
                if is_biv:
                    raw_chips = (
                        row_for_classification.get("Location Chips")
                        or keep_row.get("Location Chips")
                        or details.get("Location Chips")
                        or ""
                    )

                    chips_set = {p.strip().upper() for p in re.split(r"[|,;/]", str(raw_chips)) if p.strip()}

                    # Only ever upgrade Fail -> Pass (never downgrade Pass -> Fail)
                    if "WA" in chips_set:
                        row_for_classification["WA Rule"] = "Pass"
                        keep_row["WA Rule"] = "Pass"

                    if ("USA" in chips_set) or ("US" in chips_set):
                        row_for_classification["US Rule"] = "Pass"
                        keep_row["US Rule"] = "Pass"

                    if ("CAN" in chips_set) or ("CANADA" in chips_set):
                        row_for_classification["Canada Rule"] = "Pass"
                        keep_row["Canada Rule"] = "Pass"

                    log_print(
                        f"{_box('ROW CHECK')}{DOT6}WA Rule= {row_for_classification.get('WA Rule')} "
                        f" |  Remote Rule= {row_for_classification.get('Remote Rule')} "
                        f" |  Canada Rule= {row_for_classification.get('Canada Rule')} "
                    )

                    log_print(
                        f"{_box('ROW CHECK')}{DOT6}US Rule= {row_for_classification.get('US Rule')} "
                        f" |  Location= {row_for_classification.get('Location')}"
                        f" |  US Rule= {row_for_classification.get('US Rule')} "
                    )

                is_keep, reason = classify_keep_or_skip(
                    row_for_classification,
                    CLASSIFIER_CONFIG,
                    seen_keys_this_run,
                )

                if not is_keep:
                    skip_row = _normalize_skip_defaults({
                        **keep_row,
                        "Reason Skipped": reason or "Filtered by rules",
                    })
                    _inherit_debug_rows(skip_row, keep_row)
                    _log_and_record_skip(link, reason or "Filtered by rules", skip_row)
                    continue

                vis, score, mark = _public_sanity_checks(keep_row)
                keep_row["Visibility Status"] = vis
                keep_row["Confidence Score"]  = score
                keep_row["Confidence Mark"]   = mark

                salary_blocked = False
                if _salary_status in ("near_min", "below_floor"):
                    if _salary_status == "near_min" and detect_salary_max and detect_salary_max >= SOFT_SALARY_FLOOR:
                        keep_row["Salary Rule"]       = "soft_keep"
                        keep_row["Salary Near Min"]   = detect_salary_max
                        keep_row["Visibility Status"] = "quiet"
                        keep_row["Confidence Mark"]   = "🟠"
                    elif _salary_status == "below_floor":
                        rule_label = keep_row.get("Salary Rule", "below_floor")
                        row = {
                            "Title":               keep_row["Title"],
                            "Job ID (Vendor)":     keep_row["Job ID (Vendor)"],
                            "Job ID (Numeric)":    keep_row["Job ID (Numeric)"],
                            "Job Key":             keep_row["Job Key"],
                            "Company":             keep_row["Company"],
                            "Career Board":        keep_row["Career Board"],
                            "Location":            keep_row["Location"],
                            "Posted":              keep_row["Posted"],
                            "Posting Date":        keep_row["Posting Date"],
                            "Valid Through":       keep_row["Valid Through"],
                            "Reason Skipped":      f"... out of target range (status={_salary_status}, max={detect_salary_max}",
                            "Apply URL":           keep_row["Apply URL"],
                            "Description Snippet": keep_row["Description Snippet"],
                            "WA Rule":             keep_row.get("WA Rule", ""),
                            "BC Rule":             keep_row.get("BC Rule", ""),
                            "ON Rule":             keep_row.get("ON Rule", ""),
                            "Remote Rule":         keep_row.get("Remote Rule", ""),
                            "US Rule":             keep_row.get("US Rule", ""),
                            "Canada Rule":         keep_row.get("Canada Rule", ""),
                            "Salary Max Detected": keep_row.get("Salary Max Detected", ""),
                            "Salary Rule":         rule_label,
                            "Salary Status":       keep_row.get("Salary Status", ""),
                            "Salary Note":         keep_row.get("Salary Note", ""),
                            "Salary Near Min":     keep_row.get("Salary Near Min", ""),
                            "Location Chips":      keep_row.get("Location Chips", ""),
                            "Applicant Regions":   keep_row.get("Applicant Regions", ""),
                            "Applicant Regions Source": keep_row.get("Applicant Regions Source", ""),
                        }

                        _inherit_debug_rows(row, keep_row)
                        _log_and_record_skip(link, row.get("Reason Skipped") or reason or "Filtered by rules", row)
                        salary_blocked = True

                if not salary_blocked:
                    if _record_keep(keep_row):
                        _log_keep_to_terminal(keep_row)
                    job = keep_row


            except Exception as e:
                # Catch ANY unexpected error for this job and record it
                tb_str = traceback.format_exc()
                err_msg = f"ERROR during job processing: {e}"


                log_print(f"{_box('ERROR')} {err_msg}")
                #log_print(f"{_box('WARN')} {DOT3}{DOTW} Failed to fetch listing page: {listing_url}")
                log_print(f"{_box('TRACEBACK')} {DOTL}Traceback (trimmed): {tb_str[-800:]}")


                # Minimal row: we always keep the Job URL and error reason
                error_row = {
                    "Title": "",
                    "Job ID (Vendor)": "",
                    "Job ID (Numeric)": "",
                    "Job Key": "",
                    "Company": "",
                    "Career Board": career_board_name(link),
                    "Location": "",
                    "Posted": "",
                    "Posting Date": "",
                    "Valid Through": "",
                    "Job URL": link,
                    "Reason Skipped": err_msg,
                    "WA Rule": "",
                    "BC Rule": "",
                    "ON Rule": "",
                    "Remote Rule": "",
                    "US Rule": "",
                    "Canada Rule": "",
                    "Salary Max Detected": "",
                    "Salary Rule": "",
                    "Location Chips": "",
                    "Applicant Regions": "",
                    "Applicant Regions Source": "",
                }

                # Attach a trimmed traceback to debug rows so you can inspect later
                _append_debug_row(error_row, f"{DOTL} Traceback (trimmed): {tb_str[-800:]}")

                # NEW: print the debug rows immediately for error rows
                _print_debug_rows_for(error_row)

                log_print(f"ERROR", "err_msg", error_row)
                continue


    finally:
        progress_done()

    log_final_reminder_if_needed(GS_SHEET_URL)

    # --- SMOKE safeguard: ask before writing anything -----------------
    # args is still in scope here inside main()
    if getattr(args, "smoke", False):
        try:
            reply = input(
                "\nSMOKE run complete. Save results to CSV/Sheets? [y/N]: "
            ).strip().lower()
        except EOFError:
            # Non-interactive (CI/automation) fallback: default to "no"
            reply = "n"

        if reply not in ("y", "yes"):
            info("SMOKE run: user chose not to save results; skipping all writes.")
            kept_rows.clear()
            skipped_rows.clear()
            return
    # ------------------------------------------------------------------

    # 3) Write CSVs once per run
    if "details" in locals() and isinstance(details, dict):
        trace_chips(details, "FINAL_BEFORE_OUTPUT")
        log_line("DEBUG", f"[FIELDS] Location={details.get('Location')} | Location Chips={details.get('Location Chips')} | Applicant Regions={details.get('Applicant Regions')} | ApplicantRegions={details.get('ApplicantRegions')}")
    write_rows_csv(OUTPUT_CSV, kept_rows, KEEP_FIELDS)
    write_rows_csv(SKIPPED_CSV, skipped_rows, SKIP_FIELDS)

    # 3b) Push to Google Sheets
    push_results_to_sheets(
        GS_SHEET_URL,
        kept_rows,
        skipped_rows,
        KEEP_FIELDS,
        SKIP_FIELDS,
        tab_name=GS_TAB_NAME,
        key_path=GS_KEY_PATH,
        progress_clear=progress_clear_if_needed,
    )

    kept_count = len(kept_rows)
    skip_count = len(skipped_rows)


    info(
        f".Playwright success {PW_SUCCESS}, failures {PW_FAIL}, fallbacks {REQ_FALLBACK}",
    )
    done_log(f".Kept {kept_count}, Skipped {skip_count} "
          f"in {(datetime.now() - start_ts).seconds}s")
    done_log(f".CSV: {OUTPUT_CSV}")
    done_log(f".CSV: {SKIPPED_CSV}")

    kept_rows.clear()
    skipped_rows.clear()


    # ---- Optional Git push (controlled by GIT_PUSH_MODE) ----
    commit_msg = f"scraper: {RUN_TS} kept={kept_count} skipped={skip_count}"
#    commit_msg = f"job-scraper: {RUN_TS} kept={kept_count} skipped={skip_count}"
    if GIT_PUSH_MODE == "auto":
        git_commit_and_push(commit_msg)
    elif GIT_PUSH_MODE == "prompt":
        git_prompt_then_push(commit_msg)
    # "manual" does nothing





########################################################################
""" How to flip behavior below for GitHub push after runs:
- Edit one line: PUSH_MODE = "off" | "ask" | "auto".
- Today: keep "ask" so you control each push.
- Later: set to "auto" for hands-free commits after every run.
- To completely disable, set to "off" or comment out the call block.
 """
########################################################################

# ===== GitHub push control (toggle: "off" | "ask" | "auto") =====
#GIT_PUSH_MODE = "auto" | "prompt" | "off")   # change to "auto" later, or "off" to disable
PUSH_MODE = "off"   # change to "auto" later, or "off" to disable


"""     # --- Optional GitHub push (last thing printed) ---
    if PUSH_MODE == "auto":
        maybe_push_to_git(prompt=False,
                        auto_msg=f"job-scraper: kept {kept_count}, skipped {skip_count} @ {now_ts()}")
    elif PUSH_MODE == "ask":
        maybe_push_to_git(prompt=True,
                        auto_msg=f"job-scraper: kept {kept_count}, skipped {skip_count} @ {now_ts()}")
    # "off" does nothing
 """


import subprocess, shlex, time as _t

def _git_run(cmd: str, cwd: str | None = None) -> tuple[int, str]:
    p = subprocess.run(shlex.split(cmd), cwd=cwd, capture_output=True, text=True)
    return p.returncode, (p.stdout + p.stderr).strip()

def _git_root() -> str | None:
    code, out = _git_run("git rev-parse --show-toplevel")
    return out if code == 0 else None

def _git_has_changes(root: str) -> bool:
    code, out = _git_run("git status --porcelain", cwd=root)
    return code == 0 and bool(out.strip())

def maybe_push_to_git(prompt: bool = True, auto_msg: str | None = None):
    root = _git_root()
    if not root:
        # just display and return (NO input here)
        #progress_clear_if_needed()
        info(f".Not a Git repo. Skipping push.")
        return

    if not _git_has_changes(root):
        info(f".No file changes to commit.")
        return

    # The ONLY interactive prompt
    if prompt:
        ans = input("Push code updates to GitHub now? [y/n] ").strip().lower()
        if ans != "y":
            # display a one-liner and exit (NO second input)
            info(f".Skipped push.")
            return

    # do the push
    _git_run("git add -A", cwd=root)
    msg = auto_msg or f"job-scraper: update @ {_t.strftime('%Y-%m-%d %H:%M:%S')}"
    _git_run(f'git commit -m "{msg}"', cwd=root)
    _git_run("git pull --rebase", cwd=root)
    code, out = _git_run("git push", cwd=root)
    if code == 0:
        info(f".Pushed to GitHub.")
    else:
        log_print("⚠️ WARN",".Push failed:\n" + out)


if __name__ == "__main__":
    main()
