### Very top of scraper ###


# --- Auto-backup section for job-scraper project ---
from urllib.parse import urlparse, urljoin, urlsplit, parse_qs, urlunparse
import shutil
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from bs4 import BeautifulSoup


import builtins  # use real print here, not the later override
import sys, os, time

# for the live progress spinner thread
import threading


# --- Unified CLI args & run configuration (single parse, early) ---
import argparse

def _parse_args():
    p = argparse.ArgumentParser(
        prog="po_job_scraper.py",
        description="Product jobs scraper – full run or quick smoke."
    )
    # run shape
    p.add_argument("--smoke", action="store_true",
                   help="Fast run: fewer pages/links, tighter timeouts")
    p.add_argument("--only", type=str, default="",
                   help="Comma list of site keywords to include (e.g. 'greenhouse,workday,hubspot')")
    p.add_argument("--limit-pages", type=int, default=0,
                   help="Hard cap on listing pages visited (0 = unlimited)")
    p.add_argument("--limit-links", type=int, default=0,
                   help="Hard cap on job detail links visited (0 = unlimited)")

    # salary knobs
    p.add_argument("--floor", type=int, default=110_000,
                   help="Minimum target salary filter")
    p.add_argument("--ceil",  type=int, default=0,
                   help="Optional salary ceiling; 0 means no ceiling")

    return p.parse_args()

ARGS = _parse_args()

# Run-time flags (downstream code reads these)
SMOKE       = bool(ARGS.smoke)
ONLY_FILTER = {s.strip().lower() for s in ARGS.only.split(",") if s.strip()}  # e.g. {'workday','greenhouse'}
PAGE_CAP    = int(ARGS.limit_pages) if ARGS.limit_pages else None
LINK_CAP    = int(ARGS.limit_links) if ARGS.limit_links else None

# Salary thresholds (globals read elsewhere)
SALARY_FLOOR = int(ARGS.floor)
SALARY_CEIL  = (int(ARGS.ceil) or None)

# Push control (leave as-is if you prefer to be prompted)
PUSH_MODE = "ask"     # "auto" | "ask" | "off"

# ensure logs and progress render immediately
try:
    os.environ["PYTHONUNBUFFERED"] = "1"
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

# ---- fixed-width, wrapped logger for long lines ----
_LOG_WRAP_WIDTH = 120

from urllib.parse import urlparse, urlunparse

def _link_key(u: str) -> str:
    """
    Lowercase host, strip 'www.', drop query and fragment, and
    remove a trailing '/' so /job/abc and /job/abc/ are the same.
    """
    try:
        p = urlparse(u)
        host = p.netloc.lower().replace("www.", "")
        path = p.path.rstrip("/")
        clean = urlunparse((p.scheme, host, path, "", "", ""))
        return clean
    except Exception:
        return (u or "").strip().rstrip("/")


def _bkts() -> str:
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

def _bk_log_wrap(section: str, msg: str, indent: int = 3, width: int = _LOG_WRAP_WIDTH):
    """Timestamped, wrapped logger that does NOT rely on the later print() override."""
    ts   = _bkts()
    head = f"{ts} [{section:22}]."
    text = " ".join(str(msg or "").split())
    if not text:
        builtins.print(head)
        return
    first, rest = text[:width], text[width:]
    builtins.print(f"{head}{first}")
    prefix = "." * indent
    while rest:
        chunk, rest = rest[:width], rest[width:]
        builtins.print(f"{ts} [{section:<22}].{prefix}{chunk}")

# put near your custom print() (right after _builtin_print = builtins.print)
#def raw_print(*args, **kwargs):
#    """Bypass the timestamping print for system lines (progress, GS)."""
#    return _builtin_print(*args, **kwargs)
# Modes:
#   "manual"  = do nothing
#   "prompt"  = ask at the end of the run
#   "auto"    = commit and push automatically at the end
GIT_PUSH_MODE = "prompt"    # change to "prompt" or "auto" when you want

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
        log_print("[GIT                   ].No changes to commit.")
        return
    # push
    push = _run(["git", "push"])
    if push.returncode == 0:
        log_print("[GIT                   ].Pushed to remote.")
    else:
        log_print("[GITERR                ].Push failed:")
        for ln in (push.stdout or "").splitlines():
            log_print(f"[GITERR                ].{ln}")

def git_prompt_then_push(default_msg: str) -> None:
    try:
        ans = input("[GIT] Push changes to GitHub now? (y/n) ").strip().lower()
        if ans == "y":
            git_commit_and_push(default_msg)
    except Exception:
        # non-interactive environment
        pass

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
        _bk_log_wrap("BACKUP", "Skipped (running inside Code Archive)")
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
        _bk_log_wrap("BACKUP", f"Saved {dest}")

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
                        _bk_log_wrap("BACKUP", f"Pruned by age {b.name}")
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
                    _bk_log_wrap("BACKUP", f"Pruned by count {old.name}")
                except OSError as e:
                    progress_clear_if_needed()
                    builtins.print(f"{_bkts()} [WARN                ].Could not remove {old.name}: {e}")

# --- HOW TO USE (uncomment exactly one) ---
backup_all_py_to_archive()                  # 1) keep ALL backups
# backup_all_py_to_archive(keep_last=10)    # 2) keep last 10 per file
# backup_all_py_to_archive(max_age_days=30) # 3) delete backups older than 30 days
# backup_all_py_to_archive(keep_last=10, max_age_days=60)  # combine both
# --- End auto-backup section ---



# --- Auto-install dependencies if missing (venv-friendly) ---
import sys, subprocess, os
from datetime import datetime
import builtins, datetime as _dt
_builtin_print = builtins.print

def log_printf(msg: str, ts: str) -> None:
    """Print one line using the provided timestamp (bypass the wrapper)."""
    progress_clear_if_needed()
    _builtin_print(f"{ts} {msg}")


def print(*args, **kwargs):
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

def log(section: str, msg: str) -> None:
    print(f"[{section:<22}].{msg}")   # padding in Terminal display

def _ensure(pkg: str):
    """Import if available; otherwise pip install and stream output with timestamps."""
    try:
        __import__(pkg.replace("-", "_"))
        _bk_log_wrap("SETUP", f"Requirement already satisfied: {pkg}")
    except ImportError:
        _bk_log_wrap("SETUP", f"Installing missing dependency: {pkg} ...")
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
                _bk_log_wrap("SETUP", line.strip())   # was: log("SETUP", line.strip())


for _p in REQUIRED_PACKAGES:
    _ensure(_p)

# --- Environment banner ---
log("ENV", f"Using Python from: {sys.executable}")
log("ENV", f"Virtual environment: {'Yes' if IN_VENV else 'No'}")
log("ENV", f"Working directory: {os.getcwd()}")

DEBUG_REMOTE = False  # flip to True only when tuning remote rules


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
    "remoteok.com", "builtin.com", "simplyhired.com", "themuse.com",
    "ycombinator.com", "remote.co"
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

from urllib.parse import urlparse

def _job_key(details: dict, link: str) -> str:
    """
    Prefer a durable page/job identifier; fall back to host + last path segment.
    Works for The Muse (job_id_numeric / job_id_vendor) and is harmless for others.
    """
    jid = (details.get("job_id_numeric")
           or details.get("job_id_vendor"))
    if jid:
        return f"{details.get('Career Board','')}|{jid}"

    p = urlparse(details.get("Job URL") or link)
    last = (p.path.rstrip("/").split("/") or [""])[-1]
    # if the last segment is empty (root), use the full path
    last = last or p.path.rstrip("/")
    return f"{p.netloc.lower()}|{last.lower()}"

def parse_date_relaxed(s):
    """Parse many date strings while neutralizing stray 'tzname' tokens like 'WI' or 'IL'.
    Some job pages concatenate a location right after a date, which confuses dateutil
    and emits UnknownTimezoneWarning. We map any unknown tzname to a zero offset.
    Returns ISO date string when possible, else the original string.
    """
    try:
        dt = dateparse_date_relaxed(str(s), fuzzy=True,
                              tzinfos=lambda name, offset: tzoffset(name, 0))
        return dt.date().isoformat()
    except Exception:
        return s
from urllib import robotparser
from playwright.sync_api import sync_playwright

import csv
from pathlib import Path
# --- Workday (generic) --------------------------------------------------------
import json
import urllib.parse as up

# ── Single-line progress indicator (safe; no title contamination) ──
import sys, time

_PROGRESS_ACTIVE = False

#def _progress_print(line: str) -> None:
#    """Draw/refresh the live, single-line progress row (no newline)."""
#    global _PROGRESS_ACTIVE, _PROGRESS_WIDTH
#    sys.stdout.write("\r" + line)
#    sys.stdout.flush()
#    _PROGRESS_ACTIVE = True
#    _PROGRESS_WIDTH = len(line)


def progress(i: int, total: int, kept: int, skipped: int) -> None:
    """Repaint the spinner at ~12fps; never prints a newline."""
    global _spin_i, _last_tick
    now = time.time()
    if now - _last_tick > 0.08:  # ~12 fps
        _spin_i = (_spin_i + 1) % len(_SPINNER)
        _last_tick = now
    spin = _SPINNER[_spin_i]
    _progress_print(f"[{spin} PROGRESS           ]...{i}/{total} kept={kept} skip={skipped}")



def progress_done(i: int, total: int, kept: int, skipped: int) -> None:
    """Finalize the live row with a newline so the cursor returns to a fresh line."""
    progress(i, total, kept, skipped)
    sys.stdout.write("\n")
    sys.stdout.flush()

# Always clear the live line if the process exits unexpectedly
import atexit
#atexit.register(progress_clear_if_needed)

def log_printf(msg: str, ts: str) -> None:
    """Print exactly one line using the provided timestamp, bypassing the wrapper."""
    progress_clear_if_needed()
    _builtin_print(f"{ts} {msg}")


# --- Safe log print: always clear the live progress line first ---
def log_print(msg: str) -> None:
    progress_clear_if_needed()
    print(msg)  # your timestamped print wrapper

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

def workday_links_from_listing(listing_url: str, max_results: int = 250) -> list[str]:
    """
    Convert a Workday listing URL into real job detail links by querying the cxs JSON API.
    Handles both myworkdaysite and myworkdayjobs patterns.
    """
    p = up.urlparse(listing_url)
    host = p.netloc
    parts = [s for s in p.path.split("/") if s]
    qs = up.parse_qs(p.query)
    search = " ".join(qs.get("q", [])).strip() or ""

    # tenant rules:
    # - /recruiting/<tenant>/<site> → take <tenant> from the path
    # - <tenant>.wdX.myworkdayjobs.com/<site> → take subdomain as tenant
    # - wdX.myworkdaysite.com/recruiting/<tenant>/<site> → still take from path
    tenant = None
    if parts and parts[0] == "recruiting" and len(parts) >= 3:
        tenant = parts[1]
    else:
        sub = host.split(".")[0]
        # sub like "wd5" is not a tenant; only use subdomain when it’s not wdN
        if not re.fullmatch(r"wd\d+", sub, re.I):
            tenant = sub
        # fallback for rare /<tenant>/<site> on myworkdayjobs.com
        if not tenant and len(parts) >= 2:
            tenant = parts[0]

    if not tenant:
        return []

    jobs = _wd_jobs(host, tenant, search, limit=50, max_results=max_results)

    out: list[str] = []
    for j in jobs:
        ext = j.get("externalUrl") or j.get("externalPath") or j.get("url")
        if not ext:
            continue
        out.append(up.urljoin(f"https://{host}/", ext))

    # de-dupe while preserving order
    seen, deduped = set(), []
    for u in out:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    return deduped


def _wd_jobs(host: str, tenant: str, search: str, limit: int = 50, max_results: int = 250) -> list[dict]:
    """
    Query Workday cxs jobs endpoint and return raw job dicts.
    Correct path is: /wday/cxs/{tenant}/jobs
    """
    url = f"https://{host}/wday/cxs/{tenant}/jobs"
    out, offset = [], 0
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0"
    }
    while True:
        payload = {"limit": min(limit, max_results - len(out)), "offset": offset, "searchText": search}
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
        if r.status_code != 200:
            break
        data = r.json() or {}
        items = data.get("jobPostings", []) or []
        if not items:
            break
        out.extend(items)
        if len(out) >= max_results or len(items) < limit:
            break
        offset += limit
    return out


#from urllib.parse import urlparse as _urlparse, parse_qs, urljoin

def _set_qp(url: str, **updates) -> str:
    p = up.urlparse(url)
    q = up.parse_qs(p.query)
    for k, v in updates.items():
        q[str(k)] = [str(v)]
    new_q = up.urlencode({k:v[0] for k,v in q.items()})
    return up.urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, p.fragment))

# BeautifulSoup is available already in your file
from bs4 import BeautifulSoup

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
            log_event("WARN", "", right=f"Failed to GET listing page: {url}")
            progress_clear_if_needed()
            #log_info_done()
            break

        links = parse_hubspot_list_page(html, url)
        elapsed = time.time() - t0
        progress_clear_if_needed()
        #log_info_found(len(links), url, elapsed)

        # de-dupe across pages
        added = 0
        for u in links:
            if u not in seen:
                seen.add(u)
                out.append(u)
                added += 1

        progress_clear_if_needed()
        #log_info_done()

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
        "business analyst", "systems analyst", "business systems analyst",
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


def parse_hubspot_detail(html: str, job_url: str) -> dict:
    """Extract title/company/location/snippet; capture Apply link if present."""
    soup = BeautifulSoup(html, "html.parser")

    # Title heuristics: try <h1>, og:title, or the page title
    title = ""
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
    for sel in [
        "[data-test='job-location']",
        ".job-location",
        "meta[property='og:locale']",
    ]:
        node = soup.select_one(sel)
        if not node:
            continue
        txt = node.get("content") if node.name == "meta" else node.get_text(" ", strip=True)
        if txt:
            loc = txt.strip()
            break

    # Description snippet (lightweight)
    desc = ""
    md = soup.find("meta", attrs={"name": "description"})
    if md and md.get("content"):
        desc = md["content"].strip()
    if not desc:
        p = soup.find("p")
        if p:
            desc = p.get_text(" ", strip=True)[:300]

    # Apply URL: look for outbound ATS links, else stick with job_url
    apply_url = ""
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if any(x in href for x in ("greenhouse.io", "lever.co", "myworkdayjobs.com", "smartrecruiters.com")):
            apply_url = up.urljoin(job_url, href)
            break
    if not apply_url:
        apply_url = job_url

    # Follow once if HubSpot page is a generic hub
    if title and title.strip().lower() == "all open positions" and apply_url and apply_url != job_url:
        try:
            html2 = get_html(apply_url)
            if html2:
                d2 = extract_job_details(html2, apply_url)
                d2 = enrich_salary_fields(d2, page_host=up.urlparse(apply_url).netloc)

                title = d2.get("Title") or title
                company = d2.get("Company") or company
                loc = d2.get("Location") or loc
                job_url = apply_url
        except Exception:
            pass

    details = {
        "Title": title,
        "Company": company or "HubSpot",
        "Location": loc,
        "job_url": job_url,
        "apply_url": apply_url,
        "description_snippet": desc,
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
            log_event("WARN", "", right=f"Failed to GET listing page: {page_url}")
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

def extract_job_details(html: str, job_url: str) -> dict:
    """
    Generic page detail parser used by many boards.
    Safe and defensive: never raises, returns a dict with the keys our pipeline expects.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    host = up.urlparse(job_url).netloc
    company_from_header = None
    board_from_header = None


    # ---- Title
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

    # ---- Company (try header, JSON-LD, or obvious labels)
    company = ""
    a = (
        soup.select_one("a.job-header__jobHeaderCompanyNameProgrammatic")
        or soup.select_one("a.job-header_jobHeaderCompanyNameProgrammatic")
        or soup.select_one("header a[href*='/profiles/']")
    )
    if a:
        company = (a.get_text(" ", strip=True) or "").strip()

    if not company:
        # JSON-LD hiringOrganization.name
        try:
            for script in soup.find_all("script", type="application/ld+json"):
                txt = script.string or ""
                if "hiringOrganization" in txt:
                    import json
                    data = json.loads(txt)
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
    
    #from bs4 import BeautifulSoup
    from urllib.parse import urlparse

    def company_from_header_meta(host: str, html: str) -> str | None:
        soup = BeautifulSoup(html or "", "html.parser")

        # common across many boards
        og = soup.find("meta", attrs={"property": "og:site_name"})
        if og and og.get("content"):
            return og["content"].strip()

        t = soup.find("title")
        if t:
            txt = t.get_text(" ", strip=True)
            # common HubSpot pattern: "Job Title - Company"
            if " - " in txt:
                maybe = txt.split(" - ")[-1].strip()
                if 2 <= len(maybe) <= 80:
                    return maybe

        # board-specific fallbacks
        h = host.lower()
        if "greenhouse.io" in h:
            # Greenhouse: Company shows in header breadcrumbs or meta name
            bc = soup.select_one('[data-mapped="employer_name"], .company-name, .app-title')
            if bc:
                return bc.get_text(" ", strip=True)
        if "builtin.com" in h:
            # Built In: often og:site_name is "Built In" so fall back to company link in header
            c = soup.select_one('a[href*="/company/"], .company__name, [data-test="company-name"]')
            if c:
                return c.get_text(" ", strip=True)
        if "hubspot.com" in h:
            # HubSpot: try meta og:title "Job Title - Company"
            ogt = soup.find("meta", attrs={"property": "og:title"})
            if ogt and " - " in ogt.get("content",""):
                return ogt["content"].rsplit(" - ", 1)[-1].strip()

        return None

    # ---- Location
    loc = ""
    cand = (
        soup.select_one("[class*='location']") or
        soup.select_one("li:has(svg) + li") or
        soup.find("span", string=lambda s: s and "remote" in s.lower())
    )
    if cand:
        loc = cand.get_text(" ", strip=True)

    # ---- Description (short)
    desc = ""
    md = soup.find("meta", attrs={"name": "description"})
    if md and md.get("content"):
        desc = md["content"].strip()
    if not desc:
        p = soup.find("p")
        if p:
            desc = p.get_text(" ", strip=True)[:300]
            page_txt = soup.get_text(" ", strip=True).lower()


    # ---- Build details and normalize
    details = {
        "Title": title,
        "Company": company,
        "Career Board": infer_board_from_url(job_url),
        "Location": loc,
        "Description": desc,
        "Description Snippet": desc,     # if you keep a shorter one
        "Job URL": job_url,
        "job_url": job_url,              # legacy key you already use
    }

    # normalize title/company first
    #details = _derive_company_and_title(details, html)

    # ---- capture full page text for rules/salary parsing ----
    page_txt = soup.get_text(" ", strip=True).lower()
    details["page_text"] = page_txt

    # ---- optional: capture The Muse Job ID (avoids false salary hits) ----
    m = re.search(r"\bJob\s*ID:\s*([A-Za-z0-9_-]+),?\s*(\d+)?", page_txt, re.I)
    if m:
        details["job_id_vendor"]  = m.group(1)
        if m.group(2):
            details["job_id_numeric"] = m.group(2)
    
    # after: page_txt = soup.get_text(" ", strip=True).lower()
    details["page_text"] = page_txt          # keep if you’re using it elsewhere
    details["Description"] = soup.get_text(" ", strip=True)
    details = enrich_salary_fields(details, page_host=host)


    # Location text
    loc_text = (details.get("Location") or "").strip()
    if not loc_text and "remote" in page_txt:
        details["Location"] = "Remote"

    # Location Chips (normalize to a lowercase set, then write back as list)
    chips = set()
    lc = details.get("Location Chips")
    if isinstance(lc, (list, tuple)):
        chips.update(str(x).lower() for x in lc if x)
    elif isinstance(lc, str) and lc:
        chips.update(x.strip().lower() for x in lc.split(",") if x.strip())

    if "remote" in page_txt:
        chips.add("remote")
    if details.get("Location", "").lower() == "remote":
        chips.add("remote")

    details["Location Chips"] = sorted(chips) if chips else []

    # Country Chips: mark US/Canada eligibility if we see obvious signals.
    country = set()
    cc = details.get("Country Chips")
    if isinstance(cc, (list, tuple)):
        country.update(str(x).lower() for x in cc if x)

    # Common signals in postings
    if any(t in page_txt for t in ("united states", "u.s.", "usa")):
        country.add("us")
    if "canada" in page_txt:
        country.add("canada")

    # --- Company (prefer header; then meta/title; then URL) ---
    company_from_header = ""
    host = up.urlparse(job_url).netloc.lower()

    # The Muse
    if "themuse.com" in host:
        node = (
            soup.select_one("header .job-header__jobHeaderCompanyNameProgrammatic")
            or soup.select_one("header .job-header__jobHeaderCompanyName")
            or soup.select_one(".job-details__company a")
            or soup.select_one("a[href^='/profiles/'], a[href^='/companies/']")
        )
        if node:
            company_from_header = node.get_text(" ", strip=True)

    # We Work Remotely
    if not company_from_header and "weworkremotely.com" in host:
        node = soup.select_one(".company-card .company, .company, .listing-header-container .company")
        if node:
            company_from_header = node.get_text(" ", strip=True)

    # NoDesk
    if not company_from_header and host.endswith("nodesk.co"):
        node = soup.select_one("h1 > p, a.company a, .company")
        if node:
            company_from_header = node.get_text(" ", strip=True)

    # HubSpot
    if not company_from_header and ("hubspot.com" in host):
        node = (
            soup.select_one("[data-company-name]")
            or soup.select_one(".job-metadata a[href*='company']")
            or soup.select_one("header .company a, header .company")
        )
        if node:
            company_from_header = node.get_text(" ", strip=True)

    # Greenhouse
    if not company_from_header and ("greenhouse.io" in host):
        node = (
            soup.select_one(".company-name")
            or soup.select_one(".app-title .company")
            or soup.select_one(".opening .company")
        )
        if node:
            company_from_header = node.get_text(" ", strip=True)

    # Built In
    if not company_from_header and ("builtin.com" in host):
        node = (
            soup.select_one("[data-test='company-name']")
            or soup.select_one(".job-hero__company a")
            or soup.select_one(".job-hero__company")
        )
        if node:
            company_from_header = node.get_text(" ", strip=True)
        if not company_from_header:
            m = re.search(r"builtin\.com/company/([^/?#]+)", job_url)
            if m:
                company_from_header = m.group(1).replace("-", " ")

    company = (
        _company_from_common_selectors(soup)
        or _company_from_meta_or_title(host, soup)
        #or company_from_header
        or company_from_url_fallback(job_url)
        or ""
    )
    company = _normalize_company_name(company)
    if company:
        details["Company"] = company

    if not company:
        fallback = company_from_header_meta(host, html)
        if fallback:
            company = fallback
    details["Company"] = company or "No Company Found"


    details["Description"] = soup.get_text(" ", strip=True)
    details = enrich_salary_fields(details, page_host=host)

    details["Country Chips"] = sorted(country)

    # Final title cleanup: strip trailing/leading company from Title using Company context
    details["Title"] = normalize_title(details.get("Title"), details.get("Company"))

    # Now enrich salary with host + full text available
    details = enrich_salary_fields(details, page_host=host)

    return details

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
      A) Title is an explicit match we want
         OR
      B) Title is a fuzzy neighbor AND responsibilities look like PO/PM/BA/BSA/Scrum work
    Titles on the exclude list always fail, even if responsibilities match.
    """
    title = normalize_title(d.get("Title") or "").lower()
    snippet = (d.get("Description Snippet") or "").lower()
    full = f"{title} {snippet}"

    # hard blocks first
    if any(re.search(p, title, re.I) for p in EXCLUDE_TITLES):
        return False

    # pass on clear titles
    if any(re.search(p, title, re.I) for p in INCLUDE_TITLES_EXACT):
        return True

    # fuzzy titles need responsibility corroboration
    fuzzy_hit = any(re.search(p, title, re.I) for p in INCLUDE_TITLES_FUZZY)
    if fuzzy_hit:
        resp_hits = len(set(m.group(0) for m in RESP_SIG_RX.finditer(full)))
        return resp_hits >= 2

    return False

def _is_remote(d: dict) -> bool:
    rr  = str(d.get("Remote Rule") or "").lower()
    loc = str(d.get("Location") or "").lower()
    chips_val = d.get("Location Chips")
    chips = " ".join(map(str, chips_val)).lower() if isinstance(chips_val, (list, tuple)) else str(chips_val or "").lower()

    text = " ".join([chips, loc, rr])

    # new fast-paths
    if "flexible / remote" in text or "flexible remote" in text:
        return True
    if " remote" in text or text.startswith("remote") or "/remote" in text:
        return True

    # keep your existing allow/deny patterns below
    if re.search(r"\bremote\b", text) or "anywhere" in text or "global" in text:
        return True
    return not re.search(r"\b(on[-\s]?site|in[-\s]?office|hybrid)\b", text)

def _is_us_canada_eligible(d: dict) -> bool:
    """
    Liberal when unknown, stricter when explicit non-NA regions appear.
    Accept any strong US/Canada mentions. Reject on strong non-NA signals.
    """
    # Country chips
    country_chips = d.get("Country Chips")
    countries = set()
    if isinstance(country_chips, (list, tuple)):
        countries.update(str(x).lower() for x in country_chips if x)
    else:
        for t in str(country_chips or "").lower().split(","):
            t = t.strip()
            if t:
                countries.add(t)

    # Location chips (can be list)
    loc_chips = d.get("Location Chips")
    if isinstance(loc_chips, (list, tuple)):
        loc_chips_txt = " ".join(map(str, loc_chips)).lower()
    else:
        loc_chips_txt = str(loc_chips or "").lower()

    txt = " ".join([
        str(d.get("Location") or "").lower(),
        str(d.get("Description Snippet") or "").lower(),
        loc_chips_txt,
    ])

    # Strong positives
    if countries & {"us", "usa", "united states", "canada", "ca"}:
        return True
    if any(p in txt for p in [
        "us only", "usa only", "anywhere in the us", "anywhere in the united states",
        "eligible to work in the us", "canada"
    ]):
        return True

    # Strong non-NA negatives
    if any(n in txt for n in ["europe", "emea", "apac", "latam", "australia", "new zealand", "uk only"]):
        return False

    # Default allow if we can't tell
    return True

def build_rule_reason(d: dict) -> str:
    reasons = []
    if not _is_target_role(d):
        reasons.append("Not target role")
    if not _is_remote(d):
        reasons.append("Not remote")
    if not _is_us_canada_eligible(d):
        reasons.append("Not US/Canada-eligible")

    # salary gating: treat 'below_floor' as a hard skip
    status = str(d.get("Salary Status") or "").lower()
    if status == "below_floor":
        reasons.append("Salary below floor")

    return ", ".join(reasons) or "Filtered by rules"

AUTO_SKIP_IF_APPLIED = False  # change to True if you want auto-skip on applied
def choose_skip_reason(d: dict, technical_fallback: str) -> str:
    if AUTO_SKIP_IF_APPLIED:
        applied = (d.get("Applied?") or "").strip()
        if applied:
            return f"Already applied on {applied}"
    rule = build_rule_reason(d)
    return rule if rule else technical_fallback



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
# catches “competitive … base … salary” even with words in between
SALARY_SIGNAL_RX = re.compile(r"\b(very|highly)?\s*competitive\b.*\bbase\b.*\bsalary\b", re.I | re.S)

def _contains_any(text: str, phrases: tuple[str, ...]) -> bool:
    t = (text or "").lower()
    return any(p in t for p in phrases)


from bs4 import BeautifulSoup  # you already import bs4 earlier; safe to use here


import argparse
# --- unified argparse (KEEP this and remove any earlier parser) ---
args = _parse_args()

# expose flags the rest of the file expects
SMOKE         = bool(getattr(args, "smoke", False))
LINK_CAP      = int(getattr(args, "limit_links", 0) or 0)
PUSH_MODE     = str(getattr(args, "push", "ask"))           # if your code uses this
GIT_PUSH_MODE = str(getattr(args, "git_push_mode", "ask"))  # if your code uses this

# salary knobs kept for backward compatibility
SALARY_FLOOR  = getattr(args, "floor", None)
SALARY_CEIL   = getattr(args, "ceil",  None)


# Salary rules
SALARY_FLOOR = 110000          # your target minimum
SOFT_SALARY_FLOOR = 90000      # below this => SKIP, between here and FLOOR => QUIET keep
# If True, jobs with no detectable salary are allowed (not skipped).
KEEP_UNKNOWN_SALARY = True  # treat "unknown salary" items as KEEP (True) or SKIP (False)

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
    - a detected max value
    - a status / note
    - a placeholder (e.g. "Competitive salary")
    """
    # Raw values from the row
    status      = (row.get("Salary Status") or "").strip()
    placeholder = (row.get("Salary Placeholder") or "").strip()
    est_str     = (row.get("Salary Est. (Low-High)") or "").strip()

    max_raw     = row.get("Salary Max Detected") or ""
    near_min    = bool(row.get("Salary Near Min"))

    parts: list[str] = []

    # 1) Preferred: human-friendly estimated range, if present
    if est_str:
        parts.append(est_str)

    # 2) Fallback: just show the max we saw
    if not est_str and max_raw:
        try:
            max_int = int(str(max_raw).replace(",", "").strip())
            parts.append(f"max ≈ ${max_int:,}")
        except ValueError:
            # Couldn't parse as int – just show whatever text we have
            parts.append(str(max_raw))

    # 3) If we’re near your configured floor, add a warning tag
    if near_min:
        parts.append("⚠ near floor")

    # 4) Status / note can give extra context (e.g. “estimated”, “signal-only”)
    if status:
        parts.append(f"({status})")

    # 5) Placeholder is things like “Competitive salary”, when no numbers exist
    if placeholder:
        parts.append(placeholder)

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
    "wellfound.com", "www.wellfound.com",
    "welcometothejungle.com", "www.welcometothejungle.com", 
    "app.welcometothejungle.com", "us.welcometothejungle.com",
    "workingnomads.com", "www.workingnomads.com",
    # JS-heavy boards that need Playwright
    "myworkdayjobs.com", "wd1.myworkdayjobs.com", "myworkdaysite.com", 
    "wd5.myworkdaysite.com","ashbyhq.com", "jobs.ashbyhq.com",
}


PLAYWRIGHT_DOMAINS.update({
    "www.ycombinator.com",
    "ycombinator.com",
    "about.gitlab.com",
    "zapier.com",
    "www.hubspot.com",
    "hubspot.com",
})

try:
    from playwright.sync_api import sync_playwright
except Exception:
    sync_playwright = None  # run without Playwright if not available

KNOWN = {
    # …keep existing…
    "remoteok.com": "Remote OK",
    "builtin.com": "Built In",
    "wellfound.com": "Wellfound",
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
    "Date Scraped","Title","Company",
    "Career Board",            # added
    "Location","Posted","Posting Date",
    "Valid Through",           # added
    "Job URL",
    "Reason Skipped","WA Rule","Remote Rule","US Rule",
    "Salary Max Detected","Salary Rule",
    "Location Chips","Applicant Regions",
]

STARTING_PAGES = [

    # Business Analyst / Systems Analyst
    "https://www.themuse.com/search/location/remote-flexible/keyword/business-analyst",
    #"https://www.themuse.com/jobs?categories=information-technology&location=remote&query=business%20analyst",
    #"https://www.builtin.com/jobs?search=business%20analyst&remote=true",
    "https://www.simplyhired.com/search?q=systems+analyst&l=remote",

    # Scrum Master / RTE
    "https://remotive.com/remote-jobs/product?search=scrum%20master",
    "https://www.builtin.com/jobs?search=scrum%20master&remote=true",


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

    # University of Washington (Workday) — focused keyword searches
    # These are narrow enough that you won’t fetch all 565 jobs.
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=product%20manager",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=product%20owner",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=business%20analyst",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=systems%20analyst",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=business%20systems%20analyst",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=scrum%20master",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=release%20train%20engineer",


    # Ascensus (Workday tenant) — focused role searches
    "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=product%20manager",
    "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=product%20owner",
    "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=business%20analyst",
    "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=systems%20analyst",
    "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=scrum%20master",
    # optional: release train engineer / RTE
    "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers/search?q=release%20train%20engineer",

    "https://jobs.ashbyhq.com/zapier",

    # The Muse works well
    "https://www.themuse.com/jobs?categories=product&location=remote",
    "https://www.themuse.com/jobs?categories=management&location=remote",

    # YC jobs (Playwright-friendly)
    "https://www.ycombinator.com/jobs/role/product-manager",

    # Remote OK
    "https://remoteok.com/?location=CA,US,region_NA",

    # Built In (JS-heavy → Playwright)
    "https://www.builtin.com/jobs?search=product%20manager&remote=true",
    "https://www.builtin.com/jobs?search=product%20owner&remote=true",

    # HubSpot (server-rendered listings; crawl like a board)
    "https://www.hubspot.com/careers/jobs?q=product&;page=1",
    # If you want a tighter filter and HubSpot supports it, you can also try:
    # "https://www.hubspot.com/careers/jobs?page=1&functions=product&location=Remote%20-%20USA",


    # Wellfound (AngelList Talent) (JS-heavy → Playwright)
    "https://wellfound.com/role/r/product-manager",

    # Welcome to the Jungle (JS-heavy → Playwright)
    "https://www.welcometothejungle.com/en/jobs?query=product%20manager&remote=true",
]

assert all(u.startswith("http") for u in STARTING_PAGES), "A STARTING_PAGES entry is missing a comma."


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

# ---- US state normalization and "excluded states" extractor ----
HOME_STATE = "Washington"   # set your home state once

_US_STATE_NAMES = [
    "alabama","alaska","arizona","arkansas","california","colorado","connecticut","delaware",
    "florida","georgia","hawaii","idaho","illinois","indiana","iowa","kansas","kentucky","louisiana",
    "maine","maryland","massachusetts","michigan","minnesota","mississippi","missouri","montana",
    "nebraska","nevada","new hampshire","new jersey","new mexico","new york","north carolina",
    "north dakota","ohio","oklahoma","oregon","pennsylvania","rhode island","south carolina",
    "south dakota","tennessee","texas","utah","vermont","virginia","washington","west virginia",
    "wisconsin","wyoming","washington d.c.","district of columbia","dc"
]
_US_ABBR = {
    "al":"alabama","ak":"alaska","az":"arizona","ar":"arkansas","ca":"california","co":"colorado",
    "ct":"connecticut","de":"delaware","fl":"florida","ga":"georgia","hi":"hawaii","id":"idaho",
    "il":"illinois","in":"indiana","ia":"iowa","ks":"kansas","ky":"kentucky","la":"louisiana",
    "me":"maine","md":"maryland","ma":"massachusetts","mi":"michigan","mn":"minnesota","ms":"mississippi",
    "mo":"missouri","mt":"montana","ne":"nebraska","nv":"nevada","nh":"new hampshire","nj":"new jersey",
    "nm":"new mexico","ny":"new york","nc":"north carolina","nd":"north dakota","oh":"ohio","ok":"oklahoma",
    "or":"oregon","pa":"pennsylvania","ri":"rhode island","sc":"south carolina","sd":"south dakota",
    "tn":"tennessee","tx":"texas","ut":"utah","vt":"vermont","va":"virginia","wa":"washington",
    "wv":"west virginia","wi":"wisconsin","wy":"wyoming","dc":"washington d.c."
}
_STATES_RX = re.compile(r"\b(" + "|".join(
    [re.escape(s) for s in _US_STATE_NAMES] + list(_US_ABBR.keys())
) + r")\b", re.I)

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



# Canada eligibility signals
CA_HINTS = re.compile(
    r"\b(canada|canadian|eligible to work in canada|authorized to work in canada|"
    r"(?:^|[^A-Z])(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)(?:[^A-Z]|$)|"
    r"time\s*zone\s*[:\-]?\s*(ast|adt|nst|ndt))\b",
    re.I,
)

REMOTE_KEYWORDS = [
    r"\bremote\b", r"remote[-\s]?first", r"\b(us|u\.s\.)\s*remote\b",
    r"\banywhere\b.*\b(us|u\.s\.)\b", r"\bwork from home\b"
]
ONSITE_BLOCKERS = [
    r"\bon[-\s]?site\b", r"\boffice[-\s]?based\b"
]
SINGLE_CITY_PATTERNS = [
    r"\bnew york( city)?\b|\bnyc\b", r"\bsan francisco\b|\bsf\b",
    r"\bseattle\b", r"\baustin\b", r"\blondon\b", r"\bparis\b"
    # add more as you encounter them
]


# ---- Helpers ----
# Exact column order for "keep" CSV
KEEP_FIELDS = [
    "Applied?","Reason","Date Scraped",
    "Title","Company",
    "Career Board",            # NEW (between Company and Location)
    "Location","Posted","Posting Date",
    "Valid Through",           # NEW (between Posting Date and Job URL)
    "Job URL","Apply URL","Description Snippet",
    "WA Rule","Remote Rule","US Rule",
    "Salary Max Detected","Salary Rule","Salary Near Min",
    "Salary Status", "Salary Note", "Salary Est. (Low-High)",
    "Location Chips","Applicant Regions",
    "Visibility Status","Confidence Score","Confidence Mark",
]


# Exact column order for "skip" CSV
SKIP_FIELDS = [
    "Date Scraped","Title","Company",
    "Career Board",            # NEW
    "Location","Posted","Posting Date",
    "Valid Through",           # NEW
    "Job URL","Reason Skipped",
    "WA Rule","Remote Rule","US Rule",
    "Salary Max Detected","Salary Rule","Salary Near Min",
    "Salary Status", "Salary Note", "Salary Est. (Low-High)",
    "Location Chips","Applicant Regions",
]

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
            if attempt == retries:
                for ln in f"Failed to GET {url}\n{e}".splitlines():
                    log_event("WARN", ln)
                return None
            time.sleep(backoff * (attempt + 1))


def career_board_name(url: str) -> str:
    """Return a human-friendly name for the site that hosts the job page."""
    host = up.urlparse(url).netloc.lower().replace("www.", "")
    # Friendly names for common hosts
    KNOWN = {
        "nodesk.co": "NoDesk",
        "careerb​uilder.com": "CareerBuilder",
        "weworkremotely.com": "We Work Remotely",
        "remoteok.com": "Remote OK",
        "remotive.com": "Remotive",
        "simplyhired.com": "SimplyHired",
        "themuse.com": "The Muse",
        "ycombinator.com": "Y Combinator",
        "jobs.lever.co": "Lever",
        "boards.greenhouse.io": "Greenhouse",
        "workday.com": "Workday",
        "myworkdaysite.com": "Workday",
        "wd5.myworkdaysite.com": "Workday",
        "glassdoor.com": "Glassdoor",
        "builtin.com": "Built In",
        "www.builtin.com": "Built In",
        "welcometothejungle.com": "Welcome to the Jungle",
        # in career_board_name(url) or similar mapping
        "ascensushr.wd1.myworkdayjobs.com": "Ascensus (Workday)",
        "wd5.myworkdaysite.com/recruiting/uw/": "University of WA (Workday)",
        "myworkdayjobs.com": "Workday",
        "myworkdaysite.com": "Workday",
        "edtech.com": "EdTech",
        "edtechjobs.io": "EdTech Jobs",

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

    # Built In
    if "builtin.com" in host:
        # /job/<category>/<slug>/<id>
        return path.startswith("/job/") and path.count("/") >= 3

    # Wellfound (AngelList Talent)
    if "wellfound.com" in host:
        # They use several patterns; accept /jobs/<id-or-slug> and /l/<slug>
        return (path.startswith("/jobs/") and path.count("/") >= 2) or path.startswith("/l/")

    # Welcome to the Jungle (web + app)
    if "welcometothejungle.com" in host or "app.welcometothejungle.com" in host:
        # e.g., /en/companies/<company>/jobs/<slug> or /en/jobs/<id>
        return "/jobs/" in path and not path.endswith("/jobs") and path.count("/") >= 3

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
    cap = 120 if ("ashbyhq.com" in base_host or "myworkdayjobs.com" in base_host or "myworkdaysite.com" in base_host) else None

    # --- Ashby special-case: accept both "/{org}/{slug}" and "/{org}/jobs/{id-or-slug}" ---
    if "ashbyhq.com" in base_host:
        for a in anchors:
            href = a["href"].strip()
            full = up.urljoin(base_url, href)
            p = up.urlparse(full)
            if p.netloc.lower().endswith("ashbyhq.com"):
                if re.fullmatch(r"/[^/]+/[^/]+/?", p.path) and "departmentid=" not in p.query.lower():
                    links.add(full)
                elif re.fullmatch(r"/[^/]+/jobs/[^/]+/?", p.path):
                    links.add(full)
            if cap and sum(1 for L in links if base_host in L) >= cap:
                break
        return list(links)

    # --- default path (everything else) ---
    for a in anchors:
        href = a["href"].strip()
        full = up.urljoin(base_url, href)
        if is_job_detail_url(full):
            links.add(full)
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

    elif host.endswith("builtin.com") or host.endswith("www.builtin.com"):
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
    t = html.unescape(str(t)).strip()
    # light cleanup only
    #t = re.sub(r"\s+", " ", t)

    if not company:
        return t

    comp_norm = _norm_company_for_compare(company)
    if not comp_norm:
        return t

    # Try strip trailing “… – Company”
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
 

PLACEHOLDER_RX = re.compile(r"search by company rss feeds public api", re.I)

def best_location_for_display(ld: dict, chips: str, scraped_loc: str) -> str:
    # 1) Prefer explicit region tokens in chips (skip pure "Remote")
    if chips:
        tokens = [p.strip() for p in chips.split("|") if p.strip()]
        tokens = [t for t in tokens if t.lower() not in ("remote", "global", "anywhere")]
        for key in ("US", "USA", "United States", "Canada", "North America", "Europe", "EU", "EMEA", "UK"):
            for t in tokens:
                if key.lower() in t.lower():
                    return t
        if tokens:
            return tokens[0]

    # 2) LD-JSON locations
    locs = ld.get("locations") or []
    if locs:
        nice = [l for l in locs if l and l.lower() not in ("remote", "global", "anywhere")]
        if nice:
            return ", ".join(nice[:2])

    # 3) Scraped location unless it was the crawler placeholder
    if scraped_loc and not PLACEHOLDER_RX.search(scraped_loc):
        return scraped_loc

    # 4) Last resort
    return "Remote"

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


def enrich_salary_fields(d: dict, page_host: str | None = None) -> dict:
    """
    Single source of truth for salary parsing + classification.

    Produces:
      - Salary Max Detected (int or "")
      - Salary Est. (Low-High) (str)
      - Salary Near Min (int or "")
      - Salary Status: at_or_above | near_min | below_floor | estimated | unknown
      - Salary Placeholder: e.g., "Competitive base salary" when phrases present but no numbers
      - Salary Rule: compact label for Sheets filtering
      - Salary Range Text: raw text we scanned (debugging)
    """
    import re

    # ---- Config (uses your global floors if present) ----
    SALARY_FLOOR = 100_000  # your floor
    SOFT_SALARY_FLOOR = 85_000

    # --- init (prevents UnboundLocalError) ---
    est_low = None
    est_high = None
    nums = []
    max_detected = None
    salary_signal = False

    try:
        floor = SALARY_FLOOR
    except NameError:
        floor = 120_000
    try:
        soft = SOFT_SALARY_FLOOR
    except NameError:
        soft = max(0, floor - 15_000)

    TRIGGER_PHRASES = (
        "competitive base", "competitive salary", "competitive compensation",
        "very competitive", "highly competitive", "market competitive",
        "salary commensurate", "commensurate with experience",
        "market rate", "salary doe", "doe", "compensation package",
    )
    # ---- Build a single blob from fields we already have ----
    sr = d.get("Salary Range", "")
    sr_text = sr.get("Text") if isinstance(sr, dict) else (sr or "")
    parts = [
        d.get("Title", ""),
        d.get("Company", ""),
        d.get("Location", ""),
        d.get("Description") or d.get("Description Snippet") or "",
        (d.get("page_text", "") or "")[:4000],  # new
    ]
    # ---- build/cleanup text for salary scan ----
    # (keep your existing joins above)
    blob = " ".join([
        str(d.get("Title","")),
        str(d.get("Company","")),
        str(d.get("Description Snippet","") or d.get("Description","") or ""),
        str(d.get("page_text",""))  # <-- ensure this is included
    ]).lower()
    blob_clean = re.sub(r"\s+", " ", blob).strip()
    
    import re

    # 1) Remove anything on / after a “Job ID:” line (Muse puts a huge numeric tail here)
    blob = re.sub(r"(?mi)^\s*job\s*id\s*:.*$", "", blob)

    # 2) Remove very long naked digit blobs (7+ digits) that are almost never salaries
    blob = re.sub(r"\b\d{7,}\b", "", blob)

    # 3) Kill URLs and querystrings (often pack long digits)
    blob = re.sub(r"https?://\S+", "", blob)

    # --- harden against benefit numbers like 401(k) / 403(b) / 457(b) etc. ---
    # operate on the current working text: 'blob'
    blob = re.sub(r"\b401\s*\(\s*k\s*\)|\b401k\b", "401k_plan", blob, flags=re.I)
    blob = re.sub(r"\b403\s*\(\s*b\s*\)|\b403b\b", "403b_plan", blob, flags=re.I)
    blob = re.sub(r"\b457\s*\(\s*b\s*\)|\b457b\b", "457b_plan", blob, flags=re.I)

    # neutralize retirement-plan strings so they can't be misread as $401,000 etc.
    blob_clean = re.sub(r"\b401\s*[\(\-\s]*k\)?\b", "retirement_plan", blob_clean, flags=re.I)
    blob_clean = re.sub(r"\b401k\b", "retirement_plan", blob_clean, flags=re.I)
    blob_clean = re.sub(r"\b457\s*b\b", "retirement_plan", blob_clean, flags=re.I)

    # neutralize retirement-plan tokens so they can't look like money
    blob_clean = re.sub(r"\b401\s*\(?k\)?\b", "401k plan", blob_clean, flags=re.I)
    blob_clean = re.sub(r"\b457\s*b\b", "457b plan", blob_clean, flags=re.I)


    # ---- helpers for number normalization ----
    def _to_money_int(s: str | None) -> int | None:
        if not s:
            return None
        t = str(s).strip().lower()
        # keep a memory of whether 'k' was present
        has_k = t.endswith("k")
        # strip all non-digits
        digits = re.sub(r"[^\d]", "", t)
        if not digits:
            return None
        try:
            n = int(digits)
        except Exception:
            return None
        if has_k:
            n *= 1000
        return n

    # ---- money patterns ----
    # We ONLY count numbers that carry a salary signal:
    #  - currency symbol/word near number, OR
    #  - explicit 'k' suffix (e.g., 160k), OR
    #  - '/year', 'per year', 'annual', 'yr'
    MONEY_RX = re.compile(r"""
    (?:
        (?P<cur>\$|usd|eur|gbp)\s*               # currency (required if no 'k' or per-year words)
    )?
    (?P<num>
        \d{1,3}(?:[ ,]\d{3})+ |                  # 120,000  or  1 200 000
        \d{2,3}                                  # 80 .. 999 (with optional k)
    )
    (?P<ksuf>\s*[kK])?                            # 120k
    (?:
        \s*(?:/year|per\s*year|per\s*annum|annual|yr) # optional per-year words
    )?
    """, re.X)

    TRIGGER_PHRASES = (
        "competitive base", "competitive salary", "competitive compensation",
        "very competitive", "highly competitive", "market competitive",
        "salary commensurate", "commensurate with experience",
        "market rate", "salary doe", "doe", "compensation package",
    )
    # ---- region-preferring extraction (Washington first) ----
    def _find_range_for(region: str, text: str):
        """
        Matches a sentence like:
        "In Washington, the expected compensation ... between $90,000.00 and $120,000.00"
        Returns (lo, hi) as ints or None.
        """
        rx = re.compile(
            rf"{region}[^$]{{0,160}}\$?\s*([0-9][\d,\.]+)\s*(?:to|-|–|—|and)\s*\$?\s*([0-9][\d,\.]+)",
            re.I | re.S
        )
        m = rx.search(text)
        if m:
            lo = _to_int(m.group(1))
            hi = _to_int(m.group(2))
            if lo and hi:
                return lo, hi
        return None

    # Prefer Washington first (also catches "California and Washington")
    est_low = est_high = None
    max_detected = None  # ensure this is initialized before we may reference it

    wa_hit = _find_range_for("washington", blob_clean)
    if wa_hit:
        est_low, est_high = wa_hit
        max_detected = est_high  # seed with WA high end
        status = "at_or_above" if max_detected >= floor else ("near_min" if max_detected >= soft else "below_floor")
        rule = "At/above min" if status=="at_or_above" else ("Near min" if status=="near_min" else "Below floor")

        d["Salary Max Detected"] = max_detected
        d["Salary Est. (Low-High)"] = f"{est_low:,}–{est_high:,}"
        d["Salary Near Min"] = max_detected if status=="near_min" else ""
        d["Salary Status"] = status
        d["Salary Rule"] = rule
        d["Salary Range Text"] = sr_text or ""
        d["Salary Note"] = d.get("Salary Note","")
        d["Salary Placeholder"] = d.get("Salary Placeholder","")
        # We still continue into the generic scan below, but WA values are already set.
        return d

        # ---- scan (collect span-aware hits) ----
    nums = []
    status = ""
    est_low = est_high = None

    # --- scan (collect span-aware hits) ---
    hits = []           # (value, local_span_text)
    for m in MONEY_RX.finditer(blob_clean):
        full = m.group(0).lower()

        if "401k" in full or "401(k)" in full or "457b" in full or "457(b)" in full:
            continue  # skip retirement plan numbers

        # require a clear per-year cue so 401(k)/457(b) etc. don’t look like salaries
        has_per_year = any(w in full for w in ("/year", " per year", " per annum", " annual", " yr"))
        if not has_per_year:
            continue

        val  = _to_money_int(full)  # or your (num + k-suffix) handling as you have it
        if val is None or val < 20_000 or val > 1_000_000:
            continue

        # keep nearby text so we can detect “washington” proximity later
        span = blob_clean[max(0, m.start()-120): m.end()+120]
        hits.append((val, span))

    # prefer any hit whose nearby text mentions Washington; otherwise take the overall max
    wa_hits = [h for h in hits if "washington" in h[1]]
    pick = (max(wa_hits, key=lambda h: h[0]) if wa_hits
            else (max(hits, key=lambda h: h[0]) if hits else None))

    max_detected = pick[0] if pick else (max(nums) if nums else None)
    if nums:
        est_low, est_high = min(nums), max(nums)


    max_detected = max(nums) if nums else None
    if nums:
        lo = min(nums)
        hi = max(nums)
        est_low, est_high = lo, hi

    # ---- phrase signal even when no numbers ----
    phrase_hit = any(p in blob for p in TRIGGER_PHRASES)
    if phrase_hit:
        d["Salary Status"] = d.get("Salary Status") or "estimated"
        d["Salary Rule"] = d.get("Salary Rule") or "Signal"
        d["Salary Note"] = d.get("Salary Note") or "Competitive salary language on page"
        d["Salary Placeholder"] = d.get("Salary Placeholder") or "Competitive base salary"
        return d

    # ---- classify ----
    try:
        floor = SALARY_FLOOR
    except NameError:
        floor = 100_000

    try:
        soft = SOFT_SALARY_FLOOR
    except NameError:
        soft = max(0, floor - 15_000)

    if max_detected is not None:
        if max_detected >= floor:
            status = "at_or_above"
        elif max_detected >= soft:
            status = "near_min"
        else:
            status = "below_floor"
    else:
        status = "estimated" if phrase_hit else "unknown"

    rule = (
        "At/above min" if status == "at_or_above" else
        "Near min"     if status == "near_min"     else
        "Below floor"  if status == "below_floor"  else
        "Estimated"    if status == "estimated"    else
        "Unknown"
    )

    # Optional human notes
    if any(p in blob for p in TRIGGER_PHRASES):
        d["Salary Status"] = d.get("Salary Status") or "Signal"
        d["Salary Note"] = d.get("Salary Note") or "Competitive salary language on page"
        d["Salary Placeholder"] = d.get("Salary Placeholder") or "[💲 SALARY]"

    # ---- write fields back ----
    d["Salary Max Detected"] = max_detected or ""
    d["Salary Est. (Low-High)"] = f"{est_low:,}–{est_high:,}" if (est_low and est_high) else ""
    d["Salary Near Min"] = max_detected if status == "near_min" else ""
    d["Salary Status"] = status
    d["Salary Rule"] = rule
    d["Salary Range Text"] = blob[:300]  # optional for debug
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
    

def fetch_html_with_playwright(url, user_agent=USER_AGENT, engine="chromium"):
    if sync_playwright is None:
        return None
    try:
        with sync_playwright() as p:
            browser_type = getattr(p, engine)  # "chromium" | "firefox" | "webkit"
            browser = browser_type.launch(headless=True, args=["--no-sandbox"])
            context = browser.new_context(user_agent=user_agent)
            page = context.new_page()
            page.goto(url, timeout=PW_GOTO_TIMEOUT, wait_until="domcontentloaded")
            
            try:
                #from urllib.parse import urlparse
                h = up.urlparse(url).netloc.lower()

                if h.endswith("jobs.ashbyhq.com"):
                    # Wait until job cards/links render (Ashby uses <a href="/<org>/<slug>">)
                    page.wait_for_selector("a[href*='/jobs/']:not([href$='/jobs'])", timeout=PW_WAIT_TIMEOUT*2)
                    # Gentle scroll to trigger lazy loads
                    page.mouse.wheel(0, 2500)
                    page.wait_for_timeout(800)

                elif h.endswith("myworkdayjobs.com") or h.endswith("myworkdaysite.com"):
                    # Workday often needs a little extra settle time
                    page.wait_for_timeout(1200)
                
                elif h.endswith("wellfound.com"):
                    page.wait_for_selector("a[href^='/jobs/'], a[href^='/l/']", timeout=PW_WAIT_TIMEOUT*2)
                    page.mouse.wheel(0, 3000)
                    page.wait_for_timeout(800)
                
                elif h.endswith("welcometothejungle.com"):
                    # Wait for the main content
                    page.wait_for_selector("main, [data-testid='job-offer']", timeout=PW_WAIT_TIMEOUT*2)

                    # Click any visible "View more" expanders so hidden sections load
                    try:
                        # Role-based locator first
                        buttons = page.get_by_role("button", name=re.compile(r"view more", re.I))
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
                pass

                        # Auto-scroll for EdTech listing pages (infinite scroll)
            try:
                #from urllib.parse import urlparse
                host = up.urlparse(url).netloc.lower()
                path = up.urlparse(url).path or "/"

                needs_autoscroll = (
                    host in {"edtech.com", "www.edtech.com"}
                    and "/jobs/" in path
                    and path.endswith("-jobs")
                )

                if needs_autoscroll:
                    # 3a) click 'More' buttons if present
                    try:
                        for _ in range(50):
                            btn = page.query_selector("button:has-text('More'), a:has-text('More'), button:has-text('Load'), a:has-text('Load')")
                            if not btn:
                                break
                            btn.click()
                            page.wait_for_timeout(900)
                    except Exception:
                        pass

                    # 3b) then deep autoscroll until stable
                    _autoscroll_listing(
                        page,
                        link_css='a[href^="/jobs/"]:not([href$="-jobs"])',
                        max_loops=1100,
                        idle_ms=2000
                    )
            except Exception:
                # don’t fail the run if autoscroll logic hiccups
                pass

            # (this `try:` already existed below — leave it)
            try:
                page.wait_for_selector(
                    "a[href^='/job/'], a[href*='/jobs/'], a[href*='/remote-jobs/'], "
                    "a:has(h2), a:has(h3), main article",
                    timeout=PW_WAIT_TIMEOUT,
                )

            except Exception:
                pass
            # after page.goto(...)
            try:
                page.wait_for_selector(
                    "script[src*='greenhouse.io/embed/job_board'], "
                    "iframe[src*='greenhouse'], "
                    "a[href*='jobs.lever.co'], "
                    "a[href*='ashbyhq.com']",
                    timeout=PW_WAIT_TIMEOUT
                )
            except Exception:
                pass  # fall back to whatever's loaded
            html = page.content()
            context.close()
            browser.close()
            return html
    except Exception as e:
        msg = f"Playwright failed on {url} ({e.__class__.__name__}):\n{e}\nFalling back to requests."
        for ln in msg.splitlines():
            log_event("WARN", ln)
        return None



def get_html(url):
    domain = up.urlparse(url).netloc.lower()
    if domain in PLAYWRIGHT_DOMAINS:
        html = fetch_html_with_playwright(url)
        return html  # do not attempt requests() fallback for PW-only sites
    resp = polite_get(url)
    return resp.text if resp else None

#import re
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
        log_print(f"[CAREERS               ]...Probing {url}")
        html = get_html(url)
        if not html:
            log_event("WARN", "", right=f"Failed to GET listing page: {url}")
            progress_clear_if_needed()
            log_print(f"[CAREERS               ]...Could not fetch {url}")
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
            log_print(f"[CAREERS               ]...{url} -> {len(found)} board(s)")
            pages.extend(sorted(found))
        else:
            progress_clear_if_needed()
            log_print(f"[CAREERS               ]....No ATS links found on {url}")

    return pages

import json
from datetime import datetime, timedelta, timezone

def now_ts() -> str:
    # ISO-like stamp sortable to the second
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _coerce_list(x): return x if isinstance(x, list) else ([] if x is None else [x])

def _short(s, width=125):
    s = " ".join((s or "").split())
    return (s[: width - 1] + "…") if len(s) > width else s

def parse_jobposting_ldjson(html):
    """Parse schema.org JobPosting blocks from inline JSON-LD."""
    soup = BeautifulSoup(html, "html.parser")

    def _set_if(d: dict, key: str, val):
        if val not in (None, "", []):
            d[key] = val

    out = {}
    for s in soup.find_all("script", type="application/ld+json"):
        raw = s.string or s.text or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        items = data if isinstance(data, list) else [data]
        for it in items:
            if not isinstance(it, dict):
                continue
            t = it.get("@type") or it.get("@type".lower())
            # Accept JobPosting in any casing or list form
            if isinstance(t, list):
                t = t[0] if t else None
            if not t or "jobposting" not in str(t).lower():
                continue

            # Title (handle both 'title' and 'Title')
            raw_title = it.get("title") or it.get("Title")
            if raw_title:
                _set_if(out, "Title", normalize_title(raw_title))

            # Company
            org = it.get("hiringOrganization") or it.get("hiringorganization")
            if isinstance(org, dict):
                _set_if(out, "Company", org.get("name"))

            # Job location type
            _set_if(out, "job_location_type",
                    it.get("jobLocationType") or it.get("joblocationtype") or "")

            # Locations (stringify address)
            locs = []
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
            if (it.get("jobLocationType") in ("TELECOMMUTE", "https://schema.org/Telecommute")
                and "Remote" not in locs):
                locs.append("Remote")
            if locs:
                _set_if(out, "locations", [l for l in locs if l])

            # Applicant location requirements -> applicant_regions
            apps = it.get("applicantLocationRequirements") or it.get("applicantlocationrequirements")
            regions = []
            def _as_list(x): return x if isinstance(x, list) else ([x] if x else [])
            for a in _as_list(apps):
                if isinstance(a, dict):
                    v = (a.get("name")
                         or a.get("addressRegion") or a.get("addressregion")
                         or a.get("addressCountry") or a.get("addresscountry"))
                    if isinstance(v, str) and v.strip():
                        regions.append(v.strip())
                elif isinstance(a, str) and a.strip():
                    regions.append(a.strip())
            if regions:
                _set_if(out, "applicant_regions", regions)

            # Dates
            dp = it.get("datePosted") or it.get("dateposted")
            if dp:
                try:
                    iso = datetime.fromisoformat(dp.replace("Z", "+00:00")).date().isoformat()
                except Exception:
                    iso = dp
                _set_if(out, "date_posted", iso)

            vt = it.get("validThrough") or it.get("validthrough")
            if vt:
                try:
                    iso = datetime.fromisoformat(vt.replace("Z", "+00:00")).date().isoformat()
                except Exception:
                    try:
                        #iso = dateparser.parse(vt).date().isoformat()
                        iso = parse_date_relaxed(vt)
                    except Exception:
                        iso = vt
                _set_if(out, "valid_through", iso)

    return out


# --- Role taxonomy: allow ONLY these families (title-first), plus close neighbors by responsibility ---

# exact title hits we want
INCLUDE_TITLES_EXACT = [
    r"\bproduct owner\b",
    r"\bproduct manager\b",
    r"\bgroup product manager\b", r"\bstaff product manager\b", r"\bprincipal product manager\b",
    r"\bbusiness analyst\b",
    r"\bsystems analyst\b",
    r"\bbusiness systems analyst\b",
    r"\bbusiness system analyst\b",
    r"\bbusiness systems engineer\b",
    r"\bscrum master\b",
    r"\brelease train engineer\b", r"\brte\b",
    
]

# looser title hits we will allow only if responsibilities also match
INCLUDE_TITLES_FUZZY = [
    r"\bproduct\s+operations?\b", r"\bprod\s*ops\b",
    r"\bproduct\s+analyst\b",
    r"\bbusiness\s+analyst\b",
    r"\brequirements?\s+(?:analyst|engineer)\b",
    r"\bsolutions?\s+analyst\b",
    r"\bimplementation\s+analyst\b",
    r"\btechnical\s+program\s+manager\b",       # only with responsibilities (see _is_target_role)
    r"\bbusiness\s+systems?\s+analyst\b",     # Business System(s) Analyst
    r"\boperations?\s+business\s+analyst\b",  # Operations Business Analyst
    r"(senior\s+)?business\s+(system|systems|intelligence)?\s*analyst"
    r"|operations\s+business\s+analyst"
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
    r"\b(product\s+marketing|growth|brand|demand\s+gen)\b",
    r"\b(project\s+manager)\b(?!.*\bproduct\b)",
    r"\b(data|financial|research|credit)\s+analyst\b",
    r"\bdata\s+(scientist|engineer)\b",
    r"\b(?:ml|ai)\s+(engineer|scientist)\b",
    r"\b(dev|backend|frontend|full[-\s]?stack|software|platform|sre|qa|test)\s+engineer\b",
    r"\bdesigner|ux|ui|visual\s+design|graphic\s+design\b",
    r"\bsales|account\s+manager|customer\s+success|support\b",
    r"\bhr|recruiter|talent|people\s+ops\b",
    r"\bfinance|payroll|bookkeep|accountant\b",
    r"\boperations?\b(?!.*\bproduct\b)",
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
            "business analyst", "systems analyst", "business systems analyst",
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

# --- ANSI color helpers ---
RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"
RED = "\033[31m"; YELLOW = "\033[33m"; GREEN = "\033[32m"; CYAN = "\033[36m"

# Choose colors by level
LEVEL_COLOR = {
    "ERROR": RED,
    "WARN":  YELLOW,
    "INFO":  CYAN,
    "KEEP":  GREEN,
    "SKIP":  DIM,
    "SETUP": RESET,
    "ENV":   RESET,
    "DONE":  RESET,
    "GS":    CYAN,
}


# unique tracking to avoid double-appends across the run
_seen_kept_urls: set[str] = set()
_seen_skip_urls: set[str] = set()
kept_count: int = 0
skip_count: int = 0

def _log_keep_to_terminal(row: dict) -> None:
    ts = now_ts()
    title = (row.get("Title") or "").strip()
    url   = (row.get("Job URL") or "").strip()
    desc = (row.get("Description") or "").strip()
    company, board = _company_and_board_for_terminal(row)
    salary_line    = _fmt_salary_line(row)
    
    progress_clear_if_needed()
    log_print(f"[{ts}] [KEEP                  ].{title}")
    log_print(f"[{ts}] [KEEP                  ]...{board} → {company}")
    if url:
        log_print(f"[{ts}] [KEEP                 ].{url}")
    if salary_line:
        log_print(f"[{ts}] [KEEP     ] {salary_line}")
    log_print(f"[{ts}] [✅ DONE               ].")

    progress_clear_if_needed()


#def _log_skip_terminal(row: dict, link: str, rule_reason: str) -> None:
#    ts = now_ts()
#    title = (row.get("Title") or "").strip() or _title_from_url(link)
#    company, board = _company_and_board_for_terminal(row)
#
#    c = LEVEL_COLOR.get("SKIP", DIM)  # dim
#    r = RESET
#
#    progress_clear_if_needed()
#    log_print(f"{c}[SKIP                  ].{title}{r}")
#    log_print(f"{c}[SKIP                  ]...{board} → {company}{r}")
#    log_print(f"{c}[SKIP                  ]...{link}{r}")
#    log_print(f"{c}[💭 REASON             ]... → {rule_reason}{r}")
#    log_print(f"{c}[🚫 DONE               ].{r}")
#    progress_clear_if_needed()



DOT1 = "...."      # 4 dots
DOT2 = "......"    # 6 dots

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
    # repaint the progress line if one is active
    #refresh_progress()


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
    return _box("INFO ")

def _found_box(n: int) -> str:
    # right-align the count to 3 spaces: 0..999
    return _box(f"🔎 FOUND {n:>3}")

def _done_box() -> str:
    return _box("✔ DONE")




# =====================================================================
# 🔍 Remote / Onsite detection + confidence constants
# =====================================================================
from datetime import datetime


KEEP_REASON_FIELD = "Reason"           # lives only in Table1
SKIP_REASON_FIELD = "Reason Skipped"   # lives only in SKIPPED tab

SIMPLYHIRED_BASE = "https://www.simplyhired.com"

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
    text   = ""
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        ats_ok = (r.status_code == 200)
        text   = r.text or ""
    except Exception:
        pass

    # If we previously fetched the company careers page HTML, you can pass it in later;
    # for now we just use the URL host as a proxy (Lever/Greenhouse/Ashby usually = public)
    host = up.urlparse(url).netloc.lower()
    listed_on_careers = any(h in host for h in ("greenhouse", "lever", "ashbyhq", "workday", "icims", "smartrecruiters"))
    has_recent_date = has_recent(text, days=45)  # you already have has_recent()

    # Reuse your compact scorer → (visibility, score, mark)
    return label_visibility(
        ats_status_200 = ats_ok,
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

REL_POSTED_RE = re.compile(r"(\d+)\s*(day|days|d|hour|hours|h|minute|minutes|min)s?\s*ago", re.I)
def compute_posting_date_from_relative(rel_text):
    m = REL_POSTED_RE.search(rel_text or ""); 
    if not m: return None
    n, unit = int(m.group(1)), m.group(2).lower()
    today = datetime.now(timezone.utc).date()
    if unit.startswith(("day","d")): return (today - timedelta(days=n)).isoformat()
    return today.isoformat()


# --- Salary helpers ---
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

        if any(w in surround for w in ("organizations", "users", "employees", "customers")) and not has_currency and not unit:
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


def to_keep_sheet_row(d: dict) -> dict:
    return {
        "Applied?": d.get("Applied?",""),
        "Reason": d.get("Reason",""),        # <— keep this
        "Date Scraped": d.get("Date Scraped") or now_ts(),
        "Title": d.get("Title",""),
        "Company": d.get("Company",""),
        "Career Board": d.get("Career Board", ""),
        "Location": d.get("Location",""),
        "Posted": d.get("Posted",""),
        "Posting Date": d.get("Posting Date",""),
        "Valid Through": d.get("Valid Through", ""),
        "Job URL": d.get("Job URL",""),
        "Apply URL": d.get("Apply URL",""),
        "Description Snippet": d.get("Description Snippet",""),
        "WA Rule": d.get("WA Rule",""),
        "Remote Rule": d.get("Remote Rule",""),
        "US Rule": d.get("US Rule",""),
        "Salary Max Detected": d.get("Salary Max Detected",""),
        "Salary Rule": d.get("Salary Rule",""),
        "Salary Status": d.get("Salary Status",""),
        "Salary Note": d.get("Salary Note",""),
        "Salary Est. (Low-High)": d.get("Salary Est. (Low-High)",""),
        "Location Chips": d.get("Location Chips",""),
        "Applicant Regions": d.get("Applicant Regions",""),
        "Visibility Status": d.get("Visibility Status",""),
        "Confidence Score": d.get("Confidence Score",""),
        "Confidence Mark": d.get("Confidence Mark",""),
    }

def to_skipped_sheet_row(d: dict) -> dict:
    return {
        "Date Scraped": d.get("Date Scraped") or now_ts(),
        "Title": d.get("Title",""),
        "Company": d.get("Company",""),
        "Career Board": d.get("Career Board", ""),
        "Location": d.get("Location",""),
        "Posted": d.get("Posted",""),
        "Posting Date": d.get("Posting Date",""),
        "Valid Through": d.get("Valid Through", ""),
        "Job URL": d.get("Job URL",""),
        "Reason Skipped": d.get("Reason Skipped",""),
        "WA Rule": d.get("WA Rule",""),
        "Remote Rule": d.get("Remote Rule",""),
        "US Rule": d.get("US Rule",""),
        "Salary Max Detected": d.get("Salary Max Detected",""),
        "Salary Rule": d.get("Salary Rule",""),
        "Location Chips": d.get("Location Chips",""),
        "Applicant Regions": d.get("Applicant Regions",""),
        "Apply URL": d.get("Apply URL",""),
        "Description Snippet": d.get("Description Snippet",""),
        "Visibility Status": d.get("Visibility Status",""),
        "Confidence Score": d.get("Confidence Score",""),
        "Confidence Mark": d.get("Confidence Mark",""),
    }

# ==== Make GS lines non-timestamped (match your example format)
def push_rows_to_google_sheet(rows, keys, tab_name=None):
    """
    Append a list of dict rows to Google Sheets in one batch.
    - rows: list[dict]
    - keys: list[str]
    - tab_name: optional, overrides the default tab name
    """
    if not rows:
        return

    try:
        import gspread
#        # Import Credentials defensively: some environments may not expose
#        # google.oauth2.service_account directly (static analyzers can also flag it).
#       try:
        from google.oauth2.service_account import Credentials
#       except Exception:
#            # Try an alternative access pattern and provide a clear fallback.
#            try:
#                from google.oauth2 import service_account as _sa
#                Credentials = _sa.Credentials
#            except Exception:
#                progress_clear_if_needed()
#                log_print("[GS                    ].Skipping Sheets push; missing google oauth credentials library (install 'google-auth').")
#                return
    except Exception as e:
        progress_clear_if_needed()
        log_print(f"[GS                    ].Skipping Sheets push; missing libs: {e}")
        return

    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(GS_KEY_PATH, scopes=scopes)
        client = gspread.authorize(creds)

        sh = client.open_by_url(GS_SHEET_URL)
        target_tab = tab_name or GS_TAB_NAME

        try:
            ws = sh.worksheet(target_tab)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=target_tab, rows="2000", cols="40")


        existing = ws.row_values(1)

        # Make sure the sheet has enough columns for the header
        if ws.col_count < len(keys):
            ws.resize(rows=ws.row_count, cols=len(keys))

        if not existing:
            # No header yet → write it once
            ws.update('A1', [keys], value_input_option="USER_ENTERED")
        # else: leave existing header exactly as-is


        values = [[str(r.get(k, "")) for k in keys] for r in rows]

        CHUNK = 200
        for i in range(0, len(values), CHUNK):
            ws.append_rows(values[i:i+CHUNK], value_input_option="USER_ENTERED")

        progress_clear_if_needed()
        log_print(f"[GS                    ].Appended {len(values)} rows to '{target_tab}'.")
    except Exception as e:
        progress_clear_if_needed()
        log_print(f"[GSERR                 ].Failed to push to Google Sheets: {e}")

def fetch_prior_decisions():
#    """Fetch prior Applied? and Reason values from Google Sheets."""
#    try:
    import gspread
    from google.oauth2.service_account import Credentials
#    except Exception as e:
#        progress_clear_if_needed()
#        log_print(f"[GS                    ].Skipping loading prior decisions; missing libs: {e}")
#        return {}

#    try:
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds  = Credentials.from_service_account_file(GS_KEY_PATH, scopes=scopes)
    client = gspread.authorize(creds)
    sh     = client.open_by_url(GS_SHEET_URL).worksheet(GS_TAB_NAME)
#    ws     = sh.worksheet(GS_TAB_NAME)

    # get_all_records uses row 1 as header, returns list of dicts
    records = sh.get_all_records()  # respects your current headers
    prior = {}
    for r in records:
        url = r.get("job_url") or r.get("Job URL")  # tolerant if header renamed
        if url:
            prior[url] = (r.get("Applied?",""), r.get("Reason",""))
    return prior
#    except Exception as e:
#        progress_clear_if_needed()
#        log_print(f"[GS                    ].Failed to fetch prior decisions: {e}")
#        return {}

import sys
# {_stamp()}
import sys as _sys

# ===== Progress line (carriage-return) =====
import re, time, sys


###############################
### KEEP THIS SECTION BELOW ###
###############################
# ===== unified PROGRESS helpers (single source of truth) =====
import sys, re, time

# spinner frames
_SPINNER = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
LEVEL_WIDTH = 22  # keep this equal to your other logger's width- padding in Terminal display
_last_tick = 0.0
_spin_i = 0


# live progress state
_p = {
    "active": False,   # spinner is on screen
    "width":  0,       # printable width of the last drawn line (no ANSI)
    "spin":   0,       # index into _SPINNER
    "total":  0,       # total work items (optional; for display)
    "start":  0.0,     # start time
}

from wcwidth import wcswidth

def _ansi_strip(s: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", s or "")

def _display_width(s: str) -> int:
    # printable width for strings that may include emoji/ANSI
    return sum(max(0, wcwidth(ch)) for ch in _ansi_strip(s))

LEVEL_WIDTH = 22  # (whatever you’re using)

def _box(label: str) -> str:
    raw = (label or "")[:LEVEL_WIDTH]
    vis = _display_width(raw)
    pad = max(0, LEVEL_WIDTH - vis)
    return f"[{raw}{' ' * pad}]"

def _center_fit(label: str, width: int) -> str:
    """Truncate by display width and center-pad to a fixed width."""
    s = (label or "")
    out, used = [], 0
    for ch in s:
        w = wcswidth(ch)
        if w < 0:   # unknown width -> treat as 1
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
    sys.stdout.write("\r" + msg + "\r")
    sys.stdout.flush()
    _p["active"] = True
    _p["width"]  = len(_ansi_strip(msg))

def progress_set_total(n: int) -> None:
    global _PROGRESS_TOTAL
    _PROGRESS_TOTAL = int(n or 0)
    _p["total"] = _PROGRESS_TOTAL

# keep this function where your logger utils live, near _p / _box / _progress_print
def progress_clear_if_needed() -> None:
    if _p["active"]:
        # carriage return + clear to end of line, then flush
        sys.stdout.write("\r" + " " * _p["width"] + "\r")
        sys.stdout.flush()
        _p["active"] = False
        _p["width"] = 0

# public alias used by the rest of the code
progress_clear_if_needed = progress_clear_if_needed
atexit.register(progress_clear_if_needed)

import atexit
try:
    atexit.register(progress_clear_if_needed)
except NameError:
    # if a refactor temporarily renamed the function, fail silently
    pass

progress_clear_if_needed = progress_clear_if_needed



def progress_line(spin: str) -> str:
    """
    Compose the human-readable single-line status.
    NOTE: This uses your existing globals: kept_count, skip_count, now_ts().
    If these are not yet defined above this block, either move this block
    below them or change this function to fetch them another way.
    """
    processed = kept_count + skip_count   # <-- your globals
    total     = _p.get("total", 0) or "?"
    ts        = now_ts()                  # <-- your helper
    return f"{ts} {_box('PROGRESS ')} {spin} {processed}/{total} kept={kept_count} skip={skip_count}"

def progress_tick(msg: str | None = None) -> None:
    """Advance spinner one frame and repaint the sticky line."""
    if not _p["active"]:
        return
    _p["spin"] = (_p["spin"] + 1) % len(_SPINNER)
    spin = _SPINNER[_p["spin"]]
    line = progress_line(spin)
    if msg:
        line += f"  {msg}"
    _progress_print(line)

def start_spinner(total: int | None = None, msg: str | None = None) -> None:
    """Start (or restart) the spinner."""
    _p.update({"active": True, "spin": 0, "total": total or 0, "start": time.time()})
    spin = _SPINNER[_p["spin"]]
    line = progress_line(spin)
    if msg:
        line += f"  {msg}"
    _progress_print(line)

def stop_spinner(final_msg: str | None = None) -> None:
    """Erase spinner line; optionally print a final 'DONE' line."""
    if _p["active"]:
        progress_clear_if_needed()
    if final_msg:
        log_print(f"{now_ts()} {_box('DONE ')} {final_msg}")
# ===== end unified PROGRESS helpers =====

###############################
### KEEP THIS SECTION ABOVE ###
###############################

# Backward-compat wrapper so your existing calls still work

def progress(i: int, total: int, kept: int, skipped: int) -> None:
    """Repaint the spinner at ~12fps; never prints a newline."""
    global _spin_i, _last_tick
    now = time.time()
    if now - _last_tick > 0.08:  # ~12 fps
        _spin_i = (_spin_i + 1) % len(_SPINNER)
        _last_tick = now
    spin = _SPINNER[_spin_i]
    line = f"[{now_ts()}] {_box(f'{spin} PROGRESS ')}...{i}/{total} kept={kept} skip={skipped}"
    _progress_print(line)


# --- Background spinner heartbeat --------------------------------------------
from threading import Thread, Event
_spinner_stop = Event()
_spinner_thread = None

def refresh_progress() -> None:
    """Repaint the last progress line after multi-line logs."""
#    global _p
#    if _PROGRESS_ACTIVE and _last_progress:
#        log_print(_last_progress, end="\r", flush=True)

# -------------------------------------------------------------------------------


CURRENT_SOURCE = ""  # global tag used by log_event

def set_source_tag(url: str):
    """Set a short source tag like 'remotive.com' or 'simplyhired.com'."""
    global CURRENT_SOURCE
    host = up.urlparse(url).netloc.replace("www.", "")
    CURRENT_SOURCE = host


def _normalize_job_defaults(d: dict) -> dict:
    """Ensure Title Case + space keys exist for export to CSV/Sheets."""
    d.setdefault("Applied?", "")
    d.setdefault("Reason", "")         # <— keep
    d.setdefault("Date Scraped", now_ts())

    d["Title"]               = d.get("Title")               or d.get("title", "")
    d["Company"]             = d.get("Company")             or d.get("company", "")
    d["Career Board"]        = d.get("Career Board")        or d.get("career_board", "")
    d["Location"]            = d.get("Location")            or d.get("display_location", "")
    d["Posted"]              = d.get("Posted")              or d.get("posted", "")
    d["Posting Date"]        = d.get("Posting Date")        or d.get("posting_date", "")
    d["Valid Through"]       = d.get("Valid Through")       or d.get("valid_through", "")
    d["Job URL"]             = d.get("Job URL")             or d.get("job_url", "")
    # If board still empty, infer from URL host
    if not d.get("Career Board") and d.get("Job URL"):
        d["Career Board"] = infer_board_from_url(d["Job URL"])
    d["Apply URL"]           = d.get("Apply URL")           or d.get("apply_url", "")
    d["Description Snippet"] = d.get("Description Snippet") or d.get("description_snippet", "")
    d["WA Rule"]             = d.get("WA Rule")             or d.get("wa_rule", "Default")
    d["Remote Rule"]         = d.get("Remote Rule")         or d.get("remote_rule", "Default")
    d["US Rule"]             = d.get("US Rule")             or d.get("us_rule", "Default")
    d["Salary Max Detected"] = d.get("Salary Max Detected") or d.get("salary_max_detected", "")
    d["Salary Rule"]         = d.get("Salary Rule")         or d.get("salary_rule", "")


    d.setdefault("Location Chips", d.get("location_chips", ""))
    d.setdefault("Applicant Regions", d.get("applicant_regions", ""))
    d.setdefault("Visibility Status", d.get("visibility_status", ""))
    d.setdefault("Confidence Score", d.get("confidence_score", ""))
    d.setdefault("Confidence Mark", d.get("confidence_mark", ""))
    d.setdefault("Career Board", d.get("career_board",""))
    d.setdefault("Valid Through",d.get("valid_through",""))
    return d

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


def _to_int(x):
    t = str(x).lower()
    # normalize thousands/nbsp and punctuation
    t = (t.replace("\u00a0", "")  # nbsp
           .replace(" ", "")
           .replace(",", "")
           .strip())
    # ignore benefit tokens like "401k_plan" we injected above
    if "401k_plan" in t or "403b_plan" in t or "457b_plan" in t:
        return None
    try:
        # allow simple "109000", "109.000", "109k"
        if t.endswith("k") and t[:-1].replace(".", "", 1).isdigit():
            return int(float(t[:-1]) * 1000)
        return int(float(t))
    except Exception:
        return None


def _wrap_lines(s: str, width: int = 120) -> list[str]:
    s = " ".join((s or "").split())
    if not s:
        return [""]
    return [s[i:i+width] for i in range(0, len(s), width)]


def _salary_payload_22(d: dict) -> str:
    """
    Terminal payload text, padded or trimmed to exactly 22 visible cells.
    Cases:
      [💲 SALARY $90,000  ]...Near min
      [💲 SALARY $90,000  ]...At or above min
      [💲 SALARY $xxx,xxx ]...Estimated
      [💲 SALARY          ]...Missing or Unknown
    """
    status   = str(d.get("Salary Status","")).strip().lower()
    est      = str(d.get("Salary Est. (Low–High)","")).strip()
    note     = str(d.get("Salary Note","")).strip()
    max_det  = d.get("Salary Max Detected")
    try:
        max_det = int(str(max_det).replace(",","")) if max_det not in (None,"") else None
    except Exception:
        max_det = None

    if max_det:
        amt = f"${max_det:,}"
        tail = "At or above min" if status == "at_or_above" else "Near min"
        return _vis_pad(f"{amt} ]... {tail}", 22)

    if est:
        amt = f"${est.replace(' – ', '–')}"  # keep tight dash
        return _vis_pad(f"{amt} ]...Estimated", 22)

    placeholder = str(d.get("Salary Placeholder") or "").strip()
    if placeholder:
        return _vis_pad(f"{placeholder} ]...Signal only", 22)

    return _vis_pad("Missing or Unknown", 22)



# ============================================================


def log_event(level: str,
              left: str = "",
              right=None,
              *, job=None, url: str | None = None,
              width: int = 120, **_):
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
                log_print(f"{color}{tag}.{DOT1}{ln}{RESET}")
        #refresh_progress()
        return

    # Detailed KEEP / SKIP layout
    title = left_txt or job_dict.get("Title", "") or ""

    if lvl == "KEEP":
        progress_clear_if_needed()
        # --- Title (first line) ---
        title_txt = (job_dict.get("Title") or "").strip()
        if title_txt:
            for ln in _wrap_lines(title_txt, width=width):
                log_print(f"{color}{_box('KEEP ')}.{ln}{RESET}")

        company = (job_dict.get("Company") or "").strip() or "Missing Company"
        board   = ((job_dict.get("Career Board") or job_dict.get("career_board") or "").strip()
                or "Missing Board")

        # --- Company / Board (friendly fallbacks) ---
        #company = (job_dict.get("Company") or "Missing Company").strip()
        #board   = (job_dict.get("Career Board") or "Missing Board").strip()
        progress_clear_if_needed()
        log_print(f"{color}{_box('KEEP ')}....{board}.....{company}{RESET}")
        
        # --- URL line ---
        if url:
            for ln in _wrap_lines(url, width=width):
                progress_clear_if_needed()
                log_print(f"{color}{_box('KEEP ')}....{ln}{RESET}")

        # --- Always show Salary line with fixed 22-cell width ---
        log_print(f"{color}{_box('💲 SALARY ')}....{_salary_payload_22(job_dict)}{RESET}")
        


        if vis or score or mark:
            log_print(f"{color}{_conf_box(vis, score, mark)}.{RESET}")

        log_print(f"{color}{_box('✅ DONE ')}.{RESET}")
        #refresh_progress()
        return

    if lvl == "SKIP":
        progress_clear_if_needed()
        log_print(f"{color}{_box('SKIP ')}.{title}{RESET}")

        if career_board or company:
            progress_clear_if_needed()
            log_print(f"{color}{_box('SKIP ')}....{career_board}.....{company}{RESET}")

        if url:
            for ln in _wrap_lines(url, width=width):
                progress_clear_if_needed()
                log_print(f"{color}{_box('SKIP ')}....{ln}{RESET}")

        reason = job_dict.get("Reason Skipped") or ""
        if reason:
            for ln in _wrap_lines(reason, width=width):
                progress_clear_if_needed()
                log_print(f"{color}{_box('SKIP ')}....{ln}{RESET}")

        if vis or score or mark:
            log_print(f"{color}{_conf_box(vis, score, mark)}{RESET}")
        progress_clear_if_needed()
        log_print(f"{color}{_box('🚫 DONE ')}.{RESET}")
        #refresh_progress()
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
    
def _log_and_record_skip(link: str, rule_reason: str, row: dict) -> None:
    """
    Normalize + record a skipped job and emit SKIP logs to the terminal.

    This centralizes:
      * ensuring the row has the fields the CSV/Sheets expect
      * tracking skip counts / de-dupe via `_record_skip`
      * the multi-line SKIP logging you were using before
    """
    # --- normalize the row and make sure core fields are present ---
    if row is None:
        row = {}

    # make sure these are filled even if the caller forgot
    row.setdefault("Job URL", link)
    row.setdefault(
        "Reason Skipped",
        rule_reason or row.get("Reason Skipped", "Filtered by rules"),
    )

    row = _normalize_skip_defaults(row)

        # --- record the skip once (updates `skipped_rows` + `skip_count`) ---
    _record_skip(row, reason=rule_reason)

    # --- pretty terminal logging (restored from your old helper) ---
    c = LEVEL_COLOR.get("SKIP", RESET)
    r = RESET
    ts = now_ts()  # keeps the same signature/behavior as other log helpers

    # title logic: prefer explicit title, fall back to URL-derived title
    title_in = (row.get("Title") or "").strip()
    t_from_url = _title_from_url(link)
    title = title_in or t_from_url or "<missing title>"

    # board/company for display
    company, board = _company_and_board_for_terminal(row)

    progress_clear_if_needed()
    log_print(f"{c}[SKIP                  ].{title}{r}")
    log_print(f"{c}[SKIP                  ]...{board} → {company}{r}")
    log_print(f"{c}[SKIP                  ]...{link}{r}")
    log_print(f"{c}[💭 REASON             ]... → {rule_reason}{r}")
    log_print(f"{c}[🚫 DONE               ].{r}")
    progress_clear_if_needed()


# alias so older call sites without the underscore still work
def log_and_record_skip(link: str, rule_reason: str, keep_row: dict) -> None:
    _log_and_record_skip(link, rule_reason, keep_row)


# ── Keep/Skip recorders (no recursion; stable counters; de-dupe) ──
#kept_rows: list[dict] = []
#skipped_rows: list[dict] = []
#_seen_kept_urls: set[str] = set()
#_seen_skip_urls: set[str] = set()
#kept_count: int = 0
#skip_count: int = 0

def _normalize_skip_defaults(row: dict) -> dict:
    # ensure required keys exist for SKIPPED CSV
    row = dict(row or {})
    row.setdefault("Date Scraped", now_ts())
    row.setdefault("Title", "")
    row.setdefault("Company", "")
    row.setdefault("Career Board", row.get("Board","") or "")
    row.setdefault("Location", "")
    row.setdefault("Posted", "")
    row.setdefault("Posting Date", "")
    row.setdefault("Valid Through", "")
    row.setdefault("Job URL", row.get("job_url","") or row.get("Job URL","") or "")
    row.setdefault("Reason Skipped", row.get("Reason","") or "Filtered by rules")
    row.setdefault("WA Rule", row.get("WA Rule",""))
    row.setdefault("Remote Rule", row.get("Remote Rule",""))
    row.setdefault("US Rule", row.get("US Rule",""))
    row.setdefault("Salary Max Detected", row.get("Salary Max Detected",""))
    row.setdefault("Salary Rule", row.get("Salary Rule",""))
    row.setdefault("Salary Status", row.get("Salary Status",""))
    row.setdefault("Salary Note", row.get("Salary Note",""))
    row.setdefault("Salary Est. (Low-High)", row.get("Salary Est. (Low-High)",""))
    row.setdefault("Location Chips", row.get("Location Chips",""))
    row.setdefault("Applicant Regions", row.get("Applicant Regions",""))
    row.setdefault("Apply URL", row.get("Apply URL","") or row.get("job_url",""))
    row.setdefault("Description Snippet", row.get("Description Snippet",""))
    row.setdefault("Visibility Status", row.get("Visibility Status",""))
    row.setdefault("Confidence Score", row.get("Confidence Score",""))
    row.setdefault("Confidence Mark", row.get("Confidence Mark",""))
    return row

def _record_keep(row: dict) -> None:
    global kept_count
    url = (row.get("Job URL") or row.get("job_url") or "").strip()
    if url:
        if url in _seen_kept_urls:
            return
        _seen_kept_urls.add(url)

    row.setdefault("Reason", row.get("Reason Skipped", ""))
    kept_rows.append(to_keep_sheet_row(row))
    kept_count += 1
    progress_clear_if_needed()

def _record_skip(row: dict, reason: str) -> None:
    global skip_count
    url = (row.get("Job URL") or row.get("job_url") or "").strip()
    if url:
        if url in _seen_skip_urls:
            return
        _seen_skip_urls.add(url)

    if reason:
        row["Reason Skipped"] = reason

    skipped_rows.append(to_skipped_sheet_row(row))
    skip_count += 1

    # Clear the progress line so the next block starts clean
    progress_clear_if_needed()



##########################################################
##########################################################
##########################################################


# ---- Main ----
kept_rows = []       # the “keep” rows in internal-key form
skipped_rows = []    # the “skip” rows in internal-key form

#start_ts = None  # define at module level

def main():
#    global raw_print
    global kept_count, skip_count#, start_ts  # add start_ts to globals
#    start_ts = datetime.now()
    args = _parse_args()

    # Optional SMOKE presets
#    # make these globals visible outside main()
#    global SMOKE, SALARY_FLOOR, SALARY_CEIL
    SMOKE = args.smoke or os.getenv("SMOKE") == "1"
    PAGE_CAP = args.limit_pages or (3 if SMOKE else 0)       # visit ≤ 3 listing pages
    LINK_CAP = args.limit_links or (40 if SMOKE else 0)      # visit ≤ 40 job links
    ONLY_KEYS = [s.strip().lower() for s in args.only.split(",") if s.strip()]

    SALARY_FLOOR = args.floor
    SALARY_CEIL  = args.ceil or None

    # If you use requests, tighten timeouts on smoke runs
    if SMOKE:
        os.environ.setdefault("SCRAPER_TIMEOUT_SECS", "5")

    global kept_count, skip_count
    start_ts = datetime.now()
    progress_clear_if_needed()
    log_print("[INFO                  ].Starting run")
#    start_ts = datetime.now()

    # Carry-forward map from Google Sheets: url -> (Applied?, Reason)
#    global prior_decisions
    prior_decisions = {}
    try:
        prior_decisions = fetch_prior_decisions()
        progress_clear_if_needed()
        log_print(f"[GS                    ].Loaded {len(prior_decisions)} prior decisions for carry-forward.")
#        log_print(f"[{now_ts()}] [GS                    ].Loaded {len(prior_decisions)} prior decisions for carry-forward.")
    except Exception as e:
        progress_clear_if_needed()
        log_print(f"[GS                    ].No prior decisions loaded ({e}). Continuing without carry-forward.")
#        log_print(f"[{now_ts()}] [GS                    ].No prior decisions loaded ({e}). Continuing without carry-forward.")

    # 1) Build the final set of listing pages
    pages = STARTING_PAGES + expand_career_sources()

    # 2) Optional caps and filters (driven by CLI flags)
    if ONLY_KEYS:
        pages = [u for u in pages if any(k in u.lower() for k in ONLY_KEYS)]

    if PAGE_CAP:
        pages = pages[:PAGE_CAP]

    if SMOKE:
        pages = pages[:1]  # keep one source in smoke runs

    total_pages = len(pages)
    # ⬇️ inside main(), before any board-specific if/else that gathers links
#    all_detail_links: list[str] = []

    # 3) Collect detail links from each listing page (your existing loop stays here)
    all_detail_links = []

    # Keep only selected sites when --only is used
    if ONLY_KEYS:
        pages = [u for u in pages if any(k in u.lower() for k in ONLY_KEYS)]

    # Cap listing pages
    if PAGE_CAP:
        pages = pages[:PAGE_CAP]

    total_pages = len(pages)

    pages = STARTING_PAGES + expand_career_sources()
    total_pages = len(pages)

    if SMOKE:
        pages = pages[:1]          # one listing source only
        total_pages = len(pages)

    # --- Normalize & dedupe all collected job URLs ---
    def _norm_url(u: str) -> str:
        from urllib.parse import urlparse, urlunparse, parse_qsl

        if not u:
            return ""

        # Ensure it's a clean string
        u = str(u).strip().replace(" ", "")

        p = urlparse(u)

        # Lowercase host and path, remove trailing slashes, fragments, query params
        clean_path = p.path.strip().rstrip("/").lower()

        # Strip all query params (utm_, ref, etc.)
        clean = urlunparse((
            p.scheme.lower(),
            p.netloc.lower(),
            clean_path,
            "", "", ""
        ))

        return clean

    from urllib.parse import urlparse, parse_qsl, urlencode  # (at top of file if not already imported)

    def link_key(u: str) -> str:
        """
        Produce a stable key so duplicate listing/detail URLs collapse across boards.
        - lowercases host and path
        - strips 'www.' and trailing slashes
        - drops tracking/pagination query params (utm_*, ref, source, page, p, start)
        - ignores fragments
        """
        p = urlparse(str(u))
        host = p.netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        path = p.path.rstrip("/").lower()

        # keep only meaningful query params
        drop = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
                "ref", "source", "src", "page", "p", "start"}
        kept = []
        for k, v in parse_qsl(p.query, keep_blank_values=False):
            if k.lower() in drop:
                continue
            kept.append((k.lower(), v))
        q = urlencode(kept, doseq=True)

        return f"{host}{path}?{q}" if q else f"{host}{path}"

    # De-duplicate by normalized link key
    _seen = set()
    deduped_links = []
    for u in all_detail_links:
        k = _link_key(u)
        if not k or k in _seen:
            continue
        _seen.add(k)
        deduped_links.append(u)

    if len(deduped_links) != len(all_detail_links):
        log_print(f"[ DE-DUPE ] Reduced {len(all_detail_links)} -> {len(deduped_links)} unique URLs")
        

    all_detail_links = deduped_links


    processed_keys: set[str] = set()

    # 1) Gather job detail links from each listing page
    for i, listing_url in enumerate(pages, start=1):
        t0 = time.time()
        if "hubspot.com/careers/jobs" not in listing_url:
            progress_clear_if_needed()
        set_source_tag(listing_url)
        html = get_html(listing_url)
        if not html:
            log_event("WARN", "", right=f"Failed to fetch listing page: {listing_url}")
            progress_clear_if_needed()
            #log_info_done()
            #progress(i, total_pages, kept_count, skip_count)
            continue

        # derive host safely from the listing URL
        p = up.urlparse(listing_url if isinstance(listing_url, str) else str(listing_url))
        host = p.netloc.lower().replace("www.", "")

        # HubSpot listing → handle pagination here and continue
        if "hubspot.com" in host and "/careers/jobs" in listing_url:
            links = collect_hubspot_links(listing_url, max_pages=25)
            all_detail_links.extend(links)

            # NEW: show "[🔎 FOUND XX ]....candidate job links on hubspot.com in Ys"
            elapsed = time.time() - t0
            progress_clear_if_needed()
            #log_info_found(len(links), listing_url, elapsed)
            #log_info_done()
            #progress(i, total_pages, kept_count, skip_count)
            continue

        # Workday listing → detail expansion (Ascensus and similar tenants)
        if host.endswith("myworkdayjobs.com") or host.endswith("myworkdaysite.com"):
            try:
                wd_detail_links = workday_links_from_listing(listing_url, max_results=250)
                if wd_detail_links:
                    all_detail_links.extend(wd_detail_links)

                    # NEW: print a FOUND line for Workday expansions too
                    elapsed = time.time() - t0
                    progress_clear_if_needed()
                    #log_info_found(len(wd_detail_links), listing_url, elapsed)
                    #log_info_done()
                    #progress(i, total_pages, kept_count, skip_count)
                    continue
            except Exception as e:
                log_event("WARN", f"Workday expansion failed: {e}")

        else:
            # existing branches...
            # Collector: site-specific for HubSpot/SimplyHired, generic for others
            if "hubspot.com/careers/jobs" in listing_url:
                # The collector logs per-page (page=1..N) itself,
                # so do NOT do the generic single-page logging above.
                links = collect_hubspot_links(listing_url, max_pages=25)

            elif "simplyhired.com/search" in listing_url:
                links = collect_simplyhired_links(listing_url)

            else:
                links = find_job_links(html, listing_url)


        # HubSpot pagination (page=1..N)
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
                if page_num > 20:  # safety cap
                    break

            log_event("INFO", f"Found {len(hubspot_links)}.candidate job links on hubspot.com")
            all_detail_links.extend(hubspot_links)
            progress_clear_if_needed()
            #log_info_done()
            #progress(i, total_pages, kept_count, skip_count)
            continue

        # Collector: site-specific for SimplyHired, generic for others
        if "simplyhired.com/search" in listing_url:
            links = collect_simplyhired_links(listing_url)
        else:
            links = find_job_links(html, listing_url)
        progress_clear_if_needed()
        #log_info_found(len(links), listing_url, time.time() - t0)
        all_detail_links.extend(links)

    # Optional caps (leave both; LINK_CAP takes precedence if you set it)
    # SMOKE keeps the shorter cap if you’re using it
    # Cap detail links for smoke runs
    # Optional caps
    if LINK_CAP:
        all_detail_links = all_detail_links[:LINK_CAP]

    if SMOKE:
        all_detail_links = all_detail_links[:20]  # exact 20 for smoke

    # --- Final normalization & deduplication ---
    from urllib.parse import urlparse, urlunparse
    import re

    def _normalize_link(url: str) -> str:
        if not url:
            return ""
        u = str(url).strip()
        p = urlparse(u)
        path = p.path.rstrip("/").lower()
        query = re.sub(r"(utm_[^=&]+|gh_src|ref|referrer|source)=.*?(&|$)", "", p.query, flags=re.I)
        query = re.sub(r"&{2,}", "&", query).strip("&?")
        return urlunparse((p.scheme.lower(), p.netloc.lower(), path, "", query, ""))

    deduped, seen = [], set()
    for link in all_detail_links:
        norm = _normalize_link(link)
        if norm in seen:
            continue
        seen.add(norm)
        deduped.append(link)

    log_print(f"[🧹 DE-DUPE            ] Reduced {len(all_detail_links)} → {len(deduped)} unique URLs")
    all_detail_links = deduped

    # Cap after dedupe
    if SMOKE:
        all_detail_links = all_detail_links[:20]

    # === HARD DEDUPE AFTER EXPANSIONS, BEFORE PROGRESS TOTALS ===
    from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

    def _norm_url(u: str) -> str:
        """Lowercase host, strip tracking params and trailing slash, keep stable path."""
        p = urlparse(u)
        # strip common trackers everywhere
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
            if k.lower() not in {"utm_source","utm_medium","utm_campaign","utm_term","utm_content",
                                "ref","source","_hsmi","_hsenc"}]
        clean = urlunparse((
            p.scheme,
            p.netloc.lower(),
            p.path.rstrip("/"),
            "",  # params
            urlencode(q, doseq=True),
            ""   # fragment
        ))
        return clean

    _seen, deduped = set(), []
    for u in all_detail_links:
        nu = _norm_url(u)
        if nu in _seen:
            continue
        _seen.add(nu)
        deduped.append(nu)
    all_detail_links = deduped
    log_print("[: DE-DUPE ] Reduced {} → {} unique URLs".format(
        len(deduped) + (len(_seen)-len(deduped)), len(all_detail_links)))
    # =============================================================

    # now set your totals AFTER dedupe (and AFTER any smoke cap)
    if SMOKE:
        all_detail_links = all_detail_links[:20]
    progress_set_total(len(all_detail_links))  # <- total comes from the deduped list
    kept_count = 0
    skip_count = 0

    progress_set_total(len(all_detail_links))
    progress_clear_if_needed()
    _seen_kept_urls.clear()
    _seen_skip_urls.clear()
    kept_count = 0
    skip_count = 0

    # 2) Visit each job link and extract details
#    start_spinner()
    # Cap the number of job links in smoke runs
    if SMOKE:
        all_detail_links = all_detail_links[:20]
        progress_set_total(len(all_detail_links))   # set the total after caps
        start_ts = datetime.now()
        start_spinner()

    try:
        for j, link in enumerate(all_detail_links, start=1):
            # advance spinner and repaint the live line
            progress_tick(j)                                   # move spinner to next frame
            progress(i, len(all_detail_links), kept_count, skip_count)  # update counts text

            set_source_tag(listing_url)
            # 1) fetch html (or however you already do it)
            html = get_html(link)  # <-- make sure this exists in your code


            # A) Could not fetch detail page → record a minimal SKIP and continue
            if not html:
                default_reason = "Failed to fetch job detail page"
                board = career_board_name(link)
                title_for_log = _title_for_log({"Title": ""}, link)

                skip_row = _normalize_skip_defaults({
                    "Job URL": link,
                    "Apply URL": link,
                    "Title": title_for_log,
                    "Company": "",
                    "Career Board": board,
                    "Valid Through": "",
                    "Reason Skipped": choose_skip_reason({"Job URL": link}, technical_fallback=default_reason),
                    "WA Rule": "default",
                    "Remote Rule": "unknown_or_onsite",
                    "US Rule": "default",
                    "Salary Max Detected": "",
                    "Salary Rule": "",
                    "Salary Near Min": "",
                    "Salary Status": "",
                    "Salary Note": "",
                    "Salary Est. (Low-High)": "",
                    "Location Chips": "",
                    "Applicant Regions": "",
                })

                progress_clear_if_needed()
                _log_and_record_skip(link, rule_reason, skip_row)
                progress(i, len(all_detail_links), kept_count, skip_count)
                progress_clear_if_needed()
                continue

            # B) We have HTML → parse details and enrich salary
            details = extract_job_details(html, link)
            details = enrich_salary_fields(details, page_host=up.urlparse(link).netloc)
            details["Career Board"] = details.get("Career Board") or infer_board_from_url(details.get("Job URL") or link)

            # always normalize and enrich before rules/decisions
            details = _normalize_job_defaults(details)

            # ---- hard de-dupe by job key (prevents double writes to Sheet/CSV) ----
            jk = _job_key(details, link)
            if jk in processed_keys:
                skip_row = _normalize_skip_defaults({
                    "Job URL": link,
                    "Title": title_for_log,
                    "Company": details.get("Company","") or company_from_url_fallback(details.get("Job URL") or link),
                    "Career Board": details.get("Career Board",""),
                    "Reason": "DE-DUPE",
                    "WA Rule": "default",
                    "US Rule": "default",
                    "Remote Rule": "unknown_or_onsite",
                })
                _log_and_record_skip(link, "DE-DUPE", skip_row)   # uses your counter increment
                # skip path (we skipped the job)
                skip_count += 1
                progress(i, len(all_detail_links), kept_count, skip_count)
                progress_clear_if_needed()
                continue
            else:
                processed_keys.add(jk)
            # ----------------------------------------------------------------------


            # compute derived fields once
            details["WA Rule"] = details.get("WA Rule", "default")
            details["Remote Rule"] = details.get("Remote Rule", "default")
            details["US Rule"] = details.get("US Rule", "default")
            details["Reason"] = details.get("Reason","")  # leave as-is unless you set it
            vis, score, mark = compute_visibility_and_confidence(details)
            details["Visibility Status"] = vis
            details["Confidence Score"]  = score
            details["Confidence Mark"]   = mark

            #_record_keep(to_keep_sheet_row(details))
            #progress(i, len(all_detail_links), kept_count, skip_count)

            # choose a specific reason (uses title/snippet/regions after enrich)
            reason = choose_skip_reason(details, technical_fallback="Filtered by rules")

            # C) If extractor itself marked as removed/archived → SKIP with details
            if details.get("Reason Skipped"):
                company_out = details.get("Muse Company") or details.get("Company", "")
                row = _normalize_skip_defaults({
                    "Title": normalize_title(details.get("Title", ""), details.get("Company", "")),
                    "Company": details.get("Company", "") or "Missing Company",
                    "Career Board": details.get("Career Board", "") or "Missing Board",
                    "Location": details.get("Location", ""),
                    "Description Snippet": details.get("description_snippet", ""),
                    "Posted": details.get("Posted", ""),
                    "Posting Date": details.get("posting_date",""),
                    "Valid Through": details.get("valid_through",""),
                    "Job URL": job_url,
                    "Apply URL": details.get("apply_url", job_url),
                    "Reason Skipped": details.get("Reason Skipped",""),
                })
                progress_clear_if_needed()
                _log_and_record_skip(link, rule_reason, skip_row)
                progress(i, len(all_detail_links), kept_count, skip_count)
                progress_clear_if_needed()
                continue

            # D) Ascensus (Workday) post-filter: only keep Product/BA/Scrum family roles
            #host = up.urlparse(listing_url).netloc.lower()
            host = up.urlparse(link).netloc.lower()
            details = enrich_salary_fields(details, page_host=host)
            if "ascensushr.wd1.myworkdayjobs.com/ascensuscareers" in host:
                # reuse your intent/role test
                if not _is_target_role(details):
                    skip_row = _normalize_skip_defaults({
                        "Job URL": details.get("job_url", link),
                        "Title": normalize_title(details.get("Title", ""), details.get("Company", "")),
                        "Company": company_from_url_fallback(link), 
                        "Career Board": career_board_name(link),
                        "Valid Through": details.get("Valid Through", ""),
                        "Reason Skipped": "Not target role (Ascensus filter)",
                    })
                    progress_clear_if_needed()
                    _log_and_record_skip(link, rule_reason, skip_row)
                    progress(i, len(all_detail_links), kept_count, skip_count)
                    progress_clear_if_needed()
                    continue


            # add this line so the enricher knows we are on themuse.com
            details = enrich_salary_fields(details, page_host=up.urlparse(link).netloc)

            keep_row_extra = {
                "Salary Status": details.get("Salary Status", ""),
                "Salary Note": details.get("Salary Note", ""),
                "Salary Max Detected": details.get("Salary Max Detected", ""),
                "Salary Rule": details.get("Salary Rule", ""),
                "Salary Near Min": details.get("Salary Near Min", ""),
                "Salary Est. (Low-High)": details.get("Salary Est. (Low-High)", ""),
                "Salary": details.get("Salary", "") or details.get("Salary Placeholder", ""),
            } 
            # Basic remote/US rules (lightweight; you can expand later)
            remote_flag = details.get("is_remote_flag", "unknown_or_onsite")
            remote_rule = "default" if remote_flag == "remote" else "no_remote_signal"
            us_rule = "default"  # placeholder until you add stricter US checks
            wa_rule = "default"  # placeholder for WA logic
            
            # Normalize company and title before we build keep_row
            details = _derive_company_and_title(details)

            # Build a normalized “keep” row
            keep_row = {
                "Applied?": "",
                "Reason": "",
                "Date Scraped": now_ts(),
                "Title": normalize_title(details.get("Title", ""), details.get("Company", "")),
                "Job ID (Vendor)": details.get("job_id_vendor",""),
                "Job ID (Numeric)": details.get("job_id_numeric",""),
                "Company": details.get("Company", "") or "Missing Company",
                "Career Board": details.get("Career Board", "") or "Missing Board",
                "Location": details.get("Location", ""),
                "Posted": details.get("Posted", ""),
                "Posting Date": details.get("posting_date", ""),
                "Valid Through": details.get("valid_through", ""),
                "Job URL": details.get("job_url", link),
                "Apply URL": details.get("apply_url", link),
                "Description Snippet": details.get("description_snippet", ""),
                "WA Rule": "default",
                "Remote Rule": details.get("is_remote_flag", "unknown_or_onsite"),
                "US Rule": "default",
                "Location Chips": details.get("location_chips", ""),
                "Applicant Regions": details.get("applicant_regions", ""),
            }
            keep_row.update(keep_row_extra)   # <-- brings in Salary Status/Note/Rule/Max/etc.

            # Build the bracketed SALARY line for Terminal output
            salary_line = _fmt_salary_line(keep_row)

            # (optional) stash it on the row for any later printers
            keep_row["__salary_line"] = salary_line

            # --- PUBLIC sanity pass (light network ping + heuristics) ---
            vis2, score2, mark2 = _public_sanity_checks(keep_row)
            # One pass, trust the sanity checks
            vis, score, mark = _public_sanity_checks(keep_row)
            keep_row["Visibility Status"] = vis
            keep_row["Confidence Score"]  = score
            keep_row["Confidence Mark"]   = mark

            # Carry forward Applied? and Reason from prior runs, if present
            prev = prior_decisions.get(keep_row["Job URL"])
            prev_decisions = prior_decisions.get(keep_row["Job URL"])

            if prev:
                applied_prev, reason_prev = prev
                if applied_prev and not keep_row.get("Applied?"):
                    keep_row["Applied?"] = applied_prev
                if reason_prev and not keep_row.get("Reason"):
                    keep_row["Reason"] = reason_prev

            # --- Rule-based skip gate: title / remote / US-Canada eligibility ---
            rule_reason = build_rule_reason({
                "Title":                keep_row["Title"],
                "Remote Rule":          keep_row["Remote Rule"],
                "Location Chips":       keep_row["Location Chips"],
                "Applicant Regions":    keep_row["Applicant Regions"],
                "Location":             keep_row["Location"],
                "Description Snippet":  keep_row["Description Snippet"],
                "Job URL":              keep_row["Job URL"],
            })

            if rule_reason and rule_reason != "Filtered by rules":
                # Build a SKIPPED row with the same fields you would have kept
                skip_row = _normalize_skip_defaults({
                    "Job URL":              keep_row["Job URL"],
                    "Title":                keep_row["Title"],
                    "Company":              keep_row["Company"],
                    "Career Board":         keep_row["Career Board"],
                    "Location":             keep_row["Location"],
                    "Posted":               keep_row["Posted"],
                    "Posting Date":         keep_row["Posting Date"],
                    "Valid Through":        keep_row["Valid Through"],
                    "Reason Skipped":       keep_row.get("Reason Skipped", ""),
                    "Apply URL":            keep_row["Apply URL"],
                    "Description Snippet":  keep_row["Description Snippet"],
                    "WA Rule": wa_rule,
                    "Remote Rule": remote_rule,
                    "US Rule": us_rule,
                    "Salary Max Detected":  keep_row_extra["Salary Max Detected"],
                    "Salary Rule":          keep_row_extra["Salary Rule"],
                    "Salary Status":        keep_row_extra["Salary Status"],
                    "Salary Note":          keep_row_extra["Salary Note"],
                    "Salary Near Min":      keep_row_extra["Salary Near Min"],
                    "Location Chips":       keep_row["Location Chips"],
                    "Applicant Regions":    keep_row["Applicant Regions"],
                })

                progress_clear_if_needed()
                _log_and_record_skip(link, rule_reason, details)
                progress(i, len(all_detail_links), kept_count, skip_count)
                progress_clear_if_needed()
                continue


            # --- End rule-based skip gate ---


            # Compute visibility + confidence for the export
            vis, score, mark = compute_visibility_and_confidence({
                "Job URL": keep_row["Job URL"],
                "Company": keep_row["Company"],
                "Posting Date": keep_row["Posting Date"],
                "Salary Max Detected": keep_row["Salary Max Detected"],
                "Description Snippet": keep_row["Description Snippet"],
                "Location": keep_row["Location"],
                "remote_rule": remote_rule,
                "us_rule": us_rule,
                "Title": keep_row["Title"],
            })

            # One pass, trust the sanity checks
            vis, score, mark = _public_sanity_checks(keep_row)
            keep_row["Visibility Status"] = vis
            keep_row["Confidence Score"]  = score
            keep_row["Confidence Mark"]   = mark

            # --- Salary gate driven by enrich_salary_fields ----------------------
            sal_status    = (details.get("Salary Status") or "").strip().lower()
            detected_max  = details.get("Salary Max Detected")
            detected_max  = _to_int(detected_max) if detected_max not in (None, "") else None

            if sal_status in ("near_min", "below_floor"):
                if sal_status == "near_min" and detected_max and detected_max >= SOFT_SALARY_FLOOR:
                    # SOFT-KEEP: under target, but close enough to keep quietly
                    keep_row["Salary Rule"]        = "soft_keep"
                    keep_row["Salary Near Min"]    = detected_max
                    keep_row["Visibility Status"]  = "quiet"
                    keep_row["Confidence Mark"]    = "🟠"
                elif sal_status == "below_floor":
                    # Convert to SKIP with a clear reason
                    rule_label = keep_row_extra.get("Salary Rule", "below_floor")
                    row = {
                        "Title":                keep_row["Title"],
                        "Company":              keep_row["Company"],
                        "Career Board":         keep_row["Career Board"],
                        "Location":             keep_row["Location"],
                        "Posted":               keep_row["Posted"],
                        "Posting Date":         keep_row["Posting Date"],
                        "Valid Through":        keep_row["Valid Through"],
                        "Reason Skipped":       "Salary out of target range",
                        "Apply URL":            keep_row["Apply URL"],
                        "Description Snippet":  keep_row["Description Snippet"],
                        "WA Rule":              wa_rule,
                        "Remote Rule":          remote_rule,
                        "US Rule":              us_rule,
                        "Salary Max Detected":  keep_row_extra.get("Salary Max Detected", ""),
                        "Salary Rule":          rule_label,
                        "Salary Status":        keep_row_extra.get("Salary Status", ""),
                        "Salary Note":          keep_row_extra.get("Salary Note", ""),
                        "Salary Near Min":      keep_row_extra.get("Salary Near Min", ""),
                        "Location Chips":       keep_row.get("Location Chips", ""),
                        "Applicant Regions":    keep_row.get("Applicant Regions", ""),
                    }
                    _log_and_record_skip(link, rule_reason, skip_row)
                    kept_count += 1
                    progress_clear_if_needed()
                    continue



            else:
                # Normal KEEP (salary OK or not limiting)
                #keep_row.pop("Salary Near Min", None)   # keep clean
                _record_keep(keep_row)
                progress(i, len(all_detail_links), kept_count, skip_count)
                log_event("KEEP", _title_for_log(keep_row, link), right=keep_row)

                job = keep_row
            # ---------------------------------------------------------------------
        # stop spinner and clear sticky line once we finish
        stop_spinner()
        progress_done(kept_count + skip_count, len(all_detail_links), kept_count, skip_count)
        progress_clear_if_needed()

    finally:
        #refresh_progress()
        pass
#    total = len(all_detail_links)  # safe: list exists, even if empty

    # 3) Write CSVs
    write_rows_csv(OUTPUT_CSV, kept_rows, KEEP_FIELDS)
    write_rows_csv(SKIPPED_CSV, skipped_rows, SKIP_FIELDS)

    # 3b) Push to Google Sheets
    push_rows_to_google_sheet([to_keep_sheet_row(r) for r in kept_rows], KEEP_FIELDS, tab_name=GS_TAB_NAME)
    push_rows_to_google_sheet([to_skipped_sheet_row(r) for r in skipped_rows], SKIPPED_KEYS, tab_name="Skipped")


    progress_clear_if_needed()
    log_print(f"[DONE                  ].Kept {kept_count}, skipped {skip_count} "
          f"in {(datetime.now() - start_ts).seconds}s")
    progress_clear_if_needed()
    log_print(f"[🧹 DE-DUPE            ].Reduced {len(seen)} unique out of {len(all_detail_links)} total URLs")
    progress_clear_if_needed()
    log_print(f"[DONE                  ].CSV: {OUTPUT_CSV}")
    progress_clear_if_needed()
    log_print(f"[DONE                  ].CSV: {SKIPPED_CSV}")

    # Final flush
    if kept_rows:
        write_rows_csv(OUTPUT_CSV, kept_rows, KEEP_FIELDS)
        kept_rows.clear()
    if skipped_rows:
        write_rows_csv(SKIPPED_CSV, skipped_rows, SKIP_FIELDS)
        skipped_rows.clear()


    # ---- Optional GitHub push (one place, at the end) ----
#    if (kept_count + skip_count) > 0:
    commit_msg = f"job-scraper: {RUN_TS} kept={kept_count}, skipped={skip_count}"
    if PUSH_MODE == "auto":
        maybe_push_to_git(prompt=False, auto_msg=commit_msg)
    elif PUSH_MODE == "ask":
        maybe_push_to_git(prompt=True,  auto_msg=commit_msg)
    # "off" -> do nothing



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
        progress_clear_if_needed()
#        log_print(f"[{now_ts()}] [GIT                   ].Not a Git repo. Skipping push.")
        log_print("[INFO                  ].Not a Git repo. Skipping push.")
        #refresh_progress()
        return
    if not _git_has_changes(root):
        # just display and return (NO input here)
        progress_clear_if_needed()
#        log_print(f"[{now_ts()}] [GIT                   ].No file changes to commit.")
        log_print("[INFO                  ].No file changes to commit.")
        #refresh_progress()
        return

    # The ONLY interactive prompt
    if prompt:
#        ans = input(f"[{now_ts()}] [GIT                   ].Push code update to GitHub now? (y/n) ").strip().lower()
        ans = input("Push code updates to GitHub now? [y/n] ").strip().lower()
        if ans != "y":
            # display a one-liner and exit (NO second input)
            progress_clear_if_needed()
#            log_print(f"[{now_ts()}] [GIT                   ].Skipped push.")
            log_print("[INFO                  ].Skipped push.")
            #refresh_progress()

            return

    # do the push
    _git_run("git add -A", cwd=root)
    msg = auto_msg or f"job-scraper: update @ {_t.strftime('%Y-%m-%d %H:%M:%S')}"
    _git_run(f'git commit -m "{msg}"', cwd=root)
    _git_run("git pull --rebase", cwd=root)
    code, out = _git_run("git push", cwd=root)
    if code == 0:
        progress_clear_if_needed()
        log_print("[INFO                  ].Pushed to GitHub.")
    else:
        progress_clear_if_needed()
        log_print("[WARN                  ].Push failed:\n" + out)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Product/PO job scraper"
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Run a quick test: cap to 20 links and show progress."
    )
    parser.add_argument(
        "--link-cap",
        type=int,
        default=None,
        help="Hard-cap number of detail links to process (overrides discovery count)."
    )
    parser.add_argument(
        "--push",
        choices=["auto", "ask", "off"],
        default=None,
        help="Git push mode at the end (auto=commit+push, ask=prompt, off=skip)."
    )

    args = parser.parse_args()
    SMOKE = getattr(args, "smoke", False) or bool(os.environ.get("SMOKE"))


    # Expose flags to the rest of the module (your code reads these globals)
    if args.smoke:
        SMOKE = True
    if args.link_cap is not None:
        LINK_CAP = int(args.link_cap)
    if args.push is not None:
        PUSH_MODE = args.push

    # run
    main()
