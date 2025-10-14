# --- Auto-backup section for job-scraper project ---
import shutil
from pathlib import Path
from datetime import datetime, timedelta

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
        print("[BACKUP] Skipped (running inside Code Archive)")
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
        print(f"[BACKUP] Saved {dest}")

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
                        print(f"[BACKUP] Pruned by age {b.name}")
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
                    print(f"[BACKUP] Pruned by count {old.name}")
                except OSError as e:
                        print(f"[WARN] Could not remove {old.name}: {e}")

# --- HOW TO USE (uncomment exactly one) ---
backup_all_py_to_archive()                  # 1) keep ALL backups
# backup_all_py_to_archive(keep_last=10)    # 2) keep last 10 per file
# backup_all_py_to_archive(max_age_days=30) # 3) delete backups older than 30 days
# backup_all_py_to_archive(keep_last=10, max_age_days=60)  # combine both
# --- End auto-backup section ---



# --- Auto-install dependencies if missing (venv-friendly) ---
import sys, subprocess, os
from datetime import datetime

REQUIRED_PACKAGES = [
    "requests",
    "requests-cache",
    "beautifulsoup4",
    "python-dateutil",
    "playwright",
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
    print(f"{_stamp()} [{section:<15}] {msg}")

def _ensure(pkg: str):
    """Import if available; otherwise pip install and stream output with timestamps."""
    try:
        __import__(pkg.replace("-", "_"))
        log("SETUP", f"Requirement already satisfied: {pkg}")
    except ImportError:
        log("SETUP", f"Installing missing dependency: {pkg} ...")
        cmd = [sys.executable, "-m", "pip", "install", pkg]
        # If not in a venv, fallback to --user to avoid Homebrew's externally-managed error
        if not IN_VENV:
            cmd.append("--user")
        proc = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        for line in proc.stdout.splitlines():
            if line.strip():
                log("SETUP", line.strip())

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
from dateutil import parser as dateparser
from urllib import robotparser
from playwright.sync_api import sync_playwright

import csv
from pathlib import Path

def write_rows_csv(path: str, rows: list[dict], header_fields: list[str]):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    new_file = not Path(path).exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header_fields, extrasaction="ignore")
        if new_file:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)

ROLE_RX = re.compile(r"\b(product\s+(manager|owner)|product\s+management)\b", re.I)


import urllib.parse

MAX_PAGES_SIMPLYHIRED = 2  # bump later if you like

def collect_simplyhired_links(listing_url: str) -> list[str]:
    """Collect job detail links from a SimplyHired search listing."""
    found: list[str] = []
    seen = set()

    page_url = listing_url
    pages = 0
    while page_url and pages < MAX_PAGES_SIMPLYHIRED:
        html = get_html(page_url)  # you already have get_html
        if not html:
            log_event("WARN", f"Failed to GET listing page: {listing_url}")
            continue
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

        # Follow pagination via rel=next if present
        next_link = soup.find("a", rel=lambda v: v and "next" in v)
        if not next_link:
            # try common aria-label for next
            next_link = soup.find("a", attrs={"aria-label": "Next"})
        if next_link and next_link.get("href"):
            page_url = urllib.parse.urljoin(SIMPLYHIRED_BASE, next_link["href"])
            pages += 1
        else:
            break

    return found

def _is_pm_po(d: dict) -> bool:
    t = (d.get("Title") or "").lower()
    return bool(ROLE_RX.search(t))

def _is_remote(d: dict) -> bool:
    rr   = str(d.get("Remote Rule", "")).lower()
    chip = (d.get("Location Chips") or "").lower()
    loc  = (d.get("Location") or "").lower()
    text = " ".join([chip, loc, rr])
    if re.search(r"\bremote\b", text) or "anywhere" in text or "global" in text:
        return not re.search(r"\b(on[-\s]?site|in[-\s]?office|hybrid)\b", text)
    return False

def _is_us_canada_eligible(d: dict) -> bool:
    """
    Liberal when unknown, but STRICT when we see explicit non-NA regions.
    Accept if any signal mentions US/Canada. Reject if any signal mentions strong non-NA regions.
    On remote-first boards with no explicit regions, allow unless copy strongly excludes NA.
    """
    regions = " ".join([
        str(d.get("Applicant Regions") or ""),
        str(d.get("Location Chips") or ""),
        str(d.get("Location") or ""),
    ])
    regions_low = regions.lower()
    desc_low = (d.get("Description Snippet") or "").lower()

    # 1) If the text lists strong non-NA regions (e.g., India/Philippines/Sri Lanka), reject.
    if NON_NA_STRONG.search(regions_low) or NON_NA_STRONG.search(desc_low):
        return False

    # 2) If we positively see US/Canada anywhere, accept.
    NA_HIT = re.compile(r"\b(us|u\.s\.?|united states|usa|canada|canadian)\b", re.I)
    if NA_HIT.search(regions_low) or NA_HIT.search(desc_low):
        return True

    # 3) Remote-first boards with no explicit regions → soft allow unless text strongly excludes NA.
    url = (d.get("Job URL") or d.get("job_url") or "").lower()
    host = urlparse(url).netloc.replace("www.", "")
    if host in REMOTE_BOARDS and SOFT_US_FOR_REMOTE_BOARDS:
        text = regions_low + " " + desc_low
        if EU_STRONG.search(text) or NON_US_STRONG.search(text) or NON_NA_STRONG.search(text):
            return False
        return True

    # 4) If still unknown but it clearly says "Remote" with no constraints, lean allow.
    if "remote" in regions_low:
        return True

    return False

def build_rule_reason(d: dict) -> str:
    reasons = []
    if not _is_pm_po(d):
        reasons.append("Not Product Owner/PM")
    if not _is_remote(d):
        reasons.append("Not remote")
    if not _is_us_canada_eligible(d):
        reasons.append("Not US/Canada-eligible")
    return ", ".join(reasons) or "Filtered by rules"

AUTO_SKIP_IF_APPLIED = False  # change to True if you want auto-skip on applied
def choose_skip_reason(d: dict, technical_fallback: str) -> str:
    if AUTO_SKIP_IF_APPLIED:
        applied = (d.get("Applied?") or "").strip()
        if applied:
            return f"Already applied on {applied}"
    rule = build_rule_reason(d)
    return rule if rule else technical_fallback


import builtins, datetime as _dt
_builtin_print = print
def print(*args, **kwargs):
    ts = _dt.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    if args:
        args = (f"{ts} {args[0]}",) + args[1:]
    else:
        args = (ts,)
    return _builtin_print(*args, **kwargs)

# --- URL helpers used throughout ---
from urllib.parse import urlparse, urljoin, parse_qs, unquote

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


import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--floor", type=int, default=110_000)
parser.add_argument("--ceil",  type=int, default=0)
args = parser.parse_args()
SALARY_FLOOR = args.floor
SALARY_CEIL  = args.ceil or None
KEEP_UNKNOWN_SALARY = True  # keep jobs when no salary is detected


PLAYWRIGHT_DOMAINS = {
    "edtech.com", "www.edtech.com",
    "builtin.com", "www.builtin.com",
    "wellfound.com", "www.wellfound.com",
    "welcometothejungle.com", "www.welcometothejungle.com", "app.welcometothejungle.com", "us.welcometothejungle.com"
}




PLAYWRIGHT_DOMAINS.update({"www.ycombinator.com", "ycombinator.com"})

try:
    from playwright.sync_api import sync_playwright
except Exception:
    sync_playwright = None  # run without Playwright if not available



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

SKIPPED_KEYS = [
    "Date Scraped","Title","Company",
    "Career Board",            # added
    "Location","Posted","Posting Date",
    "Valid Through",           # added
    "Job URL",
    "Reason Skipped","WA Rule","Remote Rule","US Rule",
    "Salary Max Detected","Salary Rule",
    "Location Chips","Applicant Regions",
    "Visibility Status","Confidence Score","Confidence Mark",
]

STARTING_PAGES = [
    # Remote-friendly product boards that serve real HTML
    "https://remotive.com/remote-jobs/product?locations=Canada%2BUSA",
    "https://weworkremotely.com/categories/remote-product-jobs",
    "https://nodesk.co/remote-jobs/product/",
    "https://workingnomads.com/jobs?query=product",
    "https://www.simplyhired.com/search?q=product+owner&l=remote",

    # The Muse works well
    "https://www.themuse.com/jobs?categories=product&location=remote",
    "https://www.themuse.com/jobs?categories=management&location=remote",

    # YC jobs (Playwright-friendly)
    "https://www.ycombinator.com/jobs/role/product-manager",
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
    r"brazil|argentina|colombia|apac|latam)\b",
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


REMOTE_REGEX = re.compile(r"\b(remote|remote-friendly|work from home|work-from-home|wfh)\b", re.I)



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
    "Salary Max Detected","Salary Rule",
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
    "Salary Max Detected","Salary Rule",
    "Location Chips","Applicant Regions",
]

def can_fetch(url):
    """Check robots.txt with a timeout so it never hangs."""
    parsed = urlparse(url)
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
    req_host = urlparse(url).netloc.lower()
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            final_host = urlparse(resp.url).netloc.lower()
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
    host = urlparse(url).netloc.lower().replace("www.", "")
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
        "glassdoor.com": "Glassdoor",
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

def is_job_detail_url(u):
    p = urlparse(u)
    host = p.netloc.lower()
    path = p.path.lower()

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
        return "/jobs/" in path and path.count("/") >= 3 and "category" not in path

    # edtech
    if "edtech.com" in host:
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


    # Default heuristic
    return bool(re.search(r"/(job|jobs|position|opening|careers?)/", path, re.I))



def should_skip_url(u: str) -> bool:
    """Return True for URLs we never want to treat as job details."""
    if not u:
        return True
    p = urlparse(u)
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


def normalize_text(node):
    if not node:
        return ""
    return " ".join(node.get_text(" ", strip=True).split())

def find_job_links(listing_html, base_url):
    soup = BeautifulSoup(listing_html, "html.parser")
    anchors = soup.find_all("a", href=True)
    links = set()
    base_host = urlparse(base_url).netloc.lower()
    if base_host in BLOCKED_DOMAINS:
        return []

    for a in anchors:
        full = urljoin(base_url, a["href"])
        host = urlparse(full).netloc.lower()
        if host in BLOCKED_DOMAINS:
            continue
        if should_skip_url(full):
            continue
        if is_job_detail_url(full):
            links.add(full)
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

def gather_location_chips(job_url: str, soup: BeautifulSoup, ld: dict, existing: list[str] | None = None) -> str:
    """Pull chips/badges that hint remote/region/type from common boards + ldjson."""
    host = urlparse(job_url).netloc.replace("www.", "")
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
# drop text like " ... @Company" at the end
_AT_TAIL_RX = re.compile(r"\s+@\s+[\w.&'()\- ]+\s*$", re.I)
# drop text like " – Company" or " | Company" suffixes
_TRAILING_COMPANY_RX = re.compile(r"\s*[–\-|:]\s*[@]?\s*[\w.&'()\- ]+\s*$", re.I)

def normalize_title(t: str) -> str:
    if not t:
        return ""
    t = _html.unescape(str(t))
    t = _EMOJI_RX.sub("", t)

    # remove prefixes like [Hiring], Hiring:, [Remote], etc.
    t = _TAG_PREFIX_RX.sub("", t).strip()

    # remove trailing " @Company" if present
    t = _AT_TAIL_RX.sub("", t).strip()

    # remove trailing " – Company" / " | Company" fallbacks
    t = _TRAILING_COMPANY_RX.sub("", t).strip()

    # collapse whitespace and trim obvious dividers
    t = re.sub(r"\s{2,}", " ", t).strip(" -|·")
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

def extract_job_details(job_html, job_url):
    DESCRIPTION_PREVIEW_CHARS = 600
    soup = BeautifulSoup(job_html, "html.parser")
 
    # --- Detect dead / removed postings (e.g., NoDesk red banner) ---
    raw_text = job_html if isinstance(job_html, str) else ""
    dead_reason = _detect_dead_post(urlparse(job_url).netloc.lower(),
                                    soup,
                                    raw_text)
    
    
    if dead_reason:
        # Try to salvage some basics even when expired:
        title = (soup.title.string.strip() if soup.title and soup.title.string else "")
        return {
            "reason_skipped": dead_reason,
            "job_url": job_url,
            "title": title,
            "company": "",
            "display_location": "",
            "posting_date": "",
            "posted": "",
            "career_board": career_board_name(job_url),  # NEW
            "valid_through": "",                         # NEW
            "location_chips": "",

        }

    Title = company = location = posted = description = ""
    Title = normalize_title(Title)
    apply_url = job_url

    host = urlparse(job_url).netloc.lower()

    # NEW — attempt to read validThrough from any JSON-LD blocks
    valid_through = ""
    try:
        for tag in soup.find_all("script", type="application/ld+json"):
            import json
            data = json.loads(tag.string or "{}")
            # unwrap @graph if present
            nodes = data.get("@graph", []) if isinstance(data, dict) else []
            candidates = [data] + (nodes if isinstance(nodes, list) else [])
            for node in candidates:
                if isinstance(node, dict) and node.get("@type") in ("JobPosting", "Posting", "Vacancy"):
                    vt = node.get("validThrough") or node.get("valid_through")
                    if vt:
                        # normalize to YYYY-MM-DD
                        from dateutil import parser as dateparser
                        valid_through = dateparser.parse(vt).date().isoformat()
                        raise StopIteration
    except StopIteration:
        pass
    except Exception:
        pass

    # --- Glassdoor specific parsing ---
    if "glassdoor.com" in host and "/job-listing/" in job_url:
        # Title
        h1 = soup.select_one('[data-test="jobTitle"]') or soup.find("h1")
        if h1:
            Title = h1.get_text(" ", strip=True)

        # Company
        c = soup.select_one('[data-test="employerName"]')
        if not c:
            c = soup.select_one('[data-test="jobHeader"] [data-test="employerName"]')
        if c:
            company = c.get_text(" ", strip=True)

        # Location
        loc = soup.select_one('[data-test="Location"]') or soup.select_one('[data-test="jobLocation"]')
        if loc:
            location = loc.get_text(" ", strip=True)

        # Posted date
        posted_el = soup.select_one('[data-test="postedOn"]')
        if posted_el:
            posted_raw = posted_el.get_text(" ", strip=True)
            try:
                from dateutil import parser as dateparser
                posted = dateparser.parse(posted_raw, fuzzy=True).date().isoformat()
            except Exception:
                posted = posted_raw

        # Description
        desc = (
            soup.select_one('[data-test="jobDescriptionContent"]')
            or soup.select_one('#JobDescriptionContainer')
            or soup.select_one('article')
        )
        if desc:
            description = " ".join(desc.get_text(" ", strip=True).split())

    # --- Title fallback (slug -> words) ---
    if not Title:
        p = urlparse(job_url)
        segs = [s for s in p.path.split("/") if s]
        if segs:
            Title = " ".join(segs[-1].replace("-", " ").split())
        else:
            Title = "Product Owner"




    # LD+JSON
    ld = parse_jobposting_ldjson(job_html)
    Title = ld.get("Title") or Title
    company = ld.get("Company") or company or company_from_url_fallback(job_url) or company
    if not location and ld.get("locations"):
        location = ", ".join(ld["locations"])
    posting_date = ld.get("date_posted")

    # site meta
    # site meta (only trust on ATS/company hosts, not on boards)
    meta_org = soup.find("meta", attrs={"property": "og:site_name"}) or soup.find("meta", attrs={"name": "application-name"})
    if meta_org and meta_org.get("content"):
        site_name = meta_org["content"].strip()
        host = urlparse(job_url).netloc.lower().replace("www.", "")
        is_board = any(host.endswith(h) for h in BOARD_HOSTS)
        is_ats   = any(host.endswith(h) for h in ATS_HOSTS)
        if is_ats and not company:
            company = site_name  # OK on ATS
        # else: ignore on boards/aggregators, keep whatever we already found

    text_all = normalize_text(soup)
    loc_match = re.search(r"\b(Location|Based in|Location:)\s*[:\-]?\s*([A-Za-z0-9,\s\-]+)", text_all, re.I)
    if loc_match:
        location = loc_match.group(2).strip()
    elif REMOTE_REGEX.search(text_all):
        location = "Remote"

    date_match = re.search(r"(Posted|Date posted|Published)\s*[:\-]?\s*([A-Za-z0-9, \-]+)", text_all, re.I)
    if date_match:
        posted_raw = date_match.group(2).strip()
        try:
            posted = dateparser.parse(posted_raw).date().isoformat()
        except Exception:
            posted = posted_raw

    if not posting_date and posted:
        pd = compute_posting_date_from_relative(posted)
        if pd:
            posting_date = pd

    # description
    for sel in ("article", ".job-description", ".description", ".job-body", ".posting", ".job-posting"):
        node = soup.select_one(sel)
        if node:
            description = normalize_text(node)
            break
    if not description:
        best = ""
        for d in soup.find_all("div"):
            txt = normalize_text(d)
            if len(txt) > len(best):
                best = txt
        description = best[:5000]

    # apply link (best-effort)
    for a in soup.find_all("a", href=True):
        t = a.get_text(" ", strip=True).lower()
        if "apply" in t or "submit" in t or "apply now" in t:
            apply_url = urljoin(job_url, a["href"])
            break

    location_chips = gather_location_chips(job_url, soup, ld, [])

    ld_remote = (ld.get("job_location_type", "").lower() == "telecommute")
    is_hybrid = bool(HYBRID_REGEX.search(location_chips) or HYBRID_REGEX.search(text_all))
    is_onsite = bool(ONSITE_REGEX.search(location_chips) or ONSITE_REGEX.search(text_all))
    is_remote = bool(REMOTE_BADGE.search(location_chips) or REMOTE_BADGE.search(text_all) or ld_remote)
    if is_hybrid or is_onsite:
        is_remote = False

    display_location = best_location_for_display(ld, location_chips, location)

    # if chips show a city, ST and we still lack location, use it
    if not location and location_chips:
        m = re.search(r"\b[A-Za-z .'-]+,\s*[A-Z]{2}\b", location_chips)
        if m:
            location = m.group(0)

    # last resort, never return empty title
    if not Title:
        # try slug-based guess or at least a placeholder
        p = urlparse(job_url)
        segs = [s for s in p.path.split('/') if s]
        if segs:
            Title = " ".join(segs[-1].replace('-', ' ').split()).strip() or "Product Owner"
        else:
            Title = "Product Owner"
    
    # always return a dict
    return {
        "Title": Title,
        "Company": company,
        "Location": display_location,
        "Posted": posted,
        "posting_date": posting_date or "",
        "description_snippet": description[:DESCRIPTION_PREVIEW_CHARS],
        "job_url": job_url,
        "apply_url": apply_url,
        # new audit fields
        "is_remote_flag": "remote" if is_remote else ("hybrid" if is_hybrid else "unknown_or_onsite"),
        "location_chips": location_chips,
        "applicant_regions": ", ".join(ld.get("applicant_regions", [])),
        # ✅ add these:
        "valid_through": valid_through,
        "career_board": career_board_name(job_url),
    }
    

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
                page.wait_for_selector(
                    "a[href^='/job/'], a[href*='/jobs/'], a[href*='/remote-jobs/'], "
                    "a:has(h2), a:has(h3), main article",
                    timeout=PW_WAIT_TIMEOUT,
                )
            except Exception:
                pass
            html = page.content()
            context.close()
            browser.close()
            return html
    except Exception as e:
        msg = f"Playwright failed on {url} ({e.__class__.__name__}):\n{e}\nFalling back to requests."
        for ln in msg.splitlines():
            log_event("WARN", ln)
        return None

        return None

def get_html(url):
    domain = urlparse(url).netloc.lower()
    if domain in PLAYWRIGHT_DOMAINS:
        html = fetch_html_with_playwright(url)
        return html  # do not attempt requests() fallback for PW-only sites
    resp = polite_get(url)
    return resp.text if resp else None


def expand_career_sources():
    """Return a list of ATS job board URLs discovered on company careers pages."""
    pages = []
    for url in CAREER_PAGES:
        html = get_html(url)
        if not html:
            log_event("WARN", f"Failed to GET listing page: {listing_url}")
            _progress_clear_if_needed()
            print(f"[CAREERS        ]....Could not fetch {url}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        found = set()

        # Scan visible links
        for a in soup.find_all("a", href=True):
            full = urljoin(url, a["href"])
            host = urlparse(full).netloc.lower()
            path = urlparse(full).path.lower()

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
                qs = parse_qs(urlparse(src).query)
                slug = qs.get("for", [None])[0]
                if slug:
                    found.add(f"https://job-boards.greenhouse.io/embed/job_board?for={slug}")

        if found:
            _progress_clear_if_needed()
            print(f"[CAREERS        ]....{url} -> {len(found)} board(s)")
            pages.extend(sorted(found))
        else:
            _progress_clear_if_needed()
            print(f"[CAREERS        ]....No ATS links found on {url}")

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
            _set_if(out, "Title", it.get("title") or it.get("Title"))

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
                        iso = dateparser.parse(vt).date().isoformat()
                    except Exception:
                        iso = vt
                _set_if(out, "valid_through", iso)

    return out


# --- Role taxonomy & signal regexes ---

INCLUDE_TITLES_EXACT = [
    r"\bproduct manager\b", r"\bproduct owner\b",
    r"\bgroup product manager\b", r"\bstaff product manager\b",
    r"\bprincipal product manager\b",
]
INCLUDE_TITLES_FUZZY = [
    r"\b(technical )?program manager\b", r"\bproduct operations?\b", r"\bprod ops\b",
    r"\bproduct analyst\b", r"\bproduct strategist\b", r"\bplatform product\b",
]
POTENTIAL_ALLIED_TITLES = [
    r"\bbusiness analyst\b", r"\bsystems analyst\b", r"\brequirements analyst\b",
    r"\bimplementation analyst\b", r"\bsolutions? analyst\b",
]
EXCLUDE_TITLES = [
    r"\b(marketing|growth|brand) (manager|lead|director)\b",
    r"\bfinancial analyst\b", r"\bdata analyst\b",
    r"\bproduct marketing manager\b",
    r"\bproject manager\b(?!.*product)",
]

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
        "/careers", "boards.greenhouse.io", "lever.co", "myworkdayjobs.com",
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
    for kw in ("product manager", "product owner", "pm"):
        if kw in Title:
            role_hits += 1
    score += min(role_hits * 5, 10)

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

# Track whether a progress line is “live”
_PROGRESS_ACTIVE = False
_PROGRESS_WIDTH  = 0


# unique tracking to avoid double-appends
_seen_kept_urls  = set()
_seen_skip_urls  = set()
kept_count = 0
skip_count = 0

def _record_keep(row: dict):
    """Append a keep row once and bump kept_count."""
    global kept_count, kept_rows
    u = row.get("Job URL") or row.get("job_url") or ""
    if u and u in _seen_kept_urls:
        return
    if u:
        _seen_kept_urls.add(u)
    kept_rows.append(row)
    kept_count += 1

def _record_skip(row: dict):
    """Append a skip row once and bump skip_count."""
    global skip_count, skipped_rows
    u = row.get("Job URL") or row.get("job_url") or ""
    if u and u in _seen_skip_urls:
        return
    if u:
        _seen_skip_urls.add(u)
    skipped_rows.append(row)
    skip_count += 1




def _progress_clear_if_needed():
    """If a carriage-return progress is active, erase it in-place."""
    global _PROGRESS_ACTIVE, _PROGRESS_WIDTH
    if _PROGRESS_ACTIVE:
        sys.stdout.write("\r" + (" " * _PROGRESS_WIDTH) + "\r")  # erase
        sys.stdout.flush()
        _PROGRESS_ACTIVE = False
        _PROGRESS_WIDTH  = 0

LEVEL_WIDTH = 15  # keep this equal to your other logger's width

def _emit_lines(color_code: str, tag: str, text: str, width: int = 90):
    """
    Print a long message as multiple aligned rows so the terminal never wraps.
    The timestamp is added by your overridden print(), so we just emit lines.
    """
    s = " ".join((text or "").split())
    while s:
        chunk, s = s[:width], s[width:]
        print(f"{color_code}{tag}.{chunk}{RESET}")


def log_event(level: str, title: str, info: str = "~~~~", job: dict | None = None, color: str | None = None):
    """
    KEEP:
      [KEEP] <Title>
      [KEEP] <Company | Location>
      [🟠 QUIET  54]
      [KEEP] <snippet...>

    SKIP:
      [SKIP] <Title>
      [SKIP] <Company | Location>
      [SKIP] REASON: ...
    """
    _progress_clear_if_needed()
    lvl = level.upper()
    c = color if color is not None else LEVEL_COLOR.get(lvl, RESET)

    # left tag width = LEVEL_WIDTH, classic bracket block
    tag = f"[{lvl:<{LEVEL_WIDTH}}]"

    def _emit(text: str):
        if text:
            print(f"{c}{tag}.{text}{RESET}")

    def _emit_badge(mark: str, vis: str, score: str):
        # Single-line badge, no extra space before the closing bracket
        badge_text = f"{mark} {vis:<9} {score}".strip()
        padded = badge_text.ljust(LEVEL_WIDTH).rstrip()
        print(f"{c}[{padded}].{RESET}")

    # -------- actual output ----------
    if lvl == "KEEP":
        _emit(_short(title, 90))
        # Company | Location
        if job:
            comp = (job.get("Company") or "").strip()
            loc  = (job.get("Location") or "").strip()
            if comp or loc:
                line = comp if comp and not loc else (loc if loc and not comp else f"{comp} | {loc}")
                _emit(_short(line, 90))
            # Badge line
            vis   = str(job.get("Visibility Status", "")).upper()
            score = str(job.get("Confidence Score", "")).strip()
            mark  = str(job.get("Confidence Mark", "")).strip()
            if vis or score or mark:
                _emit_badge(mark, vis, score)
            # Snippet
            desc = (job.get("Description Snippet") or "").replace("\n", " ")
            if desc:
                _emit(_short(desc, 120))

    elif lvl == "SKIP":
        _emit(_short(title, 90))
        if job:
            comp = (job.get("Company") or "").strip()
            loc  = (job.get("Location") or "").strip()
            if comp or loc:
                line = comp if comp and not loc else (loc if loc and not comp else f"{comp} | {loc}")
                _emit(_short(line, 90))
        if info:
            _emit_lines(c, tag, f"REASON: {info}", width=90)

    else:
        _emit(_short(title, 90))
        if info:
            _emit(_short(info, 90))



DOT1 = "...."      # 4 dots
DOT2 = "......"    # 6 dots

def _host(u: str) -> str:
    try:
        return urlparse(u).netloc.replace("www.", "")
    except Exception:
        return u

def log_info_processing(url: str):
    _progress_clear_if_needed()
    print(f"[INFO           ]{DOT1}Processing listing page: {url}")

def log_info_found(n: int, url: str):
    _progress_clear_if_needed()
    print(f"[INFO           ]{DOT2}Found {n} candidate job links on {_host(url)}")

def log_info_done(url: str, elapsed_s: float):
    _progress_clear_if_needed()
    print(f"[INFO           ]{DOT2}Done {url} in {elapsed_s:.1f}s")



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
    t = (Title + " " + desc[:2000]).lower()
    score = 0
    if any(re.search(p, t) for p in INCLUDE_TITLES_EXACT): score += 50
    if any(re.search(p, t) for p in INCLUDE_TITLES_FUZZY): score += 30
    if any(re.search(p, t) for p in POTENTIAL_ALLIED_TITLES): score += 15
    score += 10 * sum(bool(re.search(p, t)) for p in POS_SIGNALS)
    score -= 10 * sum(bool(re.search(p, t)) for p in NEG_SIGNALS)
    if any(re.search(p, t) for p in EXCLUDE_TITLES): score -= 40
    return score


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



def company_from_url_fallback(url):
    p = urlparse(url); parts = [x for x in p.path.strip("/").split("/") if x]; host = p.netloc.lower()
    if "jobs.lever.co" in host and parts: return parts[0]
    if "boards.greenhouse.io" in host and parts: return parts[0]
    if "job-boards.greenhouse.io" in host:
        q = parse_qs(p.query); return (q.get("for") or [None])[0]
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
    return annual_max, False
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
        from google.oauth2.service_account import Credentials
    except Exception as e:
        _progress_clear_if_needed()
        print(f"[GS             ].Skipping Sheets push; missing libs: {e}")
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
            ws = sh.add_worksheet(Title=target_tab, rows="2000", cols="40")


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

        _progress_clear_if_needed()
        print(f"[GS             ].Appended {len(values)} rows to '{target_tab}'.")
    except Exception as e:
        _progress_clear_if_needed()
        print(f"[GSERR          ].Failed to push to Google Sheets: {e}")

def fetch_prior_decisions():
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds  = Credentials.from_service_account_file(GS_KEY_PATH, scopes=scopes)
    client = gspread.authorize(creds)
    sh     = client.open_by_url(GS_SHEET_URL).worksheet(GS_TAB_NAME)

    # get_all_records uses row 1 as header, returns list of dicts
    records = sh.get_all_records()  # respects your current headers
    prior = {}
    for r in records:
        url = r.get("job_url") or r.get("Job URL")  # tolerant if header renamed
        if url:
            prior[url] = (r.get("Applied?",""), r.get("Reason",""))
    return prior

import sys

_PROGRESS_ACTIVE = False  # keep it with your color/log helpers

# No longer used, but kept for compatibility
#def progress(i, total, kept, skipped):
#    """One-line carriage-return progress updater."""
#    global _PROGRESS_ACTIVE
#    #msg = f"[PROGRESS       ]....{i}/{total} kept={kept} skip={skipped}.........."
#    msg = f"[PROGRESS       ]....{i}/{total} kept={kept} skip={skipped}"
#    sys.stdout.write(msg + "\r")
#    sys.stdout.flush()
#    _PROGRESS_ACTIVE = True

import sys
# {_stamp()}
import sys as _sys

def progress(i, total, kept, skipped):
    """One-line carriage-return progress updater with timestamp."""
    global _PROGRESS_ACTIVE, _PROGRESS_WIDTH
    ts = _dt.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    msg = f"{ts} [PROGRESS       ]....{i}/{total} kept={kept} skip={skipped}"
    # rewrite the same line (pad to previous width so old chars are erased)
    padded = msg.ljust(_PROGRESS_WIDTH)
    _sys.stdout.write("\r" + padded)
    _sys.stdout.flush()
    _PROGRESS_ACTIVE = True
    _PROGRESS_WIDTH = max(_PROGRESS_WIDTH, len(msg))


CURRENT_SOURCE = ""  # global tag used by log_event

def set_source_tag(url: str):
    """Set a short source tag like 'remotive.com' or 'simplyhired.com'."""
    global CURRENT_SOURCE
    host = urlparse(url).netloc.replace("www.", "")
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


def _normalize_skip_defaults(d: dict) -> dict:
    """Minimal set for the Skipped tab, using Title Case + space keys."""
    d.setdefault("Date Scraped", now_ts())
    d.setdefault("Title", "")
    d.setdefault("Company", "")
    d.setdefault("Career Board", "")
    d.setdefault("Location", "")
    d.setdefault("Posted", "")
    d.setdefault("Posting Date", "")
    d.setdefault("Job URL", "")
    d.setdefault("Valid Through", "")
    d.setdefault("Apply URL", "")
    d.setdefault("Description Snippet", "")
    d.setdefault("Reason Skipped", "")
    d.setdefault("WA Rule", "Default")
    d.setdefault("Remote Rule", "Default")
    d.setdefault("US Rule", "Default")
    d.setdefault("Salary Max Detected", "")
    d.setdefault("Salary Rule", "")
    d.setdefault("Location Chips", "")
    d.setdefault("Applicant Regions", "")
    d.setdefault("Visibility Status", "")
    d.setdefault("Confidence Score", "")
    d.setdefault("Confidence Mark", "")
    return d

def _title_for_log(d: dict, link: str) -> str:
    """
    Prefer the parsed Title. If empty or looks like site chrome, derive from the URL slug.
    """
    t = (d.get("Title") or "").strip()

    # filter out obvious non-titles that sometimes get scraped
    bad = ("remotive", "rss feeds", "public api", "my account", "log in",
           "boost your career", "job search tips", "employers", "find remote jobs")
    if t:
        low = t.lower()
        if not any(b in low for b in bad):
            return t

    # strong slug fallback
    p = urlparse(link)
    segs = [s for s in p.path.split("/") if s]
    if segs:
        slug = segs[-1].split("?")[0]
        slug = re.sub(r"-?\d{5,}$", "", slug)          # strip numeric ids at end
        slug = slug.replace("-", " ").strip()
        if slug:
            return slug.title()

        t = normalize_title(t)
        return t

    return "Assumed Title: Product Owner"


##########################################################
##########################################################
##########################################################


# ---- Main ----
kept_rows = []       # the “keep” rows in internal-key form
skipped_rows = []    # the “skip” rows in internal-key form


def main():
    start_ts = datetime.now()
    print("[INFO           ] Starting run")

    # Carry-forward map from Google Sheets: url -> (Applied?, Reason)
    prior_decisions = {}
    try:
        prior_decisions = fetch_prior_decisions()
        _progress_clear_if_needed()
        print(f"[GS             ].Loaded {len(prior_decisions)} prior decisions for carry-forward.")
    except Exception as e:
        _progress_clear_if_needed()
        print(f"[GS             ].No prior decisions loaded ({e}). Continuing without carry-forward.")


    total_pages = len(STARTING_PAGES)
    all_detail_links = []

    # 1) Gather job detail links from each listing page
    all_detail_links: list[str] = []
    total_pages = len(STARTING_PAGES)

    for i, listing_url in enumerate(STARTING_PAGES, start=1):
        t0 = time.time()

        # a) Processing header
        log_info_processing(listing_url)

        html = get_html(listing_url)
        if not html:
            # warn and finish this source
            log_event("WARN", f"Failed to GET listing page: {listing_url}")
            log_info_done(listing_url, time.time() - t0)
            progress(i, total_pages, kept_count, skip_count)
            continue

        # Collector: site-specific for SimplyHired, generic for others
        if "simplyhired.com/search" in listing_url:
            links = collect_simplyhired_links(listing_url)
        else:
            links = find_job_links(html, listing_url)

        # b) Found summary
        log_info_found(len(links), listing_url)

        # c) The KEEP / SKIP / WARN / ERROR logs for each job
        all_detail_links.extend(links)

        # d) Done footer
        log_info_done(listing_url, time.time() - t0)

        # keep your live progress line in sync
        progress(i, total_pages, kept_count, skip_count)

        #set_source_tag("")  # stop tagging until the next source header


    # De-dup links
    all_detail_links = list(dict.fromkeys(all_detail_links))
    _progress_clear_if_needed()
    print(f"[INFO           ] Collected {len(all_detail_links)} unique job links")

    # 2) Visit each job link and extract details
    for j, link in enumerate(all_detail_links, start=1):
        html = get_html(link)
        
        if not html:
            # Prefer a rule reason if we already know enough; otherwise show technical
            job = {"Job URL": link}
            default_reason = "Failed to fetch job detail page"
            reason = choose_skip_reason(job, default_reason)
            _record_skip(_normalize_skip_defaults({
                "Job URL": link,
                "Title": _title_for_log({"Title": ""}, link),
                "Company": "",
                "Career Board": career_board_name(link),   # ← add
                "Valid Through": "",                       # optional but nice to keep shape
                "Reason Skipped": reason,
            }))
            log_event("SKIP", _title_for_log({"Title": ""}, link), reason, job=job)
            progress(j, len(all_detail_links), kept_count, skip_count)
            continue

        details = extract_job_details(html, link)

        # If extractor marked it as removed/archived, record a skip
        if details.get("Reason Skipped"):
            row = _normalize_skip_defaults({
                "Title": details.get("Title", ""),
                "Company": details.get("Company", ""),      # ← add
                "Career Board": details.get("career_board",""),     # added
                "Location": details.get("display_location", ""),    # ← add
                "Description Snippet": details.get("description_snippet", ""),
                "Posted": details.get("Posted", ""),
                "Posting Date": details.get("posting_date",""),
                "Valid Through": details.get("valid_through",""),   # added
                "Job URL": link,
                "Apply URL": details.get("apply_url", ""),
                "Reason Skipped": details["Reason Skipped"],
                "WA Rule": details["wa_rule"],
                "Remote Rule": details.get("remote_rule", "unknown_or_onsite"),
                "US Rule": "default",  # placeholder until you add stricter US checks
                "Salary Max Detected": "",
                "Salary Rule": "",
                "Location Chips": details.get("location_chips", ""),
                "Applicant Regions": details.get("applicant_regions", ""),
            })
            log_event("SKIP", _title_for_log(row, link), row["Reason Skipped"], job=row)
            _record_skip(row)
            progress(j, len(all_detail_links), kept_count, skip_count)
            continue

        # Salary evaluation based on description text
        annual_max, salary_out_of_bounds = eval_salary(details.get("description_snippet", ""))

        # Basic remote/US rules (lightweight; you can expand later)
        remote_flag = details.get("is_remote_flag", "unknown_or_onsite")
        remote_rule = "default" if remote_flag == "remote" else "no_remote_signal"
        us_rule = "default"  # placeholder until you add stricter US checks
        wa_rule = "default"  # placeholder for WA logic

        # Build a normalized “keep” row
        keep_row = {
            "Applied?": "",
            "Reason": "",
            "Date Scraped": now_ts(),
            "Title": details.get("Title", ""),
            "Career Board": details.get("career_board",""),
            "Company": details.get("Company", ""),
            "Location": details.get("display_location", ""),
            "Posted": details.get("Posted", ""),
            "Posting Date": details.get("posting_date", ""),
            "Valid Through": details.get("valid_through",""),
            "Job URL": details.get("job_url", link),
            "Apply URL": details.get("apply_url", link),
            "Description Snippet": details.get("description_snippet", ""),
            "WA Rule": wa_rule,
            "Remote Rule": remote_rule,
            "US Rule": us_rule,
            "Salary Max Detected": annual_max if annual_max is not None else "",
            "Salary Rule": ("out_of_bounds" if salary_out_of_bounds else "in_range_or_missing"),
            "Location Chips": details.get("location_chips", ""),
            "Applicant Regions": details.get("applicant_regions", ""),
        }

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
            "Title": keep_row["Title"],
            "Remote Rule": keep_row["Remote Rule"],
            "Location Chips": keep_row["Location Chips"],
            "Applicant Regions": keep_row["Applicant Regions"],
            "Location": keep_row["Location"],
            "Description Snippet": keep_row["Description Snippet"],
            "Job URL": keep_row["Job URL"],
        })

        if rule_reason and rule_reason != "Filtered by rules":
            # Build a SKIPPED row with the same fields you would have kept
            skip_row = _normalize_skip_defaults({
                "Job URL": keep_row["Job URL"],
                "Title": keep_row["Title"],
                "Company": keep_row["Company"],
                "Career Board": keep_row["Career Board"],
                "Location": keep_row["Location"],
                "Posted": keep_row["Posted"],
                "Posting Date": keep_row["Posting Date"],
                "Valid Through": keep_row["Valid Through"],
                "Reason Skipped": rule_reason,
                "Apply URL": keep_row["Apply URL"],
                "Description Snippet": keep_row["Description Snippet"],
                "WA Rule": wa_rule,
                "Remote Rule": remote_rule,
                "US Rule": us_rule,
                "Salary Max Detected": keep_row["Salary Max Detected"],
                "Salary Rule": keep_row["Salary Rule"],
                "Location Chips": keep_row["Location Chips"],
                "Applicant Regions": keep_row["Applicant Regions"],
            })
            log_event("SKIP", _title_for_log(skip_row, link), rule_reason, job=skip_row)
            _record_skip(skip_row)
            progress(j, len(all_detail_links), kept_count, skip_count)
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
        keep_row["Visibility Status"] = vis
        keep_row["Confidence Score"] = score
        keep_row["Confidence Mark"] = mark

        # If you want to skip purely on salary bounds, do it here before compute_visibility.
        if salary_out_of_bounds:
            # --- SKIP path (salary out of bounds) ---
            skip_row = _normalize_skip_defaults({
                "Job URL": keep_row["Job URL"],
                "Title": keep_row["Title"],
                "Company": keep_row["Company"],
                "Career Board": keep_row["Career Board"],
                "Location": keep_row["Location"],
                "Posted": keep_row["Posted"],
                "Posting Date": keep_row["Posting Date"],
                "Valid Through": keep_row.get("Valid Through",""),
                "Reason Skipped": "Salary outside configured bounds",
                "Apply URL": keep_row["Apply URL"],
                "Description Snippet": keep_row["Description Snippet"],
                "WA Rule": wa_rule,
                "Remote Rule": remote_rule,
                "US Rule": us_rule,
                "Salary Max Detected": keep_row["Salary Max Detected"],
                "Salary Rule": "out_of_bounds",
                "Location Chips": keep_row["Location Chips"],
                "Applicant Regions": keep_row["Applicant Regions"],
            })
            _record_skip(skip_row)
            log_event("SKIP", _title_for_log(skip_row, link), skip_row["Reason Skipped"], job=skip_row)

        else:
            # --- KEEP path ---
            _record_keep(keep_row)
            log_event(
                "KEEP",
                _title_for_log(keep_row, link),
                f"{keep_row.get('Company','')} | {keep_row.get('Location','')}",
                job=keep_row
            )

        # one progress update for both paths
        progress(j, len(all_detail_links), kept_count, skip_count)


    # 3) Write CSVs
    write_rows_csv(OUTPUT_CSV, kept_rows, KEEP_FIELDS)
    write_rows_csv(SKIPPED_CSV, skipped_rows, SKIP_FIELDS)

    # 3b) Push to Google Sheets
    push_rows_to_google_sheet([to_keep_sheet_row(r) for r in kept_rows], KEEP_FIELDS, tab_name=GS_TAB_NAME)
    push_rows_to_google_sheet([to_skipped_sheet_row(r) for r in skipped_rows], SKIPPED_KEYS, tab_name="Skipped")


    _progress_clear_if_needed()
    print(f"[DONE           ] Kept {kept_count}, skipped {skip_count} "
          f"in {(datetime.now() - start_ts).seconds}s")
    _progress_clear_if_needed()
    print(f"[DONE           ] CSV: {OUTPUT_CSV}")
    _progress_clear_if_needed()
    print(f"[DONE           ] CSV: {SKIPPED_CSV}")


if __name__ == "__main__":
    main()
