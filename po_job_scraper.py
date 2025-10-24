# --- Auto-backup section for job-scraper project ---
import shutil
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

import builtins  # use real print here, not the later override

# ---- fixed-width, wrapped logger for long lines ----
_LOG_WRAP_WIDTH = 120

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

# Modes:
#   "manual"  = do nothing
#   "prompt"  = ask at the end of the run
#   "auto"    = commit and push automatically at the end
GIT_PUSH_MODE = "manual"    # change to "prompt" or "auto" when you want

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
        print("[GIT                   ].No changes to commit.")
        return
    # push
    push = _run(["git", "push"])
    if push.returncode == 0:
        print("[GIT                   ].Pushed to remote.")
    else:
        print("[GITERR                ].Push failed:")
        for ln in (push.stdout or "").splitlines():
            print(f"[GITERR                ].{ln}")

def git_prompt_then_push(default_msg: str) -> None:
    try:
        ans = input("[GIT] Push changes to GitHub now? (y/N) ").strip().lower()
        if ans == "y":
            git_commit_and_push(default_msg)
    except Exception:
        # non-interactive environment
        pass



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
from dateutil import parser as dateparser
from urllib import robotparser
from playwright.sync_api import sync_playwright

import csv
from pathlib import Path
# --- Workday (generic) --------------------------------------------------------
import json

from urllib.parse import urlparse, parse_qs, urljoin

def workday_links_from_listing(listing_url: str, max_results: int = 250) -> list[str]:
    """
    Convert a Workday listing URL (…myworkdayjobs.com/<tenant>/<site>?q=…)
    into real job detail links by querying the cxs JSON API.
    """
    p = urlparse(listing_url)
    host = p.netloc
    # path: /<tenant>/<site> or /recruiting/<tenant>/<site>
    parts = [s for s in p.path.split("/") if s]
    if not parts:
        return []
    # Workday has both styles; pick the last two segments as tenant/site safely
    if parts[0] == "recruiting" and len(parts) >= 3:
        tenant, site = parts[1], parts[2]
    else:
        tenant = parts[0]
        site   = parts[1] if len(parts) > 1 else tenant

    qs = parse_qs(p.query or "")
    search = " ".join(qs.get("q", [])).strip() or ""

    jobs = _wd_jobs(host, tenant, search, limit=50, max_results=max_results)
    out: list[str] = []
    for j in jobs:
        # Workday returns either `externalPath` or `externalUrl` depending on tenant
        ext = j.get("externalUrl") or j.get("externalPath") or j.get("url")
        if not ext:
            continue
        # externalPath is like "/en-US/ascensuscareers/details/<slug>/<id>"
        url = urljoin(f"https://{host}/", ext)
        out.append(url)
    # de-dupe
    seen, deduped = set(), []
    for u in out:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    return deduped


def _wd_jobs(host: str, tenant: str, search: str, limit: int = 50, max_results: int = 250) -> list[dict]:
    """
    Query Workday cxs jobs endpoint and return raw job dicts.
    host: e.g., "wd5.myworkdaysite.com"
    tenant: e.g., "uw"  (UW is 'uw')
    search: free-text (we include role + optional location terms)
    """
    url = f"https://{host}/wday/cxs/{tenant}/{tenant}/jobs"
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
        items = data.get("jobPostings", []) or data.get("jobPostings", [])
        if not items:
            break
        out.extend(items)
        if len(out) >= max_results or len(items) < limit:
            break
        offset += limit
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
            p = urlparse(urljoin(base, href))
            segs = [s for s in p.path.split("/") if s]
            if len(segs) >= 3:  # careers / jobs / <slug-or-id>
                out.append(urljoin(base, href))

        # Still allow direct ATS links
        if any(k in href for k in ("greenhouse.io", "lever.co", "workday", "smartrecruiters.com")):
            out.append(urljoin(base, href))

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

    # Company: hardcode "HubSpot"
    company = "HubSpot"

    # Location: try meta/labels or visible labels
    # HubSpot varies; gather a reasonable guess
    loc = ""
    for sel in [
        '[data-test="job-location"]',
        'span[class*="location"]',
        'p[class*="location"]',
        'li[class*="location"]'
    ]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            loc = el.get_text(strip=True)
            break

    # Description snippet
    # grab first paragraph-ish text (strip boilerplate if needed)
    desc = ""
    main = soup.select_one("main") or soup
    paras = [p.get_text(" ", strip=True) for p in main.select("p")]
    desc = " ".join(paras[:3])[:600] if paras else ""

    # Apply URL: any obvious external apply
    apply_url = ""
    for a in soup.select('a[href]'):
        href = a["href"]
        if ("greenhouse.io" in href or "lever.co" in href or
            "workday" in href or "smartrecruiters.com" in href):
            apply_url = urljoin(job_url, href)
            break

    # fallback: if no external link, use job_url as apply
    if not apply_url:
        apply_url = job_url

    return {
        "Title": title,
        "Company": company,
        "Location": loc,                        # was "location"
        "job_url": job_url,
        "apply_url": apply_url,
        "description_snippet": desc,
        "career_board": "HubSpot (Public)"
    }

def collect_simplyhired_links(listing_url: str) -> list[str]:
    """Collect job detail links from a SimplyHired search listing."""
    found: list[str] = []
    seen = set()

    page_url = listing_url
    pages = 0
    while page_url and pages < MAX_PAGES_SIMPLYHIRED:
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

def _looks_like_id(s: str | None) -> bool:
    return bool(s and ID_LIKE_RX.fullmatch(s.strip()))

def _title_from_url(job_url: str) -> str:
    p = urlparse(job_url)
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
    """True if the job matches our titles or has strong responsibility signals."""
    t = normalize_title(d.get("Title") or "").lower()
    desc = (d.get("Description Snippet") or "").lower()

    # Title hits
    if any(re.search(p, t, re.I) for p in INCLUDE_TITLES_EXACT + INCLUDE_TITLES_FUZZY + POTENTIAL_ALLIED_TITLES):
        return True

    # Responsibilities: require at least 2 distinct hits across title + snippet
    hits = len(set(m.group(0) for m in RESP_SIG_RX.finditer(f"{t} {desc}")))
    return hits >= 2

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
    Liberal when unknown, strict when explicit non-NA regions appear.
    Accept if any signal mentions US/Canada. Reject on strong non-NA signals.
    For 'remote except these states', treat as eligible if HOME_STATE is not excluded.
    """
    regions = " ".join([
        str(d.get("Applicant Regions") or ""),
        str(d.get("Location Chips") or ""),
        str(d.get("Location") or ""),
    ])
    regions_low = regions.lower()
    desc_low = (d.get("Description Snippet") or "").lower()

    # 1) strong non-NA region mention -> not eligible
    if NON_NA_STRONG.search(regions_low) or NON_NA_STRONG.search(desc_low):
        return False

    # 2) positive NA mention -> eligible
    NA_HIT = re.compile(r"\b(us|u\.s\.?|united states|usa|canada|canadian)\b", re.I)
    if NA_HIT.search(regions_low) or NA_HIT.search(desc_low):
        return True

    # 2b) remote with excluded states -> eligible iff HOME_STATE not in excluded set
    excluded = _extract_excluded_states(regions_low + " " + desc_low)
    if excluded:
        my_state = (HOME_STATE or "").strip().lower()
        if my_state and my_state not in excluded:
            return True
        return False

    # 3) remote-first boards with no explicit regions -> soft allow unless copy says otherwise
    url = (d.get("Job URL") or d.get("job_url") or "").lower()
    host = urlparse(url).netloc.replace("www.", "")
    if host in REMOTE_BOARDS and SOFT_US_FOR_REMOTE_BOARDS:
        combined = regions_low + " " + desc_low
        if EU_STRONG.search(combined) or NON_US_STRONG.search(combined) or NON_NA_STRONG.search(combined):
            return False
        return True

    # 4) unknown but clearly says 'remote' with no constraints -> allow
    if "remote" in regions_low:
        return True

    return False

def build_rule_reason(d: dict) -> str:
    reasons = []
    if not _is_target_role(d):
        reasons.append("Not target role")
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

# Salary rules
SALARY_FLOOR = 110_000          # your target minimum
SOFT_SALARY_FLOOR = 90_000      # below this => SKIP, between here and FLOOR => QUIET keep


PLAYWRIGHT_DOMAINS = {
    "edtech.com", "www.edtech.com",
    "builtin.com", "www.builtin.com",
    "wellfound.com", "www.wellfound.com",
    "welcometothejungle.com", "www.welcometothejungle.com", 
    "app.welcometothejungle.com", "us.welcometothejungle.com"
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
    "https://www.themuse.com/jobs?categories=information-technology&location=remote&query=business%20analyst",
    "https://www.builtin.com/jobs?search=business%20analyst&remote=true",
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
    "https://www.edtech.com/jobs/fully-remote-jobs?Cat=Operations"

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

    "https://jobs.ashbyhq.com/zapier?departmentId=9276c6c4-a022-4990-9cf6-5c6ace283aff",

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
    "https://www.hubspot.com/careers/jobs?page=1&query=product",
    # If you want a tighter filter and HubSpot supports it, you can also try:
    # "https://www.hubspot.com/careers/jobs?page=1&functions=product&location=Remote%20-%20USA",


    # Wellfound (AngelList Talent) (JS-heavy → Playwright)
    "https://wellfound.com/role/r/product-manager",

    # Welcome to the Jungle (JS-heavy → Playwright)
    "https://www.welcometothejungle.com/en/jobs?query=product%20manager&remote=true",
]

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
    r"ukraine|brazil|argentina|colombia|apac|latam)\b",
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
        "myworkdaysite.com": "Workday",
        "wd5.myworkdaysite.com": "Workday",
        "glassdoor.com": "Glassdoor",
        "bulitin.com": "Built In",
        "www.builtin.com": "Built In",
        "welcometothejungle.com": "Welcome to the Jungle",
        # in career_board_name(url) or similar mapping
        "ascensushr.wd1.myworkdayjobs.com": "Ascensus (Workday)",
        "myworkdayjobs.com": "Workday",
        "myworkdaysite.com": "Workday",

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
    p = urlparse(u)
    host = p.netloc.lower()
    path = p.path
    q = p.query.lower()

    # Workday (both tenants)
    if host.endswith("myworkdayjobs.com") or host.endswith("myworkdaysite.com"):
        # real jobs live under /job/ or /details/; listing pages use /search
        return ("/job/" in path or "/details/" in path) and "/search" not in path

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

def find_job_links(listing_html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(listing_html, "html.parser")
    anchors = soup.find_all("a", href=True)
    links: set[str] = set()

    base_host = urlparse(base_url).netloc.lower()
    cap = 120 if ("ashbyhq.com" in base_host or "myworkdayjobs.com" in base_host or "myworkdaysite.com" in base_host) else None

    for a in anchors:
        href = a["href"].strip()
        full = urljoin(base_url, href)
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

def normalize_title(t: str) -> str:
    """Return a clean, comparable job title."""
    if not t:
        return ""

    import html, re
    t = html.unescape(str(t)).strip()

    # Remove leading [Hiring], [Remote], brackets, emojis, etc.
    t = re.sub(r"^\s*\[[^\]]+\]\s*", "", t).strip()

    # Drop “@Company” suffix if present
    t = re.sub(r"\s*@\s*[\w .,&'’\-]+$", "", t).strip()

    # Remove locale/gender markers & contract boilerplate that often trail titles
    JUNK_TRAIL = r"(?:\(\s*m\s*\/\s*f\s*\/\s*d\s*\)|\(\s*m\s*\/\s*w\s*\/\s*d\s*\)|Vollzeit|Full[-\s]*time|Part[-\s]*time|Contract|Freelance|CDI|CDD|Zeit|Temps|Tempo)\b"
    t = re.sub(rf"\s*(?:{JUNK_TRAIL})(?:\s*[,/|·•-]\s*(?:{JUNK_TRAIL}))*\s*$", "", t, flags=re.I).strip()

    # Collapse whitespace & common punctuation noise
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\s+[-–—]\s+$", "", t)

    # Canonical capitalization (but keep acronyms)
    if t.isupper():
        t = t.title()
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

    # After you’ve parsed soup and before building the return dict
    Title = _best_title(soup, job_url, Title if 'Title' in locals() else None)

 
    # --- Detect dead / removed postings (e.g., NoDesk red banner) ---
    raw_text = job_html if isinstance(job_html, str) else ""
    dead_reason = _detect_dead_post(urlparse(job_url).netloc.lower(),
                                    soup,
                                    raw_text)
    
    
    if dead_reason:
        # Try to salvage some basics even when expired:
        title = (soup.title.string.strip() if soup.title and soup.title.string else "")
        return {
            "Reason Skipped": dead_reason,
            "Job URL": job_url,
            "Title": title or "Product Owner",
            "Company": "",
            "Location": "",
            "Posting Date": "",
            "Posted": "",
            "Career Board": career_board_name(job_url),
            "Valid Through": "",
            "Location Chips": "",
            "Description Snippet": "",
            "Apply URL": job_url,
            # light rules so the SKIP row looks consistent
            "WA Rule": "default",
            "Remote Rule": "unknown_or_onsite",
            "US Rule": "default",
        }
    Title = ""
    company = ""
    location = ""
    posted = ""
    description = ""
    apply_url = job_url


    host = urlparse(job_url).netloc.lower()

    # Add this near the top of extract_job_details after host = ...
    if "greenhouse.io" in host or "job-boards.greenhouse.io" in host:
        # Try strong title sources first
        if not Title:
            og = soup.find("meta", property="og:title")
            if og and og.get("content"):
                Title = og["content"].strip()
        if not Title:
            h1 = soup.find("h1") or soup.select_one("[data-testid='title'], .app-title, .opening h2")
            if h1 and h1.get_text(strip=True):
                Title = h1.get_text(strip=True)

        # If still a number, use the previous path segment (the slug before /jobs/<id>)
        if not Title:
            p = urlparse(job_url)
            segs = [s for s in p.path.split("/") if s]
            if len(segs) >= 2 and segs[-1].isdigit():
                Title = segs[-2].replace("-", " ")

        Title = normalize_title(Title)


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

    if "hubspot.com" in urlparse(job_url).netloc:
        return parse_hubspot_detail(job_html, job_url)


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
    
    # (your existing code that builds desc_snippet)
    # desc_snippet = ...  ← your last snippet-building line

    # --- build + refine the snippet (wrap/trim before refining) ---
    desc_snippet = " ".join((description or "").split())[:DESCRIPTION_PREVIEW_CHARS]

    # Final, robust title choice and cleanup
    Title = _best_title(soup, job_url, Title)
    Title = normalize_title(Title)

    # Prefer a strong lead sentence on Remotive pages
    host = _host(job_url)
    desc_snippet = _refine_remotive_snippet(host, soup, desc_snippet, Title)

    # ➜ normalize the final title right before returning
    Title = normalize_title(Title)

    # always return a dict
    return {
        "Title": Title,
        "Company": company,
        "Location": location,
        "Posted": posted,
        "posting_date": posting_date or "",
        "description_snippet": desc_snippet,
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
                from urllib.parse import urlparse
                h = urlparse(url).netloc.lower()

                if h.endswith("jobs.ashbyhq.com"):
                    # Wait until job cards/links render (Ashby uses <a href="/<org>/<slug>">)
                    page.wait_for_selector("a[href^='/" + urlparse(url).path.strip('/').split('/')[0] + "/'], a[href*='/jobs/']", timeout=PW_WAIT_TIMEOUT*2)
                    # Gentle scroll to trigger lazy loads
                    page.mouse.wheel(0, 2000)
                    page.wait_for_timeout(800)

                elif h.endswith("myworkdayjobs.com") or h.endswith("myworkdaysite.com"):
                    # Workday often needs a little extra settle time
                    page.wait_for_timeout(1200)
            except Exception:
                pass

                        # Auto-scroll for EdTech listing pages (infinite scroll)
            try:
                from urllib.parse import urlparse
                host = urlparse(url).netloc.lower()
                path = urlparse(url).path or "/"

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
    domain = urlparse(url).netloc.lower()
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
        _progress_clear_if_needed()
        print(f"[CAREERS               ]....Probing {url}")
        html = get_html(url)
        if not html:
            log_event("WARN", "", right=f"Failed to GET listing page: {url}")
            _progress_clear_if_needed()
            print(f"[CAREERS               ]....Could not fetch {url}")
            continue

        
        soup = BeautifulSoup(html, "html.parser")
        found = set()

        # Scan visible links
        for a in soup.find_all("a", href=True):
            full = urljoin(url, a["href"])
            host = urlparse(full).netloc.lower()
            path = urlparse(full).path.lower()

            # 1) Greenhouse embedded via <iframe>
            for iframe in soup.find_all("iframe", src=True):
                src = iframe["src"]
                if "greenhouse" in src and "for=" in src:
                    qs = parse_qs(urlparse(src).query)
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
                qs = parse_qs(urlparse(src).query)
                slug = qs.get("for", [None])[0]
                if slug:
                    found.add(f"https://job-boards.greenhouse.io/embed/job_board?for={slug}")

        if found:
            _progress_clear_if_needed()
            print(f"[CAREERS               ]....{url} -> {len(found)} board(s)")
            pages.extend(sorted(found))
        else:
            _progress_clear_if_needed()
            print(f"[CAREERS               ]....No ATS links found on {url}")

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
                        iso = dateparser.parse(vt).date().isoformat()
                    except Exception:
                        iso = vt
                _set_if(out, "valid_through", iso)

    return out


# --- Role taxonomy & signal regexes ---

# --- Role taxonomy & signal regexes ---

INCLUDE_TITLES_EXACT = [
    r"\bproduct manager\b", r"\bproduct owner\b",
    r"\bgroup product manager\b", r"\bstaff product manager\b",
    r"\bprincipal product manager\b",
    r"\bbusiness analyst\b", r"\bsystems analyst\b", r"\bbusiness systems analyst\b",
    r"\bscrum master\b", r"\brelease train engineer\b", r"\brte\b",
]

INCLUDE_TITLES_FUZZY = [
    r"\b(technical )?program manager\b", r"\bproduct operations?\b", r"\bprod ops\b",
    r"\bproduct analyst\b", r"\bproduct strategist\b", r"\bplatform product\b",
    r"\brequirements? (?:analyst|engineer)\b", r"\bsolutions? analyst\b", r"\bimplementation analyst\b",
]

POTENTIAL_ALLIED_TITLES = [
    r"\bbusiness analyst\b", r"\bsystems analyst\b", r"\bbusiness systems analyst\b",
    r"\bscrum master\b", r"\brelease train engineer\b", r"\brte\b",
]

EXCLUDE_TITLES = [
    r"\b(marketing|growth|brand) (manager|lead|director)\b",
    r"\bfinancial analyst\b", r"\bdata analyst\b",
    r"\bproduct marketing manager\b",
    r"\bproject manager\b(?!.*product)",
]

# Responsibility signals (your 2a–2f, plus close neighbors)
RESPONSIBILITY_SIGNALS = [
    r"\bgather(ing)? and analy(z|s)ing feedback\b",
    r"\bfacilitat(e|ing) communication\b", r"\bstakeholders?\b",
    r"\balign(ment)?\b.*\b(business objectives?|product goals?)\b",
    r"\bprioriti[sz]e\b", r"\bmanage\b.*\bbacklog\b", r"\bproduct backlog\b",
    r"\bsupport\b.*\b(dev(elopment)?|engineering|qa|testing)\b",
    r"\brequirements?\b.*\b(analysis|definition|elicitation|specification)\b",
    r"\buser stor(y|ies)\b", r"\bacceptance criteria\b", r"\bsprint(s)?\b",
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

LEVEL_WIDTH = 20  # keep this equal to your other logger's width- padding in Terminal display

def _emit_lines(color_code: str, tag: str, text: str, width: int = 90):
    """
    Print a long message as multiple aligned rows so the terminal never wraps.
    The timestamp is added by your overridden print(), so we just emit lines.
    """
    s = " ".join((text or "").split())
    while s:
        chunk, s = s[:width], s[width:]
        print(f"{color_code}{tag}.{chunk}{RESET}")


DOT1 = "...."      # 4 dots
DOT2 = "......"    # 6 dots

def _host(u: str) -> str:
    try:
        return urlparse(u).netloc.replace("www.", "")
    except Exception:
        return u

def log_info_processing(url: str):
    _progress_clear_if_needed()
    c = LEVEL_COLOR.get("INFO", RESET)
    msg = "Processing listing page: " + url
    for ln in _wrap_lines(msg, width=90):
        print(f"{c}{_info_box()}.{ln}{RESET}")

def log_info_found(n: int, url: str, elapsed_s: float):
    _progress_clear_if_needed()
    c = LEVEL_COLOR.get("INFO", RESET)
    host = _host(url)
    # Example: [🔎 FOUND 60           ]....candidate job links on edtech.com in 71.7s
    print(f"{c}{_found_box(n)}....candidate job links on {host} in {elapsed_s:.1f}s{RESET}")

def log_info_done():
    _progress_clear_if_needed()
    c = LEVEL_COLOR.get("INFO", RESET)
    # Example: [✔ DONE                ].
    print(f"{c}{_done_box()}." + f"{RESET}")

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
    host = urlparse(url).netloc.lower()
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
        print(f"[GS                    ].Skipping Sheets push; missing libs: {e}")
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

        _progress_clear_if_needed()
        print(f"[GS                    ].Appended {len(values)} rows to '{target_tab}'.")
    except Exception as e:
        _progress_clear_if_needed()
        print(f"[GSERR                 ].Failed to push to Google Sheets: {e}")

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

import sys
# {_stamp()}
import sys as _sys

def progress(i, total, kept, skipped):
    """One-line carriage-return progress updater with timestamp."""
    global _PROGRESS_ACTIVE, _PROGRESS_WIDTH
    ts = _dt.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    msg = f"{ts} [PROGRESS              ]....{i}/{total} kept={kept} skip={skipped}"
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
    # normalize first so trailing " @Company" / "[Hiring]" etc. are removed
    t = normalize_title((d.get("Title") or "").strip())

    # filter out obvious non-titles that sometimes get scraped
    bad = ("remotive", "rss feeds", "public api", "my account", "log in",
           "boost your career", "job search tips", "employers", "find remote jobs")
    if t:
        low = t.lower()
        if not any(b in low for b in bad):
            return t

    # strong slug fallback (kept as-is)
    p = urlparse(link)
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
_STATUS_W   = 22                  # exactly 20 visible cells inside [ … ]

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

def _box(text: str) -> str:
    """Render a fixed-width bracket like: [🟠 QUIET  48        ] with visual padding."""
    return f"[{_vis_pad(text, _STATUS_W)}]"



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
    

def _to_int(x):
    try:
        return int(float(str(x).replace(",", "").strip()))
    except Exception:
        return None
# ===== console helpers (place just above def log_event) =====
import textwrap



def _wrap_lines(s: str, width: int = 90) -> list[str]:
    """Wrap for console; indent continuation by 3 spaces."""
    if not s:
        return []
    lines = textwrap.wrap(s, width=width)
    if not lines:
        return []
    first = lines[:1]
    rest  = [("   " + ln) for ln in lines[1:]]  # 3-space indent after first
    return first + rest

def _salary_box(label: str, amount: int | None, plus: bool = False) -> str:
    if not amount:
        return ""
    amt = f"${amount:,.0f}{'+' if plus else ''}"
    # right-justify against a fixed target width like "$100,000+"
    return _box(f"💲 {label:<8} {amt.rjust(len('$100,000+'))}")



# ============================================================


def log_event(
    level: str,
    title: str,
    right: str = "",
    job: dict | None = None,
    salary_near_min: int | None = None,
):
    # one-line progress uses carriage return; clear it before printing
    _progress_clear_if_needed()

    lvl = (level or "").upper().strip()
    color = LEVEL_COLOR.get(lvl, RESET)

    # Resolve fields for KEEP
    company      = (job or {}).get("Company", "") if job else ""
    career_board = (job or {}).get("Career Board", "") if job else ""
    desc_snippet = (job or {}).get("Description Snippet", "") if job else ""
    score_str    = str((job or {}).get("Confidence Score", "") or "").strip()

    if lvl == "KEEP":
        print(f"{color}{_box('KEEP')}.{title}{RESET}")

        # Optional wrapped note
        if right:
            for ln in _wrap_lines(right, width=90):
                print(f"{color}{_box('KEEP')}.{ln}{RESET}")

        # Board · Company
        if company or career_board:
            print(f"{color}{_box('KEEP')}....{(career_board or '')}.....{(company or '')}{RESET}")

        # URL
        url = (job or {}).get("Job URL") or (job or {}).get("job_url") or (job or {}).get("Apply URL")
        if url:
            print(f"{color}{_box('KEEP')}....{url}{RESET}")

        # Visibility + Confidence
        vis   = (job or {}).get("Visibility Status") or ""
        score = (job or {}).get("Confidence Score") or ""
        mark  = (job or {}).get("Confidence Mark") or ""
        if vis or score or mark:
            print(f"{color}{_conf_box(vis, score, mark)}{RESET}")


        # Salary badge(s)
        status = (job or {}).get("Salary Status", "")
        near   = _to_int((job or {}).get("Salary Near Min"))
        det    = _to_int((job or {}).get("Salary Max Detected"))

        if status == "near_min" and near:
            print(f"{color}{_salary_box('NEAR MIN', near)}{RESET}")
        elif status == "at_or_above" and det:
            # optional: show a neutral SALARY line when it meets/beat target
            print(f"{color}{_salary_box('SALARY', det, plus=True)}{RESET}")
        # else: no salary line for low/below_floor/unknown

        # End-cap: a clean visual break
        print(f"{color}{_box('✅ KEEP')}.{RESET}")
        return

    if lvl == "SKIP":
        print(f"{color}{_box('SKIP')}.{title}{RESET}")

        # Board · Company (match KEEP)
        if company or career_board:
            print(f"{color}{_box('SKIP')}....{(career_board or '')}.....{(company or '')}{RESET}")

        # URL next
        url = (job or {}).get("Job URL") or (job or {}).get("job_url") or (job or {}).get("Apply URL")
        if url:
            print(f"{color}{_box('SKIP')}....{url}{RESET}")

        # Reason, wrapped, after the metadata
        if right:
            for ln in _wrap_lines((right or ""), width=90):
                print(f"{color}{_box('SKIP')}.{ln}{RESET}")

        # End-cap
        print(f"{color}{_box('🚫 SKIP')}.{RESET}")
        return


    if lvl == "WARN":
        for ln in _wrap_lines((right or ""), width=90):
            print(f"{color}{_box('WARN')}.{ln}{RESET}")
        return

    # INFO / other
    if right:
        for ln in _wrap_lines(right, width=90):
            print(f"{color}{_box(lvl)}.{ln}{RESET}")
    else:
        print(f"{color}{_box(lvl)}.{title}{RESET}")
    
import textwrap

def _wrap_lines(text: str, width: int = 90) -> list[str]:
    text = (text or "").replace("\n", " ").strip()
    return textwrap.wrap(text, width=width, break_long_words=False, break_on_hyphens=True)

def _rule(details: dict, key: str, default="default"):
    # allows both snake_case and Title Case with space
    return details.get(key) or details.get(key.replace("_", " ").title()) or default


##########################################################
##########################################################
##########################################################


# ---- Main ----
kept_rows = []       # the “keep” rows in internal-key form
skipped_rows = []    # the “skip” rows in internal-key form


def main():
    start_ts = datetime.now()
    print("[INFO                  ].Starting run")

    # Carry-forward map from Google Sheets: url -> (Applied?, Reason)
    prior_decisions = {}
    try:
        prior_decisions = fetch_prior_decisions()
        _progress_clear_if_needed()
        print(f"[GS                    ].Loaded {len(prior_decisions)} prior decisions for carry-forward.")
    except Exception as e:
        _progress_clear_if_needed()
        print(f"[GS                    ].No prior decisions loaded ({e}). Continuing without carry-forward.")

        # Build the final set of listing pages:
    pages = STARTING_PAGES + expand_career_sources()
    total_pages = len(pages)
    all_detail_links = []

    # 1) Gather job detail links from each listing page
    for i, listing_url in enumerate(pages, start=1):
        t0 = time.time()
        log_info_processing(listing_url)
        html = get_html(listing_url)
        if not html:
            log_event("WARN", "", right=f"Failed to fetch listing page: {listing_url}")
            log_info_done()
            progress(i, total_pages, kept_count, skip_count)
            continue

        host = urlparse(listing_url).netloc

        # Workday listing pages → use the JSON API instead of DOM links
        if host.endswith("myworkdayjobs.com") or host.endswith("myworkdaysite.com"):
            links = workday_links_from_listing(listing_url)
        else:
            # existing branches...
            if "simplyhired.com/search" in listing_url:
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
                log_info_processing(page_url)
                html = get_html(page_url)
                links = parse_hubspot_list_page(html or "", base="https://www.hubspot.com")
                if not links:
                    break
                hubspot_links.extend(links)
                page_num += 1
                if page_num > 20:  # safety cap
                    break

            log_event("INFO", f"Found {len(hubspot_links)} candidate job links on hubspot.com")
            all_detail_links.extend(hubspot_links)
            log_info_done()
            progress(i, total_pages, kept_count, skip_count)
            continue

        # Collector: site-specific for SimplyHired, generic for others
        if "simplyhired.com/search" in listing_url:
            links = collect_simplyhired_links(listing_url)
        else:
            links = find_job_links(html, listing_url)

        log_info_found(len(links), listing_url, time.time() - t0)
        all_detail_links.extend(links)
        log_info_done()
        progress(i, total_pages, kept_count, skip_count)

    # De-dup links
    all_detail_links = list(dict.fromkeys(all_detail_links))
    _progress_clear_if_needed()
    print(f"[INFO                  ].Collected {len(all_detail_links)} unique job links")

    # 2) Visit each job link and extract details
    for j, link in enumerate(all_detail_links, start=1):
        html = get_html(link)
        
        if not html:
            # Prefer a rule reason if we already know enough; otherwise show technical
                default_reason = "Failed to fetch job detail page"
                board = career_board_name(link)
                title_for_log = _title_for_log({"Title": ""}, link)

                skip_row = _normalize_skip_defaults({
                    "Job URL": link,
                    "Title": title_for_log,
                    "Company": "",
                    "Career Board": board,
                    "Valid Through": "",
                    "Reason Skipped": choose_skip_reason({"Job URL": link}, default_reason),
                })
                _record_skip(skip_row)
                log_event("SKIP", title_for_log, skip_row["Reason Skipped"], job=skip_row)
                progress(j, len(all_detail_links), kept_count, skip_count)
                continue
        
        details = extract_job_details(html, link)

        # If extractor marked it as removed/archived, record a skip
        if details.get("Reason Skipped"):
            row = _normalize_skip_defaults({
                "Title": details.get("Title", ""),
                "Company": details.get("Company", ""),      # ← add
                "Career Board": details.get("career_board",""),     # added
                "Location": details.get("Location", ""),    # ← add
                "Description Snippet": details.get("description_snippet", ""),
                "Posted": details.get("Posted", ""),
                "Posting Date": details.get("posting_date",""),
                "Valid Through": details.get("valid_through",""),   # added
                "Job URL": link,
                "Apply URL": details.get("apply_url", ""),
                "Reason Skipped": details["Reason Skipped"],
                "WA Rule": details.get("wa_rule") or details.get("WA Rule") or "default",
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

        # --- Ascensus (Workday) post-filter: only keep Product/BA/Scrum-family roles ---
        host = urlparse(link).netloc.lower()
        if "ascensushr.wd1.myworkdayjobs.com" in host:
            # Reuse your unified role test (titles + responsibility signals)
            if not _is_target_role(details):
                skip_row = _normalize_skip_defaults({
                    "Job URL": link,
                    "Title": _title_for_log(details, link),
                    "Company": details.get("Company", ""),
                    "Career Board": career_board_name(link),
                    "Valid Through": details.get("Valid Through", ""),
                    "Reason Skipped": "Not target role (Ascensus filter)",
                })
                _record_skip(skip_row)
                log_event(
                    "SKIP",
                    _title_for_log(skip_row, link),
                    right=skip_row["Reason Skipped"],
                    job=skip_row,
                )

        # --- Salary: detect and label (goes just before keep_row = {...}) ---
        # Build a single text blob to scan for pay figures
        _text_for_salary = " ".join([
            details.get("description_snippet", ""),
            details.get("Location Chips", ""),
            details.get("Location", "")
        ])

        # --- Salary evaluation based on description text ---
        annual_max, salary_out_of_bounds = eval_salary(details.get("description_snippet", "") or "")
        status, badge = _salary_status(annual_max, None, None)

        keep_row_extra = {
            "Salary Status": status,                        # at_or_above | near_min | low | below_floor | unknown
            "Salary Note": badge,                           # e.g., "$105,000"
            "Salary Max Detected": annual_max or "",
            "Salary Rule": ("out_of_bounds" if (salary_out_of_bounds or status == "below_floor") else "in_range_or_missing"),
            "Salary Near Min": (annual_max if status == "near_min" else ""),
        }

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
            "Title": normalize_title(details.get("Title", "")),
            "Company": details.get("Company", ""),
            "Career Board": details.get("career_board", ""),
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
        keep_row.update(keep_row_extra)  # <-- bring in Salary Status/Note/Max/Rule/Near Min

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
                "Salary Max Detected": keep_row_extra["Salary Max Detected"],
                "Salary Rule":         keep_row_extra["Salary Rule"],
                "Salary Status":       keep_row_extra["Salary Status"],
                "Salary Note":         keep_row_extra["Salary Note"],
                "Salary Near Min":     keep_row_extra["Salary Near Min"],
                "Location Chips": keep_row["Location Chips"],
                "Applicant Regions": keep_row["Applicant Regions"],
            })
            log_event("SKIP", _title_for_log(skip_row, link), right=skip_row["Reason Skipped"], job=skip_row)

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

        # One pass, trust the sanity checks
        vis, score, mark = _public_sanity_checks(keep_row)
        keep_row["Visibility Status"] = vis
        keep_row["Confidence Score"]  = score
        keep_row["Confidence Mark"]   = mark

        # --- Salary gate with soft-keep band ---------------------------------
        if salary_out_of_bounds:
            # Robustly detect the salary max we already parsed for this job
            # Adjust the lhs if your variable name differs
            detected_max = None
            if 'annual_max' in locals() and annual_max:
                detected_max = _to_int(annual_max)
            elif keep_row.get("Salary Max Detected"):
                detected_max = _to_int(keep_row.get("Salary Max Detected"))

            if detected_max and detected_max >= SOFT_SALARY_FLOOR:
                # SOFT-KEEP: under target, but close enough to keep quietly
                keep_row["Salary Rule"] = "soft_keep"
                keep_row["Salary Near Min"] = detected_max        # <-- shows in Table1
                keep_row["Visibility Status"] = "quiet"            # your existing quiet flag
                keep_row["Confidence Mark"] = "🟠"

                _record_keep(keep_row)

                # Console: show the badge and align brackets
                log_event(
                    "KEEP",
                    _title_for_log(keep_row, link),   # you already have this helper
                    job=keep_row,
                    salary_near_min=detected_max,     # or whatever variable holds the near-min int
                )
                progress(j, len(all_detail_links), kept_count, skip_count)  # keep this after printing

                job = keep_row  # for any downstream usage


            else:
                # Hard SKIP: salary too low or unknown below soft floor
                reason = f"Salary below floor (max={detected_max:,.0f})" if detected_max else "Salary below floor (unknown max)"
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
                "Salary Max Detected": keep_row_extra["Salary Max Detected"],
                "Salary Rule":         keep_row_extra["Salary Rule"],
                "Salary Status":       keep_row_extra["Salary Status"],
                "Salary Note":         keep_row_extra["Salary Note"],
                "Salary Near Min":     keep_row_extra["Salary Near Min"],
                "Location Chips": keep_row["Location Chips"],
                "Applicant Regions": keep_row["Applicant Regions"],
                })
                skip_row["Reason Skipped"] = reason

                _record_skip(skip_row)

                # ▼ ADD THIS LINE
                log_event("SKIP", _title_for_log(skip_row, link), right=skip_row["Reason Skipped"], job=skip_row)


                progress(j, len(all_detail_links), kept_count, skip_count)
                continue


        else:
            # Normal KEEP (salary OK or not limiting)
            keep_row.pop("Salary Near Min", None)   # keep clean
            _record_keep(keep_row)
            log_event(
                "KEEP",
                _title_for_log(keep_row, link),
                job=keep_row,
            )
            progress(j, len(all_detail_links), kept_count, skip_count)   # <— add this line


            job = keep_row
        # ---------------------------------------------------------------------


    # 3) Write CSVs
    write_rows_csv(OUTPUT_CSV, kept_rows, KEEP_FIELDS)
    write_rows_csv(SKIPPED_CSV, skipped_rows, SKIP_FIELDS)

    # 3b) Push to Google Sheets
    push_rows_to_google_sheet([to_keep_sheet_row(r) for r in kept_rows], KEEP_FIELDS, tab_name=GS_TAB_NAME)
    push_rows_to_google_sheet([to_skipped_sheet_row(r) for r in skipped_rows], SKIPPED_KEYS, tab_name="Skipped")


    _progress_clear_if_needed()
    print(f"[DONE                  ].Kept {kept_count}, skipped {skip_count} "
          f"in {(datetime.now() - start_ts).seconds}s")
    _progress_clear_if_needed()
    print(f"[DONE                  ].CSV: {OUTPUT_CSV}")
    _progress_clear_if_needed()
    print(f"[DONE                  ].CSV: {SKIPPED_CSV}")

    # ---- Optional GitHub push (one place, at the end) ----
    commit_msg = f"job-scraper: {RUN_TS} kept={kept_count}, skipped={skip_count}"
    if PUSH_MODE == "auto":
        maybe_push_to_git(prompt=False, auto_msg=commit_msg)
    elif PUSH_MODE == "ask":
        maybe_push_to_git(prompt=True,  auto_msg=commit_msg)
    # "off" -> do nothing



    # ---- Optional Git push (controlled by GIT_PUSH_MODE) ----
    commit_msg = f"scraper: {RUN_TS} kept={kept_count} skipped={skip_count}"
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
PUSH_MODE = "ask"   # change to "auto" later, or "off" to disable


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
        print("[INFO                  ].Not a Git repo. Skipping push.")
        return
    if not _git_has_changes(root):
        print("[INFO                  ].No file changes to commit.")
        return

    if prompt:
        ans = input("Push code updates to GitHub now? [y/N] ").strip().lower()
        if ans != "y":
            print("[INFO                  ].Skipped push.")
            return

    _git_run("git add -A", cwd=root)
    msg = auto_msg or f"job-scraper: update @ {_t.strftime('%Y-%m-%d %H:%M:%S')}"
    _git_run(f'git commit -m "{msg}"', cwd=root)
    _git_run("git pull --rebase", cwd=root)
    code, out = _git_run("git push", cwd=root)
    if code == 0:
        print("[INFO                  ].Pushed to GitHub.")
    else:
        print("[WARN                  ].Push failed:\n" + out)



if __name__ == "__main__":
    main()
