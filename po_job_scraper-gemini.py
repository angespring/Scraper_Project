import os
import re
import time
import sys
import atexit
import textwrap
import urllib.parse as up
import datetime as dt
from datetime import datetime, timedelta, timezone
from threading import Thread, Event
from wcwidth import wcswidth, wcwidth

# Assume these global variables, constants, and external functions are defined elsewhere or imported
# We will define a minimal set here for the file to be self-contained for the refactor.
RESET = "\x1b[0m"
RED = "\x1b[31m"
GREEN = "\x1b[32m"
YELLOW = "\x1b[33m"
BLUE = "\x1b[34m"
MAGENTA = "\x1b[35m"
CYAN = "\x1b[36m"
WHITE = "\x1b[37m"

LEVEL_COLOR = {
    "INFO": CYAN,
    "WARN": YELLOW,
    "ERROR": RED,
    "SKIP": MAGENTA,
    "KEEP": GREEN,
    "DONE": GREEN,
    "PROGRESS": BLUE,
}

# --- Minimal necessary globals/placeholders (must be defined for main() to run) ---
_PROGRESS_TOTAL = 0
kept_count = 0
skip_count = 0
kept_rows = []
skipped_rows = []
_seen_kept_urls = set()
_seen_skip_urls = set()

# Global config placeholders
GS_KEY_PATH = "credentials.json"
GS_SHEET_URL = "https://docs.google.com/spreadsheets/d/your_sheet_id/edit"
GS_TAB_NAME = "Keep"
REQUEST_TIMEOUT = 30
HEADERS = {"User-Agent": "Mozilla/5.0"}
RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_CSV = f"kept_jobs_{RUN_TS}.csv"
SKIPPED_CSV = f"skipped_jobs_{RUN_TS}.csv"
KEEP_FIELDS = ["Applied?", "Reason", "Date Scraped", "Title", "Company", "Career Board", "Location", "Posted", "Posting Date", "Valid Through", "Job URL", "Apply URL", "Description Snippet", "WA Rule", "Remote Rule", "US Rule", "Salary Max Detected", "Salary Rule", "Salary Status", "Salary Note", "Salary Est. (Low-High)", "Location Chips", "Applicant Regions", "Visibility Status", "Confidence Score", "Confidence Mark"]
SKIP_FIELDS = ["Date Scraped", "Title", "Company", "Career Board", "Location", "Posted", "Posting Date", "Valid Through", "Job URL", "Reason Skipped", "WA Rule", "Remote Rule", "US Rule", "Salary Max Detected", "Salary Rule", "Location Chips", "Applicant Regions", "Apply URL", "Description Snippet", "Visibility Status", "Confidence Score", "Confidence Mark"]
SKIPPED_KEYS = SKIP_FIELDS

# Rule placeholders
INCLUDE_TITLES_EXACT = []
INCLUDE_TITLES_FUZZY = []
POTENTIAL_ALLIED_TITLES = []
EXCLUDE_TITLES = []
RESP_SIG_RX = re.compile(r"product management|agile|scrum|kanban", re.I)
ONSITE_BLOCKERS = [r"must be present", r"report to office"]
REMOTE_KEYWORDS = [r"remote", r"work from home"]
SINGLE_CITY_PATTERNS = [r"los angeles, ca", r"new york, ny"]
KEEP_UNKNOWN_SALARY = True

# Salary thresholds (Refactored to be set by CLI args later)
SALARY_TARGET_MIN = 110_000
SALARY_NEAR_DELTA = 15_000
SALARY_HARD_FLOOR = 90_000
SOFT_SALARY_FLOOR = 90_000 # Used in the final loop

# URL collection placeholders
STARTING_PAGES = []

# Function placeholders
def now_ts(): return datetime.now(timezone.utc).isoformat()
def log_print(msg, end='\n', flush=True): sys.stdout.write(msg + end); sys.stdout.flush()
def get_html(url): return None # Placeholder
def career_board_name(url): return "Unknown Board" # Placeholder
def normalize_title(title, company=None): return title # Placeholder
def _title_from_url(link): return "" # Placeholder
def _company_and_board_for_terminal(row): return "Company", "Board" # Placeholder
def choose_skip_reason(details, technical_fallback): return technical_fallback # Placeholder
def compute_visibility_and_confidence(details): return "unknown", 40, "⚪" # Placeholder
def _derive_company_and_title(details): return details # Placeholder
def _is_target_role(details): return True # Placeholder
def _job_key(details, link): return link # Placeholder
def build_rule_reason(details): return None # Placeholder
def workday_links_from_listing(url, max_results): return [] # Placeholder
def collect_hubspot_links(url, max_pages): return [] # Placeholder
def parse_hubspot_list_page(html, base): return [] # Placeholder
def collect_simplyhired_links(url): return [] # Placeholder
def find_job_links(html, listing_url): return [] # Placeholder
def expand_career_sources(): return [] # Placeholder
def write_rows_csv(filename, rows, keys): pass # Placeholder
def git_commit_and_push(msg): pass # Placeholder
def git_prompt_then_push(msg): pass # Placeholder
def _fmt_salary_line(row): return "Placeholder" # Placeholder
def has_recent(text, days): return False # Placeholder

# --- End minimal necessary globals/placeholders ---


# --- Chunk 1: Utility functions (kept) ---
def _wrap_lines(s: str, width: int = 120) -> list[str]:
    s = " ".join((s or "").split())
    if not s:
        return [""]
    return textwrap.wrap(s, width=width)

# --- Chunk 5/6/7: Progress/Log Helpers ---
DOT1 = "...."      # 4 dots
DOT2 = "......"    # 6 dots
LEVEL_WIDTH = 22
_SPINNER = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
_p = {"active": False, "width": 0, "spin": 0, "total": 0, "start": 0.0}

def _ansi_strip(s: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", s or "")

def _box(label: str) -> str:
    raw = (label or "")[:LEVEL_WIDTH]
    vis = wcswidth(_ansi_strip(raw))
    pad = max(0, LEVEL_WIDTH - vis)
    return f"[{raw}{' ' * pad}]"

def _progress_print(msg: str) -> None:
    """Draw/overwrite the single progress line in-place."""
    sys.stdout.write("\r" + msg + "\r")
    sys.stdout.flush()
    _p["active"] = True
    _p["width"]  = len(_ansi_strip(msg))

def progress_clear_if_needed() -> None:
    if _p["active"]:
        sys.stdout.write("\r" + " " * _p["width"] + "\r")
        sys.stdout.flush()
        _p["active"] = False
        _p["width"] = 0

atexit.register(progress_clear_if_needed)

def progress_set_total(n: int) -> None:
    global _PROGRESS_TOTAL
    _PROGRESS_TOTAL = int(n or 0)
    _p["total"] = _PROGRESS_TOTAL

def progress_line(spin: str) -> str:
    """Compose the human-readable single-line status."""
    processed = kept_count + skip_count
    total     = _p.get("total", 0) or "?"
    ts        = now_ts()
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
    progress_clear_if_needed()
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

# Removed the duplicate and obsolete 'progress' function wrapper.
# Removed the unused 'refresh_progress' function.

def _info_box() -> str: return _box("INFO ")
def _found_box(n: int) -> str: return _box(f"🔎 FOUND {n:>3}")
def _done_box() -> str: return _box("✔ DONE")

def log_info_processing(url: str):
    progress_clear_if_needed()
    c = LEVEL_COLOR.get("INFO", RESET)
    msg = "Processing listing page: " + url
    for ln in _wrap_lines(msg, width=120):
        log_print(f"{c}{_info_box()}.{ln}{RESET}")

def log_info_found(n: int, url: str, elapsed_s: float):
    progress_clear_if_needed()
    c = LEVEL_COLOR.get("INFO", RESET)
    host = up.urlparse(url).netloc.replace("www.", "")
    log_print(f"{c}{_found_box(n)}.candidate job links on {host} in {elapsed_s:.1f}s{RESET}")

def log_info_done():
    progress_clear_if_needed()
    c = LEVEL_COLOR.get("INFO", RESET)
    log_print(f"{c}{_done_box()}" + f"{RESET}")

# --- Salary Helpers (Centralized Logic) ---
def _money_to_number(num_str, has_k=False):
    s = num_str.replace(",", "").strip()
    val = float(s)
    if has_k:
        val *= 1_000
    return val

def _annualize(amount, unit, hours_per_week=40, weeks_per_year=52):
    if not unit:
        return amount
    u = unit.lower()
    if u in ("hour", "hr"):
        return amount * hours_per_week * weeks_per_year
    if u in ("day", "daily"):
        return amount * 5 * weeks_per_year
    if u in ("week", "wk", "weekly"):
        return amount * weeks_per_year
    if u in ("month", "mo", "monthly"):
        return amount * 12
    if u in ("year", "yr", "annum", "annual"):
        return amount
    return amount

def _salary_status(max_detected: int | None, est_low: int | None, est_high: int | None):
    val = max_detected or (est_high if est_high else None)
    if val is None:
        return "unknown", ""
    if val >= SALARY_TARGET_MIN:
        return "at_or_above", f"${val:,}+"
    if val >= SALARY_TARGET_MIN - SALARY_NEAR_DELTA:
        return "near_min", f"${val:,}"
    if val < SALARY_HARD_FLOOR:
        return "below_floor", f"${val:,}"
    return "low", f"${val:,}"

def detect_salary_max(text):
    if not text:
        return None
    t = text.replace("\u2013", "-").replace("\u2014", "-")

    pattern = re.compile(
        r"""
        \$?\s*([0-9][\d,]*(?:\.\d+)?)\s*([kK])?
        (?:\s*(?:-|to)\s*\$?\s*([0-9][\d,]*(?:\.\d+)?)\s*([kK])?)?
        (?:\s*(?:/|\bper\b|\ba\b)\s*(hour|hr|day|week|wk|month|mo|year|yr|annual|annum))?
        """,
        re.I | re.X,
    )

    def _bare_yearish(num_str, has_k, unit):
        if unit or has_k: return False
        s = num_str.replace(",", "").split(".")[0]
        if not s.isdigit(): return False
        n = int(s)
        if 1900 <= n <= 2100 or n < 10_000: return True
        return False

    annual_max = None
    for m in pattern.finditer(t):
        n1, k1, n2, k2, unit = m.groups()

        if _bare_yearish(n1, bool(k1), unit) and (not n2 or _bare_yearish(n2, bool(k2), unit)):
            continue

        surround = t[max(0, m.start()-20): m.end()+20].lower()
        has_currency = "$" in surround or " usd" in surround
        has_pay_word = any(w in surround for w in ("salary", "compensation", "pay", "per year", "yr", "annual", "base", "ote"))
        if not has_currency and not has_pay_word and not (k1 or k2 or unit):
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
    """Returns (annual_max, should_skip)."""
    annual_max = detect_salary_max(text)
    if annual_max is None:
        return annual_max, (not KEEP_UNKNOWN_SALARY)

    sal_floor = globals().get("SALARY_FLOOR")
    sal_ceil = globals().get("SALARY_CEIL")

    if sal_floor and annual_max < sal_floor:
        return annual_max, True
    if sal_ceil and annual_max > sal_ceil:
        return annual_max, True
    return annual_max, (not KEEP_UNKNOWN_SALARY)


# --- REFACTORED: Centralized Salary Logic (Goal 1) ---
def enrich_salary_fields(d: dict, page_host: str = "") -> dict:
    """
    Centralizes all salary parsing and rule application into the job details dictionary.
    Sets: Salary Max Detected, Salary Rule, Salary Status, Salary Note, Salary Est. (Low-High).
    """
    text = (d.get("Description Snippet") or d.get("Title") or "").strip()
    full_text = text + " " + (d.get("Description") or "")

    # 1. Detect max annual salary
    annual_max = detect_salary_max(full_text)

    # 2. Apply rule/status
    status, badge_text = _salary_status(annual_max, None, None)

    d["Salary Max Detected"] = int(annual_max) if annual_max is not None else ""
    d["Salary Status"] = status
    d["Salary Note"] = badge_text # Used to store the detected value/range/note
    d["Salary Rule"] = "gated" if status == "below_floor" else "pass"
    d["Salary Est. (Low-High)"] = "" # Placeholder for full range detection if needed

    # Apply global floor/ceil check
    sal_floor = globals().get("SALARY_FLOOR")
    sal_ceil = globals().get("SALARY_CEIL")

    if sal_floor and annual_max is not None and annual_max < sal_floor:
        d["Salary Rule"] = "gated_by_floor"
        d["Salary Status"] = "below_floor"
        d["Reason Skipped"] = f"Salary below hard floor (${sal_floor:,})"
    elif sal_ceil and annual_max is not None and annual_max > sal_ceil:
        d["Salary Rule"] = "gated_by_ceil"
        d["Reason Skipped"] = f"Salary above hard ceil (${sal_ceil:,})"

    return d


# --- REFACTORED: Normalization (Goal 3: Location Chips) ---
def _as_str(x) -> str:
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", "ignore")
        except Exception:
            return str(x)
    return str(x or "")

def infer_board_from_url(url: str) -> str:
    BOARD_MAP = {
        "boards.greenhouse.io": "Greenhouse", "greenhouse.io": "Greenhouse",
        "jobs.lever.co": "Lever", "lever.co": "Lever",
        "workday.com": "Workday", "ashbyhq.com": "Ashby",
        "myworkdayjobs.com": "Workday", "icims.com": "iCIMS",
        "smartrecruiters.com": "SmartRecruiters", "remotive.com": "Remotive",
    }
    host = up.urlparse(url or "").netloc.lower()
    for key, name in BOARD_MAP.items():
        if host.endswith(key):
            return name
    return ""

def _normalize_job_defaults(d: dict) -> dict:
    """Ensure Title Case + space keys exist for export to CSV/Sheets and normalize complex types."""
    # Ensure keys exist
    d.setdefault("Applied?", "")
    d.setdefault("Reason", "")
    d.setdefault("Date Scraped", now_ts())

    # Map snake_case to Title Case
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

    # If board still empty, infer from URL host
    if not d.get("Career Board") and d.get("Job URL"):
        d["Career Board"] = infer_board_from_url(d["Job URL"])

    # --- Goal 3: Safely handle Location Chips (List or String) ---
    chips = d.get("Location Chips") or d.get("location_chips", "")
    if isinstance(chips, (list, tuple)):
        chips = ", ".join(chips)
    d["Location Chips"] = chips

    # Set other metadata defaults
    d.setdefault("Applicant Regions", d.get("applicant_regions", ""))
    d.setdefault("Visibility Status", d.get("visibility_status", ""))
    d.setdefault("Confidence Score", d.get("confidence_score", ""))
    d.setdefault("Confidence Mark", d.get("confidence_mark", ""))
    d.setdefault("Valid Through", d.get("valid_through", ""))

    return d

# --- Log/Record Helpers ---
def to_keep_sheet_row(d: dict) -> dict:
    return {k: d.get(k, "") for k in KEEP_FIELDS}

def to_skipped_sheet_row(d: dict) -> dict:
    return {k: d.get(k, "") for k in SKIP_FIELDS}

def _normalize_skip_defaults(row: dict) -> dict:
    # Use the unified normalization helper first, then ensure Reason Skipped is set
    row = _normalize_job_defaults(row)
    row.setdefault("Reason Skipped", row.get("Reason", "") or "Filtered by rules")
    # Clean up fields only relevant to KEEP rows
    row.pop("Applied?", None)
    row.pop("Reason", None)
    return row

def _record_keep(row: dict) -> None:
    global kept_count
    url = (row.get("Job URL") or row.get("job_url") or "").strip()
    if url and url in _seen_kept_urls: return
    _seen_kept_urls.add(url)

    row.setdefault("Reason", row.get("Reason Skipped", ""))
    kept_rows.append(to_keep_sheet_row(row))
    kept_count += 1
    progress_clear_if_needed()

def _record_skip(row: dict, reason: str) -> None:
    global skip_count
    url = (row.get("Job URL") or row.get("job_url") or "").strip()
    if url and url in _seen_skip_urls: return
    _seen_skip_urls.add(url)

    if reason:
        row["Reason Skipped"] = reason

    skipped_rows.append(to_skipped_sheet_row(row))
    skip_count += 1
    progress_clear_if_needed()

def _log_and_record_skip(link: str, rule_reason: str, row: dict) -> None:
    """Normalize + record a skipped job and emit SKIP logs to the terminal."""
    if row is None: row = {}
    row.setdefault("Job URL", link)
    row.setdefault("Reason Skipped", rule_reason or row.get("Reason Skipped", "Filtered by rules"))
    row = _normalize_skip_defaults(row)
    _record_skip(row, reason=rule_reason) # Record first

    # --- pretty terminal logging ---
    c, r = LEVEL_COLOR.get("SKIP", RESET), RESET
    title = (row.get("Title") or "").strip() or _title_from_url(link) or "<missing title>"
    company, board = _company_and_board_for_terminal(row)

    progress_clear_if_needed()
    log_print(f"{c}{_box('SKIP ')}.{title}{r}")
    log_print(f"{c}{_box('SKIP ')}...{board} → {company}{r}")
    log_print(f"{c}{_box('SKIP ')}...{link}{r}")
    log_print(f"{c}{_box('💭 REASON ')}... → {rule_reason}{r}")
    log_print(f"{c}{_box('🚫 DONE ')}.{r}")
    progress_clear_if_needed()

def log_and_record_skip(link: str, rule_reason: str, keep_row: dict) -> None:
    _log_and_record_skip(link, rule_reason, keep_row)


# --- Main Logic ---
def main():
    global kept_count, skip_count, SALARY_FLOOR, SALARY_CEIL, PUSH_MODE

    # Placeholder for argument parsing
    class Args:
        smoke = False
        limit_pages = None
        limit_links = None
        only = ""
        floor = 0
        ceil = None
        push = None
    
    # Use a dummy args object for simplicity in the refactored file
    args = Args()
    SMOKE = args.smoke or os.getenv("SMOKE") == "1"
    PAGE_CAP = args.limit_pages or (3 if SMOKE else 0)
    LINK_CAP = args.limit_links or (40 if SMOKE else 0)
    ONLY_KEYS = [s.strip().lower() for s in args.only.split(",") if s.strip()]

    SALARY_FLOOR = args.floor
    SALARY_CEIL  = args.ceil

    if SMOKE:
        os.environ.setdefault("SCRAPER_TIMEOUT_SECS", "5")

    start_ts = datetime.now()
    progress_clear_if_needed()
    log_print(f"{_box('INFO ')}.Starting run")

    prior_decisions = {}
    # Placeholder for fetch_prior_decisions
    # try: prior_decisions = fetch_prior_decisions() ... except...

    # 1) Build the final set of listing pages
    pages = STARTING_PAGES + expand_career_sources()

    if ONLY_KEYS:
        pages = [u for u in pages if any(k in u.lower() for k in ONLY_KEYS)]

    if PAGE_CAP:
        pages = pages[:PAGE_CAP]

    if SMOKE:
        pages = pages[:1]

    total_pages = len(pages)
    all_detail_links = []
    
    # --- Collect detail links (Loop 1) ---
    for i, listing_url in enumerate(pages, start=1):
        t0 = time.time()
        set_source_tag(listing_url)
        html = get_html(listing_url)
        if not html:
            log_event("WARN", "", right=f"Failed to fetch listing page: {listing_url}")
            progress_clear_if_needed()
            continue

        p = up.urlparse(listing_url)
        host = p.netloc.lower().replace("www.", "")
        links = []
        
        # Site-specific collectors (Workday/HubSpot/SimplyHired logic simplified)
        if "hubspot.com" in host and "/careers/jobs" in listing_url:
            links = collect_hubspot_links(listing_url, max_pages=25)
        elif host.endswith("myworkdayjobs.com") or host.endswith("myworkdaysite.com"):
            try:
                links = workday_links_from_listing(listing_url, max_results=250)
            except Exception as e:
                log_event("WARN", f"Workday expansion failed: {e}")
        elif "simplyhired.com/search" in listing_url:
            links = collect_simplyhired_links(listing_url)
        else:
            links = find_job_links(html, listing_url)

        elapsed = time.time() - t0
        log_info_found(len(links), listing_url, elapsed)
        all_detail_links.extend(links)
        progress_clear_if_needed()

    # --- Final normalization & deduplication ---
    def _norm_url(u: str) -> str:
        """Lowercase host, strip tracking params and trailing slash, keep stable path."""
        from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
        p = urlparse(u)
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
            if k.lower() not in {"utm_source","utm_medium","utm_campaign","utm_term","utm_content",
                                "ref","source","_hsmi","_hsenc", "page", "p", "start"}]
        clean = urlunparse((
            p.scheme,
            p.netloc.lower().replace("www.", ""),
            p.path.rstrip("/"),
            "", urlencode(q, doseq=True), ""
        ))
        return clean

    _seen, deduped = set(), []
    for u in all_detail_links:
        nu = _norm_url(u)
        if nu in _seen:
            continue
        _seen.add(nu)
        deduped.append(u)
        
    initial_link_count = len(all_detail_links)
    all_detail_links = deduped
    log_print(f"{_box('INFO ')}.[🧹 DE-DUPE].Reduced {initial_link_count} → {len(deduped)} unique URLs")

    # Optional caps (after dedupe)
    if LINK_CAP:
        all_detail_links = all_detail_links[:LINK_CAP]
    if SMOKE:
        all_detail_links = all_detail_links[:20]

    # --- Setup for Loop 2 ---
    progress_set_total(len(all_detail_links))
    kept_count, skip_count = 0, 0
    _seen_kept_urls.clear()
    _seen_skip_urls.clear()
    processed_keys: set[str] = set()
    start_spinner(len(all_detail_links))

    # --- Process each job link (Loop 2) ---
    for j, link in enumerate(all_detail_links, start=1):
        progress_tick(f"{j}/{len(all_detail_links)}")
        set_source_tag(link)

        # 1) Fetch HTML
        html = get_html(link)

        # A) Fetch failure
        if not html:
            default_reason = "Failed to fetch job detail page"
            skip_row = _normalize_skip_defaults({"Job URL": link, "Reason Skipped": default_reason})
            _log_and_record_skip(link, default_reason, skip_row)
            continue

        # B) Parse details and enrich salary (Goal 1: Centralized)
        details = extract_job_details(html, link)
        details = enrich_salary_fields(details, page_host=up.urlparse(link).netloc)
        details = _normalize_job_defaults(details) # (Goal 3: Location Chips)

        # C) Hard De-dupe by job key
        jk = _job_key(details, link)
        if jk in processed_keys:
            _log_and_record_skip(link, "DE-DUPE", details)
            continue
        processed_keys.add(jk)

        # D) Extractor marked as removed/archived
        if details.get("Reason Skipped"):
            _log_and_record_skip(link, details["Reason Skipped"], details)
            continue

        # E) Apply visibility/confidence
        vis, score, mark = _public_sanity_checks(details)
        details.update({"Visibility Status": vis, "Confidence Score": score, "Confidence Mark": mark})

        # F) Carry forward prior decisions
        prev = prior_decisions.get(details["Job URL"])
        if prev:
            applied_prev, reason_prev = prev
            if applied_prev and not details.get("Applied?"): details["Applied?"] = applied_prev
            if reason_prev and not details.get("Reason"): details["Reason"] = reason_prev

        # G) Rule-based skip gate (Remote/Title/Location)
        rule_reason = build_rule_reason(details)

        if rule_reason and rule_reason != "Filtered by rules":
            _log_and_record_skip(link, rule_reason, details)
            continue

        # H) Salary skip gate (uses fields set by enrich_salary_fields)
        sal_status = (details.get("Salary Status") or "").strip().lower()
        detected_max = _to_int(details.get("Salary Max Detected"))

        if sal_status == "below_floor":
            # Convert to SKIP
            skip_reason = f"Salary out of target range: ${detected_max:,}"
            _log_and_record_skip(link, skip_reason, details)
            continue
        elif sal_status == "near_min":
            # Soft-keep: near minimum, but keep it quietly (Confidence Mark reflects this)
            details["Salary Rule"] = "soft_keep"
            details["Confidence Mark"] = "🟠"
            _record_keep(details)
            log_event("KEEP", _title_for_log(details, link), right=details)
        else:
            # Normal KEEP (salary OK, unknown, or at/above target)
            _record_keep(details)
            log_event("KEEP", _title_for_log(details, link), right=details)

    # --- Finalization ---
    stop_spinner()

    # 3) Write Outputs
    write_rows_csv(OUTPUT_CSV, kept_rows, KEEP_FIELDS)
    write_rows_csv(SKIPPED_CSV, skipped_rows, SKIP_FIELDS)
    push_rows_to_google_sheet(kept_rows, KEEP_FIELDS, tab_name=GS_TAB_NAME)
    push_rows_to_google_sheet(skipped_rows, SKIP_FIELDS, tab_name="Skipped")

    progress_clear_if_needed()
    log_print(f"{_box('DONE ')}.Kept {kept_count}, skipped {skip_count} in {(datetime.now() - start_ts).seconds}s")
    log_print(f"{_box('DONE ')}.CSV: {OUTPUT_CSV}")
    log_print(f"{_box('DONE ')}.CSV: {SKIPPED_CSV}")

    # Final GitHub push logic (using simplified PUSH_MODE)
    commit_msg = f"job-scraper: {RUN_TS} kept={kept_count}, skipped={skip_count}"
    if PUSH_MODE == "auto":
        maybe_push_to_git(prompt=False, auto_msg=commit_msg)
    elif PUSH_MODE == "ask":
        maybe_push_to_git(prompt=True, auto_msg=commit_msg)


# Assuming remaining log/git helpers are defined below main

def _public_sanity_checks(keep_row: dict) -> tuple[str, int, str]:
    # Placeholder implementation
    url = (keep_row.get("Job URL") or "").strip()
    ats_ok = True if url.startswith("http") else False
    listed_on_careers = False
    has_recent_date = False
    return label_visibility(
        ats_status_200 = ats_ok,
        listed_on_careers = listed_on_careers,
        in_org_feed = False,
        has_recent_date = has_recent_date,
        last_seen_days = 0,
        cache_only = False,
    )

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

def _to_int(x):
    t = str(x).lower().replace("\u00a0", "").replace(" ", "").replace(",", "").strip()
    if not t or "401k_plan" in t: return None
    try:
        if t.endswith("k") and t[:-1].replace(".", "", 1).isdigit(): return int(float(t[:-1]) * 1000)
        return int(float(t))
    except Exception:
        return None

def push_rows_to_google_sheet(rows, keys, tab_name=None):
    # Placeholder implementation to prevent crash
    progress_clear_if_needed()
    log_print(f"[{_box('GS ')}].Skipping Sheets push to '{tab_name}'; function is placeholder.")

def fetch_prior_decisions():
    # Placeholder implementation
    return {}

def maybe_push_to_git(prompt: bool = True, auto_msg: str | None = None):
    # Placeholder implementation
    progress_clear_if_needed()
    log_print(f"[{_box('INFO ')}].Skipped Git push; function is placeholder.")

def log_event(level: str, left: str = "", right=None, *, job=None, url: str | None = None, width: int = 120, **_):
    # Simplified log_event placeholder
    progress_clear_if_needed()
    lvl, color = (level or "").upper(), LEVEL_COLOR.get(level, RESET)
    tag = _box(lvl)
    title = left or (job.get("Title") if isinstance(job, dict) else "") or (right.get("Title") if isinstance(right, dict) else "")
    link = url or (job.get("Job URL") if isinstance(job, dict) else "")
    
    log_print(f"{color}{tag}.{title}{RESET}")
    if link:
        log_print(f"{color}{tag}....{link}{RESET}")
    if lvl == "KEEP" or lvl == "SKIP":
        log_print(f"{color}{_box('DONE ')}.{RESET}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Product/PO job scraper")
    parser.add_argument("--smoke", action="store_true", help="Run a quick test.")
    parser.add_argument("--link-cap", type=int, default=None, help="Hard-cap number of detail links.")
    parser.add_argument("--floor", type=int, default=SALARY_HARD_FLOOR, help="Hard salary minimum.")
    parser.add_argument("--ceil", type=int, default=None, help="Hard salary maximum.")
    parser.add_argument("--push", choices=["auto", "ask", "off"], default="off", help="Git push mode.")
    parser.add_argument("--only", type=str, default="", help="Filter sources by keyword.")

    args = parser.parse_args()
    SMOKE = getattr(args, "smoke", False) or bool(os.environ.get("SMOKE"))
    LINK_CAP = args.link_cap
    SALARY_FLOOR = args.floor
    SALARY_CEIL = args.ceil
    PUSH_MODE = args.push
    
    # Note: Global variables like STARTING_PAGES and external function definitions 
    # (e.g., get_html, extract_job_details) would be required for actual execution.

    main()