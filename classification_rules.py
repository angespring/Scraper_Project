# classification_rules.py

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from datetime import datetime
from logging_utils import debug
import re

DEBUG_LOCATION = False



@dataclass
class ClassificationConfig:
    mode: str = "review"  # "review" or "strict"
    allow_missing_salary: bool = True
    allow_near_min_salary: bool = True
    strict_age_policy: bool = False
    max_post_age_days: int = 90
    allowed_cities: Set[str] = None
    allowed_states: Set[str] = None
    allowed_countries: Set[str] = None

    def __post_init__(self):
        if self.allowed_cities is None:
            self.allowed_cities = {"seattle"}  # extend later
        if self.allowed_states is None:
            self.allowed_states = {"wa", "washington"}
        if self.allowed_countries is None:
            self.allowed_countries = {"us", "ca"}


 


from dataclasses import dataclass
from typing import Iterable
import re

@dataclass(frozen=True)
class WorkModeTerms:
    remote: tuple[str, ...]
    hybrid: tuple[str, ...]
    onsite: tuple[str, ...]

WORK_MODE_TERMS = WorkModeTerms(
    remote=(
        "remote", "fully remote", "remote-first", "remote first", "work from home", "wfh",
        "telework", "distributed",
    ),
    hybrid=(
        "hybrid", "partially remote", "part remote", "flexible", "flex", "few days at home",
        "x days in office", "x days in the office",
    ),
    onsite=(
        "onsite", "on-site", "on site", "in-office", "in office", "on premises", "on-premises",
        "office-based", "office based",
    ),
)

# --- Work mode classification (single source of truth) ---

_NEG_REMOTE_RX   = re.compile(r"\b(not\s+remote|no\s+remote|non\s+remote)\b", re.I)
_HYBRID_RX       = re.compile(r"\bhybrid\b", re.I)
_ONSITE_RX       = re.compile(r"\b(on[-\s]?site|in[-\s]?office|in\s+person|on[-\s]?premises)\b", re.I)

# Your “treat as Remote, watch it” policy for these
_FLEX_REMOTE_RX  = re.compile(r"\bflexible\s*/\s*remote\b|\bflexible\s+remote\b", re.I)

_REMOTE_RX       = re.compile(
    r"\b(remote|anywhere|global|wfh|work\s+from\s+home|telework|distributed)\b",
    re.I,
)

def classify_work_mode(text: str) -> str:
    """
    Returns: "Remote" | "Hybrid" | "Onsite" | "Unknown"
    Rules:
      - Hybrid/Onsite wins over Remote
      - flexible remote counts as Remote (your preference)
    """
    t = (text or "").strip()
    if not t:
        return "Unknown"

    if _NEG_REMOTE_RX.search(t):
        return "Onsite"

    if _HYBRID_RX.search(t):
        return "Hybrid"
    if _ONSITE_RX.search(t):
        return "Onsite"

    if _FLEX_REMOTE_RX.search(t):
        return "Remote"

    if _REMOTE_RX.search(t):
        return "Remote"

    return "Unknown"



def classify_keep_or_skip(
    row: Dict[str, Any],
    config: ClassificationConfig,
    seen_keys_this_run: Set[str],
) -> Tuple[bool, str]:
    """
    Returns (is_keep, reason_string).
    In review mode, reason_string may contain multiple reasons separated by " | ".
    In strict mode, only a single primary reason is returned.
    """
    reasons: List[str] = []

    # STEP 1: core fields
    if not _has_core_fields(row):
        reasons.append("Missing core fields (Title, Company, Job URL)")

    # STEP 2: dedupe in this run only
    job_key = _get_job_key(row)
    if not job_key:
        reasons.append("Missing job key")
    else:
        if job_key in seen_keys_this_run:
            reasons.append("Duplicate in current run")
        else:
            seen_keys_this_run.add(job_key)

    # STEP 3: region
    if not _region_gate(row):
        reasons.append(_region_gate_reason(row))

    # STEP 4: remote/location
    if not _remote_location_gate(row, config):
        reasons.append("Location or remote rules not met")

    # STEP 5: role
    if not _role_gate(row):
        reasons.append("Not a target role")

    # STEP 6: salary
    salary_ok, salary_reason = _salary_gate(row, config)
    if not salary_ok and salary_reason:
        reasons.append(salary_reason)

    # STEP 7: staleness
    age_ok, age_reason = _staleness_gate(row, config)
    if not age_ok and age_reason:
        reasons.append(age_reason)

    # DECISION
    if not reasons:
        return True, ""

    if config.mode == "review":
        return False, " | ".join(reasons)

    # strict: pick the first reason for now
    return False, reasons[0]


# ---------- Helpers below ----------

def _has_core_fields(row: Dict[str, Any]) -> bool:
    return bool(row.get("Title")) and bool(row.get("Company")) and bool(row.get("Job URL"))


def _get_job_key(row: Dict[str, Any]) -> Optional[str]:
    key = row.get("Job Key")
    if key:
        return str(key)
    return "|".join(
        str(row.get(field, "") or "").strip().lower()
        for field in ("Company", "Title", "Job URL")
    )


def _as_listish(v) -> List[str]:
    if v is None:
        return []
    if isinstance(v, (list, tuple, set)):
        return [str(x).strip().lower() for x in v if str(x).strip()]
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        # Prefer pipe because your scraper uses it a lot
        if "|" in s:
            parts = [p.strip() for p in s.split("|")]
        elif "," in s:
            parts = [p.strip() for p in s.split(",")]
        else:
            parts = s.split()
        return [p.lower() for p in parts if p]
    return [str(v).strip().lower()] if str(v).strip() else []


def _region_gate(row: Dict[str, Any]) -> bool:
    us_rule = (row.get("US Rule") or "").strip().lower()
    canada_rule = (row.get("Canada Rule") or "").strip().lower()

    applicant_regions = _as_listish(row.get("Applicant Regions"))
    chips = _as_listish(row.get("Location Chips"))

    # Normalize common phrases into “only” blocks
    joined = " ".join(applicant_regions + chips)

    # Hard “only” exclusions stay exclusions (handle both token and phrase variants)
    only_blocks = (
        ("eu only", "europe only"),
        ("uk only",),
        ("apac only",),
        ("latam only", "latin america only"),
    )
    for phrases in only_blocks:
        if any(p in joined for p in phrases):
            return False

    # If either region is eligible, pass
    if us_rule == "pass":
        return True
    if canada_rule == "pass":
        return True

    # Fallback inference from chips/regions
    is_canada_implied = ("canada" in chips) or ("canada" in applicant_regions) or ("ca" in chips)
    is_us_implied = ("us" in chips) or ("usa" in chips) or ("united states" in joined) or ("na" in chips)

    if is_canada_implied:
        return True
    if is_us_implied:
        return True

    # If US is explicitly fail and Canada is not pass, fail
    if us_rule == "fail":
        return False

    # Default conservative
    return False


def _region_gate_reason(row: Dict[str, Any]) -> str:
    us_rule = (row.get("US Rule") or "").strip().lower() or "missing"
    ca_rule = (row.get("Canada Rule") or "").strip().lower() or "missing"

    applicant_regions = _as_listish(row.get("Applicant Regions"))
    chips = _as_listish(row.get("Location Chips"))
    joined = " ".join(applicant_regions + chips)

    # If we hit a hard only exclusion, say that explicitly
    only_hits = []
    if "eu only" in joined or "europe only" in joined:
        only_hits.append("EU only")
    if "uk only" in joined:
        only_hits.append("UK only")
    if "apac only" in joined:
        only_hits.append("APAC only")
    if "latam only" in joined or "latin america only" in joined:
        only_hits.append("LATAM only")

    if only_hits:
        return f"Region Gate failed ({', '.join(only_hits)})"

    # Otherwise explain the rule state
    chips_preview = "|".join(chips[:8])
    regions_preview = "|".join(applicant_regions[:8])
    return f"Region Gate failed (US={us_rule}, Canada={ca_rule}, chips={chips_preview}, regions={regions_preview})"



def _location_debug_line(
    row: dict,
    remote_rule: str,
    chips,
    countries,
    states,
    cities,
) -> None:
    """
    Emit a compact, multi-line debug summary of the location rules.

    Uses logging_utils.debug(), so it plays nicely with the progress line.
    """
    if not DEBUG_LOCATION:
        return

    from logging_utils import debug  # local import to avoid cycles
    title = (row.get("Title") or "").strip()
    board = (row.get("Career Board") or "").strip()
    company = (row.get("Company") or "").strip()

    header_parts = []
    if title:
        header_parts.append(title)
    if company:
        header_parts.append(company)
    if board:
        header_parts.append(f"via {board}")

    header = " | ".join(header_parts) or "(no title)"

    # First line: simple header
    debug("")  # spacer to break cleanly from any in-progress log line
    debug(f"LOCATION DEBUG {header}")

    # Second part: details, manually wrapped to avoid super-long lines
    payload = (
        f"remote_rule={remote_rule} | "
        f"chips={list(chips)} | "
        f"countries={sorted(countries)} | "
        f"states={sorted(states)} | "
        f"cities={sorted(cities)}"
    )

    width = 70  # rough wrap
    for i in range(0, len(payload), width):
        chunk = payload[i : i + width]
        debug(f".LOCATION DEBUG {chunk}")


def _remote_location_gate(row: Dict[str, Any], config: ClassificationConfig) -> bool:
    rr = (row.get("Remote Rule") or "")
    loc = (row.get("Location") or "")
    raw_chips = row.get("Location Chips") or []

    if isinstance(raw_chips, str):
        chips = [c.strip().lower() for c in re.split(r"[|,;/]", raw_chips) if c.strip()]
    else:
        chips = [str(c).strip().lower() for c in raw_chips if str(c).strip()]

    countries, states, cities = _extract_location_signals(chips, config)

    # One classifier call, one decision
    mode_text = " ".join([rr, loc, " ".join(chips)])
    mode = classify_work_mode(mode_text).lower()

    _location_debug_line(row, mode, chips, countries, states, cities)

    # Treat Hybrid like Remote for now, if that is your current preference
    # If later you want Hybrid to be separate, you can change only this line.
    is_remote_like = mode in {"remote", "hybrid"}

    if mode == "onsite":
        # onsite rules unchanged
        if countries and countries.issubset(config.allowed_countries):
            return True
        if cities & config.allowed_cities:
            return True
        if states & config.allowed_states and not cities:
            return True
        return False

    if is_remote_like:
        if states & config.allowed_states:
            return True
        if countries and countries.issubset(config.allowed_countries):
            return True
        if countries - config.allowed_countries:
            if cities & config.allowed_cities:
                return True
            if not cities:
                return True
            return False
        return True

    # Unknown
    if countries and countries.issubset(config.allowed_countries):
        return True
    if countries - config.allowed_countries and cities:
        if not (cities & config.allowed_cities):
            return False

    return True


def _extract_location_signals(
    chips: Iterable[str],
    config: ClassificationConfig,
) -> Tuple[Set[str], Set[str], Set[str]]:
    countries: Set[str] = set()
    states: Set[str] = set()
    cities: Set[str] = set()

    for chip in chips:
        token = chip.lower()
        if token in {"us", "united states", "usa"}:
            countries.add("us")
        elif token in {"canada", "ca"}:
            countries.add("ca")
        elif token in {"portugal", "pt"}:
            countries.add("pt")
        elif token in {"wa", "washington"}:
            states.add("wa")
        elif token in {"atlanta", "seattle", "lisboa", "lisbon", "vancouver"}:
            cities.add(token)
        # extend mapping over time

    return countries, states, cities


TARGET_ROLE_KEYWORDS = [
    "product manager",
    "product owner",
    "product management",
    "business analyst",
    "business systems analyst",
]

EXCLUDED_ROLE_KEYWORDS = [
    "sales development",
    "account executive",
    "sdr",
    "marketing manager",
    "data engineer",
    "devops",
    "systems administrator",
]


def _role_gate(row: Dict[str, Any]) -> bool:
    title = (row.get("Title") or "").lower()
    snippet = (row.get("Description Snippet") or "").lower()
    text = " ".join([title, snippet])

    if any(bad in text for bad in EXCLUDED_ROLE_KEYWORDS):
        return False

    if any(good in text for good in TARGET_ROLE_KEYWORDS):
        return True

    return False


def _salary_gate(row: Dict[str, Any], config: ClassificationConfig) -> Tuple[bool, str]:
    status = (row.get("Salary Status") or "").strip().lower()

    if status == "below_floor":
        return False, "Salary below floor"

    if status == "near_min":
        if config.allow_near_min_salary:
            return True, "Salary near minimum threshold"
        return False, "Salary near minimum threshold"

    if status in {"missing", "signal_only"}:
        if config.allow_missing_salary:
            return True, "Salary missing; manual review"
        return False, "Salary missing"

    return True, ""


def _parse_date(s: str) -> Optional[datetime]:
    s = s.strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _staleness_gate(row: Dict[str, Any], config: ClassificationConfig) -> Tuple[bool, str]:
    posting_date_str = row.get("Posting Date") or ""
    valid_through_str = row.get("Valid Through") or ""

    posting_date = _parse_date(posting_date_str)
    valid_through = _parse_date(valid_through_str)

    now = datetime.utcnow()

    if valid_through and valid_through < now:
        if config.strict_age_policy:
            return False, "Posting expired"
        return True, "Posting expired; manual review"

    if posting_date:
        age_days = (now - posting_date).days
        if age_days > config.max_post_age_days:
            if config.strict_age_policy:
                return False, "Posting too old"
            return True, "Posting older than preferred range"

    return True, ""
