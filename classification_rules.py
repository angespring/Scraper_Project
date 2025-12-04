# classification_rules.py

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from datetime import datetime
from logging_utils import debug


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
        reasons.append("Not eligible for US/Canada applicants")

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


def _region_gate(row: Dict[str, Any]) -> bool:
    us_rule = (row.get("US Rule") or "").strip().lower()
    applicant_regions = [s.lower() for s in (row.get("Applicant Regions") or [])]

    if "eu only" in applicant_regions or "europe only" in applicant_regions:
        return False
    if "uk only" in applicant_regions:
        return False
    if "apac only" in applicant_regions:
        return False

    if us_rule == "fail":
        return False

    return True

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
        f"remote_rule={remote_rule}  |  "
        f"chips={list(chips)}  |  "
        f"countries={sorted(countries)}  |  "
        f"states={sorted(states)}  |  "
        f"cities={sorted(cities)}"
    )

    width = 70  # rough wrap; doesn’t need to be perfect
    for i in range(0, len(payload), width):
        chunk = payload[i : i + width]
        debug(f"..LOCATION DEBUG    {chunk}")


def _remote_location_gate(row: Dict[str, Any], config: ClassificationConfig) -> bool:
    remote_rule = (row.get("Remote Rule") or "").strip().lower()

    raw_chips = row.get("Location Chips") or []
    if isinstance(raw_chips, str):
        chips = [c.strip().lower() for c in raw_chips.split(",") if c.strip()]
    else:
        chips = [str(c).strip().lower() for c in raw_chips]

    countries, states, cities = _extract_location_signals(chips, config)

    _location_debug_line(row, remote_rule, chips, countries, states, cities)


    # Onsite
    if remote_rule == "onsite":
        if cities & config.allowed_cities:
            return True
        if states & config.allowed_states and not cities:
            return True
        return False

    # Remote / hybrid
    if remote_rule in ("remote", "hybrid"):
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

    # Unknown remote rule
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
