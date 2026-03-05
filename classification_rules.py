# classification_rules.py

from __future__ import annotations
from dataclasses import dataclass, field
import os
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from datetime import datetime
from config import geo_constants
from logging_utils import debug
import re
from config.geo_constants import (
    CAN_PROV_MAP,
    DC_NAME_TOKENS,
    WA_STATE_NAME_TOKENS,
    US_STATE_ABBR_TO_NAME,
    US_STATE_NAME_TO_ABBR,
)
from config.debug_flags import debug_print, load_debug_config
DEBUG_CFG = load_debug_config()
CA_PROV_ABBRS_LOWER = {v.lower() for v in CAN_PROV_MAP.values()}
CA_PROV_NAMES_LOWER = set(CAN_PROV_MAP.keys())  # already lowercase in geo_constants
DEBUG_LOCATION = False

@dataclass
class ClassificationConfig:
    mode: str = "review"  # "review" or "strict"
    allow_missing_salary: bool = True
    allow_near_min_salary: bool = True
    strict_age_policy: bool = False
    max_post_age_days: int = 90

    # Gate behavior
    require_us: bool = False  # <-- ADD THIS (fixes REQUIRE_US-style errors)

    # Allow lists
    allowed_cities: Set[str] = field(default_factory=set)
    allowed_states: Set[str] = field(default_factory=set)
    allowed_countries: Set[str] = field(default_factory=set)

    # Optional: where to persist this gate's pass/fail into the row
    rule_field: Optional[str] = None

    def __post_init__(self):
        if not self.allowed_cities or not self.allowed_states:
            cities, states = _allowed_from_locality_hints()

            if not self.allowed_cities:
                self.allowed_cities = cities

            if not self.allowed_states:
                self.allowed_states = states

        if not self.allowed_countries:
            self.allowed_countries = {"us", "can"}

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

def _allowed_from_locality_hints() -> tuple[set[str], set[str]]:
    """
    Returns (allowed_cities, allowed_states) derived from LOCALITY_HINTS.

    - allowed_states are the region keys like WA, BC, ON (lowercased)
    - allowed_cities are safe city tokens defined explicitly in LOCALITY_HINTS["tokens"]
      excluding 2-letter state/province abbreviations
    """

    allowed_states: set[str] = set()
    allowed_cities: set[str] = set()

    for region_key, cfg in (geo_constants.LOCALITY_HINTS or {}).items():
        if not region_key:
            continue

        # region keys are authoritative states/provinces
        allowed_states.add(region_key.strip().lower())

        for token in (cfg.get("tokens") or []):
            s = str(token).strip().lower()
            if not s:
                continue

            # skip 2-letter state/province codes
            if len(s) == 2:
                continue

            allowed_cities.add(s)

    return allowed_cities, allowed_states

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
    if not _region_gate(row, config):
        reasons.append(_region_gate_reason(row, config))

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

    for i, r in enumerate(reasons):
        if not isinstance(r, str):
            print(f"[DEBUG] reasons[{i}] is {type(r).__name__}: {r!r}")

    # DECISION
    if not reasons:
        return True, ""

    if any(not isinstance(r, str) for r in reasons):
        bad = [(type(r).__name__, r) for r in reasons if not isinstance(r, str)]
        print("[DEBUG] reasons contains non-strings:", bad)

    if config.mode == "review":
        return False, " | ".join([str(r) for r in reasons if r])

    # strict: pick the first reason for now
    return False, str(reasons[0])

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

def _region_gate(row: Dict[str, Any], config: Any) -> bool:
    def _norm_rule(v: str) -> str:
        s = (v or "").strip().lower()
        return "pass" if s == "pass" else "fail" if s == "fail" else ""

    us_rule = _norm_rule(row.get("US Rule"))
    canada_rule = _norm_rule(row.get("Canada Rule"))

    applicant_regions = _as_listish(row.get("Applicant Regions"))
    chips = _as_listish(row.get("Location Chips"))

    # For phrase matching, do NOT include chips
    joined_regions = " ".join(applicant_regions).lower()
    # For broad signal fallback, chips are fine
    joined_all = " ".join(applicant_regions + chips).lower()

    # Hard “only” exclusions (non target regions)
    only_blocks = (
        ("eu only", "europe only"),
        ("uk only", "united kingdom only"),
        ("apac only",),
        ("latam only", "latin america only"),
    )
    if any(any(p in joined_regions for p in phrases) for phrases in only_blocks):
        return False

    # US-only / Canada-only driven by explicit text phrases (not chips)
    if any(p in joined_regions for p in ("us only", "usa only", "united states only")):
        return us_rule == "pass"
    if any(p in joined_regions for p in ("canada only", "can only")):
        return canada_rule == "pass"

    # Explicit pass wins
    if us_rule == "pass" or canada_rule == "pass":
        return True

    # If config forces US
    if getattr(config, "require_us", False):
        # If Canada is explicitly allowed, keep parity
        if canada_rule == "pass":
            return True
        return us_rule == "pass"

    # Fallback truth signals
    ar_set = {a.strip().lower() for a in applicant_regions if a.strip()}
    chips_set = {c.strip().upper() for c in chips if c.strip()}

    if ar_set & {"us", "can", "na", "global"}:
        return True
    if chips_set & {"USA", "CAN"}:
        return True

    # As a last resort, allow if joined has broad region tokens
    if any(p in joined_all for p in ("north america", "na", "global", "worldwide")):
        return True

    return False

def _region_gate_reason(row: Dict[str, Any], config: Any) -> str:
    def _norm_rule(v: str) -> str:
        s = (v or "").strip().lower()
        return "pass" if s == "pass" else "fail" if s == "fail" else ""

    us_rule = _norm_rule(row.get("US Rule"))
    canada_rule = _norm_rule(row.get("Canada Rule"))

    applicant_regions = _as_listish(row.get("Applicant Regions"))
    chips = _as_listish(row.get("Location Chips"))

    joined_regions = " ".join(applicant_regions).lower()

    if "eu only" in joined_regions or "europe only" in joined_regions:
        return "Region Gate failed (EU only)"
    if "uk only" in joined_regions or "united kingdom only" in joined_regions:
        return "Region Gate failed (UK only)"
    if "apac only" in joined_regions:
        return "Region Gate failed (APAC only)"
    if "latam only" in joined_regions or "latin america only" in joined_regions:
        return "Region Gate failed (LATAM only)"

    if any(p in joined_regions for p in ("us only", "usa only", "united states only")):
        return "Region Gate failed (US-only posting but US Rule != Pass)" if us_rule != "pass" else "Region Gate passed"
    if any(p in joined_regions for p in ("canada only", "can only")):
        return "Region Gate failed (Canada-only posting but Canada Rule != Pass)" if canada_rule != "pass" else "Region Gate passed"

    if getattr(config, "require_us", False) and us_rule != "pass" and canada_rule != "pass":
        return "Region Gate failed (config.require_us=True but US Rule != Pass and Canada Rule != Pass)"

    # Generic fallback (no signal)
    ar_preview = ", ".join(sorted({a for a in applicant_regions if a})) or "none"
    chips_preview = "|".join(sorted({c.upper() for c in chips if c})) or "none"
    return f"Region Gate failed (no explicit US/CAN/NA/Global signal; Applicant Regions={ar_preview}; Chips={chips_preview})"

def _location_debug_line(
    row: dict,
    label: str,
    remote_rule: str,
    chips,
    countries,
    states,
    cities,
    applied=None,
) -> None:
    """
    Emit a compact, multi-line debug summary of the location rules.

    label: "RAW" (pre-equivalence) or "GATE" (post-equivalence) or any short tag
    applied: optional list of applied equivalence mappings
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

    debug("")  # spacer to break cleanly from any in-progress log line
    debug(f"LOCATION DEBUG [{label}] {header}")

    applied_txt = ""
    if applied:
        applied_txt = f" | applied={list(applied)}"

    payload = (
        f"remote_rule={remote_rule} | "
        f"chips={list(chips)} | "
        f"countries={sorted(countries)} | "
        f"states={sorted(states)} | "
        f"cities={sorted(cities)}"
        f"{applied_txt}"
    )

    width = 70
    for i in range(0, len(payload), width):
        chunk = payload[i : i + width]
        debug(f".LOCATION DEBUG {chunk}")

def _remote_location_gate(row: dict, config) -> bool:
    DEBUG_LOCATION = False
    job_url = (row.get("Job URL") or "")
    company = (row.get("Company") or "")

    if "builtin.com/job/senior-product-owner-digital-training-products/8422182" in job_url or company.lower() == "boeing":
        DEBUG_LOCATION = True

    debug("[REMOTE_LOCATION_GATE] HIT v2026-03-01a")
    rr = (row.get("Remote Rule") or "")
    loc = (row.get("Location") or "")
    raw_chips = row.get("Location Chips") or []
    text = row.get("_PAGE_TEXT") or row.get("Page Text") or ""

    # NOTE: this is the debug line you will want to remove or turn off later
    # global DEBUG_LOCATION
    # DEBUG_LOCATION = True

    if isinstance(raw_chips, str):
        chips = [c.strip().lower() for c in re.split(r"[|,;/]", raw_chips) if c.strip()]
    else:
        chips = [str(c).strip().lower() for c in raw_chips if str(c).strip()]

    # Expand common country aliases so signal extraction matches chips
    chips_aug = list(chips)
    if "can" in chips_aug and "canada" not in chips_aug:
        chips_aug.append("canada")
    if "us" in chips_aug and "usa" not in chips_aug:
        chips_aug.append("usa")
    if "usa" in chips_aug and "us" not in chips_aug:
        chips_aug.append("us")

    mode_text = " ".join([rr, loc])
    mode = classify_work_mode(mode_text).lower()
    is_remote_like = mode in {"remote", "hybrid"}
    is_remote = mode == "remote"
    is_hybrid = mode == "hybrid"
    is_onsite = mode == "onsite"
    is_non_remote = is_hybrid or is_onsite

    def _word_tokens(s: str) -> list[str]:
        return re.findall(r"[a-z]{2,}", (s or "").lower())

    tokens_for_signals = (
        chips_aug
        + _as_listish(loc)
        + _word_tokens(loc)
        + _as_listish(text)
    )

    countries_truth, states_truth, cities_truth = _extract_location_signals(tokens_for_signals, config)

    countries_truth = set(countries_truth or [])
    states_truth = set(states_truth or [])
    cities_truth = set(cities_truth or [])

    countries_gate, states_gate, cities_gate, remote_equiv_ok, eq_applied = _apply_equivalence(
        countries_truth, states_truth, cities_truth, remote_equiv_ok=is_remote
    )

    # -----------------------------------------
    # Remote: country eligibility short circuit
    # If candidate allows US and job includes US, pass (regardless of which US states are listed)
    # If candidate allows CAN and job includes CAN, pass (regardless of which provinces are listed)
    # -----------------------------------------
    ar_set = {r.strip().lower() for r in _as_listish(row.get("Applicant Regions")) if r and str(r).strip()}

    if is_remote:
        has_us_country = bool(countries_gate & {"us", "usa"})
        has_can_country = bool(countries_gate & {"can", "canada"})

        if ("us" in ar_set) and has_us_country:
            debug(
                "[REMOTE_LOCATION_GATE_SHORTCIRCUIT] PASS_US_COUNTRY "
                f"ar={sorted(ar_set)!r} countries_gate={sorted(countries_gate)!r} "
                f"states_gate={sorted(states_gate)!r} cities_gate={sorted(cities_gate)!r}"
            )
            _location_debug_line(row, "GATE", mode, chips, countries_gate, states_gate, cities_gate, applied=["SHORTCIRCUIT_US"])
            return True

        if ("can" in ar_set) and has_can_country:
            debug(
                "[REMOTE_LOCATION_GATE_SHORTCIRCUIT] PASS_CAN_COUNTRY "
                f"ar={sorted(ar_set)!r} countries_gate={sorted(countries_gate)!r} "
                f"states_gate={sorted(states_gate)!r} cities_gate={sorted(cities_gate)!r}"
            )
            _location_debug_line(row, "GATE", mode, chips, countries_gate, states_gate, cities_gate, applied=["SHORTCIRCUIT_CAN"])
            return True

    debug(
        "[REMOTE_LOCATION_GATE_DECISION] "
        f"rr={rr!r} loc={loc!r} mode={mode!r} is_remote_like={is_remote_like} | "
        f"chips={chips!r} | "
        f"countries_truth={sorted(countries_truth)!r} states_truth={sorted(states_truth)!r} cities_truth={sorted(cities_truth)!r} | "
        f"countries_gate={sorted(countries_gate)!r} states_gate={sorted(states_gate)!r} cities_gate={sorted(cities_gate)!r}"
    )

    _location_debug_line(row, "RAW", mode, chips, countries_truth, states_truth, cities_truth)
    _location_debug_line(
        row,
        "GATE",
        mode,
        chips,
        countries_gate,
        states_gate,
        cities_gate,
        applied=(eq_applied + (["REMOTE_EQUIV_OK"] if remote_equiv_ok else [])),
    )

    allowed_states = {
        str(x).strip().lower()
        for x in (getattr(config, "allowed_states", set()) or set())
        if str(x).strip()
    }
    allowed_countries = {
        str(x).strip().lower()
        for x in (getattr(config, "allowed_countries", set()) or set())
        if str(x).strip()
    }

    # Country token aliasing to match chips and equivalence behavior
    if "us" in allowed_countries:
        allowed_countries.add("usa")
    if "can" in allowed_countries:
        allowed_countries.add("canada")

    allowed_cities = {
        str(x).strip().lower()
        for x in (getattr(config, "allowed_cities", set()) or set())
        if str(x).strip()
    }

    debug(
        "[ALLOWLISTS] "
        f"countries={sorted(allowed_countries)} "
        f"states={sorted(allowed_states)[:20]} "
        f"cities_count={len(allowed_cities)}"
    )

    if DEBUG_LOCATION:
        debug(
            "[LOCALITY_CHECK] "
            f"states_gate={sorted(states_gate)!r} allowed_states_has={sorted(states_gate & allowed_states)!r} "
            f"cities_gate={sorted(cities_gate)!r} allowed_cities_has={sorted(cities_gate & allowed_cities)!r}"
        )

    # 2) Onsite and Hybrid pass if they contain any approved locality signal
    #    Approved city OR approved state (matches your clean mental model)
    if is_non_remote:
        if (cities_gate & allowed_cities) or (states_gate & allowed_states):
            return True
        # Country-only allowed when there are no states or cities
        if (not states_gate) and (not cities_gate) and countries_gate and countries_gate.issubset(allowed_countries):
            return True
        return False

    # 3) Remote rules
    if is_remote:
        # If an allowed state or province is present, pass
        if states_gate & allowed_states:
            return True

        # If any explicit states exist and NONE are allowed, fail
        # This catches things like Remote + CA, IL, DC with no WA anchor
        if states_gate:
            return False

        # If countries are explicitly present and all are allowed, pass
        if countries_gate and countries_gate.issubset(allowed_countries):
            return True

        # If an explicit non allowed country appears with no allowed anchor, fail
        if countries_gate and (countries_gate - allowed_countries):
            return False

        # Pure “Remote” with no usable geo signal: allow (your policy)
        return True

    # 4) Unknown mode fallback
    # Keep your prior default, but I recommend being conservative:
    # If explicit states exist and none allowed, fail.
    if states_gate and not (states_gate & allowed_states):
        return False

    # If explicit allowed country, pass
    if countries_gate and countries_gate.issubset(allowed_countries):
        return True

    return True

def _apply_equivalence(
    countries: Iterable[str] | None,
    states: Iterable[str] | None,
    cities: Iterable[str] | None,
    *,
    remote_equiv_ok: bool = True,
) -> Tuple[Set[str], Set[str], Set[str], bool, List[str]]:
    """
    Stable return contract: always a 5-tuple
    (countries_set, states_set, cities_set, remote_equiv_ok, applied_list)
    """

    c_set: Set[str] = {_norm_token(x) for x in (countries or []) if str(x).strip()}
    # states should remain lowercase
    s_set: Set[str] = {_norm_token(x) for x in (states or []) if str(x).strip()}
    city_set: Set[str] = {_norm_token(x) for x in (cities or []) if str(x).strip()}

    applied: List[str] = []

    # --------------------------
    # Apply your existing rules
    # --------------------------
    # Example patterns (replace with your real equivalence rules):
    # - normalize US/USA
    if "us" in c_set:
        c_set.remove("us")
        c_set.add("usa")
        applied.append("country:us->usa")

    # Keep remote_equiv_ok as a boolean, never None
    remote_ok = bool(remote_equiv_ok)

    return c_set, s_set, city_set, remote_ok, applied

def _norm_token(s: str) -> str:
    return (s or "").strip().lower()

def _extract_location_signals(tokens, config):
    countries = set()
    states = set()
    cities = set()

    wa_cities = set(getattr(config, "WA_CITIES", []) or [])
    wa_cities = {_norm_token(x) for x in wa_cities if str(x).strip()}
    wa_cities.discard("washington")
    wa_cities.discard("dc")
    wa_cities.discard("washington dc")

    for raw in tokens or []:
        tok = _norm_token(raw)
        if not tok:
            continue

        # Normalize punctuation and collapse whitespace
        tok = re.sub(r"[^a-z0-9\s]", " ", tok)
        tok = " ".join(tok.split())

        # Explicit DC
        if tok in DC_NAME_TOKENS:
            states.add("dc")
            countries.add("us")
            continue

        # Explicit Washington State
        if tok in WA_STATE_NAME_TOKENS:
            states.add("wa")
            countries.add("us")
            continue

        # Plain "washington" stays ambiguous on purpose
        if tok == "washington":
            continue

        # Two letter abbreviations like "wa", "ca", "dc"
        if tok in US_STATE_ABBR_TO_NAME:
            # "wa" is explicit and allowed by policy, "dc" is explicit and allowed
            states.add(tok)
            countries.add("us")
            continue

        # Full state names, but "washington" is not present in the dict by design
        if tok in US_STATE_NAME_TO_ABBR:
            abbr = US_STATE_NAME_TO_ABBR[tok]
            # Extra safety: do not allow WA from name mapping even if it sneaks back in
            if abbr == "wa":
                continue
            states.add(abbr)
            countries.add("us")
            continue

        # WA city signals
        if tok in wa_cities:
            cities.add(tok)
            states.add("wa")
            countries.add("us")
            continue

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

def _push_reason(reasons: List[str], r: Any) -> None:
    if not r:
        return
    reasons.append(r if isinstance(r, str) else str(r))

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
