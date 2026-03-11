import re
from typing import List, Set

from config.geo_regex import CAN_PROV_NAME_RX, CAN_PROV_ABBR_RX
from config.geo_constants import LOCALITY_HINTS
_US_RX = re.compile(r"\bU\.?S\.?A?\b", re.I)

from typing import List, Set
import re

def tokenize_location_chips(loc: str, page_text: str) -> List[str]:
    """
    Location chip tokenizer with two goals:
    1) Avoid CA ambiguity. Do not convert the word "California" to the token "CA".
    2) Only emit US state abbreviations when we see real 2-letter tokens in a location-like pattern.
    """
    chips: List[str] = []

    loc = loc or ""
    page_text = page_text or ""

    combined = f"{loc} {page_text}".strip()
    low = combined.lower()

    # ----------------------------
    # Helpers
    # ----------------------------
    def add(*vals: str) -> None:
        for v in vals:
            if v:
                chips.append(v)

    def has_token(tok: str) -> bool:
        return re.search(rf"(?<![A-Za-z0-9]){re.escape(tok)}(?![A-Za-z0-9])", combined) is not None

    def has_state_token(st: str) -> bool:
        return re.search(rf"(?:^|[\s,(/|]){st}(?:$|[\s,)/|])", combined) is not None

    def locality_hit(code: str) -> bool:
        hints = LOCALITY_HINTS.get(code, {})
        any_terms = hints.get("any", [])
        token_terms = hints.get("tokens", [])
        return any(term in low for term in any_terms + token_terms)

    # ----------------------------
    # Canada signals
    # ----------------------------
    has_canada_context = (
        "canada" in low
        or has_token("CAN")
        or CAN_PROV_NAME_RX.search(combined) is not None
        or CAN_PROV_ABBR_RX.search(combined) is not None
    )

    if has_canada_context:
        add("CAN")

    # ----------------------------
    # US country signal
    # ----------------------------
    if _US_RX.search(combined) or ("united states" in low) or (" usa " in f" {low} "):
        add("USA")

    # ----------------------------
    # US state tokens
    # ----------------------------
    has_ca_token = has_state_token("CA")

    if has_ca_token and not has_canada_context:
        add("CA", "USA")

    if has_state_token("WA"):
        add("WA", "USA")

    # ----------------------------
    # Province chips
    # ----------------------------
    if re.search(r"\bontario\b", low) or has_state_token("ON"):
        add("ON", "CAN")

    if re.search(r"\bbritish columbia\b", low) or has_state_token("BC"):
        add("BC", "CAN")

    # ----------------------------
    # Locality hint fallbacks from geo_constants
    # ----------------------------
    if locality_hit("WA"):
        add("USA", "WA")

    if locality_hit("CA") and not has_canada_context:
        add("USA", "CA")

    if locality_hit("BC"):
        add("CAN", "BC")

    if locality_hit("ON"):
        add("CAN", "ON")

    # ----------------------------
    # Deduplicate while preserving order
    # ----------------------------
    seen: Set[str] = set()
    out: List[str] = []
    for c in chips:
        if c in seen:
            continue
        seen.add(c)
        out.append(c)

    return out

def derive_locked_location_chips(loc_text: str) -> str:
    """
    Derive Location Chips from location text only (no page text).

    Used to enforce the invariant:
    if _LOCK_LOCATION_CHIPS is True, Location Chips must not be empty.

    Returns a pipe-delimited uppercase string, or "" if nothing can be derived.
    """
    loc_text = (loc_text or "").strip()
    if not loc_text:
        return ""

    try:
        chips = tokenize_location_chips(loc_text, "")
    except Exception:
        return ""

    cleaned = sorted({c.strip().upper() for c in chips if c and str(c).strip()})
    return "|".join(cleaned)