import re
from typing import List, Set

from config.geo_regex import CAN_PROV_NAME_RX, CAN_PROV_ABBR_RX

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
        # strict token boundary, case sensitive input
        return re.search(rf"(?<![A-Za-z0-9]){re.escape(tok)}(?![A-Za-z0-9])", combined) is not None

    def has_state_token(st: str) -> bool:
        # Require location-like context so prose does not trigger it.
        # Examples that should match: ", CA", "(CA)", "CA,", "CA /", "CA |"
        return re.search(rf"(?:^|[\s,(/|]){st}(?:$|[\s,)/|])", combined) is not None

    # ----------------------------
    # Canada guardrail
    # ----------------------------
    if (
        "canada" in low
        or has_token("CAN")  # strict token only, no uppercasing prose
        or CAN_PROV_NAME_RX.search(combined)
        or CAN_PROV_ABBR_RX.search(combined)
    ):
        add("CAN")

    # ----------------------------
    # US country signal
    # ----------------------------
    # Avoid pronoun "us" by using your regex plus a couple safe phrases.
    if _US_RX.search(combined) or ("united states" in low) or (" usa " in f" {low} "):
        add("USA")

    # ----------------------------
    # US state tokens (explicit only)
    # ----------------------------
    # CA and WA only when we see explicit 2-letter tokens in location-like context.
    if has_state_token("CA"):
        add("CA", "USA")

    if has_state_token("WA"):
        add("WA", "USA")

    # ----------------------------
    # City term fallbacks
    # ----------------------------
    # WA city terms
    wa_terms = {"seattle", "bellevue", "tacoma", "spokane", "redmond"}
    if any(t in low for t in wa_terms):
        add("USA", "WA")

    # CA city terms
    ca_terms = {"san francisco", "los angeles", "san diego", "sacramento", "oakland", "san jose"}
    if any(t in low for t in ca_terms):
        add("USA", "CA")

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