# config/geo_regex.py

import re

# Province names can be matched case insensitively
CAN_PROV_NAME_RX = re.compile(
    r"\b(?:"
    r"ontario|"
    r"quebec|québec|"
    r"british columbia|"
    r"alberta|"
    r"manitoba|"
    r"saskatchewan|"
    r"new brunswick|"
    r"nova scotia|"
    r"prince edward island|"
    r"newfoundland and labrador|"
    r"northwest territories|"
    r"nunavut|"
    r"yukon"
    r")\b",
    re.I,
)

# Abbreviations must be uppercase and "location-like" (surrounded by separators)
CAN_PROV_ABBR_RX = re.compile(
    r"(?<![A-Z0-9])(?:BC|AB|SK|MB|ON|QC|NB|NS|PE|NL|NT|NU|YT)(?![A-Z0-9])"
)

# Canonical province abbreviation regex (case insensitive)
CAN_PROV_ABBR_ANYCASE_RX = re.compile(
    r"\b(?:BC|AB|SK|MB|ON|QC|NB|NS|NL|PE|YT|NT|NU)\b",
    re.I,
)

# Backward compatible alias
CAN_PROV_RX = CAN_PROV_ABBR_ANYCASE_RX

PLACEHOLDER_RX = re.compile(r"search by company rss feeds public api", re.I)

COUNTRY_RX = {
    "CANADA": re.compile(r"\b(canada|can)\b", re.I),
    "US": re.compile(r"\b(united states|usa|us)\b", re.I),
    "MEXICO": re.compile(r"\bmexico\b", re.I),
    "UK": re.compile(r"\buk|united kingdom\b", re.I),
}

# Canada eligibility signals
CA_HINTS = re.compile(
    r"\b(canada|canadian|eligible to work in canada|authorized to work in canada|"
    r"(?:^|[^A-Z])(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)(?:[^A-Z]|$)|"
    r"time\s*zone\s*[:\-]?\s*(ast|adt|nst|ndt))\b",
    re.I,
)

REMOTE_KEYWORDS = [
    r"\bremote\b",
    r"remote[-\s]?first",
    r"\b(us|u\.s\.)\s*remote\b",
    r"\banywhere\b.*\b(us|u\.s\.)\b",
    r"\bwork from home\b",
]

ONSITE_BLOCKERS = [
    r"\bon[-\s]?site\b",
    r"\boffice[-\s]?based\b",
]

SINGLE_CITY_PATTERNS = [
    r"\bnew york( city)?\b|\bnyc\b",
    r"\bsan francisco\b|\bsf\b",
    r"\bseattle\b",
    r"\baustin\b",
    r"\blondon\b",
    r"\bparis\b",
]
