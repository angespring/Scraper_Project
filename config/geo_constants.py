# config/geo_constants.py

# ----------------------------
# Canada provinces and territories
# ----------------------------

CAN_PROV_MAP = {
    "alberta": "AB",
    "british columbia": "BC",
    "manitoba": "MB",
    "new brunswick": "NB",
    "newfoundland and labrador": "NL",
    "nova scotia": "NS",
    "ontario": "ON",
    "prince edward island": "PE",
    "quebec": "QC",
    "saskatchewan": "SK",
    "northwest territories": "NT",
    "nunavut": "NU",
    "yukon": "YT",
}

# ----------------------------
# US state normalization
# ----------------------------

HOME_STATE = "Washington"

# Canonical abbreviation -> full name (lowercase)
_US_ABBR_TO_NAME = {
    "al": "alabama",
    "ak": "alaska",
    "az": "arizona",
    "ar": "arkansas",
    "ca": "california",
    "co": "colorado",
    "ct": "connecticut",
    "de": "delaware",
    "fl": "florida",
    "ga": "georgia",
    "hi": "hawaii",
    "id": "idaho",
    "il": "illinois",
    "in": "indiana",
    "ia": "iowa",
    "ks": "kansas",
    "ky": "kentucky",
    "la": "louisiana",
    "me": "maine",
    "md": "maryland",
    "ma": "massachusetts",
    "mi": "michigan",
    "mn": "minnesota",
    "ms": "mississippi",
    "mo": "missouri",
    "mt": "montana",
    "ne": "nebraska",
    "nv": "nevada",
    "nh": "new hampshire",
    "nj": "new jersey",
    "nm": "new mexico",
    "ny": "new york",
    "nc": "north carolina",
    "nd": "north dakota",
    "oh": "ohio",
    "ok": "oklahoma",
    "or": "oregon",
    "pa": "pennsylvania",
    "ri": "rhode island",
    "sc": "south carolina",
    "sd": "south dakota",
    "tn": "tennessee",
    "tx": "texas",
    "ut": "utah",
    "vt": "vermont",
    "va": "virginia",
    "wa": "washington",
    "wv": "west virginia",
    "wi": "wisconsin",
    "wy": "wyoming",
    "dc": "district of columbia",
}

# Extractor tokens, all lowercase
DC_NAME_TOKENS = {
    "dc",
    "d c",
    "d.c",
    "d.c.",
    "district of columbia",
    "washington dc",
    "washington d c",
    "washington d.c",
    "washington d.c.",
}

WA_STATE_NAME_TOKENS = {
    "wa",
    "washington state",
    "state of washington",
}

# Strict two letter validation, lowercase tokens
US_STATE_ABBRS_UPPER = {abbr.upper() for abbr in _US_ABBR_TO_NAME.keys()}
# Backward compatible: strict two letter validation for US states (lowercase, includes dc)
US_STATE_ABBRS = frozenset(_US_ABBR_TO_NAME.keys())
US_STATE_NAMES = set(_US_ABBR_TO_NAME.values())  # includes "district of columbia"

# Useful for chips allow list, uppercase tokens
US_STATE_CHIPS = {abbr.upper() for abbr in _US_ABBR_TO_NAME.keys()}  # includes "DC"

# ----------------------------
# Public mappings for lookups
# ----------------------------

# Abbreviation -> full name (lowercase)
US_STATE_ABBR_TO_NAME = dict(_US_ABBR_TO_NAME)

# Full name -> abbreviation (lowercase)
# IMPORTANT: do NOT map plain "washington" to "wa" because it collides with "Washington, DC".
US_STATE_NAME_TO_ABBR = {name: abbr for abbr, name in _US_ABBR_TO_NAME.items()}

# Remove ambiguous mapping if present
US_STATE_NAME_TO_ABBR.pop("washington", None)  # keep washington ambiguous

# Treat these as explicit DC signals
DC_PHRASES = DC_NAME_TOKENS

# Treat these as explicit Washington State signals
WA_STATE_PHRASES = WA_STATE_NAME_TOKENS

# ----------------------------
# Region normalization (for applicant regions, not location chips)
# ----------------------------
# Important rule:
# - "can" is Canada token
# - DO NOT use "ca" as Canada token anywhere. CA is California.

REGION_SYNONYMS = {
    "usa": "us",
    "united states": "us",
    "canada": "can",
    "north america": "na",
    "worldwide": "global",
}

ALLOWED_REGION_TOKENS = {"us", "can", "na", "global"}

REGION_TOKENS = {
    "united states": "USA",
    "u.s.": "USA",
    "usa": "USA",
    "us": "USA",
    "us only": "USA",
    "north america": "NA",

    "canada": "CAN",
    "canadian": "CAN",

    "europe": "EU",
    "uk": "UK",
    "united kingdom": "UK",
    "emea": "EMEA",
    "apac": "APAC",
    "asia": "APAC",
    "australia": "APAC",
    "latam": "LATAM",
    "south america": "LATAM",
}

APPLICANT_REGION_TOKENS = {
    # US
    "united states of america": "us",
    "united states": "us",
    "u.s.": "us",
    "usa": "us",
    "us": "us",

    # Canada
    "canada": "can",
    "canadian": "can",
    "can": "can",

    # North America
    "north america": "na",

    # Global
    "worldwide": "global",
    "global": "global",
}

# ----------------------------
# Location chips
# ----------------------------

LOCATION_CHIP_SYNONYMS = {
    # Canada
    "CANADA": "CAN",
    "CANADIAN": "CAN",

    # US
    "UNITED STATES": "USA",
    "UNITED STATES OF AMERICA": "USA",
    "US": "USA",
    "U.S.": "USA",

    # DC long forms (if they ever appear as chips)
    "WASHINGTON D.C.": "DC",
    "DISTRICT OF COLUMBIA": "DC",
}

# All Canada province/territory abbreviations (uppercase)
CAN_PROV_ABBRS = {v.upper() for v in CAN_PROV_MAP.values()}
CAN_PROV_ABBRS_LOWER = {v.lower() for v in CAN_PROV_MAP.values()}
# Backward compatible aliases
CA_PROV_MAP = CAN_PROV_MAP
US_STATE_ABBRS = frozenset(_US_ABBR_TO_NAME.keys())

LOCALITY_HINTS = {
    "WA": {
        "any": [
            "seattle, wa",
            "seattle campus",
            "harborview medical center",
            "uw medical center",
            "south lake union",
            "renton, wa",
            "tacoma, wa",
            "bellevue, wa",
            "kirkland, wa",
            "redmond, wa",
        ],
        "tokens": [
            "seattle",
            "wa",
            "washington state",
            "bellevue",
            "redmond",
            "tacoma",
            "spokane",
            "everett",
            "kirkland",
            "renton",
        ],
    },
    "BC": {
        "any": [
            "vancouver, bc",
            "vancouver bc",
            "vancouver",
            "british columbia",
            "bc, can",
            "bc, canada",
            "burnaby",
            "richmond, bc",
            "surrey, bc",
            "victoria, bc",
            "coquitlam, bc",
        ],
        "tokens": ["vancouver", "british columbia"],  # optional: add "bc" if needed
    },
    "ON": {
        "any": [
            "toronto, on",
            "toronto on",
            "toronto",
            "ontario",
            "on, can",
            "on, canada",
            "mississauga",
            "ottawa",
            "kitchener",
            "waterloo",
            "hamilton",
            "london, on",
        ],
        "tokens": ["toronto", "ontario"],  # drop "on"
    },
}

# Provinces you actively use in gates
CAN_PROV_CHIPS = {"BC", "ON"}

# Macro chips you want to persist
MACRO_LOCATION_CHIPS = {"USA", "CAN", "PHL", "REMOTE"}

# Allowed chips includes:
# - all US states (incl DC)
# - province chips you use
# - macro chips
ALLOWED_LOCATION_CHIPS = set().union(
    US_STATE_CHIPS,
    CAN_PROV_CHIPS,
    MACRO_LOCATION_CHIPS,
)

# ----------------------------
# Misc mapping used elsewhere
# ----------------------------

PATH_LOC_MAP = {
    "seattle": "Seattle, WA",
    "seattle-non-campus": "Seattle, Non-Campus",
    "Seattle, Non-Campus": "Seattle, Non-Campus",
    "tacoma": "Tacoma, WA",
    "harborview": "Harborview Medical Center, Seattle, WA",
    "montlake": "Seattle, WA",
}
