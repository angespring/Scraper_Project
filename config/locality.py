# config/locality.py

import json
import os
from typing import Any, Dict, List, Optional

_LOCALITY_HINTS_CACHE: Optional[Dict[str, Any]] = None

def load_locality_hints() -> Dict[str, Any]:
    global _LOCALITY_HINTS_CACHE
    if _LOCALITY_HINTS_CACHE is not None:
        return _LOCALITY_HINTS_CACHE

    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, "locality_hints.json")

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("locality_hints.json root must be an object")
        _LOCALITY_HINTS_CACHE = data
    except Exception:
        _LOCALITY_HINTS_CACHE = {}

    return _LOCALITY_HINTS_CACHE


def matches_locality(region_key: str, loc: str, chips: List[str], job_url: str = "") -> bool:
    cfg = load_locality_hints().get(region_key, {}) or {}
    phrases = [str(x).lower() for x in cfg.get("phrases", []) if str(x).strip()]
    tokens = [str(x).lower() for x in cfg.get("tokens", []) if str(x).strip()]

    low_loc = (loc or "").lower()
    chips_text = "|".join(chips or []).lower()
    low_url = (job_url or "").lower()
    haystacks = (low_loc, chips_text, low_url)

    for p in phrases:
        if any(p in h for h in haystacks):
            return True

    for t in tokens:
        if any(t in h for h in haystacks):
            return True

    return False
