# edsurge_jobs.py

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urlencode, urljoin

import requests


ALGOLIA_URL = "https://dizr5e00vc-dsn.algolia.net/1/indexes/*/queries"
ALGOLIA_APP_ID = "DIZR5E00VC"
ALGOLIA_API_KEY = "b469f980cc115e08e919c29907aa6420"

EDSURGE_BASE_URL = "https://www.edsurge.com"

ALGOLIA_INDEX = "EsEdsurgeJobSearchJS"

# Your choice from earlier
DEFAULT_QUERY = "Product"

# You can bump this if you want more than 25 at a time
DEFAULT_HITS_PER_PAGE = 50


@dataclass
class EdSurgeJob:
    source: str = "edsurge"
    title: str = ""
    company: str = ""
    location_raw: Optional[str] = None
    remote_flag: Optional[bool] = None
    category: Optional[str] = None
    role: Optional[str] = None
    job_type: Optional[str] = None
    experience_level: Optional[str] = None
    organization_type: Optional[str] = None
    posted_side: Optional[str] = None
    posted_at_utc: Optional[int] = None
    listing_url: Optional[str] = None
    apply_url: Optional[str] = None  # will use listing_url unless you later fetch detail page
    object_id: Optional[str] = None

    def to_row(self) -> dict:
        """
        Map into your generic row structure.
        Adjust the keys to match base_row_from_listing / CSV schema.
        """
        return {
            "source": self.source,
            "title": self.title,
            "company": self.company,
            "location_raw": self.location_raw,
            "remote_flag": self.remote_flag,
            "category": self.category,
            "role": self.role,
            "job_type": self.job_type,
            "experience_level": self.experience_level,
            "organization_type": self.organization_type,
            "posted_side": self.posted_side,
            "posted_at_utc": self.posted_at_utc,
            "listing_url": self.listing_url,
            "apply_url": self.apply_url,
            "edsurge_object_id": self.object_id,
        }


def _build_params(query: str, page: int = 0, hits_per_page: int = DEFAULT_HITS_PER_PAGE) -> dict:
    """
    Build the Algolia 'params' query string.
    This needs to be a URL encoded query string inside the JSON body.
    """
    params_dict = {
        "query": query,
        "hitsPerPage": hits_per_page,
        "page": page,
        # Ask for the same facets the UI uses
        "facets": '["remote","job_type","experience_level","organization_type","category"]',
        "tagFilters": "",
    }
    # urlencode will turn the dict into "query=Product&hitsPerPage=50&..."
    return {"requests": [{"indexName": ALGOLIA_INDEX, "params": urlencode(params_dict)}]}


def _fetch_page(session: requests.Session, query: str, page: int) -> dict:
    headers = {"Content-Type": "application/json"}
    params = {
        "x-algolia-application-id": ALGOLIA_APP_ID,
        "x-algolia-api-key": ALGOLIA_API_KEY,
    }
    payload = _build_params(query=query, page=page)

    resp = session.post(ALGOLIA_URL, params=params, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _parse_hits(result_json: dict) -> List[EdSurgeJob]:
    results = result_json.get("results", [])
    if not results:
        return []

    hits = results[0].get("hits", [])
    jobs: List[EdSurgeJob] = []

    for hit in hits:
        title = hit.get("title", "").strip()
        company = hit.get("organization_name", "").strip()

        # Location
        city_name = (hit.get("city_name") or "").strip()
        cities_list = hit.get("cities_list") or []
        location_raw = city_name or ", ".join(cities_list) or None

        listing_path = hit.get("link", "")
        listing_url = urljoin(EDSURGE_BASE_URL, listing_path)

        job = EdSurgeJob(
            title=title,
            company=company,
            location_raw=location_raw,
            remote_flag=hit.get("remote"),
            category=hit.get("category"),
            role=hit.get("role"),
            job_type=hit.get("job_type"),
            experience_level=hit.get("experience_level"),
            organization_type=hit.get("organization_type"),
            posted_side=hit.get("item_side_secondary"),
            posted_at_utc=hit.get("published_at_utc") or hit.get("created_at_utc"),
            listing_url=listing_url,
            apply_url=listing_url,  # you can swap this later if you follow the link to ATS
            object_id=hit.get("objectID"),
        )
        jobs.append(job)

    return jobs


def scrape_edsurge_jobs(
    query: str = DEFAULT_QUERY,
    max_pages: int = 3,
    session: Optional[requests.Session] = None,
) -> List[dict]:
    """
    Pull jobs from EdSurge for a given search term.
    For you, default is 'Product' with up to max_pages pages.
    """
    if session is None:
        session = requests.Session()

    all_rows: List[dict] = []
    page = 0

    while page < max_pages:
        data = _fetch_page(session, query=query, page=page)
        results = data.get("results", [])
        if not results:
            break

        meta = results[0]
        nb_pages = meta.get("nbPages", 1)

        jobs = _parse_hits(data)
        if not jobs:
            break

        all_rows.extend(job.to_row() for job in jobs)

        page += 1
        if page >= nb_pages:
            break

    return all_rows
