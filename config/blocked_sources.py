"""Blocked domains and URL prefixes extracted from po_job_scraper.py.

This module is not wired into the scraper yet. It exists so the source and
blocking rules can be reviewed and managed separately before any code change
in po_job_scraper.py.
"""

BLOCKED_DOMAINS = {
    "careerbuilder.com",
    "glassdoor.com",
    "indeed.com",
    "jobspresso.co",
    "producthunt.com",
    "www.careerbuilder.com",
    "www.glassdoor.com",
    "www.indeed.com",
    "www.producthunt.com",
}


BLOCKED_URL_PREFIXES_BY_DOMAIN = {
    "dhigroupinc.com": [
        "https://dhigroupinc.com/careers/default.aspx",
    ],
    "edtechjobs.io": [
        "https://edtechjobs.io/jobs/product-management",
        "https://edtechjobs.io/jobs/contract",
        "https://edtechjobs.io/jobs/higher-ed",
        "https://edtechjobs.io/jobs/leadership",
        "https://edtechjobs.io/jobs/technical-leadership",
        "https://edtechjobs.io/jobs/remote-position",
        "https://edtechjobs.io/jobs/ai-driven-products",
        "https://edtechjobs.io/jobs/educational-innovation",
        "https://edtechjobs.io/jobs/technology-leadership",
        "https://edtechjobs.io/jobs/digital-transformation",
        "https://edtechjobs.io/jobs/artificial-intelligence",
        "https://edtechjobs.io/jobs/design-leadership",
        "https://edtechjobs.io/jobs/early-childhood-education",
        "https://edtechjobs.io/jobs/saas-leadership",
        "https://edtechjobs.io/jobs/stakeholder-management",
    ],
    "main.hercjobs.org": [
        "https://main.hercjobs.org/jobs/saved",
        "https://main.hercjobs.org/jobs/dualsearch",
    ],
    "nodesk.co": [
        "https://nodesk.co/remote-jobs/us",
        "https://nodesk.co/remote-jobs/other",
        "https://nodesk.co/remote-jobs/asia",
        "https://nodesk.co/remote-jobs/operations",
        "https://nodesk.co/remote-jobs/full-time",
        "https://nodesk.co/remote-jobs/uk",
        "https://nodesk.co/remote-jobs/product-marketing",
        "https://nodesk.co/remote-jobs/sql",
        "https://nodesk.co/remote-jobs/customer-support",
        "https://nodesk.co/remote-jobs/new",
        "https://nodesk.co/remote-jobs/europe",
        "https://nodesk.co/remote-jobs/part-time",
        "https://nodesk.co/remote-jobs/product-manager",
        "https://nodesk.co/remote-jobs/data/",
        "https://nodesk.co/remote-jobs/ai/",
    ],
    "washington.edu": [
        "https://www.washington.edu/jobs",
        "https://washington.edu/jobs",
    ],
    "weworkremotely.com": [
        "https://weworkremotely.com/remote-jobs/new",
        "https://weworkremotely.com/remote-jobs/new?utm_content=post-job-cta&utm_source=wwr-accounts-nav-mobile",
    ],
}


BLOCKED_URL_PREFIXES = [
    url
    for domain_urls in BLOCKED_URL_PREFIXES_BY_DOMAIN.values()
    for url in domain_urls
]
