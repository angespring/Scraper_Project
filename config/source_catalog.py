"""Source-centric scraper metadata extracted from po_job_scraper.py.

This module is not wired into the scraper yet. It is a cleanup target for
future imports once po_job_scraper.py is ready to consume config modules.
"""

PLAYWRIGHT_DOMAINS = {
    "about.gitlab.com",
    "app.welcometothejungle.com",
    "ashbyhq.com",
    "builtin.com",
    "builtinseattle.com",
    "builtinvancouver.org",
    "dice.com",
    "edtech.com",
    "edtechjobs.io",
    "hubspot.com",
    "jobs.ashbyhq.com",
    "myworkdayjobs.com",
    "myworkdaysite.com",
    "remoteok.com",
    "us.welcometothejungle.com",
    "wd1.myworkdayjobs.com",
    "wd5.myworkdaysite.com",
    "wellfound.com",
    "welcometothejungle.com",
    "workingnomads.com",
    "ycombinator.com",
    "zapier.com",
    "www.builtin.com",
    "www.builtinseattle.com",
    "www.builtinvancouver.org",
    "www.dice.com",
    "www.edtech.com",
    "www.edtechjobs.io",
    "www.hubspot.com",
    "www.remoteok.com",
    "www.wellfound.com",
    "www.welcometothejungle.com",
    "www.workingnomads.com",
    "www.ycombinator.com",
}


KNOWN_SOURCE_LABELS = {
    "builtin.com": "Built In",
    "www.builtin.com": "Built In",
    "builtinseattle.com": "Built In Seattle",
    "www.builtinseattle.com": "Built In Seattle",
    "builtinvancouver.org": "Built In Vancouver",
    "www.builtinvancouver.org": "Built In Vancouver",
    "remoteok.com": "Remote OK",
    "wellfound.com": "Wellfound",
    "welcometothejungle.com": "Welcome to the Jungle",
}


CAREER_PAGES_BY_DOMAIN = {
    "about.gitlab.com": [
        "https://about.gitlab.com/jobs/all-jobs/",
    ],
    "zapier.com": [
        "https://zapier.com/jobs#job-openings",
    ],
}


REMOTE_WHITELIST_DOMAINS = [
    "remoteok.com",
    "weworkremotely.com",
]


SOURCE_METADATA = {
    "builtin.com": {"label": "Built In", "needs_playwright": True},
    "builtinseattle.com": {"label": "Built In Seattle", "needs_playwright": True},
    "builtinvancouver.org": {"label": "Built In Vancouver", "needs_playwright": True},
    "dice.com": {"label": "Dice", "needs_playwright": True},
    "hubspot.com": {"label": "HubSpot", "needs_playwright": True},
    "remoteok.com": {"label": "Remote OK", "needs_playwright": True, "remote_default": True},
    "wellfound.com": {"label": "Wellfound", "needs_playwright": True},
    "welcometothejungle.com": {"label": "Welcome to the Jungle", "needs_playwright": True},
    "workingnomads.com": {"label": "Working Nomads", "needs_playwright": True},
    "ycombinator.com": {"label": "Y Combinator", "needs_playwright": True},
    "about.gitlab.com": {"label": "GitLab Careers", "needs_playwright": True},
    "zapier.com": {"label": "Zapier Careers", "needs_playwright": True},
    "themuse.com": {"label": "The Muse", "needs_playwright": False},
    "main.hercjobs.org": {"label": "HERC", "needs_playwright": False},
    "remotive.com": {"label": "Remotive", "needs_playwright": False},
    "simplyhired.com": {"label": "SimplyHired", "needs_playwright": False},
    "wd5.myworkdaysite.com": {"label": "Workday", "needs_playwright": True},
    "jobs.ashbyhq.com": {"label": "Ashby", "needs_playwright": True},
    "workatastartup.com": {"label": "Work at a Startup", "needs_playwright": False},
    "edtech.com": {"label": "EdTech", "needs_playwright": True},
    "edtechjobs.io": {"label": "EdTech Jobs", "needs_playwright": True},
    "nodesk.co": {"label": "NoDesk", "needs_playwright": False},
    "weworkremotely.com": {"label": "We Work Remotely", "needs_playwright": False, "remote_default": True},
}


SIMPLYHIRED_BASE_URL = "https://www.simplyhired.com"
