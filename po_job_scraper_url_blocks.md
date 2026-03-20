# URL And Domain Blocks Extracted From `po_job_scraper.py`

Reference-only copy of the top-level config blocks in `po_job_scraper.py` that contain URLs, domains, or URL-like hostnames.

Original source file: `po_job_scraper.py`

Reorganized Python config files now exist in `config/`:

- `config/blocked_sources.py`
- `config/source_catalog.py`
- `config/search_seed_urls.py`

Those files are the cleaner, domain-first structure to use for future migration. This markdown file remains the verbatim source snapshot.

## `BLOCKED_DOMAINS` from line 811

```python
BLOCKED_DOMAINS = {
    "glassdoor.com", "www.glassdoor.com",
    "indeed.com", "www.indeed.com",
    "careerbuilder.com", "www.careerbuilder.com",
    "producthunt.com", "www.producthunt.com",
    "jobspresso.co", "www.jobspresso.co",
}
```

## `PLAYWRIGHT_DOMAINS` from line 7281

```python
PLAYWRIGHT_DOMAINS = {
    "edtech.com", "www.edtech.com",
    "edtechjobs.io/", "www.edtechjobs.io",
    "builtin.com", "www.builtin.com",
    "builtinseattle.com", "www.builtinseattle.com",
    "remoteok.com", "www.remoteok.com",
    "builtinvancouver.org", "www.builtinvancouver.org",
    "wellfound.com", "www.wellfound.com",
    "welcometothejungle.com", "www.welcometothejungle.com",
    "app.welcometothejungle.com", "us.welcometothejungle.com",
    "workingnomads.com", "www.workingnomads.com",
    # JS heavy boards that need Playwright
    "dice.com", "www.dice.com",
    "myworkdayjobs.com", "wd1.myworkdayjobs.com", "myworkdaysite.com",
    "wd5.myworkdaysite.com", "ashbyhq.com", "jobs.ashbyhq.com",
}
```

## `PLAYWRIGHT_DOMAINS.update(...)` from line 7299

```python
PLAYWRIGHT_DOMAINS.update({
    "www.hubspot.com",
    "hubspot.com",
    "www.ycombinator.com",
    "ycombinator.com",
    "about.gitlab.com",
    "zapier.com",
})
```

## `KNOWN` from line 7313

```python
KNOWN = {
    # …keep existing…
    "remoteok.com": "Remote OK",
    "builtin.com": "Built In",
    "www.builtin.com": "Built In",
    "builtinseattle.com": "Built In Seattle",
    "www.builtinseattle.com": "Built In Seattle",
    "wellfound.com": "Wellfound",
    "builtinvancouver.org": "Built In Vancouver",
    "www.builtinvancouver.org": "Built In Vancouver",
    "welcometothejungle.com": "Welcome to the Jungle",
}
```

## `USER_AGENT` and `GS_SHEET_URL` from lines 7338 and 7351

```python
USER_AGENT = "AngeJobScraper/1.0 (+https://linkedin.com/in/angespring)"
GS_SHEET_URL = "https://docs.google.com/spreadsheets/d/1UloVHEsBxvMJ3WeQ8XkHvtIrL1cQ2CiyD50bsOb-Up8/edit?gid=1531552984#gid=1531552984"
```

## `BLOCKED_URL_PREFIXES` from line 7387

```python
BLOCKED_URL_PREFIXES = [
    "https://www.washington.edu/jobs",
    "https://washington.edu/jobs",  # just in case
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
    "https://weworkremotely.com/remote-jobs/new",
    "https://weworkremotely.com/remote-jobs/new?utm_content=post-job-cta&utm_source=wwr-accounts-nav-mobile",
    "https://dhigroupinc.com/careers/default.aspx",
    "https://main.hercjobs.org/jobs/saved",
    "https://main.hercjobs.org/jobs/dualsearch",
    "https://edtechjobs.io/jobs/product-management",
    "https://edtechjobs.io/jobs/contract",
    "https://edtechjobs.io/jobs/higher-ed",
    "https://edtechjobs.io/jobs/leadership",
    "https://edtechjobs.io/jobs/technical-leadership",
    "https://edtechjobs.io/jobs/remote-position"
    "https://edtechjobs.io/jobs/ai-driven-products",
    "https://edtechjobs.io/jobs/educational-innovation"
    "https://edtechjobs.io/jobs/technology-leadership",
    "https://edtechjobs.io/jobs/digital-transformation",
    "https://edtechjobs.io/jobs/artificial-intelligence",
    "https://edtechjobs.io/jobs/design-leadership",
    "https://edtechjobs.io/jobs/early-childhood-education",
    "https://edtechjobs.io/jobs/saas-leadership",
    "https://edtechjobs.io/jobs/stakeholder-management",
]
```

## `STARTING_PAGES` from line 7428

```python
STARTING_PAGES = [

    # Preferred SMOKE target: The Muse (keeps lightweight, server-rendered HTML)
    #"https://www.themuse.com/jobs?categories=product&location=remote",



    "https://main.hercjobs.org/jobs?keywords=Business+Analyst&place=canada%2Cnationwide",

    # Product Manager / Product Owner
    "https://www.themuse.com/search/location/remote-flexible/keyword/product+manager",
    "https://www.themuse.com/jobs?categories=information-technology&location=remote&query=product%20manager",
    "https://www.builtin.com/jobs?search=product%20manager&remote=true",
    "https://www.builtin.com/jobs?search=associate%20product%20manager&remote=true",
    "https://www.builtin.com/jobs?search=product%20owner&remote=true",
    "https://www.builtinseattle.com/jobs?search=product%20manager&remote=true",
    "https://www.builtinseattle.com/jobs?search=associate%20product%20manager&remote=true",
    "https://www.builtinseattle.com/jobs?search=product%20owner&remote=true",
    "https://builtinvancouver.org/jobs?search=Product+Manager",
    "https://builtinvancouver.org/jobs?search=associate%20product%20manager&remote=true",
    "https://builtinvancouver.org/jobs?search=Product+Owner",
    "https://www.simplyhired.com/search?q=product+manager&l=remote",
    "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=product+manager",
    "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=product+owner",
    "https://builtinvancouver.org/jobs/product-management/product-manager",
    "https://builtinvancouver.org/jobs/product-management/product-owner",
    "https://jobright.ai/jobs/search?value=product+owner",
    "https://jobright.ai/jobs/search?value=product+manager",




    # HubSpot (server-rendered listings; crawl like a board)
    #"https://www.hubspot.com/careers/jobs?q=product&;page=1",
    # If you want a tighter filter and HubSpot supports it, you can also try:
    # "https://www.hubspot.com/careers/jobs?page=1&functions=product&location=Remote%20-%20USA",


    # University of Washington (Workday) — focused keyword searches
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=business%20analyst",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=systems%20analyst",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=business%20systems%20analyst",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=product%20manager",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=product%20owner",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=scrum%20master",
    "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=release%20train%20engineer",



    # University of British Columbia (Workday) — focused keyword searches
    #"https://ubc.wd10.myworkdayjobs.com/en-US/ubcstaffjobs/jobs?q=business+analyst",


    # Business Analyst / Systems Analyst
    "https://www.themuse.com/search/location/remote-flexible/keyword/business-analyst",
    "https://www.themuse.com/jobs?categories=information-technology&location=remote&query=business%20analyst",
    "https://www.themuse.com/jobs?categories=information-technology&location=remote&query=systems%20analyst",
    "https://www.builtin.com/jobs?search=business%20analyst&remote=true",
    "https://www.builtin.com/jobs?search=systems%20analyst&remote=true",
    "https://www.builtin.com/jobs?search=agile&remote=true",
    "https://www.builtinseattle.com/jobs?search=agile&remote=true",
    "https://www.builtinseattle.com/jobs?search=business%20analyst&remote=true",
    "https://www.builtinseattle.com/jobs?search=systems%20analyst&remote=true",
    "https://builtinvancouver.org/jobs?search=Business+Analyst",
    "https://builtinvancouver.org/jobs?search=Systems+Analyst",
    "https://www.simplyhired.com/search?q=systems+analyst&l=remote",
    "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=systems+analyst",
    "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=business+analyst",
    "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=business+systems+analyst",



    # Scrum Master / RTE
    "https://remotive.com/remote-jobs/product?search=scrum%20master",
    "https://www.builtin.com/jobs?search=scrum%20master&remote=true",
    "https://www.builtinseattle.com/jobs?search=scrum%20master&remote=true",
    "https://builtinvancouver.org/jobs?search=Scrum+Master",
    "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=scrum+master",
    "https://jobright.ai/jobs/search?value=agile",
    "https://jobright.ai/jobs/search?value=scrum+master",
    "https://jobright.ai/jobs/search?value=release+train+engineer",





    # Remote-friendly product boards that serve real HTML
    "https://remotive.com/remote-jobs/product?locations=Canada%2BUSA",
    "https://weworkremotely.com/categories/remote-product-jobs",
    "https://nodesk.co/remote-jobs/product/",
    "https://www.workingnomads.com/jobs?tag=product",
    "https://www.workingnomads.com/remote-product-jobs",
    "https://www.simplyhired.com/search?q=product+owner&l=remote",
    "https://www.edtech.com/jobs/fully-remote-jobs?Cat=Product%20Development",
    "https://www.edtech.com/jobs/fully-remote-jobs?Cat=Information%20Technology",
    "https://www.edtech.com/jobs/fully-remote-jobs?Cat=Operations",
    "https://edtechjobs.io/jobs/product-management?location=Remote",
    "https://edtechjobs.io/jobs/business-analysis?location=Remote",


    # Ascensus (Workday tenant) — focused role searches
    # "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=product%20manager",                   # 20251227- removed to lesson the amount of jobs scraped can add back if desired
    # "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=product%20owner",                     # 20251227- removed to lesson the amount of jobs scraped can add back if desired
    # "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=business%20analyst",                  # 20251227- removed to lesson the amount of jobs scraped can add back if desired
    # "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=systems%20analyst",                   # 20251227- removed to lesson the amount of jobs scraped can add back if desired
    # "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=scrum%20master",                      # 20251227- removed to lesson the amount of jobs scraped can add back if desired
    # optional: release train engineer / RTE
    # "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers/search?q=release%20train%20engineer",   # 20251227- removed to lesson the amount of jobs scraped can add back if desired

    "https://jobs.ashbyhq.com/zapier",

    # The Muse works well (canonical filtered URL lives at the top of STARTING_PAGES)
    "https://www.themuse.com/jobs?categories=product&location=remote",
    "https://www.themuse.com/jobs?categories=management&location=remote",

    # YC jobs (Playwright-friendly)
    "https://www.ycombinator.com/jobs/role/product-manager",
    "https://www.workatastartup.com/companies?role=product",
    "https://www.workatastartup.com/companies?role=agile",

    # Remote OK
    "https://remoteok.com/?location=CA,US,region_NA",

    # Built In (JS-heavy → Playwright)
    "https://www.builtin.com/jobs?search=product%20manager&remote=true",
    "https://www.builtin.com/jobs?search=product%20owner&remote=true",
    "https://builtinvancouver.org/jobs?search=Product+Manager",
    "https://builtinvancouver.org/jobs?search=Product+Owner",

    # HubSpot (server-rendered listings; crawl like a board)
    "https://www.hubspot.com/careers/jobs?q=product&;page=1",
    # If you want a tighter filter and HubSpot supports it, you can also try:
    # "https://www.hubspot.com/careers/jobs?page=1&functions=product&location=Remote%20-%20USA",


    # Wellfound (AngelList Talent) (JS-heavy → Playwright)
    "https://wellfound.com/role/r/product-manager",

    # Welcome to the Jungle (JS-heavy → Playwright)
    "https://www.welcometothejungle.com/en/jobs?query=product%20manager&remote=true",
    "https://app.welcometothejungle.com/companies/12Twenty#jobs-section"
    "https://app.welcometothejungle.com/companies/Microsoft#jobs-section"
    "https://app.welcometothejungle.com/companies/Google#jobs-section"
    "https://app.welcometothejungle.com/companies/Adobe#jobs-section"
    "https://app.welcometothejungle.com/companies/Asana#jobs-section"
    "https://app.welcometothejungle.com/companies/Amazon#jobs-section"
    "https://app.welcometothejungle.com/companies/Airtable#jobs-section"
    "https://app.welcometothejungle.com/companies/Beam-Benefits#jobs-section"
    "https://app.welcometothejungle.com/companies/Chime-Bank#jobs-section"
    "https://app.welcometothejungle.com/companies/Clari#jobs-section"
    "https://app.welcometothejungle.com/companies/Confluent#jobs-section"
    "https://app.welcometothejungle.com/companies/DataDog#jobs-section"
    "https://app.welcometothejungle.com/companies/Dataminr#jobs-section"
    "https://app.welcometothejungle.com/companies/Expensify#jobs-section"
    "https://app.welcometothejungle.com/companies/Figma#jobs-section"
    "https://app.welcometothejungle.com/companies/Gong-io#jobs-section"
    "https://app.welcometothejungle.com/companies/HashiCorp#jobs-section"
    "https://app.welcometothejungle.com/companies/HubSpot#jobs-section"
    "https://app.welcometothejungle.com/companies/Looker#jobs-section"
    "https://app.welcometothejungle.com/companies/MaintainX#jobs-section"
    "https://app.welcometothejungle.com/companies/Notion#jobs-section"
    "https://app.welcometothejungle.com/companies/Outreach#jobs-section"
    "https://app.welcometothejungle.com/companies/PagerDuty#jobs-section"
    "https://app.welcometothejungle.com/companies/Segment#jobs-section"
    "https://app.welcometothejungle.com/companies/Smartsheet#jobs-section"
    "https://app.welcometothejungle.com/companies/Stripe#jobs-section"
    "https://app.welcometothejungle.com/companies/Top-Hat#jobs-section"
    "https://app.welcometothejungle.com/companies/TripActions#jobs-section"
    "https://app.welcometothejungle.com/companies/UiPath#jobs-section"
    "https://app.welcometothejungle.com/companies/Vetcove#jobs-section"
    "https://app.welcometothejungle.com/companies/Zoom#jobs-section"
    "https://app.welcometothejungle.com/companies/Metabase#jobs-section"
    "https://app.welcometothejungle.com/api/jobs?query=product%20owner&locations=remote",
    "https://app.welcometothejungle.com/api/jobs?query=product",
]
```

## `CAREER_PAGES` from line 7631

```python
CAREER_PAGES = [
    "https://about.gitlab.com/jobs/all-jobs/",                                      # Greenhouse → boards.greenhouse.io/gitlab
    "https://zapier.com/jobs#job-openings",                                         # Lever → jobs.lever.co/zapier
    # add more any time
]
```

## `SIMPLYHIRED_BASE` and `SOURCE_WHITELIST_REMOTE` from lines 12149 and 12155

```python
SIMPLYHIRED_BASE = "https://www.simplyhired.com"
SOURCE_WHITELIST_REMOTE = ["weworkremotely.com", "remoteok.com"]
```
