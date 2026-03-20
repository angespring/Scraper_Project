"""Seed URLs grouped by source domain.

This module is not wired into the scraper yet. It is a domain-first cleanup of
the STARTING_PAGES list extracted from po_job_scraper.py.
"""

SEARCH_SEED_URLS_BY_DOMAIN = {
    "app.welcometothejungle.com": [
        "https://app.welcometothejungle.com/companies/12Twenty#jobs-section",
        "https://app.welcometothejungle.com/companies/Microsoft#jobs-section",
        "https://app.welcometothejungle.com/companies/Google#jobs-section",
        "https://app.welcometothejungle.com/companies/Adobe#jobs-section",
        "https://app.welcometothejungle.com/companies/Asana#jobs-section",
        "https://app.welcometothejungle.com/companies/Amazon#jobs-section",
        "https://app.welcometothejungle.com/companies/Airtable#jobs-section",
        "https://app.welcometothejungle.com/companies/Beam-Benefits#jobs-section",
        "https://app.welcometothejungle.com/companies/Chime-Bank#jobs-section",
        "https://app.welcometothejungle.com/companies/Clari#jobs-section",
        "https://app.welcometothejungle.com/companies/Confluent#jobs-section",
        "https://app.welcometothejungle.com/companies/DataDog#jobs-section",
        "https://app.welcometothejungle.com/companies/Dataminr#jobs-section",
        "https://app.welcometothejungle.com/companies/Expensify#jobs-section",
        "https://app.welcometothejungle.com/companies/Figma#jobs-section",
        "https://app.welcometothejungle.com/companies/Gong-io#jobs-section",
        "https://app.welcometothejungle.com/companies/HashiCorp#jobs-section",
        "https://app.welcometothejungle.com/companies/HubSpot#jobs-section",
        "https://app.welcometothejungle.com/companies/Looker#jobs-section",
        "https://app.welcometothejungle.com/companies/MaintainX#jobs-section",
        "https://app.welcometothejungle.com/companies/Notion#jobs-section",
        "https://app.welcometothejungle.com/companies/Outreach#jobs-section",
        "https://app.welcometothejungle.com/companies/PagerDuty#jobs-section",
        "https://app.welcometothejungle.com/companies/Segment#jobs-section",
        "https://app.welcometothejungle.com/companies/Smartsheet#jobs-section",
        "https://app.welcometothejungle.com/companies/Stripe#jobs-section",
        "https://app.welcometothejungle.com/companies/Top-Hat#jobs-section",
        "https://app.welcometothejungle.com/companies/TripActions#jobs-section",
        "https://app.welcometothejungle.com/companies/UiPath#jobs-section",
        "https://app.welcometothejungle.com/companies/Vetcove#jobs-section",
        "https://app.welcometothejungle.com/companies/Zoom#jobs-section",
        "https://app.welcometothejungle.com/companies/Metabase#jobs-section",
        "https://app.welcometothejungle.com/api/jobs?query=product%20owner&locations=remote",
        "https://app.welcometothejungle.com/api/jobs?query=product",
    ],
    "builtin.com": [
        "https://www.builtin.com/jobs?search=product%20manager&remote=true",
        "https://www.builtin.com/jobs?search=associate%20product%20manager&remote=true",
        "https://www.builtin.com/jobs?search=product%20owner&remote=true",
        "https://www.builtin.com/jobs?search=business%20analyst&remote=true",
        "https://www.builtin.com/jobs?search=systems%20analyst&remote=true",
        "https://www.builtin.com/jobs?search=agile&remote=true",
        "https://www.builtin.com/jobs?search=scrum%20master&remote=true",
    ],
    "builtinseattle.com": [
        "https://www.builtinseattle.com/jobs?search=product%20manager&remote=true",
        "https://www.builtinseattle.com/jobs?search=associate%20product%20manager&remote=true",
        "https://www.builtinseattle.com/jobs?search=product%20owner&remote=true",
        "https://www.builtinseattle.com/jobs?search=agile&remote=true",
        "https://www.builtinseattle.com/jobs?search=business%20analyst&remote=true",
        "https://www.builtinseattle.com/jobs?search=systems%20analyst&remote=true",
        "https://www.builtinseattle.com/jobs?search=scrum%20master&remote=true",
    ],
    "builtinvancouver.org": [
        "https://builtinvancouver.org/jobs?search=Product+Manager",
        "https://builtinvancouver.org/jobs?search=associate%20product%20manager&remote=true",
        "https://builtinvancouver.org/jobs?search=Product+Owner",
        "https://builtinvancouver.org/jobs/product-management/product-manager",
        "https://builtinvancouver.org/jobs/product-management/product-owner",
        "https://builtinvancouver.org/jobs?search=Business+Analyst",
        "https://builtinvancouver.org/jobs?search=Systems+Analyst",
        "https://builtinvancouver.org/jobs?search=Scrum+Master",
    ],
    "dice.com": [
        "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=product+manager",
        "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=product+owner",
        "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=systems+analyst",
        "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=business+analyst",
        "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=business+systems+analyst",
        "https://www.dice.com/jobs?filters.workplaceTypes=Remote&q=scrum+master",
    ],
    "edtech.com": [
        "https://www.edtech.com/jobs/fully-remote-jobs?Cat=Product%20Development",
        "https://www.edtech.com/jobs/fully-remote-jobs?Cat=Information%20Technology",
        "https://www.edtech.com/jobs/fully-remote-jobs?Cat=Operations",
    ],
    "edtechjobs.io": [
        "https://edtechjobs.io/jobs/product-management?location=Remote",
        "https://edtechjobs.io/jobs/business-analysis?location=Remote",
    ],
    "hubspot.com": [
        "https://www.hubspot.com/careers/jobs?q=product&;page=1",
    ],
    "jobright.ai": [
        "https://jobright.ai/jobs/search?value=product+owner",
        "https://jobright.ai/jobs/search?value=product+manager",
        "https://jobright.ai/jobs/search?value=agile",
        "https://jobright.ai/jobs/search?value=scrum+master",
        "https://jobright.ai/jobs/search?value=release+train+engineer",
    ],
    "jobs.ashbyhq.com": [
        "https://jobs.ashbyhq.com/zapier",
    ],
    "main.hercjobs.org": [
        "https://main.hercjobs.org/jobs?keywords=Business+Analyst&place=canada%2Cnationwide",
    ],
    "nodesk.co": [
        "https://nodesk.co/remote-jobs/product/",
    ],
    "remoteok.com": [
        "https://remoteok.com/?location=CA,US,region_NA",
    ],
    "remotive.com": [
        "https://remotive.com/remote-jobs/product?search=scrum%20master",
        "https://remotive.com/remote-jobs/product?locations=Canada%2BUSA",
    ],
    "simplyhired.com": [
        "https://www.simplyhired.com/search?q=product+manager&l=remote",
        "https://www.simplyhired.com/search?q=systems+analyst&l=remote",
        "https://www.simplyhired.com/search?q=product+owner&l=remote",
    ],
    "themuse.com": [
        "https://www.themuse.com/search/location/remote-flexible/keyword/product+manager",
        "https://www.themuse.com/jobs?categories=information-technology&location=remote&query=product%20manager",
        "https://www.themuse.com/search/location/remote-flexible/keyword/business-analyst",
        "https://www.themuse.com/jobs?categories=information-technology&location=remote&query=business%20analyst",
        "https://www.themuse.com/jobs?categories=information-technology&location=remote&query=systems%20analyst",
        "https://www.themuse.com/jobs?categories=product&location=remote",
        "https://www.themuse.com/jobs?categories=management&location=remote",
    ],
    "wd5.myworkdaysite.com": [
        "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=business%20analyst",
        "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=systems%20analyst",
        "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=business%20systems%20analyst",
        "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=product%20manager",
        "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=product%20owner",
        "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=scrum%20master",
        "https://wd5.myworkdaysite.com/recruiting/uw/UWHires?q=release%20train%20engineer",
    ],
    "wellfound.com": [
        "https://wellfound.com/role/r/product-manager",
    ],
    "welcometothejungle.com": [
        "https://www.welcometothejungle.com/en/jobs?query=product%20manager&remote=true",
    ],
    "weworkremotely.com": [
        "https://weworkremotely.com/categories/remote-product-jobs",
    ],
    "workatastartup.com": [
        "https://www.workatastartup.com/companies?role=product",
        "https://www.workatastartup.com/companies?role=agile",
    ],
    "workingnomads.com": [
        "https://www.workingnomads.com/jobs?tag=product",
        "https://www.workingnomads.com/remote-product-jobs",
    ],
    "ycombinator.com": [
        "https://www.ycombinator.com/jobs/role/product-manager",
    ],
}


COMMENTED_OUT_SEED_URLS_BY_DOMAIN = {
    "ascensushr.wd1.myworkdayjobs.com": [
        "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=product%20manager",
        "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=product%20owner",
        "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=business%20analyst",
        "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=systems%20analyst",
        "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers?q=scrum%20master",
        "https://ascensushr.wd1.myworkdayjobs.com/ascensuscareers/search?q=release%20train%20engineer",
    ],
    "hubspot.com": [
        "https://www.hubspot.com/careers/jobs?q=product&;page=1",
        "https://www.hubspot.com/careers/jobs?page=1&functions=product&location=Remote%20-%20USA",
    ],
    "themuse.com": [
        "https://www.themuse.com/jobs?categories=product&location=remote",
    ],
    "ubc.wd10.myworkdayjobs.com": [
        "https://ubc.wd10.myworkdayjobs.com/en-US/ubcstaffjobs/jobs?q=business+analyst",
    ],
}


SEARCH_SEED_URLS = [
    url
    for domain_urls in SEARCH_SEED_URLS_BY_DOMAIN.values()
    for url in domain_urls
]
