# Product Job Scraper

Rules-driven scraper that finds remote Product roles and writes to Google Sheets.

Built by Angela Spring — https://vanspring.me

## Quick start

```bash
git clone https://github.com/angespring/Scraper_Project.git
cd Scraper_Project

# create local env file from example and fill in your values
cp .env.example .env

# first run (creates venv, installs deps, downloads chromium, runs the scraper)
./run.sh
