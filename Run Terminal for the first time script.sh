# from your job-scraper folder
cd ~/job-scraper

# 1) create & activate a venv
python3 -m venv .venv
source .venv/bin/activate

# 2) upgrade pip inside the venv
python -m pip install -U pip

# 3) install dependencies the scraper needs
python -m pip install requests requests-cache beautifulsoup4 python-dateutil \
                       playwright gspread google-auth

# 4) (optional) install a browser for Playwright features (YC, etc.)
python -m playwright install chromium

# 5) run your script
python3 po_job_scraper.py




######################################################################################
#
#•Normal run (setup if needed, then scrape; logs to logs/ run_YYYYMMDD_HHMMSS.log):
#            bash                                                                                                                                                     • Copy code
#            ~/job-scraper/run_scraper.sh
#
#
#• Rebuild the environment from scratch:
#            bash                                                                                                                                                     • Copy code
#            ~/job-scraper/run_scraper.sh --rebuild
#
#
#• Skip installing the Playwright browser (saves a couple seconds):
#            bash                                                                                                                                                     • Copy code
#            ~/job-scraper/run_scraper.sh —-no-browser
#
#
#• Quieter setup messages:
#            bash                                                                                                                                                     • Copy code
#            ~/job-scraper/run_scraper.sh --quiet
#
######################################################################################