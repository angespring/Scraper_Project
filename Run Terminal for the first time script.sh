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
python po_job_scraper.py
