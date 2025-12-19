#!/usr/bin/env bash
set -euo pipefail

# Install Python deps
python3 -m pip install -r requirements.txt

# Install a browser Playwright can drive (first time only; ok if it no-ops later)
python3 -m playwright install --with-deps chromium >/dev/null 2>&1 || true

# Run the scraper
python3 po_job_scraper.py
