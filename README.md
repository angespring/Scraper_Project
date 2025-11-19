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

## Release notes workflow

Use the helper script to generate stakeholder-ready notes straight from git history:

```bash
python tools/generate_release_notes.py \
  --from-ref origin/main \
  --title "Smoke-mode UX polish" \
  --overview "Focused smoke runs on the first listing site plus release-notes automation."
```

By default the script inserts the new section at the top of `docs/RELEASE_NOTES.md`.
Add `--print-only` to preview the Markdown without touching the file, or swap in
`--append` if you prefer chronological ordering.
