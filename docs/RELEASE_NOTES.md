# Release Notes

This log summarizes notable milestones for the Product Job Scraper so you can
showcase project momentum with hiring managers or stakeholders.

To add a new entry automatically, run:

```bash
python tools/generate_release_notes.py \
  --from-ref origin/main \
  --title "Example Feature Release" \
  --overview "One-line story about the release."
```

You can tweak `--since`, `--append`, or `--print-only` to experiment without
touching the file.

---

## 2025-11-17 – Smoke-mode UX & Release Notes tooling

### Overview
Smoke test runs now demonstrate a single target site end-to-end and the repo
ships with a release-notes generator so you can publish PM-friendly updates with
one command.

### Highlights
- Smoke-mode limits discovery to the first listing site and caps processing to 20 jobs, preventing noise from secondary boards.
- Added a reusable `tools/generate_release_notes.py` helper plus documentation so stakeholders can pull polished notes from git history.

### Commit Details
| Commit | Description | Author | Date |
| --- | --- | --- | --- |
| (pending) | Work in progress – commit to populate via `generate_release_notes.py`. | Ange | 2025-11-17 |

