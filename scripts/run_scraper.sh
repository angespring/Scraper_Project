#!/usr/bin/env bash
set -Eeuo pipefail

# --- Settings ---------------------------------------------------------------
ROOT="${HOME}/job-scraper"
VENV="${ROOT}/.venv"
PY="${VENV}/bin/python"
PIP="${VENV}/bin/pip"
LOGDIR="${ROOT}/logs"
STAMP="$(date +"%Y%m%d_%H%M%S")"
LOG="${LOGDIR}/run_${STAMP}.log"

# Flags:
#   --rebuild       Recreate the venv from scratch
#   --no-browser    Skip Playwright browser install
#   --quiet         Less chatter during setup
REBUILD=false
INSTALL_BROWSER=true
QUIET=false
for a in "$@"; do
  case "$a" in
    --rebuild)      REBUILD=true ;;
    --no-browser)   INSTALL_BROWSER=false ;;
    --quiet)        QUIET=true ;;
    *) echo "Unknown option: $a" >&2; exit 2;;
  case
done

# --- Helpers ----------------------------------------------------------------
say() { $QUIET || echo -e "$@"; }
time_human() { perl -e 'print int($ARGV[0]/3600),"h ",int(($ARGV[0]%3600)/60),"m ",int($ARGV[0]%60),"s\n"' "$1"; }

# --- Begin ------------------------------------------------------------------
cd "$ROOT"
mkdir -p "$LOGDIR"

start_ts=$(date +%s)
say "[SETUP] Working directory: $ROOT"

# (Re)create venv if missing or requested
if $REBUILD || [ ! -x "$PY" ]; then
  say "[SETUP] Creating virtualenv…"
  python3 -m venv "$VENV"
fi

# Activate venv for this shell
# shellcheck disable=SC1091
source "$VENV/bin/activate"

# Upgrade pip (fast, quiet-ish)
say "[SETUP] Ensuring recent pip…"
python -m pip install -U pip ${QUIET:+-q}

# Install Python deps (idempotent)
DEPS=(requests requests-cache beautifulsoup4 python-dateutil playwright gspread google-auth)
say "[SETUP] Installing Python deps (${DEPS[*]})…"
python -m pip install ${QUIET:+-q} "${DEPS[@]}"

# Optionally install Playwright browser once (idempotent & quick if already present)
if $INSTALL_BROWSER; then
  say "[SETUP] Ensuring Playwright chromium is available…"
  python - <<'PY'
from pathlib import Path
try:
    from playwright.__main__ import main as pw_main  # noqa:F401
    # rough check: if drivers folder exists, assume installed
    import importlib, sys
    m = importlib.import_module("playwright._impl._driver")
    driver_dir = Path(m.compute_driver_executable()).parent
    have = driver_dir.exists()
except Exception:
    have = False
if not have:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
PY
fi

# --- Run scraper with timestamped logging -----------------------------------
say "[RUN] Starting scraper… (logs: $LOG)"
(
  echo "[RUN] Started at $(date '+%Y-%m-%d %H:%M:%S')"
  echo "[RUN] Python: $PY"
  echo "[RUN] CWD: $(pwd)"
) >"$LOG"

# Stream output to console AND log
# If you prefer a quieter console, remove `tee` and append >>"$LOG" instead.
set +e
"$PY" po_job_scraper.py 2>&1 | tee -a "$LOG"
exit_code=${PIPESTATUS[0]}
set -e

ln -sfn "$LOG" "$LOGDIR/latest.log"

end_ts=$(date +%s)
elapsed=$(( end_ts - start_ts ))
say "[DONE] Exit ${exit_code}. Elapsed: $(time_human $elapsed)"
echo "[DONE] Finished at $(date '+%Y-%m-%d %H:%M:%S'). Elapsed: $(time_human $elapsed)" >>"$LOG"

exit $exit_code
