#!/usr/bin/env bash
set -Eeuo pipefail

# --- Settings ---------------------------------------------------------------
ROOT="${HOME}/job-scraper"
VENV="${ROOT}/.venv"
PY="${VENV}/bin/python"
PIP="${VENV}/bin/pip"
LOGDIR="${ROOT}/logs"
STAMP="${RUN_TS_OVERRIDE:-$(date +"%Y%m%d_%H%M%S")}"
LOG="${LOGDIR}/run_${STAMP}.log"

# Flags:
#   --rebuild       Recreate the venv from scratch
#   --no-browser    Skip Playwright browser install
#   --quiet         Less chatter during setup
#   --stream        Mirror scraper output live to the terminal
#   --fast          Enable scraper quick-run mode (same as PW_FAST_MODE=1)
#   --              Pass remaining args directly to po_job_scraper.py
REBUILD=false
INSTALL_BROWSER=true
QUIET=false
STREAM=false
FAST=false
SCRAPER_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --rebuild)      REBUILD=true ;;
    --no-browser)   INSTALL_BROWSER=false ;;
    --quiet)        QUIET=true ;;
    --stream)       STREAM=true ;;
    --fast)         FAST=true ;;
    --)
      shift
      SCRAPER_ARGS=("$@")
      break
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 2
      ;;
  esac
  shift
done

# --- Helpers ----------------------------------------------------------------
say() { $QUIET || echo -e "$@"; }
time_human() { perl -e 'print int($ARGV[0]/3600),"h ",int(($ARGV[0]%3600)/60),"m ",int($ARGV[0]%60),"s\n"' "$1"; }
log_note() { echo "$1" >>"$LOG"; $QUIET || echo "$1"; }
file_mtime() { stat -f '%m' "$1" 2>/dev/null || echo 0; }
network_probe() {
  local url="$1"
  local timeout_seconds="$2"
  /usr/bin/curl --silent --show-error --location --head --max-time "$timeout_seconds" "$url" >/dev/null 2>&1
}

run_network_preflight() {
  local attempt
  local general_ok
  local sheets_ok

  for (( attempt=1; attempt<=NETWORK_PREFLIGHT_RETRIES; attempt++ )); do
    general_ok=0
    sheets_ok=0

    if network_probe "https://pypi.org/simple/pip/" "$NETWORK_PREFLIGHT_TIMEOUT_SECONDS" || \
       network_probe "https://about.gitlab.com/" "$NETWORK_PREFLIGHT_TIMEOUT_SECONDS"; then
      general_ok=1
    fi

    if network_probe "https://oauth2.googleapis.com/" "$NETWORK_PREFLIGHT_TIMEOUT_SECONDS"; then
      sheets_ok=1
    fi

    if (( general_ok )); then
      if (( ! sheets_ok )); then
        log_note "[PRECHECK] Reached the internet, but oauth2.googleapis.com is unavailable. Google Sheets carry-forward may fail in this run."
      fi
      return 0
    fi

    log_note "[PRECHECK] Network preflight attempt ${attempt}/${NETWORK_PREFLIGHT_RETRIES} failed. Could not reach general internet checks (pypi.org, about.gitlab.com)."
    if (( attempt < NETWORK_PREFLIGHT_RETRIES )); then
      log_note "[PRECHECK] Waiting ${NETWORK_PREFLIGHT_SLEEP_SECONDS}s before retrying. This often catches wake-from-sleep or DNS recovery."
      sleep "$NETWORK_PREFLIGHT_SLEEP_SECONDS"
    fi
  done

  log_note "[PRECHECK] Network preflight failed after ${NETWORK_PREFLIGHT_RETRIES} attempt(s). Aborting before pip install and scrape to avoid an empty run."
  return 75
}

parse_last_checkpoint() {
  local line
  line="$(grep -E 'Checkpoint saved at detail [0-9]+/[0-9]+: keep=[0-9]+, skip=[0-9]+' "$LOG" | tail -n 1 || true)"
  if [[ "$line" =~ detail[[:space:]]+([0-9]+)/([0-9]+):[[:space:]]+keep=([0-9]+),[[:space:]]+skip=([0-9]+) ]]; then
    CKPT_DETAIL="${BASH_REMATCH[1]}"
    CKPT_TOTAL="${BASH_REMATCH[2]}"
    CKPT_KEEP="${BASH_REMATCH[3]}"
    CKPT_SKIP="${BASH_REMATCH[4]}"
    return 0
  fi
  return 1
}

kill_process_tree() {
  local pid="$1"
  local child
  for child in $(pgrep -P "$pid" 2>/dev/null || true); do
    kill_process_tree "$child"
  done
  kill -TERM "$pid" 2>/dev/null || true
  sleep 2
  kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null || true
}

launch_scraper_background() {
  local resume_flag="$1"
  shift
  local -a extra_args=("$@")
  local pw_fast_mode="0"
  $FAST && pw_fast_mode="1"

  if ((${#extra_args[@]})); then
    RUN_TS_OVERRIDE="$STAMP" \
    SCRAPER_RESUME_FROM_CHECKPOINTS="$resume_flag" \
    PW_FAST_MODE="$pw_fast_mode" \
    JOB_SCRAPER_PROGRESS=0 \
    PW_MAX_FETCHES_PER_SESSION="$PW_MAX_FETCHES_PER_SESSION" \
    PW_SESSION_MAX_AGE_SECONDS="$PW_SESSION_MAX_AGE_SECONDS" \
    PW_HARD_TIMEOUT_SECONDS="$PW_HARD_TIMEOUT_SECONDS" \
    LISTING_HARD_TIMEOUT_SECONDS="$LISTING_HARD_TIMEOUT_SECONDS" \
    BUILTIN_MAX_PAGES="$BUILTIN_MAX_PAGES" \
    EDTECH_MAX_PAGES="$EDTECH_MAX_PAGES" \
    INCLUDE_UNSTABLE_SOURCES="$INCLUDE_UNSTABLE_SOURCES" \
    "$PY" po_job_scraper.py "${extra_args[@]}" >>"$LOG" 2>&1 &
  else
    RUN_TS_OVERRIDE="$STAMP" \
    SCRAPER_RESUME_FROM_CHECKPOINTS="$resume_flag" \
    PW_FAST_MODE="$pw_fast_mode" \
    JOB_SCRAPER_PROGRESS=0 \
    PW_MAX_FETCHES_PER_SESSION="$PW_MAX_FETCHES_PER_SESSION" \
    PW_SESSION_MAX_AGE_SECONDS="$PW_SESSION_MAX_AGE_SECONDS" \
    PW_HARD_TIMEOUT_SECONDS="$PW_HARD_TIMEOUT_SECONDS" \
    LISTING_HARD_TIMEOUT_SECONDS="$LISTING_HARD_TIMEOUT_SECONDS" \
    BUILTIN_MAX_PAGES="$BUILTIN_MAX_PAGES" \
    EDTECH_MAX_PAGES="$EDTECH_MAX_PAGES" \
    INCLUDE_UNSTABLE_SOURCES="$INCLUDE_UNSTABLE_SOURCES" \
    "$PY" po_job_scraper.py >>"$LOG" 2>&1 &
  fi
  SCRAPER_PID=$!
}

# --- Begin ------------------------------------------------------------------
cd "$ROOT"
mkdir -p "$LOGDIR"

start_ts=$(date +%s)
NETWORK_PREFLIGHT_RETRIES="${NETWORK_PREFLIGHT_RETRIES:-3}"
NETWORK_PREFLIGHT_SLEEP_SECONDS="${NETWORK_PREFLIGHT_SLEEP_SECONDS:-30}"
NETWORK_PREFLIGHT_TIMEOUT_SECONDS="${NETWORK_PREFLIGHT_TIMEOUT_SECONDS:-8}"
say "[SETUP] Working directory: $ROOT"

# (Re)create venv if missing or requested
if $REBUILD || [ ! -x "$PY" ]; then
  say "[SETUP] Creating virtualenv…"
  python3 -m venv "$VENV"
fi

# Activate venv for this shell
# shellcheck disable=SC1091
source "$VENV/bin/activate"

run_network_preflight
exit_code=$?
if (( exit_code != 0 )); then
  end_ts=$(date +%s)
  elapsed=$(( end_ts - start_ts ))
  ln -sfn "$LOG" "$LOGDIR/latest.log"
  say "[DONE] Exit ${exit_code}. Elapsed: $(time_human $elapsed)"
  echo "[DONE] Finished at $(date '+%Y-%m-%d %H:%M:%S'). Elapsed: $(time_human $elapsed)" >>"$LOG"
  exit "$exit_code"
fi

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
if $STREAM; then
  say "[RUN] Console streaming: on"
else
  say "[RUN] Console streaming: off (use --stream to mirror output live)"
fi
if [[ -s "$LOG" ]]; then
  (
    echo
    if [[ -n "${RUN_TS_OVERRIDE:-}" ]]; then
      echo "[RUN] Resumed at $(date '+%Y-%m-%d %H:%M:%S')"
    else
      echo "[RUN] Started at $(date '+%Y-%m-%d %H:%M:%S')"
    fi
    echo "[RUN] Python: $PY"
    echo "[RUN] CWD: $(pwd)"
    echo "[RUN] Run stamp: $STAMP"
    echo "[RUN] Keep CSV: output/product_owner_jobs_${STAMP}.csv"
    echo "[RUN] Skip CSV: output/skipped_jobs_${STAMP}.csv"
  ) >>"$LOG"
else
  (
    echo "[RUN] Started at $(date '+%Y-%m-%d %H:%M:%S')"
    echo "[RUN] Python: $PY"
    echo "[RUN] CWD: $(pwd)"
    echo "[RUN] Run stamp: $STAMP"
    echo "[RUN] Keep CSV: output/product_owner_jobs_${STAMP}.csv"
    echo "[RUN] Skip CSV: output/skipped_jobs_${STAMP}.csv"
  ) >"$LOG"
fi

PW_MAX_FETCHES_PER_SESSION="${PW_MAX_FETCHES_PER_SESSION:-12}"
PW_SESSION_MAX_AGE_SECONDS="${PW_SESSION_MAX_AGE_SECONDS:-480}"
PW_HARD_TIMEOUT_SECONDS="${PW_HARD_TIMEOUT_SECONDS:-90}"
LISTING_HARD_TIMEOUT_SECONDS="${LISTING_HARD_TIMEOUT_SECONDS:-240}"
BUILTIN_MAX_PAGES="${BUILTIN_MAX_PAGES:-3}"
EDTECH_MAX_PAGES="${EDTECH_MAX_PAGES:-2}"
INCLUDE_UNSTABLE_SOURCES="${INCLUDE_UNSTABLE_SOURCES:-0}"
WATCHDOG_STALL_SECONDS="${WATCHDOG_STALL_SECONDS:-900}"
WATCHDOG_POLL_SECONDS="${WATCHDOG_POLL_SECONDS:-30}"
WATCHDOG_MAX_RESTARTS="${WATCHDOG_MAX_RESTARTS:-3}"
WATCHDOG_BOOTSTRAP_RESUME="${WATCHDOG_BOOTSTRAP_RESUME:-0}"

say "[RUN] Playwright recycle: ${PW_MAX_FETCHES_PER_SESSION} fetches or ${PW_SESSION_MAX_AGE_SECONDS}s"
say "[RUN] Playwright hard timeout: ${PW_HARD_TIMEOUT_SECONDS}s per fetch"
say "[RUN] Listing hard timeout: ${LISTING_HARD_TIMEOUT_SECONDS}s per listing"
say "[RUN] Built In page cap: ${BUILTIN_MAX_PAGES} per query (override with env or --builtin-max-pages)"
say "[RUN] EdTech list page cap: ${EDTECH_MAX_PAGES} per source"
say "[RUN] Unstable sources included: ${INCLUDE_UNSTABLE_SOURCES} (set INCLUDE_UNSTABLE_SOURCES=1 to include)"
say "[RUN] Watchdog: stall=${WATCHDOG_STALL_SECONDS}s poll=${WATCHDOG_POLL_SECONDS}s max-restarts=${WATCHDOG_MAX_RESTARTS}"
if $FAST; then
  say "[RUN] Quick-run mode: on"
else
  say "[RUN] Quick-run mode: off"
fi

set +e
if $STREAM; then
  pw_fast_mode="0"
  $FAST && pw_fast_mode="1"
  if ((${#SCRAPER_ARGS[@]})); then
    PW_MAX_FETCHES_PER_SESSION="$PW_MAX_FETCHES_PER_SESSION" \
    PW_SESSION_MAX_AGE_SECONDS="$PW_SESSION_MAX_AGE_SECONDS" \
    PW_HARD_TIMEOUT_SECONDS="$PW_HARD_TIMEOUT_SECONDS" \
    LISTING_HARD_TIMEOUT_SECONDS="$LISTING_HARD_TIMEOUT_SECONDS" \
    BUILTIN_MAX_PAGES="$BUILTIN_MAX_PAGES" \
    EDTECH_MAX_PAGES="$EDTECH_MAX_PAGES" \
    INCLUDE_UNSTABLE_SOURCES="$INCLUDE_UNSTABLE_SOURCES" \
    RUN_TS_OVERRIDE="$STAMP" \
    PW_FAST_MODE="$pw_fast_mode" \
    "$PY" po_job_scraper.py "${SCRAPER_ARGS[@]}" 2>&1 | tee -a "$LOG"
  else
    PW_MAX_FETCHES_PER_SESSION="$PW_MAX_FETCHES_PER_SESSION" \
    PW_SESSION_MAX_AGE_SECONDS="$PW_SESSION_MAX_AGE_SECONDS" \
    PW_HARD_TIMEOUT_SECONDS="$PW_HARD_TIMEOUT_SECONDS" \
    LISTING_HARD_TIMEOUT_SECONDS="$LISTING_HARD_TIMEOUT_SECONDS" \
    BUILTIN_MAX_PAGES="$BUILTIN_MAX_PAGES" \
    EDTECH_MAX_PAGES="$EDTECH_MAX_PAGES" \
    INCLUDE_UNSTABLE_SOURCES="$INCLUDE_UNSTABLE_SOURCES" \
    RUN_TS_OVERRIDE="$STAMP" \
    PW_FAST_MODE="$pw_fast_mode" \
    "$PY" po_job_scraper.py 2>&1 | tee -a "$LOG"
  fi
  exit_code=${PIPESTATUS[0]}
else
  restart_count=0
  resume_flag=0
  declare -a scraper_extra_args=()
  if ((${#SCRAPER_ARGS[@]})); then
    scraper_extra_args=("${SCRAPER_ARGS[@]}")
  fi
  exit_code=0

  if [[ "$WATCHDOG_BOOTSTRAP_RESUME" == "1" ]]; then
    if parse_last_checkpoint; then
      next_detail_index=$(( CKPT_DETAIL + 1 ))
      if (( next_detail_index <= CKPT_TOTAL )); then
        resume_flag=1
        scraper_extra_args=()
        if ((${#SCRAPER_ARGS[@]})); then
          scraper_extra_args=("${SCRAPER_ARGS[@]}")
        fi
        scraper_extra_args+=(--start-detail-index "$next_detail_index" --end-detail-index "$CKPT_TOTAL")
        log_note "[WATCHDOG] Bootstrap resume from detail ${next_detail_index}-${CKPT_TOTAL} using existing checkpoint CSVs (keep=${CKPT_KEEP} skip=${CKPT_SKIP})."
      else
        log_note "[WATCHDOG] Bootstrap resume skipped: checkpoint already at end of window (${CKPT_DETAIL}/${CKPT_TOTAL})."
      fi
    else
      log_note "[WATCHDOG] Bootstrap resume requested but no checkpoint line found in $LOG; starting fresh."
    fi
  fi

  while true; do
    if ((${#scraper_extra_args[@]})); then
      launch_scraper_background "$resume_flag" "${scraper_extra_args[@]}"
    else
      launch_scraper_background "$resume_flag"
    fi
    log_note "[WATCHDOG] Started scraper pid ${SCRAPER_PID} (resume=${resume_flag} args='${scraper_extra_args[*]-}')"

    stalled=false
    last_mtime="$(file_mtime "$LOG")"

    while kill -0 "$SCRAPER_PID" 2>/dev/null; do
      sleep "$WATCHDOG_POLL_SECONDS"
      current_mtime="$(file_mtime "$LOG")"
      now_epoch="$(date +%s)"

      if (( current_mtime > last_mtime )); then
        last_mtime="$current_mtime"
        continue
      fi

      idle_seconds=$(( now_epoch - current_mtime ))
      if (( idle_seconds >= WATCHDOG_STALL_SECONDS )); then
        log_note "[WATCHDOG] No log activity for ${idle_seconds}s; terminating scraper pid ${SCRAPER_PID}."
        kill_process_tree "$SCRAPER_PID"
        wait "$SCRAPER_PID" 2>/dev/null || true
        stalled=true
        break
      fi
    done

    if ! $stalled; then
      wait "$SCRAPER_PID"
      exit_code=$?
      break
    fi

    if (( restart_count >= WATCHDOG_MAX_RESTARTS )); then
      log_note "[WATCHDOG] Stall recovery exhausted after ${restart_count} restart(s)."
      exit_code=124
      break
    fi

    if ! parse_last_checkpoint; then
      log_note "[WATCHDOG] Stall recovery failed: no checkpoint line found in $LOG."
      exit_code=124
      break
    fi

    next_detail_index=$(( CKPT_DETAIL + 1 ))
    if (( next_detail_index > CKPT_TOTAL )); then
      log_note "[WATCHDOG] Stall recovery failed: checkpoint already reached end of window (${CKPT_DETAIL}/${CKPT_TOTAL})."
      exit_code=124
      break
    fi

    restart_count=$(( restart_count + 1 ))
    resume_flag=1
    scraper_extra_args=()
    if ((${#SCRAPER_ARGS[@]})); then
      scraper_extra_args=("${SCRAPER_ARGS[@]}")
    fi
    scraper_extra_args+=(--start-detail-index "$next_detail_index" --end-detail-index "$CKPT_TOTAL")
    log_note "[WATCHDOG] Resume ${restart_count}/${WATCHDOG_MAX_RESTARTS} from detail ${next_detail_index}-${CKPT_TOTAL} using existing checkpoint CSVs (keep=${CKPT_KEEP} skip=${CKPT_SKIP})."
  done
fi
set -e

ln -sfn "$LOG" "$LOGDIR/latest.log"

end_ts=$(date +%s)
elapsed=$(( end_ts - start_ts ))
say "[DONE] Exit ${exit_code}. Elapsed: $(time_human $elapsed)"
echo "[DONE] Finished at $(date '+%Y-%m-%d %H:%M:%S'). Elapsed: $(time_human $elapsed)" >>"$LOG"

exit $exit_code
