"""
logging_utils.py

Small shared logging helpers for the job scraper and any utility modules.
This is intentionally lightweight and has no dependencies on po_job_scraper.py
to avoid circular imports.
"""

from __future__ import annotations

import datetime as _dt
import os
import sys
from typing import Any

# Send logs to stderr so they do not fight with the progress line on stdout
LOG_STREAM = sys.stderr

# Progress is rendered on stdout (single line), logs go to stderr
PROGRESS_STREAM = sys.stdout
_PROGRESS_ACTIVE = False



# Minimal ANSI color helpers (match po_job_scraper style)
RESET = "\033[0m"
DIM = "\033[2m"
RED = "\033[31m"
YELLOW = "\033[33m"
GREEN = "\033[32m"
CYAN = "\033[36m"

LEVEL_COLOR = {
    "ERROR": RED,
    "WARN": YELLOW,
    "INFO": CYAN,
    "KEEP": GREEN,
    "SKIP": DIM,
    "DONE": GREEN,
}


def _ansi_ok() -> bool:
    """Return True if we should emit ANSI colors."""
    try:
        return LOG_STREAM.isatty() and os.environ.get("NO_COLOR") is None
    except Exception:
        return False


def _timestamp() -> str:
    """Return a simple timestamp used in log lines."""
    return _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _log(level: str, message: str, *, prefix: str = "") -> None:
    """
    Core logger used by the helpers below.

    Example output:
    [2025-11-21 14:45:10] [INFO                  ].[GS] Pushed 2 rows to tab 'Table1'.
    """
    progress_clear_if_needed()
    ts = _timestamp()
    label = (level or "").upper()
    head = f"[{ts}] [{label:<22}]"
    prefix_part = f"[{prefix}] " if prefix else ""
    body = f"{head}.{prefix_part}{message}"

    if _ansi_ok():
        color = LEVEL_COLOR.get(label, RESET)
        body = f"{color}{body}{RESET}"

    # Match the po_job_scraper dot-prefix style and color the whole line
    print(body, file=LOG_STREAM)


# Public API for other modules to use
def log_line(
    level: str,
    msg: str,
    *,
    prefix: str = "",
    width: int | None = None,  # kept for future compatibility, not used
) -> None:
    """
    Unified logging entry point used by helper modules (e.g. Google Sheets).

    It routes to _log so all logs share the same timestamp + [LEVEL] + prefix
    formatting as the rest of the scraper.
    """
    _log(level, msg, prefix=prefix)



# Public helpers -----------------------------------------------------------

def info(message: str, *, prefix: str = "") -> None:
    """Informational message."""
    _log("INFO", message, prefix=prefix)


def warn(message: str, *, prefix: str = "") -> None:
    """Warning message (non-fatal)."""
    _log("WARN", message, prefix=prefix)
    


def debug(message: str, *, prefix: str = "") -> None:
    """Debug / verbose message."""
    _log("DEBUG", message, prefix=prefix)


def done_log(message: str, *, prefix: str = "") -> None:
    """
    End-of-run or summary style message.

    Mirrors the idea of `done_log(...)` in po_job_scraper, but kept simple.
    """
    _log("DONE", message, prefix=prefix)

def error(message: str, *, prefix: str = "") -> None:
    """Log an error-level message."""
    _log("ERROR", message, prefix=prefix)


def progress(message: str, *, prefix: str = "") -> None:
    """
    Render a single line progress update on stdout.
    Does not include timestamps so it can safely re render in place.
    """
    global _PROGRESS_ACTIVE

    prefix_part = f"[{prefix}] " if prefix else ""
    line = f"{prefix_part}{message}"

    # carriage return means: redraw this same terminal row
    print(line, end="\r", file=PROGRESS_STREAM, flush=True)
    _PROGRESS_ACTIVE = True


def progress_clear_if_needed() -> None:
    """
    If the last output was a single line progress update using \\r,
    print a newline so the next log starts on a fresh row.
    """
    global _PROGRESS_ACTIVE
    if _PROGRESS_ACTIVE:
        print("", file=PROGRESS_STREAM, flush=True)
        _PROGRESS_ACTIVE = False


def log_event(
    level: str,
    title: str,
    *,
    left: str | None = None,
    right: str | None = None,
) -> None:
    """
    Helper to write a formatted event line with padding.

    Example:
        log_event("KEEP", "Business Analyst", right="[SALARY GATE] near_min")
    """
    left = left or ""
    right = right or ""

    mid = f"{level}: {title}"
    width = 90  # keep in line with the other log formatting

    if len(mid) > width:
        mid = mid[: width - 1] + "…"

    line = f"{left}{mid:{width}}{right}"
    info(line)
