"""
Helpers for Google Sheets carry-forward and result push.

This module is optional. If gspread / google-auth are missing, we log
warnings and the caller can fall back to CSV-only behavior.
"""

from __future__ import annotations
from pathlib import Path
from typing import Any
from logging_utils import warn, info, debug, log_line
from logging_utils import log_event  # used for structured [GS] log lines




GS_LIBS_OK = False
GS_LIB_ERROR = None
HAVE_GS = False   # new: global flag used elsewhere

try:
    import gspread
    from google.oauth2.service_account import Credentials

    GS_LIBS_OK = True
    HAVE_GS = True        # we have the libs
except ImportError as e:
    GS_LIBS_OK = False
    HAVE_GS = False       # we do *not* have the libs
    GS_LIB_ERROR = str(e)






def _gs_log(level: str, msg: str) -> None:
    """
    Google Sheets-specific logging.

    Uses the same log_line formatting as the main scraper logs,
    with a [GS] prefix so messages are easy to spot.
    """
    try:
        log_event(level, msg, left="[GS] ")
    except Exception:
        # Fallback to simple line logging if the structured logger signature changes
        log_line(level, msg, prefix="GS")


def init_gs_libs() -> None:
    """
    Try to import the Google Sheets libraries once.

    Sets GS_LIBS_OK / GS_LIB_ERROR for the rest of the run.
    """
    global GS_LIBS_OK, GS_LIB_ERROR

    try:
        import gspread  # noqa: F401
        from google.oauth2.service_account import Credentials  # noqa: F401
        GS_LIBS_OK = True
        GS_LIB_ERROR = None
    except Exception as e:
        GS_LIBS_OK = False
        GS_LIB_ERROR = e


def log_startup_warning_if_needed(sheet_url: str | None) -> None:
    if sheet_url and not GS_LIBS_OK:
        msg = (
            "Google Sheets is configured but the Python libs are missing "
            f"({GS_LIB_ERROR!r}). "
            "This run will not load prior decisions or push anything to Sheets. "
            "CSV files will still be written. "
            "Most common cause: running outside the venv."
        )
        _gs_log("WARN", msg)


def log_final_reminder_if_needed(sheet_url: str | None) -> None:
    if sheet_url and not GS_LIBS_OK:
        msg = (
            "Google Sheets carry-forward and push were disabled for this run "
            "because the libs are missing. "
            "Review the CSVs in output/ and, if you want Sheets again, "
            "activate the venv and reinstall the libs."
        )
        _gs_log("WARN", msg)


def fetch_prior_decisions(
    sheet_url: str,
    key_path: str | None = None,
    tab_name: str | None = None,
) -> dict[str, tuple[str, str]]:
    """
    Fetch prior Applied?/Reason values keyed by Job URL for carry-forward.
    """
    if not sheet_url:
        return {}
    if not HAVE_GS:
        #warn("[GS] Skipping carry-forward; Google Sheets libraries are missing.")
        _gs_log("WARN", "Skipping carry-forward; Google Sheets libraries are missing.")
        return {}
    if not key_path:
        #warn("[GS] Skipping carry-forward; no key path was provided.")
        _gs_log("WARN", "Skipping carry-forward; no key path was provided.")
        return {}

    key_file = Path(key_path)
    if not key_file.exists():
        #warn(f"[GS] Skipping carry-forward; key file not found at {key_file}.")
        _gs_log("WARN", "Skipping carry-forward; key file not found at {key_file}.")
        return {}

    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(str(key_file), scopes=scopes)
        client = gspread.authorize(creds)
        sh = client.open_by_url(sheet_url)
        ws = sh.worksheet(tab_name) if tab_name else sh.sheet1

        records = ws.get_all_records()
        prior: dict[str, tuple[str, str]] = {}
        for r in records:
            url = r.get("Job URL") or r.get("job_url")
            if not url:
                continue
            applied = r.get("Applied?", "")
            reason = r.get("Reason", "")
            prior[url] = (applied, reason)
        return prior
    except Exception as e:
        err_line = str(e).splitlines()[0] if e else "Unknown error"

        #warn(
        #    "[GS] Failed to fetch prior decisions from Google Sheets. "
        #    "Review the CSVs in output/ and verify your sheet URL and credentials."
        #)
        _gs_log(
            "WARN", "Failed to fetch prior decisions from Google Sheets. "
            "Review the CSVs in output/ and verify your sheet URL and credentials."
        )
        
        #debug(f"[GS] Original prior decisions error: {err_line}")
        _gs_log("DEBUG", f"Original Sheets error: {err_line}")
        return {}


# ==== Make GS lines non-timestamped (match your example format)
def push_results_to_sheets(
    sheet_url: str | None,
    kept_rows: list[dict],
    skipped_rows: list[dict],
    keep_fields: list[str],
    skip_fields: list[str],
    tab_name: str,
    key_path: str | None,
    progress_clear=None,
) -> None:
    """Push both kept and skipped rows to Google Sheets, if a sheet URL is configured."""
    # If there is no sheet configured, just bail out quietly.
    if not sheet_url:
        return

    try:
        # KEEP rows → main tab
        push_rows_to_google_sheet(
            sheet_url,
            [to_keep_sheet_row(r) for r in kept_rows],
            keep_fields,
            tab_name=tab_name,
            key_path=key_path,
            progress_clear=progress_clear,
        )

        # SKIP rows → "Skipped" tab
        push_rows_to_google_sheet(
            sheet_url,
            [to_skipped_sheet_row(r) for r in skipped_rows],
            skip_fields,
            tab_name="Skipped",
            key_path=key_path,
            progress_clear=progress_clear,
        )

    except Exception as e:
        # High-level warning, nicely wrapped
        warn_msg = (
            "Skipping Sheets push due to error: "
            f"{e!r}. Review the CSVs in output/ and check your sheet URL and key path."
        )
        _gs_log("WARN", warn_msg)

        # Detailed debug, also wrapped but still readable
        debug_msg = f"Original Sheets error: {e!r}"
        _gs_log("DEBUG", debug_msg)


def _normalize_sheet_value(value):
    """Force all cell values to plain strings for Sheets."""
    if value is None:
        return ""

    if isinstance(value, str):
        return value

    # Things like ["remote", "remote|us"] become "remote|remote|us"
    if isinstance(value, (list, tuple, set)):
        return "|".join(str(v) for v in value)

    return str(value)


def push_rows_to_google_sheet(
    sheet_url: str,
    rows: list[dict],
    fields: list[str],
    tab_name: str,
    key_path: str | None,
    progress_clear=None,
) -> None:
    """Append rows to a specific Google Sheets tab."""
    if not sheet_url or not rows:
        return
    if not HAVE_GS:
        #warn(
        #    "[GS] Skipping Sheets push because Google Sheets libraries are not "
        #    "available. Review the CSVs in output/ and, if you want Sheets "
        #    "again, activate the venv and reinstall the libs."
        #)

        warn_msg = (
            "Skipping Sheets push because Google Sheets libraries are not "
            f"{e!r}. available. Review the CSVs in output / and, if you want Sheets "
            f"{e!r}. again, activate the venv and reinstall the libs." 
        )
        _gs_log("WARN", warn_msg)


        return
    if not key_path:
        warn("[GS] Skipping Sheets push; no key path was provided.")
        return

    key_file = Path(key_path)
    if not key_file.exists():
        warn(f"[GS] Skipping Sheets push; key file not found at {key_file}.")
        return

    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(str(key_file), scopes=scopes)
        client = gspread.authorize(creds)
        sh = client.open_by_url(sheet_url)
        ws = sh.worksheet(tab_name) if tab_name else sh.sheet1

        if progress_clear:
            progress_clear()

        header = ws.row_values(1)
        # Keep the header in sync without clearing existing data.
        # If the sheet is empty, write the header. Otherwise respect the existing header order.
        if not header:
            ws.update("A1", [fields])
            header = fields
        else:
            # Trim trailing empty header cells
            while header and not str(header[-1]).strip():
                header.pop()
            if header and header != fields:
                warn("[GS] Header on sheet differs from expected fields; appending using existing header order.")

        target_fields = header or fields
        values = [[row.get(field, "") for field in target_fields] for row in rows]
        ws.append_rows(values, value_input_option="USER_ENTERED")
        info(f"[GS] Pushed {len(values)} rows to tab '{ws.title}'.")
    except ImportError as e:
        warn(
            "[GS] Skipping Sheets push; missing libs: "
            f"{e}. Review the CSVs in output/ and, if you want Sheets again, "
            f"activate the venv and reinstall the libs."
        )
    except Exception as e:
        _gs_log(
            "WARN",
            f"Skipping Sheets push due to error: {e!r}. "
            f"Review the CSVs in output/ and check your sheet URL and key path.",
        )
        _gs_log("DEBUG", f"Original Sheets error: {e!r}")


def to_keep_sheet_row(keep_row, applied="", reason=""):
    # if explicit values are passed, use them; otherwise fall back to the row
    applied_value = applied if applied != "" else keep_row.get("Applied?", "")
    reason_value = reason if reason != "" else keep_row.get("Reason", "")
    period = keep_row.get("Period", "")


    return {
        "Applied?": _normalize_sheet_value(applied_value),
        "Reason": _normalize_sheet_value(reason_value),
        "Date Scraped": _normalize_sheet_value(keep_row.get("Date Scraped", "")),
        "Title": _normalize_sheet_value(keep_row.get("Title", "")),
        "Job ID (Vendor)": _normalize_sheet_value(keep_row.get("Job ID (Vendor)", "")),
        "Job ID (Numeric)": _normalize_sheet_value(keep_row.get("Job ID (Numeric)", "")),
        "Job Key": _normalize_sheet_value(keep_row.get("Job Key", "")),
        "Company": _normalize_sheet_value(keep_row.get("Company", "")),
        "Career Board": _normalize_sheet_value(keep_row.get("Career Board", "")),
        "Location": _normalize_sheet_value(keep_row.get("Location", "")),
        "Posted": _normalize_sheet_value(keep_row.get("Posted") or period),
        "Posting Date": _normalize_sheet_value(
            keep_row.get("Posting Date") or keep_row.get("Posted") or period
        ),
        "Valid Through": _normalize_sheet_value(keep_row.get("Valid Through", "")),
        "Job URL": _normalize_sheet_value(keep_row.get("Job URL", "")),
        "Apply URL": _normalize_sheet_value(keep_row.get("Apply URL", "")),
        "Apply URL Note": _normalize_sheet_value(keep_row.get("Apply URL Note", "")),
        "Description Snippet": _normalize_sheet_value(keep_row.get("Description Snippet", "")),
        "WA Rule": _normalize_sheet_value(keep_row.get("WA Rule", "")),
        "Remote Rule": _normalize_sheet_value(keep_row.get("Remote Rule", "")),
        "US Rule": _normalize_sheet_value(keep_row.get("US Rule", "")),
        "Salary Max Detected": _normalize_sheet_value(keep_row.get("Salary Max Detected", "")),
        "Salary Rule": _normalize_sheet_value(keep_row.get("Salary Rule", "")),
        "Salary Near Min": _normalize_sheet_value(keep_row.get("Salary Near Min", "")),
        "Salary Status": _normalize_sheet_value(keep_row.get("Salary Status", "")),
        "Salary Note": _normalize_sheet_value(keep_row.get("Salary Note", "")),
        "Salary Est. (Low-High)": _normalize_sheet_value(keep_row.get("Salary Est. (Low-High)", "")),
        "Location Chips": _normalize_sheet_value(keep_row.get("Location Chips", "")),
        "Applicant Regions": _normalize_sheet_value(keep_row.get("Applicant Regions", "")),
        "Visibility Status": _normalize_sheet_value(keep_row.get("Visibility Status", "")),
        "Confidence Score": _normalize_sheet_value(keep_row.get("Confidence Score", "")),
        "Confidence Mark": _normalize_sheet_value(keep_row.get("Confidence Mark", "")),
    }


def to_skipped_sheet_row(skip_row, applied="", reason=""):
    return {
        "Date Scraped": _normalize_sheet_value(skip_row.get("Date Scraped", "")),
        "Title": _normalize_sheet_value(skip_row.get("Title", "")),
        "Job ID (Vendor)": _normalize_sheet_value(skip_row.get("Job ID (Vendor)", "")),
        "Job ID (Numeric)": _normalize_sheet_value(skip_row.get("Job ID (Numeric)", "")),
        "Job Key": _normalize_sheet_value(skip_row.get("Job Key", "")),
        "Company": _normalize_sheet_value(skip_row.get("Company", "")),
        "Career Board": _normalize_sheet_value(skip_row.get("Career Board", "")),
        "Location": _normalize_sheet_value(skip_row.get("Location", "")),
        "Posted": _normalize_sheet_value(skip_row.get("Posted", "")),
        "Posting Date": _normalize_sheet_value(skip_row.get("Posting Date", "")),
        "Valid Through": _normalize_sheet_value(skip_row.get("Valid Through", "")),
        "Job URL": _normalize_sheet_value(skip_row.get("Job URL", "")),
        #"Apply URL": _normalize_sheet_value(skip_row.get("Apply URL", "")),
        #"Apply URL Note": _normalize_sheet_value(skip_row.get("Apply URL Note", "")),
        "Reason Skipped": _normalize_sheet_value(skip_row.get("Reason Skipped", reason)),
        "WA Rule": _normalize_sheet_value(skip_row.get("WA Rule", "")),
        "Remote Rule": _normalize_sheet_value(skip_row.get("Remote Rule", "")),
        "US Rule": _normalize_sheet_value(skip_row.get("US Rule", "")),
        "Salary Max Detected": _normalize_sheet_value(skip_row.get("Salary Max Detected", "")),
        #"Skip Rule": _normalize_sheet_value(skip_row.get("Skip Rule", "")),
        "Salary Rule": _normalize_sheet_value(skip_row.get("Salary Rule", "")),
        #"Salary Near Min": _normalize_sheet_value(skip_row.get("Salary Near Min", "")),
        #"Salary Status": _normalize_sheet_value(skip_row.get("Salary Status", "")),
        #"Salary Note": _normalize_sheet_value(skip_row.get("Salary Note", "")),
        #"Salary Est. (Low-High)": _normalize_sheet_value(skip_row.get("Salary Est. (Low-High)", "")),
        "Location Chips": _normalize_sheet_value(skip_row.get("Location Chips", "")),
        "Applicant Regions": _normalize_sheet_value(skip_row.get("Applicant Regions", "")),
    }
