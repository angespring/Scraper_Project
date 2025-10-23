import gspread
from google.oauth2.service_account import Credentials

KEY_PATH   = "/Users/ange/job-scraper/service_account.json"  # absolute path
SHEET_URL  = "https://docs.google.com/spreadsheets/d/1UloVHEsBxvMJ3WeQ8XkHvtIrL1cQ2CiyD50bsOb-Up8/edit?gid=1531552984#gid=1531552984"              # full URL to 'product jobs scraper'
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]  # Sheets only (no Drive)

creds  = Credentials.from_service_account_file(KEY_PATH, scopes=SCOPES)
client = gspread.authorize(creds)

ws = client.open_by_url(SHEET_URL).sheet1   # or .worksheet("Table1") if that’s your tab name
ws.append_row(["2025-10-06", "Test via open_by_url", "OK"], value_input_option="USER_ENTERED")
print("✅ Wrote to sheet without Drive API.")
