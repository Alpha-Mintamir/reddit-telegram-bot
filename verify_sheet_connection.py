import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.config import BotConfig
from app.integrations.google_sheets_client import GoogleSheetsClient

# Load config
config = BotConfig.from_env(require_reddit=False)
sheets = GoogleSheetsClient(config)

print(f"Connected to Google Sheet ID: {config.google_spreadsheet_id}")
print(f"Expected Sheet URL: https://docs.google.com/spreadsheets/d/{config.google_spreadsheet_id}")

# List all worksheets
all_worksheets = sheets._spreadsheet.worksheets()
print(f"\nAvailable worksheets ({len(all_worksheets)}):")
for ws in all_worksheets:
    row_count = ws.row_count
    col_count = ws.col_count
    print(f"  - '{ws.title}': {row_count} rows x {col_count} cols")

# Check Teams tab specifically
teams_tab = config.teams_tab_name
print(f"\nChecking '{teams_tab}' tab:")
try:
    ws = sheets._spreadsheet.worksheet(teams_tab)
    all_values = ws.get_all_values()
    print(f"  Total rows (including header): {len(all_values)}")
    if all_values:
        print(f"  Header row: {all_values[0]}")
        print(f"  Data rows: {len(all_values) - 1}")
        if len(all_values) > 1:
            print(f"  First data row: {all_values[1]}")
        else:
            print("  ⚠️  No data rows found (only headers)")
    else:
        print("  ⚠️  Tab is completely empty")
except Exception as e:
    print(f"  ❌ Error accessing tab: {e}")

# Try reading rows
print(f"\nReading rows using read_rows():")
teams_rows = sheets.read_rows(teams_tab)
print(f"  Found {len(teams_rows)} rows")
if teams_rows:
    print(f"  First row: {teams_rows[0]}")



