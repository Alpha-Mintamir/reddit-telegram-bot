import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.config import BotConfig
from app.integrations.google_sheets_client import GoogleSheetsClient

def _normalize_username(value):
    if not value:
        return ""
    return value.strip().lstrip("@").lower()

# Load config
config = BotConfig.from_env(require_reddit=False)
sheets = GoogleSheetsClient(config)

# Read Teams data
teams_rows = sheets.read_rows(config.teams_tab_name)
print(f"Found {len(teams_rows)} rows in Teams tab\n")

username_to_member = {}
for row in teams_rows:
    raw_username = row.get("telegram_user_id", "")
    normalized = _normalize_username(raw_username)
    member_name = row.get("member_name", "").strip()
    team_id = row.get("team_id", "").strip()
    
    print(f"Row: member_name='{member_name}', telegram_user_id='{raw_username}' (normalized: '{normalized}'), team_id='{team_id}'")
    
    if normalized and member_name:
        username_to_member[normalized] = member_name

print(f"\nUsername mappings ({len(username_to_member)}):")
for username, member in username_to_member.items():
    print(f"  '{username}' -> '{member}'")

# Test Alpha's username
test_username = "alphityy"
print(f"\nTesting match for '{test_username}':")
if test_username in username_to_member:
    print(f"  ✅ MATCHED: {username_to_member[test_username]}")
else:
    print(f"  ❌ NO MATCH")
    print(f"  Available usernames: {list(username_to_member.keys())}")



