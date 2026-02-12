#!/usr/bin/env python3
"""
Sync schema changes to Google Sheets.
Updates tabs with new headers and creates missing tabs.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.config import BotConfig
from app.integrations.google_sheets_client import GoogleSheetsClient, DEFAULT_HEADERS

def sync_schema():
    """Sync all schema changes to Google Sheets."""
    print("Syncing schema to Google Sheets...")
    
    config = BotConfig.from_env(require_reddit=False)
    sheets = GoogleSheetsClient(config)
    
    # Ensure all tabs exist with correct headers
    print("\n1. Ensuring all tabs exist with correct headers...")
    
    tabs_to_sync = {
        config.teams_tab_name: DEFAULT_HEADERS["Teams"],
        config.posts_tab_name: DEFAULT_HEADERS["PostingPlan"],
        config.reply_queue_tab_name: DEFAULT_HEADERS["ReplyQueue"],
        config.state_tab_name: DEFAULT_HEADERS["State"],
        config.metrics_tab_name: DEFAULT_HEADERS["Metrics"],
        config.test_posts_tab_name: DEFAULT_HEADERS["TestPosts"],
    }
    
    for tab_name, headers in tabs_to_sync.items():
        print(f"   [*] Syncing '{tab_name}' tab...")
        ws = sheets.get_or_create_worksheet(tab_name, headers=headers)
        
        # Check if headers need updating
        existing_headers = ws.row_values(1)
        if existing_headers != headers:
            print(f"      Updating headers: {len(existing_headers)} -> {len(headers)} columns")
            # Update headers
            ws.clear()
            ws.append_row(headers)
            print(f"      [OK] Headers updated for '{tab_name}'")
        else:
            print(f"      [OK] Headers already correct for '{tab_name}'")
    
    print("\n2. Verifying schema...")
    for tab_name in tabs_to_sync.keys():
        ws = sheets.get_or_create_worksheet(tab_name)
        headers = ws.row_values(1)
        expected_headers = tabs_to_sync[tab_name]
        if headers == expected_headers:
            print(f"   [OK] '{tab_name}': {len(headers)} columns - OK")
        else:
            print(f"   [ERROR] '{tab_name}': Headers mismatch!")
            print(f"      Expected: {expected_headers}")
            print(f"      Got: {headers}")
    
    print("\n[SUCCESS] Schema sync complete!")
    print(f"\nUpdated tabs:")
    for tab_name, headers in tabs_to_sync.items():
        print(f"  - {tab_name}: {len(headers)} columns")

if __name__ == "__main__":
    try:
        sync_schema()
    except Exception as e:
        print(f"\n[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

