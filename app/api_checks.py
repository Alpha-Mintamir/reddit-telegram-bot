from __future__ import annotations

from typing import List, Tuple

from app.config import BotConfig
from app.integrations.google_sheets_client import GoogleSheetsClient
from app.integrations.reddit_client import RedditClient
from app.integrations.telegram_client import TelegramClient


def check_telegram(config: BotConfig) -> Tuple[bool, str]:
    try:
        telegram = TelegramClient(config.telegram_bot_token)
        result = telegram.get_me().get("result", {})
        return True, f"Telegram OK: @{result.get('username', 'unknown')} (id={result.get('id', 'unknown')})"
    except Exception as exc:
        return False, f"Telegram FAILED: {exc}"


def check_sheets(config: BotConfig) -> Tuple[bool, str]:
    try:
        sheets = GoogleSheetsClient(config)
        sheets.ensure_default_schema()
        teams = sheets.read_rows(config.teams_tab_name)
        posts = sheets.read_rows(config.posts_tab_name)
        return True, f"Google Sheets OK: teams={len(teams)}, posting_plan={len(posts)}"
    except Exception as exc:
        return False, f"Google Sheets FAILED: {exc}"


def check_reddit(config: BotConfig) -> Tuple[bool, str]:
    try:
        reddit = RedditClient(config)
        user_agent_display = reddit.user_agent[:60] if reddit.user_agent else "NOT SET"
        
        # Try multiple endpoints - some are more lenient than others
        test_urls = [
            "https://www.reddit.com/r/MachineLearning/about.json",  # Subreddit info (most lenient)
            "https://www.reddit.com/r/MachineLearning/top.json?limit=1",  # Top posts
            "https://www.reddit.com/r/MachineLearning/hot.json?limit=1",  # Hot posts
        ]
        
        last_error = None
        for test_url in test_urls:
            try:
                response = reddit.session.get(test_url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    # Check if we got valid data
                    if isinstance(data, dict) and "data" in data:
                        return True, f"Reddit OK: public JSON scraping verified (User-Agent: {user_agent_display}...)"
                    elif isinstance(data, list) and len(data) > 0:
                        return True, f"Reddit OK: public JSON scraping verified (User-Agent: {user_agent_display}...)"
                elif response.status_code == 403:
                    last_error = f"403 Forbidden on {test_url}"
                    continue  # Try next URL
                else:
                    response.raise_for_status()
            except Exception as e:
                last_error = str(e)
                continue  # Try next URL
        
        # If all URLs failed with 403, this is likely Reddit blocking GitHub Actions IPs
        # The bot will still work for actual post URLs, so we'll warn but not fail completely
        if last_error and "403" in last_error:
            return False, f"Reddit 403: Test endpoints blocked (likely GitHub Actions IP). Bot will still work for actual post URLs. User-Agent: {user_agent_display}..."
        return False, f"Reddit FAILED: {last_error or 'All test URLs failed'}"
    except Exception as exc:
        error_msg = str(exc)
        if "403" in error_msg:
            user_agent_display = config.reddit_user_agent[:60] if config.reddit_user_agent else "NOT SET"
            return False, f"Reddit 403: Test endpoints blocked (likely GitHub Actions IP). Bot will still work for actual post URLs. User-Agent: {user_agent_display}..."
        return False, f"Reddit FAILED: {exc}"


def run_checks(step: str) -> int:
    checks: List[Tuple[str, bool, str]] = []
    if step in {"telegram", "all"}:
        cfg = BotConfig.from_env(require_reddit=False)
        ok, msg = check_telegram(cfg)
        checks.append(("telegram", ok, msg))
    if step in {"sheets", "all"}:
        cfg = BotConfig.from_env(require_reddit=False)
        ok, msg = check_sheets(cfg)
        checks.append(("sheets", ok, msg))
    if step in {"reddit", "all"}:
        cfg = BotConfig.from_env(require_reddit=False)
        ok, msg = check_reddit(cfg)
        checks.append(("reddit", ok, msg))

    failures = 0
    print("\nAPI Setup Check Results")
    print("-" * 40)
    for name, ok, msg in checks:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {msg}")
        if not ok:
            failures += 1
    print("-" * 40)
    return failures




