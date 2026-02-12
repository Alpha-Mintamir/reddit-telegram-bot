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
        # Test fetching a known post from r/MachineLearning
        test_url = "https://www.reddit.com/r/MachineLearning/hot.json"
        response = reddit.session.get(test_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data and len(data.get("data", {}).get("children", [])) > 0:
            return True, "Reddit OK: public JSON scraping verified on r/MachineLearning"
        return False, "Reddit FAILED: empty response from r/MachineLearning"
    except Exception as exc:
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




