import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from dotenv import load_dotenv


load_dotenv()


def _get_tab_name(env_key: str, default: str) -> str:
    """Get tab name from env var, falling back to default if empty."""
    value = os.getenv(env_key, default)
    if not value or not value.strip():
        return default
    return value.strip()


@dataclass
class BotConfig:
    telegram_bot_token: str
    google_spreadsheet_id: str
    reddit_user_agent: str
    llm_model: str = "gpt-4o-mini"
    timezone: str = "Africa/Addis_Ababa"
    daily_hour: int = 8
    daily_minute: int = 0
    poll_interval_minutes: int = 10
    dry_run: bool = False
    google_service_account_path: Optional[str] = None
    google_service_account_json: Optional[Dict[str, Any]] = None
    posts_tab_name: str = "PostingPlan"
    teams_tab_name: str = "Teams"
    reply_queue_tab_name: str = "ReplyQueue"
    state_tab_name: str = "State"

    @staticmethod
    def _parse_bool(value: Optional[str], default: bool = False) -> bool:
        if value is None:
            return default
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _parse_int(value: Optional[str], default: int) -> int:
        if value is None:
            return default
        raw = str(value).strip()
        if raw == "":
            return default
        try:
            return int(raw)
        except ValueError:
            return default

    @classmethod
    def from_env(
        cls,
        dry_run_override: Optional[bool] = None,
        require_reddit: bool = False,  # No longer required since we use public scraping
    ) -> "BotConfig":
        telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        google_spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
        reddit_user_agent = os.getenv(
            "REDDIT_USER_AGENT", "rt-cert-program-utils/telegram-reply-bot"
        ).strip()

        required = [
            ("TELEGRAM_BOT_TOKEN", telegram_bot_token),
            ("GOOGLE_SHEETS_SPREADSHEET_ID", google_spreadsheet_id),
        ]
        missing = [k for k, v in required if not v]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        service_account_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "").strip() or None
        service_account_json_raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
        service_account_json = None
        if service_account_json_raw:
            service_account_json = json.loads(service_account_json_raw)
        elif not service_account_path:
            raise ValueError(
                "Provide GOOGLE_SERVICE_ACCOUNT_PATH or GOOGLE_SERVICE_ACCOUNT_JSON"
            )

        dry_run = (
            dry_run_override
            if dry_run_override is not None
            else cls._parse_bool(os.getenv("BOT_DRY_RUN"), default=False)
        )

        return cls(
            telegram_bot_token=telegram_bot_token,
            google_spreadsheet_id=google_spreadsheet_id,
            reddit_user_agent=reddit_user_agent,
            llm_model=os.getenv("BOT_REPLY_MODEL", "gpt-4o-mini").strip(),
            timezone=os.getenv("BOT_TIMEZONE", "Africa/Addis_Ababa").strip(),
            daily_hour=cls._parse_int(os.getenv("BOT_DAILY_HOUR"), 8),
            daily_minute=cls._parse_int(os.getenv("BOT_DAILY_MINUTE"), 0),
            poll_interval_minutes=cls._parse_int(
                os.getenv("BOT_POLL_INTERVAL_MINUTES"), 10
            ),
            dry_run=dry_run,
            google_service_account_path=service_account_path,
            google_service_account_json=service_account_json,
            posts_tab_name=_get_tab_name("BOT_POSTING_TAB", "PostingPlan"),
            teams_tab_name=_get_tab_name("BOT_TEAMS_TAB", "Teams"),
            reply_queue_tab_name=_get_tab_name("BOT_REPLY_QUEUE_TAB", "ReplyQueue"),
            state_tab_name=_get_tab_name("BOT_STATE_TAB", "State"),
        )


