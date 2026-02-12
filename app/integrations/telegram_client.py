from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)


def _telegram_retry(func):
    """Decorator that retries Telegram API calls on transient failures.

    Retries on:
      - 429 Too Many Requests (respects Retry-After)
      - 5xx server errors
      - Network / timeout errors
    Does NOT retry on 4xx client errors (except 429).
    """
    MAX_RETRIES = 3
    BACKOFF_BASE = 2.0

    def wrapper(*args, **kwargs):
        last_exc: Optional[Exception] = None
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except requests.HTTPError as exc:
                resp = exc.response
                if resp is not None:
                    if resp.status_code == 429:
                        retry_after = int(resp.headers.get("Retry-After", "5"))
                        logger.warning("Telegram rate-limited (429). Sleeping %ds (attempt %d/%d)",
                                       retry_after, attempt + 1, MAX_RETRIES)
                        time.sleep(retry_after)
                        last_exc = exc
                        continue
                    if resp.status_code >= 500:
                        wait = BACKOFF_BASE ** attempt
                        logger.warning("Telegram server error %d. Retrying in %.1fs (attempt %d/%d)",
                                       resp.status_code, wait, attempt + 1, MAX_RETRIES)
                        time.sleep(wait)
                        last_exc = exc
                        continue
                # 4xx (not 429) -> don't retry
                raise
            except (requests.ConnectionError, requests.Timeout) as exc:
                wait = BACKOFF_BASE ** attempt
                logger.warning("Telegram network error: %s. Retrying in %.1fs (attempt %d/%d)",
                               exc, wait, attempt + 1, MAX_RETRIES)
                time.sleep(wait)
                last_exc = exc

        if last_exc:
            raise last_exc
        raise RuntimeError("Telegram retry exhausted without an exception (should not happen)")

    return wrapper


class TelegramClient:
    def __init__(self, bot_token: str, timeout_seconds: int = 20):
        self.bot_token = bot_token
        self.timeout_seconds = timeout_seconds
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

    @_telegram_retry
    def send_message(
        self, chat_id: str, text: str, disable_web_page_preview: bool = True
    ) -> Dict[str, Any]:
        # Telegram has a 4096 character limit per message
        if len(text) > 4096:
            logger.warning("Message too long (%d chars), truncating to 4096", len(text))
            text = text[:4090] + "\n..."

        payload = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": disable_web_page_preview,
        }
        response = requests.post(
            f"{self.base_url}/sendMessage",
            json=payload,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            raise RuntimeError(f"Telegram API error: {data}")
        return data

    def send_message_safe(self, chat_id: str, text: str, **kwargs) -> bool:
        """Send a Telegram message, returning True on success and False on failure.
        Never raises -- logs the error instead."""
        try:
            self.send_message(chat_id=chat_id, text=text, **kwargs)
            return True
        except Exception as exc:
            logger.error("Failed to send Telegram message to %s: %s", chat_id, exc)
            return False

    @_telegram_retry
    def get_updates(self, offset: Optional[int] = None, timeout: int = 0) -> Dict[str, Any]:
        params: Dict[str, Any] = {"timeout": timeout}
        if offset is not None:
            params["offset"] = offset
        response = requests.get(
            f"{self.base_url}/getUpdates",
            params=params,
            timeout=self.timeout_seconds + max(timeout, 0),
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            raise RuntimeError(f"Telegram API error: {data}")
        return data

    @_telegram_retry
    def get_me(self) -> Dict[str, Any]:
        response = requests.get(
            f"{self.base_url}/getMe",
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            raise RuntimeError(f"Telegram API error: {data}")
        return data
