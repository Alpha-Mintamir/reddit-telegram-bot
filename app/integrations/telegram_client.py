from __future__ import annotations

from typing import Any, Dict, Optional

import requests


class TelegramClient:
    def __init__(self, bot_token: str, timeout_seconds: int = 20):
        self.bot_token = bot_token
        self.timeout_seconds = timeout_seconds
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

    def send_message(
        self, chat_id: str, text: str, disable_web_page_preview: bool = True
    ) -> Dict[str, Any]:
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




