from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

import gspread

from app.config import BotConfig


DEFAULT_HEADERS = {
    "Teams": ["team_id", "member_name", "telegram_user_id", "is_active"],
    "PostingPlan": [
        "post_id",
        "team_id",
        "scheduled_date",
        "scheduled_time",
        "poster_member_name",
        "post_content",
        "reddit_post_url",
        "status",
        "last_notified_at",
    ],
    "ReplyQueue": [
        "reply_task_id",
        "post_id",
        "reddit_comment_id",
        "comment_author",
        "comment_url",
        "assigned_member_name",
        "reply_suggestion",
        "approval_status",
        "status",
        "created_at",
        "sent_at",
        "approved_at",
        "reply_posted_at",
        "reply_url",
    ],
    "Metrics": [
        "metric_id",
        "post_id",
        "reddit_post_url",
        "post_title",
        "post_created_at",
        "post_upvotes",
        "post_comments_count",
        "comment_id",
        "comment_author",
        "comment_created_at",
        "comment_upvotes",
        "reply_task_id",
        "reply_author",
        "reply_posted_at",
        "reply_upvotes",
        "response_time_hours",
        "assigned_member_name",
        "team_id",
        "metric_date",
        "updated_at",
    ],
    "TestPosts": [
        "test_id",
        "triggered_by",
        "test_topic",
        "reddit_post_url",
        "status",
        "created_at",
        "url_submitted_at",
        "last_polled_at",
        "comments_sent",
    ],
    "State": ["state_key", "state_value", "updated_at"],
}


@dataclass
class SheetsRowRef:
    row_number: int
    values: Dict[str, str]


class GoogleSheetsClient:
    def __init__(self, config: BotConfig):
        if config.google_service_account_json:
            gc = gspread.service_account_from_dict(config.google_service_account_json)
        else:
            gc = gspread.service_account(filename=config.google_service_account_path)
        self.config = config
        self._spreadsheet = gc.open_by_key(config.google_spreadsheet_id)

    @staticmethod
    def _now_utc_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def get_or_create_worksheet(self, name: str, headers: Optional[List[str]] = None):
        try:
            ws = self._spreadsheet.worksheet(name)
        except gspread.WorksheetNotFound:
            ws = self._spreadsheet.add_worksheet(title=name, rows=200, cols=30)
        if headers:
            existing = ws.row_values(1)
            if existing != headers:
                ws.clear()
                ws.append_row(headers)
        return ws

    def ensure_default_schema(self) -> None:
        mapping = {
            self.config.teams_tab_name: DEFAULT_HEADERS["Teams"],
            self.config.posts_tab_name: DEFAULT_HEADERS["PostingPlan"],
            self.config.reply_queue_tab_name: DEFAULT_HEADERS["ReplyQueue"],
            self.config.state_tab_name: DEFAULT_HEADERS["State"],
            self.config.metrics_tab_name: DEFAULT_HEADERS["Metrics"],
            self.config.test_posts_tab_name: DEFAULT_HEADERS["TestPosts"],
        }
        for tab, headers in mapping.items():
            self.get_or_create_worksheet(tab, headers=headers)

    def read_rows(self, tab_name: str) -> List[Dict[str, str]]:
        ws = self.get_or_create_worksheet(tab_name)
        rows = ws.get_all_records(default_blank="")
        return [{k: str(v).strip() if v is not None else "" for k, v in row.items()} for row in rows]

    def get_rows_with_ref(self, tab_name: str) -> List[SheetsRowRef]:
        ws = self.get_or_create_worksheet(tab_name)
        values = ws.get_all_values()
        if not values:
            return []
        headers = values[0]
        refs: List[SheetsRowRef] = []
        for idx, row in enumerate(values[1:], start=2):
            padded = row + ([""] * (len(headers) - len(row)))
            refs.append(
                SheetsRowRef(
                    row_number=idx,
                    values={headers[i]: str(padded[i]).strip() for i in range(len(headers))},
                )
            )
        return refs

    def append_row(self, tab_name: str, row_dict: Dict[str, str]) -> None:
        ws = self.get_or_create_worksheet(tab_name)
        headers = ws.row_values(1)
        if not headers:
            headers = list(row_dict.keys())
            ws.append_row(headers)
        ws.append_row([row_dict.get(h, "") for h in headers])

    def update_rows_by_id(self, tab_name: str, id_column: str, id_value: str, updates: Dict[str, str]) -> int:
        ws = self.get_or_create_worksheet(tab_name)
        refs = self.get_rows_with_ref(tab_name)
        headers = ws.row_values(1)
        count = 0
        for ref in refs:
            if ref.values.get(id_column, "") != id_value:
                continue
            for field, val in updates.items():
                if field not in headers:
                    continue
                col = headers.index(field) + 1
                ws.update_cell(ref.row_number, col, val)
            count += 1
        return count

    def get_state(self) -> Dict[str, str]:
        rows = self.read_rows(self.config.state_tab_name)
        return {row.get("state_key", ""): row.get("state_value", "") for row in rows if row.get("state_key")}

    def set_state(self, state_key: str, state_value: str) -> None:
        ws = self.get_or_create_worksheet(self.config.state_tab_name, headers=DEFAULT_HEADERS["State"])
        refs = self.get_rows_with_ref(self.config.state_tab_name)
        headers = ws.row_values(1)
        value_col = headers.index("state_value") + 1
        updated_col = headers.index("updated_at") + 1
        for ref in refs:
            if ref.values.get("state_key") == state_key:
                ws.update_cell(ref.row_number, value_col, state_value)
                ws.update_cell(ref.row_number, updated_col, self._now_utc_iso())
                return
        ws.append_row([state_key, state_value, self._now_utc_iso()])

    def known_reply_comment_ids(self) -> set[str]:
        rows = self.read_rows(self.config.reply_queue_tab_name)
        return {row.get("reddit_comment_id", "") for row in rows if row.get("reddit_comment_id")}

    def mark_post_notified(self, post_id: str, status: str = "reminded") -> None:
        self.update_rows_by_id(
            self.config.posts_tab_name,
            "post_id",
            post_id,
            {"status": status, "last_notified_at": self._now_utc_iso()},
        )

    def append_reply_task(self, row: Dict[str, str]) -> None:
        self.append_row(self.config.reply_queue_tab_name, row)
    
    def append_metric(self, row: Dict[str, str]) -> None:
        """Append a metric row to the Metrics tab."""
        self.append_row(self.config.metrics_tab_name, row)
    
    def update_reply_task_reply_info(self, task_id: str, reply_url: str, reply_posted_at: str) -> bool:
        """Update reply task with posted reply information."""
        ws = self.get_or_create_worksheet(
            self.config.reply_queue_tab_name,
            headers=DEFAULT_HEADERS["ReplyQueue"]
        )
        headers = ws.row_values(1)
        if "reply_task_id" not in headers:
            return False
        
        task_col = headers.index("reply_task_id") + 1
        reply_url_col = headers.index("reply_url") + 1 if "reply_url" in headers else None
        reply_posted_col = headers.index("reply_posted_at") + 1 if "reply_posted_at" in headers else None
        
        all_values = ws.get_all_values()
        if len(all_values) <= 1:
            return False
        
        for r in range(2, len(all_values) + 1):
            task_id_value = str(ws.cell(r, task_col).value or "").strip()
            if task_id_value == task_id:
                if reply_url_col:
                    ws.update_cell(r, reply_url_col, reply_url)
                if reply_posted_col:
                    ws.update_cell(r, reply_posted_col, reply_posted_at)
                return True
        return False
    
    def update_reply_task_approval(self, task_id: str, approval_status: str) -> bool:
        """Update approval status of a reply task."""
        ws = self.get_or_create_worksheet(
            self.config.reply_queue_tab_name, 
            headers=DEFAULT_HEADERS["ReplyQueue"]
        )
        headers = ws.row_values(1)
        if "reply_task_id" not in headers or "approval_status" not in headers:
            return False
        
        task_col = headers.index("reply_task_id") + 1
        approval_col = headers.index("approval_status") + 1
        
        all_values = ws.get_all_values()
        if len(all_values) <= 1:
            return False
        
        for r in range(2, len(all_values) + 1):
            task_id_value = str(ws.cell(r, task_col).value or "").strip()
            if task_id_value == task_id:
                ws.update_cell(r, approval_col, approval_status)
                if approval_status == "approved" and "approved_at" in headers:
                    approved_col = headers.index("approved_at") + 1
                    ws.update_cell(r, approved_col, self._now_utc_iso())
                return True
        return False

    def update_team_member_telegram_id(self, member_name: str, telegram_user_id: str) -> bool:
        ws = self.get_or_create_worksheet(self.config.teams_tab_name, headers=DEFAULT_HEADERS["Teams"])
        headers = ws.row_values(1)
        if "member_name" not in headers or "telegram_user_id" not in headers:
            return False
        name_col = headers.index("member_name") + 1
        tg_col = headers.index("telegram_user_id") + 1
        
        # Get all values to determine row count
        all_values = ws.get_all_values()
        if len(all_values) <= 1:  # Only header or empty
            return False
        
        # Iterate through data rows (skip header at index 0)
        for r in range(2, len(all_values) + 1):
            name = str(ws.cell(r, name_col).value or "").strip()
            if name.lower() == member_name.strip().lower():
                ws.update_cell(r, tg_col, telegram_user_id)
                return True
        return False

    @staticmethod
    def filter_rows(rows: Iterable[Dict[str, str]], key: str, values: set[str]) -> List[Dict[str, str]]:
        normalized = {v.strip().lower() for v in values}
        return [row for row in rows if str(row.get(key, "")).strip().lower() in normalized]


