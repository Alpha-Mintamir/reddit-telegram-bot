from __future__ import annotations

import argparse
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from app.config import BotConfig
from app.integrations.google_sheets_client import GoogleSheetsClient
from app.integrations.reddit_client import RedditClient
from app.integrations.telegram_client import TelegramClient
from app.workflow.reply_assignment import (
    build_team_members,
    filter_unseen_comments,
    get_next_member,
)
from app.workflow.reply_generator import generate_reply_suggestion, suggestion_signature


@dataclass
class RuntimeContext:
    config: BotConfig
    sheets: GoogleSheetsClient
    reddit: Optional[RedditClient]
    telegram: TelegramClient


def _now_local(config: BotConfig) -> datetime:
    return datetime.now(ZoneInfo(config.timezone))


def _today_iso(config: BotConfig) -> str:
    return _now_local(config).date().isoformat()


def _to_float(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _send_or_print(ctx: RuntimeContext, chat_id: str, text: str) -> None:
    if ctx.config.dry_run:
        print(f"[DRY-RUN] Telegram message to {chat_id}:\n{text}\n")
        return
    ctx.telegram.send_message(chat_id=chat_id, text=text)


def _normalize_username(value: Optional[str]) -> str:
    if not value:
        return ""
    return value.strip().lstrip("@").lower()


def _build_member_lookup(teams_rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    lookup: Dict[str, Dict[str, str]] = {}
    for row in teams_rows:
        name = row.get("member_name", "").strip()
        if name:
            lookup[name] = row
    return lookup


def collect_telegram_ids_once(ctx: RuntimeContext) -> int:
    teams_rows = ctx.sheets.read_rows(ctx.config.teams_tab_name)
    username_to_member: Dict[str, str] = {}
    for row in teams_rows:
        username = _normalize_username(row.get("telegram_user_id", ""))
        member_name = row.get("member_name", "").strip()
        if username and member_name:
            username_to_member[username] = member_name
            print(f"  Mapped: @{username} -> {member_name}")

    print(f"Loaded {len(username_to_member)} username mappings from sheet.")

    state = ctx.sheets.get_state()
    offset_key = "telegram_updates_offset"
    offset_raw = state.get(offset_key, "")
    offset = int(offset_raw) if str(offset_raw).isdigit() else None

    updates_resp = ctx.telegram.get_updates(offset=offset, timeout=0)
    updates = updates_resp.get("result", []) if isinstance(updates_resp, dict) else []
    if not updates:
        print("No new Telegram updates found.")
        return 0

    processed = 0
    max_update_id = offset or 0
    for update in updates:
        update_id = int(update.get("update_id", 0))
        max_update_id = max(max_update_id, update_id)

        message = update.get("message") or {}
        text = str(message.get("text", "")).strip()
        from_user = message.get("from") or {}
        chat = message.get("chat") or {}
        username = _normalize_username(from_user.get("username"))
        first_name = str(from_user.get("first_name", "")).strip()
        chat_id = str(chat.get("id", "")).strip()

        if not text.startswith("/start") or not chat_id:
            continue

        print(f"Processing /start from: username=@{username}, first_name={first_name}, chat_id={chat_id}")
        print(f"  Normalized username: '{username}' (len={len(username)})")
        print(f"  Looking in username_to_member keys: {list(username_to_member.keys())}")

        member_name = username_to_member.get(username, "")
        print(f"  Direct username lookup result: '{member_name}'")
        
        # Fallback: try matching by first_name if username doesn't match
        if not member_name and first_name:
            first_name_lower = first_name.lower()
            print(f"  Trying first_name fallback: '{first_name}' (lower: '{first_name_lower}')")
            for row in teams_rows:
                sheet_name = row.get("member_name", "").strip()
                if sheet_name.lower() == first_name_lower:
                    member_name = sheet_name
                    print(f"  Matched by first_name: {first_name} -> {member_name}")
                    break

        if member_name:
            if not ctx.config.dry_run:
                ctx.sheets.update_team_member_telegram_id(member_name, chat_id)
            _send_or_print(
                ctx,
                chat_id,
                f"Hi {member_name}, your Telegram ID is linked successfully.",
            )
            processed += 1
        else:
            print(f"  No match found. Available usernames: {list(username_to_member.keys())}")
            _send_or_print(
                ctx,
                chat_id,
                f"You are not mapped yet in the team sheet. Your Telegram username is @{username or '(none)'}. Please share your @username with the admin.",
            )

    if not ctx.config.dry_run:
        ctx.sheets.set_state(offset_key, str(max_update_id + 1))
    print(f"Collected/confirmed Telegram IDs for {processed} member(s).")
    return processed


def send_daily_posting_reminders(ctx: RuntimeContext) -> int:
    today = _today_iso(ctx.config)
    teams_rows = ctx.sheets.read_rows(ctx.config.teams_tab_name)
    posts_rows = ctx.sheets.read_rows(ctx.config.posts_tab_name)
    member_lookup = _build_member_lookup(teams_rows)
    count = 0

    for post in posts_rows:
        if post.get("scheduled_date", "").strip() != today:
            continue
        status = post.get("status", "").strip().lower()
        if status in {"done", "posted"}:
            continue
        poster_name = post.get("poster_member_name", "").strip()
        poster = member_lookup.get(poster_name)
        if not poster:
            continue
        chat_id = poster.get("telegram_user_id", "").strip()
        if not chat_id:
            continue
        post_id = post.get("post_id", "").strip() or "(missing-post-id)"
        post_content = post.get("post_content", "").strip()
        scheduled_time = post.get("scheduled_time", "").strip() or "today"
        message = (
            f"Posting reminder\n\nPost ID: {post_id}\nScheduled: {today} {scheduled_time}\n\n"
            f"Post content:\n{post_content}"
        )
        _send_or_print(ctx, chat_id=chat_id, text=message)
        if not ctx.config.dry_run and post.get("post_id"):
            ctx.sheets.mark_post_notified(post["post_id"])
        count += 1
    return count


def poll_comments_and_dispatch_replies(ctx: RuntimeContext) -> int:
    if ctx.reddit is None:
        raise RuntimeError("Reddit client is not initialized.")

    teams_rows = ctx.sheets.read_rows(ctx.config.teams_tab_name)
    posts_rows = ctx.sheets.read_rows(ctx.config.posts_tab_name)
    reply_rows = ctx.sheets.read_rows(ctx.config.reply_queue_tab_name)
    state = ctx.sheets.get_state()
    known_comment_ids = ctx.sheets.known_reply_comment_ids()

    team_members = build_team_members(teams_rows)
    recent_by_post: Dict[str, List[str]] = {}
    for row in reply_rows:
        pid = row.get("post_id", "")
        txt = row.get("reply_suggestion", "")
        if pid and txt:
            recent_by_post.setdefault(pid, []).append(txt)

    active_posts = [
        p
        for p in posts_rows
        if p.get("reddit_post_url", "").strip()
        and p.get("status", "").strip().lower() not in {"done", "cancelled"}
    ]

    sent_count = 0
    for post in active_posts:
        post_id = post.get("post_id", "").strip()
        team_id = post.get("team_id", "").strip()
        reddit_post_url = post.get("reddit_post_url", "").strip()
        if not post_id or not team_id or not reddit_post_url:
            continue

        min_created = _to_float(state.get(f"last_seen_created_utc_{post_id}", "0"), 0.0)
        comments = ctx.reddit.fetch_new_comments(
            post_url=reddit_post_url,
            known_comment_ids=known_comment_ids,
            min_created_utc=min_created,
        )
        comments = filter_unseen_comments(comments, known_comment_ids)
        if not comments:
            continue

        post_context = ctx.reddit.get_submission_context(reddit_post_url)
        latest_seen = min_created

        for comment in comments:
            latest_seen = max(latest_seen, _to_float(comment.get("created_utc", "0")))
            member, state = get_next_member(team_id, team_members, state)
            member_name = member.get("member_name", "").strip()
            chat_id = member.get("telegram_user_id", "").strip()
            if not chat_id:
                continue
            recent = recent_by_post.get(post_id, [])
            suggestion = generate_reply_suggestion(
                llm_model=ctx.config.llm_model,
                post_context=post_context,
                comment_context=comment,
                recent_suggestions=recent,
            )
            signature = suggestion_signature(suggestion)
            message = (
                f"Reply task assigned\n\nPost ID: {post_id}\nAssigned to: {member_name}\n"
                f"Comment by u/{comment.get('author', '')}\nComment URL: {comment.get('comment_url', '')}\n\n"
                f"Suggested reply:\n{suggestion}"
            )
            _send_or_print(ctx, chat_id=chat_id, text=message)

            task_row = {
                "reply_task_id": str(uuid.uuid4()),
                "post_id": post_id,
                "reddit_comment_id": comment.get("comment_id", ""),
                "comment_author": comment.get("author", ""),
                "comment_url": comment.get("comment_url", ""),
                "assigned_member_name": member_name,
                "reply_suggestion": suggestion,
                "status": "dry_run_sent" if ctx.config.dry_run else "sent",
                "created_at": datetime.utcnow().isoformat(),
                "sent_at": datetime.utcnow().isoformat(),
            }
            if not ctx.config.dry_run:
                ctx.sheets.append_reply_task(task_row)
                known_comment_ids.add(comment.get("comment_id", ""))
                recent_by_post.setdefault(post_id, []).append(suggestion)
                ctx.sheets.set_state(f"last_reply_signature_{post_id}", signature)
            sent_count += 1

        if not ctx.config.dry_run:
            ctx.sheets.set_state(f"last_seen_created_utc_{post_id}", str(latest_seen))
            for key, value in state.items():
                if key.startswith("reply_cursor_team_"):
                    ctx.sheets.set_state(key, value)

    return sent_count


def run_once(ctx: RuntimeContext) -> None:
    reminders = send_daily_posting_reminders(ctx)
    replies = poll_comments_and_dispatch_replies(ctx)
    print(f"Run complete: reminders={reminders}, reply_tasks={replies}")


def should_run_daily_reminders(last_run_date: str, config: BotConfig) -> bool:
    return last_run_date != _today_iso(config)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Telegram Reddit reply bot")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--mode", choices=["once", "daemon", "collect-ids"], default="once")
    args = parser.parse_args()

    require_reddit = args.mode != "collect-ids"
    config = BotConfig.from_env(dry_run_override=args.dry_run, require_reddit=require_reddit)
    sheets = GoogleSheetsClient(config)
    sheets.ensure_default_schema()
    telegram = TelegramClient(config.telegram_bot_token)

    if args.mode == "collect-ids":
        ctx = RuntimeContext(config=config, sheets=sheets, reddit=None, telegram=telegram)
        collect_telegram_ids_once(ctx)
        return

    ctx = RuntimeContext(
        config=config,
        sheets=sheets,
        reddit=RedditClient(config),
        telegram=telegram,
    )

    if args.mode == "once":
        run_once(ctx)
        return

    print(f"Daemon mode: poll every {config.poll_interval_minutes} minute(s)")
    last_daily_key = "last_daily_reminder_date"
    while True:
        try:
            state = sheets.get_state()
            last_daily = state.get(last_daily_key, "")
            now = _now_local(config)
            if (
                should_run_daily_reminders(last_daily, config)
                and now.hour >= config.daily_hour
                and now.minute >= config.daily_minute
            ):
                reminders = send_daily_posting_reminders(ctx)
                print(f"Daily reminders sent: {reminders}")
                if not config.dry_run:
                    sheets.set_state(last_daily_key, date.today().isoformat())
            replies = poll_comments_and_dispatch_replies(ctx)
            print(f"Reply tasks dispatched: {replies}")
        except Exception as exc:
            print(f"Bot loop error: {exc}")
        time.sleep(max(config.poll_interval_minutes, 1) * 60)


if __name__ == "__main__":
    main()


