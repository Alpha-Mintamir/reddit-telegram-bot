from __future__ import annotations

import argparse
import logging
import random
import re
import time
import traceback
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from app.config import BotConfig
from app.integrations.google_sheets_client import GoogleSheetsClient, DEFAULT_HEADERS
from app.integrations.reddit_client import RedditClient, RedditPostDeleted
from app.integrations.telegram_client import TelegramClient
from app.workflow.reply_assignment import (
    build_team_members,
    filter_unseen_comments,
    get_next_member,
)
from app.workflow.reply_generator import (
    generate_reply_suggestion,
    suggestion_signature,
    check_content_safety,
    FALLBACK_REPLY,
)

logger = logging.getLogger(__name__)

# ── Logging setup (early, so all modules benefit) ────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# ══════════════════════════════════════════════════════════════════════════
# Runtime context
# ══════════════════════════════════════════════════════════════════════════

@dataclass
class RuntimeContext:
    config: BotConfig
    sheets: GoogleSheetsClient
    reddit: Optional[RedditClient]
    telegram: TelegramClient


# ══════════════════════════════════════════════════════════════════════════
# Small helpers
# ══════════════════════════════════════════════════════════════════════════

def _now_local(config: BotConfig) -> datetime:
    return datetime.now(ZoneInfo(config.timezone))


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _today_iso(config: BotConfig) -> str:
    return _now_local(config).date().isoformat()


def _to_float(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _send_or_print(ctx: RuntimeContext, chat_id: str, text: str) -> bool:
    """Send a Telegram message (or print in dry-run). Returns success bool."""
    if ctx.config.dry_run:
        print(f"[DRY-RUN] Telegram message to {chat_id}:\n{text}\n")
        return True
    return ctx.telegram.send_message_safe(chat_id=chat_id, text=text)


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


def _build_chatid_to_member(teams_rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    """Reverse lookup: Telegram chat_id (numeric) -> member row."""
    lookup: Dict[str, Dict[str, str]] = {}
    for row in teams_rows:
        tg_id = str(row.get("telegram_user_id", "")).strip()
        if tg_id.isdigit():
            lookup[tg_id] = row
    return lookup


# Regex that matches Reddit post URLs
_REDDIT_URL_RE = re.compile(
    r"https?://(?:www\.)?reddit\.com/r/\w+/comments/\w+",
    re.IGNORECASE,
)


# ══════════════════════════════════════════════════════════════════════════
# Escalation helpers
# ══════════════════════════════════════════════════════════════════════════

def _find_alpha_telegram_id(teams_rows: List[Dict[str, str]], config: BotConfig) -> Optional[str]:
    """Find Alpha's *numeric* Telegram chat ID from Teams sheet."""
    alpha_user = config.alpha_username.lower().strip().lstrip("@")
    for row in teams_rows:
        username = str(row.get("telegram_user_id", "")).strip()
        member_name = str(row.get("member_name", "")).strip().lower()
        # The telegram_user_id may already be the numeric chat-id once linked
        if username.isdigit():
            norm = _normalize_username(row.get("member_name", ""))
            # Match by name containing 'alpha' or by the configured alpha_username
            if alpha_user in member_name or member_name == "alpha":
                return username
    # Fallback: search by stored numeric IDs where username matches
    for row in teams_rows:
        tg_id = str(row.get("telegram_user_id", "")).strip()
        if tg_id.isdigit():
            member_name = str(row.get("member_name", "")).strip().lower()
            if member_name == "alpha" or alpha_user in member_name:
                return tg_id
    return None


def _escalate_to_alpha(
    ctx: RuntimeContext,
    teams_rows: List[Dict[str, str]],
    subject: str,
    details: str,
) -> bool:
    """Send an escalation alert to Alpha via Telegram.
    Returns True if message was sent, False otherwise."""
    alpha_id = _find_alpha_telegram_id(teams_rows, ctx.config)
    if not alpha_id:
        logger.error("ESCALATION FAILED (Alpha ID not found): %s -- %s", subject, details)
        return False

    msg = (
        f"[!] ESCALATION ALERT\n\n"
        f"Issue: {subject}\n\n"
        f"{details}\n\n"
        f"Please take action."
    )
    return _send_or_print(ctx, chat_id=alpha_id, text=msg)


# ══════════════════════════════════════════════════════════════════════════
# Telegram message processing  (commands, URLs, /start, approvals)
# ══════════════════════════════════════════════════════════════════════════

_HELP_TEXT = (
    "Available commands:\n\n"
    "/start - Link your Telegram account\n"
    "/posted <reddit_url> - Submit a Reddit post URL\n"
    "  (or just paste the URL directly)\n"
    "/mystatus - See your pending tasks\n"
    "/help - Show this help message\n\n"
    "Admin commands:\n"
    "/approve_<task_id> - Approve a reply suggestion\n"
    "/reject_<task_id> - Reject a reply suggestion\n\n"
    "Test mode (Admin only):\n"
    "/test - Start a test: get a topic, post it, and see live comments"
)


def process_telegram_updates(ctx: RuntimeContext) -> int:
    """Process all pending Telegram messages: /start, URLs, approvals, etc."""
    teams_rows = ctx.sheets.read_rows(ctx.config.teams_tab_name)

    # Build lookups
    username_to_member: Dict[str, str] = {}
    for row in teams_rows:
        username = _normalize_username(row.get("telegram_user_id", ""))
        member_name = row.get("member_name", "").strip()
        if username and member_name:
            username_to_member[username] = member_name

    chatid_to_member = _build_chatid_to_member(teams_rows)

    print(f"Loaded {len(username_to_member)} username mappings, "
          f"{len(chatid_to_member)} chat-ID mappings from sheet.")

    # Fetch updates
    state = ctx.sheets.get_state()
    offset_key = "telegram_updates_offset"
    offset_raw = state.get(offset_key, "")
    offset = int(offset_raw) if str(offset_raw).isdigit() else None

    updates_resp = ctx.telegram.get_updates(offset=offset, timeout=0)
    updates = updates_resp.get("result", []) if isinstance(updates_resp, dict) else []
    if not updates:
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

        if not chat_id or not text:
            continue

        # ── Route message to handler ────────────────────────────────
        if text.startswith("/approve_") or text.startswith("/reject_"):
            _handle_approval_command(ctx, text, chat_id)

        elif text.startswith("/start"):
            _handle_start(ctx, teams_rows, username_to_member, username, first_name, chat_id)
            processed += 1

        elif text.startswith("/posted"):
            # Check if this URL is for a pending test post first
            url_match = _REDDIT_URL_RE.search(text)
            if url_match and _try_link_test_post_url(ctx, chat_id, url_match.group(0).split("?")[0].rstrip("/") + "/"):
                pass  # Handled as test post
            else:
                _handle_posted_command(ctx, teams_rows, chatid_to_member, text, chat_id)

        elif text.startswith("/test_cancel"):
            _handle_test_cancel(ctx, teams_rows, chat_id)

        elif text.startswith("/test"):
            _handle_test_command(ctx, teams_rows, chat_id)

        elif text.startswith("/mystatus"):
            _handle_mystatus(ctx, chatid_to_member, chat_id)

        elif text.startswith("/help"):
            _send_or_print(ctx, chat_id, _HELP_TEXT)

        elif _REDDIT_URL_RE.search(text):
            # Raw Reddit URL pasted -- check for test post first
            raw_url = _REDDIT_URL_RE.search(text).group(0).split("?")[0].rstrip("/") + "/"
            if not _try_link_test_post_url(ctx, chat_id, raw_url):
                _handle_posted_command(ctx, teams_rows, chatid_to_member, text, chat_id)

        else:
            # Unknown message -- gently guide
            _send_or_print(
                ctx, chat_id,
                "I didn't understand that. Send /help to see available commands.\n\n"
                "To submit your Reddit post URL, just paste the link here!"
            )

    if not ctx.config.dry_run:
        ctx.sheets.set_state(offset_key, str(max_update_id + 1))

    return processed


# -- /start handler ----------------------------------------------------------

def _handle_start(
    ctx: RuntimeContext,
    teams_rows: List[Dict[str, str]],
    username_to_member: Dict[str, str],
    username: str,
    first_name: str,
    chat_id: str,
) -> None:
    """Link a team member's Telegram ID."""
    print(f"Processing /start from: username=@{username}, "
          f"first_name={first_name}, chat_id={chat_id}")

    member_name = username_to_member.get(username, "")
    if not member_name and first_name:
        for row in teams_rows:
            sheet_name = row.get("member_name", "").strip()
            if sheet_name.lower() == first_name.lower():
                member_name = sheet_name
                break

    if member_name:
        if not ctx.config.dry_run:
            ctx.sheets.update_team_member_telegram_id(member_name, chat_id)
        _send_or_print(
            ctx, chat_id,
            f"Hi {member_name}, your Telegram ID is linked successfully!\n\n"
            f"When you post on Reddit, just paste the URL here and "
            f"I'll take care of the rest.\n\n"
            f"Send /help to see all commands."
        )
    else:
        _send_or_print(
            ctx, chat_id,
            f"You are not mapped yet in the team sheet. "
            f"Your Telegram username is @{username or '(none)'}. "
            f"Please share your @username with the admin.",
        )


# -- /approve & /reject handler ----------------------------------------------

def _handle_approval_command(ctx: RuntimeContext, text: str, chat_id: str) -> None:
    """Process /approve_<id> or /reject_<id> commands."""
    is_approve = text.startswith("/approve_")
    task_id = text.split("_", 1)[1].strip() if "_" in text else ""
    if not task_id:
        return

    approval_status = "approved" if is_approve else "rejected"
    if ctx.sheets.update_reply_task_approval(task_id, approval_status):
        action = "approved" if is_approve else "rejected"
        ctx.telegram.send_message_safe(
            chat_id=chat_id,
            text=f"Reply task {task_id} {action} successfully!"
        )
        logger.info("Task %s %s by Alpha", task_id, action)
    else:
        ctx.telegram.send_message_safe(
            chat_id=chat_id,
            text=f"Could not find task {task_id}"
        )


# -- /posted + raw URL handler -----------------------------------------------

def _handle_posted_command(
    ctx: RuntimeContext,
    teams_rows: List[Dict[str, str]],
    chatid_to_member: Dict[str, Dict[str, str]],
    text: str,
    chat_id: str,
) -> None:
    """Handle when a user sends a Reddit URL (via /posted or raw paste).

    Flow:
    1. Extract the Reddit URL from the message
    2. Identify the sender (chat_id -> member)
    3. Find their pending/reminded post in PostingPlan
    4. Update the sheet with the URL + status=posted
    5. Confirm to the user
    """
    # 1. Extract URL
    url_match = _REDDIT_URL_RE.search(text)
    if not url_match:
        _send_or_print(
            ctx, chat_id,
            "I couldn't find a valid Reddit post URL in your message.\n\n"
            "Please send a link like:\n"
            "https://www.reddit.com/r/subreddit/comments/abc123/post_title/"
        )
        return

    reddit_url = url_match.group(0)
    # Clean up: ensure it ends nicely (remove trailing fragments)
    reddit_url = reddit_url.split("?")[0].rstrip("/") + "/"

    # 2. Identify sender
    member_row = chatid_to_member.get(chat_id)
    if not member_row:
        _send_or_print(
            ctx, chat_id,
            "I don't recognize your Telegram account. "
            "Please send /start first to link your account."
        )
        return

    member_name = member_row.get("member_name", "").strip()
    logger.info("Reddit URL received from %s (chat %s): %s", member_name, chat_id, reddit_url)

    # 3. Find their pending post in PostingPlan
    posts_rows = ctx.sheets.read_rows(ctx.config.posts_tab_name)
    today = _today_iso(ctx.config)

    # Look for posts assigned to this member that need a URL
    candidate_posts = []
    for post in posts_rows:
        poster = post.get("poster_member_name", "").strip()
        status = post.get("status", "").strip().lower()
        existing_url = post.get("reddit_post_url", "").strip()

        # Match: same poster, not already posted, no URL yet
        if (poster.lower() == member_name.lower()
                and status not in {"done", "posted", "cancelled", "deleted"}
                and not existing_url):
            candidate_posts.append(post)

    if not candidate_posts:
        # Maybe they already have a URL posted -- check if they're re-submitting
        resubmit_candidates = [
            p for p in posts_rows
            if p.get("poster_member_name", "").strip().lower() == member_name.lower()
            and p.get("status", "").strip().lower() in {"posted", "reminded"}
            and p.get("scheduled_date", "").strip() == today
        ]
        if resubmit_candidates:
            post = resubmit_candidates[0]
            post_id = post.get("post_id", "")
            if not ctx.config.dry_run:
                ctx.sheets.update_rows_by_id(
                    ctx.config.posts_tab_name, "post_id", post_id,
                    {"reddit_post_url": reddit_url, "status": "posted"},
                )
            _send_or_print(
                ctx, chat_id,
                f"Updated! Post {post_id} URL has been updated to:\n{reddit_url}\n\n"
                f"The bot will now start monitoring for comments."
            )
            return

        _send_or_print(
            ctx, chat_id,
            f"Hi {member_name}, I couldn't find a pending post assigned to you.\n\n"
            f"If you believe this is a mistake, please contact the admin."
        )
        return

    # Pick the best candidate: prefer today's post, then the nearest upcoming one
    best_post = None
    for post in candidate_posts:
        sched = post.get("scheduled_date", "").strip()
        if sched == today:
            best_post = post
            break
    if not best_post:
        # Take the one with the closest scheduled date
        candidate_posts.sort(key=lambda p: p.get("scheduled_date", "9999"))
        best_post = candidate_posts[0]

    post_id = best_post.get("post_id", "").strip()
    sched_date = best_post.get("scheduled_date", "").strip()

    # 4. Update the sheet
    if not ctx.config.dry_run:
        ctx.sheets.update_rows_by_id(
            ctx.config.posts_tab_name, "post_id", post_id,
            {"reddit_post_url": reddit_url, "status": "posted"},
        )

    # 5. Confirm to user
    _send_or_print(
        ctx, chat_id,
        f"Got it, {member_name}! Your Reddit post URL has been saved.\n\n"
        f"Post ID: {post_id}\n"
        f"Scheduled: {sched_date}\n"
        f"URL: {reddit_url}\n\n"
        f"The bot will now monitor this post for comments and "
        f"send reply suggestions to your team. Great job!"
    )
    logger.info("Post %s URL updated by %s: %s", post_id, member_name, reddit_url)

    # Notify Alpha
    _escalate_to_alpha(
        ctx, teams_rows,
        "Post URL submitted",
        f"{member_name} has posted and submitted the URL for post {post_id}.\n"
        f"URL: {reddit_url}\n"
        f"Comment monitoring is now active.",
    )


# -- /mystatus handler -------------------------------------------------------

def _handle_mystatus(
    ctx: RuntimeContext,
    chatid_to_member: Dict[str, Dict[str, str]],
    chat_id: str,
) -> None:
    """Show the member their pending posts and assigned reply tasks."""
    member_row = chatid_to_member.get(chat_id)
    if not member_row:
        _send_or_print(ctx, chat_id, "I don't recognize you. Send /start first.")
        return

    member_name = member_row.get("member_name", "").strip()
    team_id = member_row.get("team_id", "").strip()

    # Pending posts (assigned to them as poster)
    posts_rows = ctx.sheets.read_rows(ctx.config.posts_tab_name)
    my_posts = [
        p for p in posts_rows
        if p.get("poster_member_name", "").strip().lower() == member_name.lower()
        and p.get("status", "").strip().lower() not in {"done", "cancelled", "deleted"}
    ]

    # Pending reply tasks (assigned to them)
    reply_rows = ctx.sheets.read_rows(ctx.config.reply_queue_tab_name)
    my_replies = [
        r for r in reply_rows
        if r.get("assigned_member_name", "").strip().lower() == member_name.lower()
        and r.get("status", "").strip().lower() in {"sent", "pending_approval", "approved"}
        and not r.get("reply_posted_at", "").strip()
    ]

    lines = [f"Status for {member_name} (Team {team_id}):\n"]

    # Posts section
    if my_posts:
        lines.append("-- Your Scheduled Posts --")
        for p in my_posts:
            pid = p.get("post_id", "?")
            sdate = p.get("scheduled_date", "?")
            status = p.get("status", "?")
            url = p.get("reddit_post_url", "").strip()
            if url:
                lines.append(f"  {pid} | {sdate} | {status} | URL submitted")
            else:
                lines.append(f"  {pid} | {sdate} | {status} | Waiting for URL")
        lines.append("")
    else:
        lines.append("No pending posts assigned to you.\n")

    # Reply tasks section
    if my_replies:
        lines.append("-- Your Pending Reply Tasks --")
        for r in my_replies:
            tid = r.get("reply_task_id", "?")[:8]
            author = r.get("comment_author", "?")
            status = r.get("status", "?")
            lines.append(f"  Task ...{tid} | Reply to u/{author} | {status}")
        lines.append("")
    else:
        lines.append("No pending reply tasks.\n")

    lines.append("Send /help for available commands.")
    _send_or_print(ctx, chat_id, "\n".join(lines))


# ══════════════════════════════════════════════════════════════════════════
# TEST MODE  (Admin-only: /test -> post -> live comment feed)
# ══════════════════════════════════════════════════════════════════════════

_TEST_TOPICS = [
    "What's one underrated tool or library in your stack that you wish more people knew about?",
    "For those who transitioned into tech from a non-CS background, what was your biggest 'aha' moment?",
    "What's a software engineering best practice that you think is actually overrated?",
    "How do you handle burnout when working on a long-running project?",
    "What's the most useful thing you learned in the last month that improved your workflow?",
    "If you could mass-delete one bad practice from every codebase, what would it be?",
    "What side project taught you the most about real-world development?",
    "What's a technology or framework that you initially hated but grew to love?",
    "How do you evaluate whether a new tech trend is worth adopting or just hype?",
    "What's your go-to strategy for debugging a problem you've never seen before?",
    "What advice would you give someone starting their first dev job?",
    "What common coding interview question do you think is completely useless?",
]


def _is_alpha(ctx: RuntimeContext, chat_id: str, teams_rows: List[Dict[str, str]]) -> bool:
    """Check if the chat_id belongs to Alpha."""
    alpha_id = _find_alpha_telegram_id(teams_rows, ctx.config)
    return alpha_id is not None and alpha_id == chat_id


def _handle_test_command(
    ctx: RuntimeContext,
    teams_rows: List[Dict[str, str]],
    chat_id: str,
) -> None:
    """Handle /test command: give Alpha a test topic to post on Reddit."""

    # Only Alpha can use /test
    if not _is_alpha(ctx, chat_id, teams_rows):
        _send_or_print(ctx, chat_id, "This command is for the admin only.")
        return

    # Check if there's already a pending test post (waiting for URL)
    test_rows = ctx.sheets.read_rows(ctx.config.test_posts_tab_name)
    pending_tests = [
        t for t in test_rows
        if t.get("status", "").strip().lower() == "waiting_for_url"
        and t.get("triggered_by", "").strip() == chat_id
    ]

    if pending_tests:
        existing = pending_tests[0]
        _send_or_print(
            ctx, chat_id,
            f"You already have a pending test!\n\n"
            f"Topic: {existing.get('test_topic', '')}\n\n"
            f"Post it on Reddit and paste the URL here.\n"
            f"Or send /test_cancel to cancel it and start a new one."
        )
        return

    # Pick a random topic
    topic = random.choice(_TEST_TOPICS)
    test_id = f"test_{uuid.uuid4().hex[:8]}"

    test_row = {
        "test_id": test_id,
        "triggered_by": chat_id,
        "test_topic": topic,
        "reddit_post_url": "",
        "status": "waiting_for_url",
        "created_at": _now_utc().isoformat(),
        "url_submitted_at": "",
        "last_polled_at": "",
        "comments_sent": "0",
    }

    if not ctx.config.dry_run:
        ctx.sheets.append_row(ctx.config.test_posts_tab_name, test_row)

    _send_or_print(
        ctx, chat_id,
        f"TEST MODE STARTED\n\n"
        f"Test ID: {test_id}\n\n"
        f"Here's your test topic:\n\n"
        f"\"{topic}\"\n\n"
        f"Steps:\n"
        f"1. Post this (or something similar) on a subreddit\n"
        f"2. Paste the Reddit URL here\n"
        f"3. I'll monitor it and send you every new comment live!\n\n"
        f"Send /test_cancel to cancel."
    )
    logger.info("Test mode started by %s: %s", chat_id, test_id)


def _handle_test_cancel(
    ctx: RuntimeContext,
    teams_rows: List[Dict[str, str]],
    chat_id: str,
) -> None:
    """Cancel a pending test post."""
    if not _is_alpha(ctx, chat_id, teams_rows):
        _send_or_print(ctx, chat_id, "This command is for the admin only.")
        return

    test_rows = ctx.sheets.read_rows(ctx.config.test_posts_tab_name)
    cancelled = 0
    for t in test_rows:
        status = t.get("status", "").strip().lower()
        if status in {"waiting_for_url", "monitoring"} and t.get("triggered_by", "").strip() == chat_id:
            test_id = t.get("test_id", "")
            if test_id and not ctx.config.dry_run:
                ctx.sheets.update_rows_by_id(
                    ctx.config.test_posts_tab_name, "test_id", test_id,
                    {"status": "cancelled"},
                )
                cancelled += 1

    if cancelled:
        _send_or_print(ctx, chat_id, f"Cancelled {cancelled} test(s). Send /test to start a new one.")
    else:
        _send_or_print(ctx, chat_id, "No active tests to cancel.")


def _try_link_test_post_url(
    ctx: RuntimeContext,
    chat_id: str,
    reddit_url: str,
) -> bool:
    """Try to match a Reddit URL to a pending test post for this user.

    Returns True if it was matched as a test post (caller should not
    handle it as a normal post).
    """
    test_rows = ctx.sheets.read_rows(ctx.config.test_posts_tab_name)
    pending = [
        t for t in test_rows
        if t.get("status", "").strip().lower() == "waiting_for_url"
        and t.get("triggered_by", "").strip() == chat_id
    ]

    if not pending:
        return False

    test_post = pending[0]
    test_id = test_post.get("test_id", "")

    if not ctx.config.dry_run:
        ctx.sheets.update_rows_by_id(
            ctx.config.test_posts_tab_name, "test_id", test_id,
            {
                "reddit_post_url": reddit_url,
                "status": "monitoring",
                "url_submitted_at": _now_utc().isoformat(),
            },
        )

    _send_or_print(
        ctx, chat_id,
        f"Test post URL saved!\n\n"
        f"Test ID: {test_id}\n"
        f"URL: {reddit_url}\n\n"
        f"I'm now monitoring this post. Every new comment will be "
        f"sent to you right here. Sit back and watch!\n\n"
        f"Send /test_cancel to stop monitoring."
    )
    logger.info("Test post URL linked: %s -> %s", test_id, reddit_url)
    return True


def poll_test_post_comments(ctx: RuntimeContext) -> int:
    """Poll comments on active test posts, generate reply suggestions,
    and send both the comment and the suggested reply to Alpha.

    This is separate from the normal workflow -- all replies go to
    Alpha directly, no team assignment, no approval queue.

    Returns count of comments sent.
    """
    if ctx.reddit is None:
        return 0

    test_rows = ctx.sheets.read_rows(ctx.config.test_posts_tab_name)
    active_tests = [
        t for t in test_rows
        if t.get("status", "").strip().lower() == "monitoring"
        and t.get("reddit_post_url", "").strip()
    ]

    if not active_tests:
        return 0

    state = ctx.sheets.get_state()
    total_sent = 0

    for test in active_tests:
        test_id = test.get("test_id", "")
        chat_id = test.get("triggered_by", "").strip()
        reddit_url = test.get("reddit_post_url", "").strip()
        prev_comments_sent = int(test.get("comments_sent", "0") or "0")

        if not chat_id or not reddit_url:
            continue

        # Check if post is still alive
        try:
            if not ctx.reddit.is_post_alive(reddit_url):
                logger.warning("Test post %s appears deleted", test_id)
                if not ctx.config.dry_run:
                    ctx.sheets.update_rows_by_id(
                        ctx.config.test_posts_tab_name, "test_id", test_id,
                        {"status": "deleted"},
                    )
                _send_or_print(
                    ctx, chat_id,
                    f"Your test post ({test_id}) appears to have been "
                    f"deleted or removed. Monitoring stopped."
                )
                continue
        except Exception:
            pass  # Continue polling anyway

        # Track which comments we've already sent for this test
        known_key = f"test_known_comments_{test_id}"
        known_ids_raw = state.get(known_key, "")
        known_ids = set(known_ids_raw.split(",")) if known_ids_raw else set()

        # Fetch all comments
        try:
            comments = ctx.reddit.fetch_new_comments(
                post_url=reddit_url,
                known_comment_ids=known_ids,
                min_created_utc=None,
            )
        except RedditPostDeleted:
            if not ctx.config.dry_run:
                ctx.sheets.update_rows_by_id(
                    ctx.config.test_posts_tab_name, "test_id", test_id,
                    {"status": "deleted"},
                )
            _send_or_print(ctx, chat_id, f"Test post {test_id} was deleted. Monitoring stopped.")
            continue
        except Exception as exc:
            logger.warning("Error polling test post %s: %s", test_id, exc)
            continue

        if not comments:
            # Update last_polled_at even if no new comments
            if not ctx.config.dry_run:
                ctx.sheets.update_rows_by_id(
                    ctx.config.test_posts_tab_name, "test_id", test_id,
                    {"last_polled_at": _now_utc().isoformat()},
                )
            continue

        # Fetch post context once for reply generation
        post_context: Optional[Dict[str, str]] = None
        try:
            post_context = ctx.reddit.get_submission_context(reddit_url)
        except Exception as exc:
            logger.warning("Could not get post context for test %s: %s", test_id, exc)

        # Collect recent suggestions for variety
        recent_suggestions: List[str] = []

        # Send each new comment + generated reply to Alpha
        new_ids = []
        for comment in comments:
            cid = comment.get("comment_id", "")
            author = comment.get("author", "[deleted]")
            body = comment.get("body", "")
            comment_url = comment.get("comment_url", "")

            if cid in known_ids:
                continue

            # ── Generate a reply suggestion ─────────────────────────
            suggestion = ""
            if post_context:
                try:
                    suggestion = generate_reply_suggestion(
                        llm_model=ctx.config.llm_model,
                        post_context=post_context,
                        comment_context=comment,
                        recent_suggestions=recent_suggestions,
                    )
                    # Safety check
                    safety_err = check_content_safety(suggestion)
                    if safety_err:
                        logger.warning("Test reply failed safety: %s", safety_err.reason)
                        suggestion = FALLBACK_REPLY
                    recent_suggestions.append(suggestion)
                except Exception as exc:
                    logger.warning("LLM error for test comment %s: %s", cid, exc)
                    suggestion = "(Could not generate reply -- LLM error)"

            # ── Build message for Alpha ─────────────────────────────
            msg_lines = [
                "NEW COMMENT on test post\n",
                f"Test ID: {test_id}",
                f"By: u/{author}",
                f"URL: {comment_url}\n",
                f"Comment:\n{body[:1000]}",
            ]

            if suggestion:
                msg_lines.extend([
                    "\n---",
                    "SUGGESTED REPLY:\n",
                    suggestion,
                    "\n---",
                    "Copy the reply above and post it on Reddit!",
                ])
            else:
                msg_lines.append(
                    "\n(No reply suggestion available -- post context could not be fetched)"
                )

            _send_or_print(ctx, chat_id, "\n".join(msg_lines))
            new_ids.append(cid)
            total_sent += 1

        # Persist known comment IDs
        if new_ids and not ctx.config.dry_run:
            known_ids.update(new_ids)
            # Remove empty strings
            known_ids.discard("")
            ctx.sheets.set_state(known_key, ",".join(known_ids))
            ctx.sheets.update_rows_by_id(
                ctx.config.test_posts_tab_name, "test_id", test_id,
                {
                    "last_polled_at": _now_utc().isoformat(),
                    "comments_sent": str(prev_comments_sent + len(new_ids)),
                },
            )

    return total_sent


# ══════════════════════════════════════════════════════════════════════════
# Daily posting reminders
# ══════════════════════════════════════════════════════════════════════════

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
            # Escalate: poster not found in Teams sheet
            _escalate_to_alpha(
                ctx, teams_rows,
                "Poster not found",
                f"Post '{post.get('post_id', '?')}' is scheduled today but poster "
                f"'{poster_name}' is not in the Teams sheet.",
            )
            continue
        chat_id = poster.get("telegram_user_id", "").strip()
        if not chat_id or not chat_id.isdigit():
            _escalate_to_alpha(
                ctx, teams_rows,
                "Poster has no Telegram ID",
                f"Post '{post.get('post_id', '?')}' is scheduled today but poster "
                f"'{poster_name}' has no linked Telegram ID. Ask them to /start the bot.",
            )
            continue
        post_id = post.get("post_id", "").strip() or "(missing-post-id)"
        post_content = post.get("post_content", "").strip()
        scheduled_time = post.get("scheduled_time", "").strip() or "today"
        message = (
            f"Posting reminder\n\n"
            f"Post ID: {post_id}\n"
            f"Scheduled: {today} {scheduled_time}\n\n"
            f"Post content:\n{post_content}\n\n"
            f"---\n"
            f"After you post on Reddit, just paste the URL here "
            f"and I'll start monitoring for comments automatically!"
        )
        _send_or_print(ctx, chat_id=chat_id, text=message)
        if not ctx.config.dry_run and post.get("post_id"):
            ctx.sheets.mark_post_notified(post["post_id"])
        count += 1
    return count


# ══════════════════════════════════════════════════════════════════════════
# Comment polling + reply dispatch  (with post-deletion & safety checks)
# ══════════════════════════════════════════════════════════════════════════

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
        p for p in posts_rows
        if p.get("reddit_post_url", "").strip()
        and p.get("status", "").strip().lower() not in {"done", "cancelled", "deleted"}
    ]

    sent_count = 0
    alpha_chat_id = _find_alpha_telegram_id(teams_rows, ctx.config)

    for post in active_posts:
        post_id = post.get("post_id", "").strip()
        team_id = post.get("team_id", "").strip()
        reddit_post_url = post.get("reddit_post_url", "").strip()
        if not post_id or not team_id or not reddit_post_url:
            continue

        # ── Check if Reddit post is still alive ─────────────────────
        try:
            if not ctx.reddit.is_post_alive(reddit_post_url):
                logger.warning("Post %s appears deleted: %s", post_id, reddit_post_url)
                if not ctx.config.dry_run:
                    ctx.sheets.update_rows_by_id(
                        ctx.config.posts_tab_name, "post_id", post_id,
                        {"status": "deleted"},
                    )
                _escalate_to_alpha(
                    ctx, teams_rows,
                    "Reddit post deleted/removed",
                    f"Post {post_id} ({reddit_post_url}) appears to have been "
                    f"deleted or removed by Reddit moderators. It has been marked "
                    f"as 'deleted' in the sheet and will no longer be polled.",
                )
                continue
        except Exception as exc:
            logger.warning("Error checking post health for %s: %s", post_id, exc)
            # Continue anyway -- we'll catch real errors on fetch_new_comments

        # ── Fetch new comments ──────────────────────────────────────
        min_created = _to_float(state.get(f"last_seen_created_utc_{post_id}", "0"), 0.0)
        try:
            comments = ctx.reddit.fetch_new_comments(
                post_url=reddit_post_url,
                known_comment_ids=known_comment_ids,
                min_created_utc=min_created,
            )
        except RedditPostDeleted:
            logger.warning("Post %s deleted while fetching comments", post_id)
            if not ctx.config.dry_run:
                ctx.sheets.update_rows_by_id(
                    ctx.config.posts_tab_name, "post_id", post_id,
                    {"status": "deleted"},
                )
            _escalate_to_alpha(
                ctx, teams_rows,
                "Reddit post deleted during polling",
                f"Post {post_id} ({reddit_post_url}) was deleted while fetching "
                f"comments. Marked as 'deleted'.",
            )
            continue
        except Exception as exc:
            logger.error("Error fetching comments for post %s: %s", post_id, exc)
            continue

        comments = filter_unseen_comments(comments, known_comment_ids)
        if not comments:
            continue

        # ── Fetch post context ──────────────────────────────────────
        try:
            post_context = ctx.reddit.get_submission_context(reddit_post_url)
        except RedditPostDeleted:
            logger.warning("Post %s deleted when getting context", post_id)
            if not ctx.config.dry_run:
                ctx.sheets.update_rows_by_id(
                    ctx.config.posts_tab_name, "post_id", post_id,
                    {"status": "deleted"},
                )
            continue
        except Exception as exc:
            logger.error("Error getting post context for %s: %s", post_id, exc)
            continue

        latest_seen = min_created

        for comment in comments:
            latest_seen = max(latest_seen, _to_float(comment.get("created_utc", "0")))

            # ── Assign team member ──────────────────────────────────
            try:
                member, state = get_next_member(team_id, team_members, state)
            except ValueError as exc:
                logger.error("No members for team %s: %s", team_id, exc)
                _escalate_to_alpha(
                    ctx, teams_rows,
                    "No active team members",
                    f"Team {team_id} has no active members. "
                    f"Cannot assign reply for post {post_id}.",
                )
                break

            member_name = member.get("member_name", "").strip()
            chat_id = member.get("telegram_user_id", "").strip()

            if not chat_id or not chat_id.isdigit():
                logger.warning("Member %s has no Telegram ID; escalating.", member_name)
                _escalate_to_alpha(
                    ctx, teams_rows,
                    "Member has no Telegram ID",
                    f"Member '{member_name}' (team {team_id}) was assigned a reply "
                    f"for post {post_id} but has no Telegram ID linked. "
                    f"Ask them to /start the bot.",
                )
                continue

            # ── Generate reply suggestion ───────────────────────────
            recent = recent_by_post.get(post_id, [])
            suggestion = generate_reply_suggestion(
                llm_model=ctx.config.llm_model,
                post_context=post_context,
                comment_context=comment,
                recent_suggestions=recent,
            )

            # Double-check safety (generate_reply_suggestion already does
            # internal checks, but we log if it fell back)
            safety_err = check_content_safety(suggestion)
            if safety_err:
                logger.warning("Suggestion failed final safety check: %s. Using fallback.", safety_err.reason)
                suggestion = FALLBACK_REPLY

            signature = suggestion_signature(suggestion)
            task_id = str(uuid.uuid4())

            # ── Dispatch ────────────────────────────────────────────
            if alpha_chat_id:
                _dispatch_with_approval(
                    ctx, alpha_chat_id, task_id, post_id, comment,
                    member_name, suggestion, known_comment_ids,
                )
            else:
                _dispatch_direct(
                    ctx, chat_id, task_id, post_id, comment,
                    member_name, suggestion, known_comment_ids,
                    recent_by_post, signature,
                )
            sent_count += 1

        # Persist state
        if not ctx.config.dry_run:
            ctx.sheets.set_state(f"last_seen_created_utc_{post_id}", str(latest_seen))
            for key, value in state.items():
                if key.startswith("reply_cursor_team_"):
                    ctx.sheets.set_state(key, value)

    return sent_count


def _dispatch_with_approval(
    ctx, alpha_chat_id, task_id, post_id, comment,
    member_name, suggestion, known_comment_ids,
):
    """Send approval request to Alpha, store task as pending_approval."""
    approval_message = (
        f"REPLY APPROVAL REQUEST\n\n"
        f"Post ID: {post_id}\n"
        f"Comment by: u/{comment.get('author', '')}\n"
        f"Comment URL: {comment.get('comment_url', '')}\n\n"
        f"Assigned to: {member_name}\n\n"
        f"Suggested reply:\n{suggestion}\n\n"
        f"Task ID: {task_id}\n"
        f"Reply /approve_{task_id} to approve, or /reject_{task_id} to reject"
    )
    _send_or_print(ctx, chat_id=alpha_chat_id, text=approval_message)

    task_row = {
        "reply_task_id": task_id,
        "post_id": post_id,
        "reddit_comment_id": comment.get("comment_id", ""),
        "comment_author": comment.get("author", ""),
        "comment_url": comment.get("comment_url", ""),
        "assigned_member_name": member_name,
        "reply_suggestion": suggestion,
        "approval_status": "pending",
        "status": "dry_run_pending" if ctx.config.dry_run else "pending_approval",
        "created_at": _now_utc().isoformat(),
        "sent_at": "",
        "approved_at": "",
        "reply_posted_at": "",
        "reply_url": "",
    }
    if not ctx.config.dry_run:
        ctx.sheets.append_reply_task(task_row)
        known_comment_ids.add(comment.get("comment_id", ""))


def _dispatch_direct(
    ctx, chat_id, task_id, post_id, comment,
    member_name, suggestion, known_comment_ids,
    recent_by_post, signature,
):
    """Send reply directly to team member (fallback when Alpha ID unknown)."""
    logger.info("Alpha ID not found. Sending directly to %s", member_name)
    message = (
        f"Reply task assigned\n\nPost ID: {post_id}\nAssigned to: {member_name}\n"
        f"Comment by u/{comment.get('author', '')}\n"
        f"Comment URL: {comment.get('comment_url', '')}\n\n"
        f"Suggested reply:\n{suggestion}"
    )
    _send_or_print(ctx, chat_id=chat_id, text=message)

    task_row = {
        "reply_task_id": task_id,
        "post_id": post_id,
        "reddit_comment_id": comment.get("comment_id", ""),
        "comment_author": comment.get("author", ""),
        "comment_url": comment.get("comment_url", ""),
        "assigned_member_name": member_name,
        "reply_suggestion": suggestion,
        "approval_status": "skipped",
        "status": "dry_run_sent" if ctx.config.dry_run else "sent",
        "created_at": _now_utc().isoformat(),
        "sent_at": _now_utc().isoformat(),
        "approved_at": "",
        "reply_posted_at": "",
        "reply_url": "",
    }
    if not ctx.config.dry_run:
        ctx.sheets.append_reply_task(task_row)
        known_comment_ids.add(comment.get("comment_id", ""))
        recent_by_post.setdefault(post_id, []).append(suggestion)
        ctx.sheets.set_state(f"last_reply_signature_{post_id}", signature)


# ══════════════════════════════════════════════════════════════════════════
# Pending approval processing
# ══════════════════════════════════════════════════════════════════════════

def process_pending_approvals(ctx: RuntimeContext) -> int:
    """Process approved reply tasks and send them to assigned team members."""
    teams_rows = ctx.sheets.read_rows(ctx.config.teams_tab_name)
    reply_rows = ctx.sheets.read_rows(ctx.config.reply_queue_tab_name)

    pending_tasks = [
        row for row in reply_rows
        if row.get("approval_status", "").strip().lower() == "approved"
        and row.get("status", "").strip().lower() in {"pending_approval", "approved"}
        and not row.get("sent_at", "").strip()
    ]

    if not pending_tasks:
        return 0

    member_lookup = _build_member_lookup(teams_rows)
    sent_count = 0

    for task in pending_tasks:
        task_id = task.get("reply_task_id", "")
        member_name = task.get("assigned_member_name", "").strip()
        suggestion = task.get("reply_suggestion", "").strip()
        comment_url = task.get("comment_url", "")
        comment_author = task.get("comment_author", "")
        post_id = task.get("post_id", "")

        if not member_name or not suggestion:
            continue

        member = member_lookup.get(member_name)
        if not member:
            _escalate_to_alpha(
                ctx, teams_rows,
                "Approved task: member not found",
                f"Task {task_id} approved but member '{member_name}' not in Teams sheet.",
            )
            continue

        chat_id = member.get("telegram_user_id", "").strip()
        if not chat_id or not chat_id.isdigit():
            _escalate_to_alpha(
                ctx, teams_rows,
                "Approved task: member has no Telegram ID",
                f"Task {task_id} approved but member '{member_name}' has no Telegram ID.",
            )
            continue

        message = (
            f"Reply task assigned\n\nPost ID: {post_id}\nAssigned to: {member_name}\n"
            f"Comment by u/{comment_author}\nComment URL: {comment_url}\n\n"
            f"Suggested reply:\n{suggestion}"
        )
        _send_or_print(ctx, chat_id=chat_id, text=message)

        if not ctx.config.dry_run:
            ctx.sheets.update_rows_by_id(
                ctx.config.reply_queue_tab_name,
                "reply_task_id", task_id,
                {"status": "sent", "sent_at": _now_utc().isoformat()},
            )
        sent_count += 1

    return sent_count


# ══════════════════════════════════════════════════════════════════════════
# Timeout / reassignment checker
# ══════════════════════════════════════════════════════════════════════════

def check_reply_timeouts_and_reassign(ctx: RuntimeContext) -> int:
    """Check for reply tasks that have been 'sent' but not acted on
    within ``reply_timeout_hours``. Reassign to another team member,
    or escalate to Alpha after max reassignments.

    Returns count of reassigned + escalated tasks.
    """
    teams_rows = ctx.sheets.read_rows(ctx.config.teams_tab_name)
    reply_rows = ctx.sheets.read_rows(ctx.config.reply_queue_tab_name)
    team_members = build_team_members(teams_rows)
    state = ctx.sheets.get_state()
    member_lookup = _build_member_lookup(teams_rows)

    timeout_delta = timedelta(hours=ctx.config.reply_timeout_hours)
    now = _now_utc()
    action_count = 0

    # Only look at tasks that were sent but not replied to
    sent_tasks = [
        row for row in reply_rows
        if row.get("status", "").strip().lower() == "sent"
        and not row.get("reply_posted_at", "").strip()
        and row.get("sent_at", "").strip()
    ]

    for task in sent_tasks:
        task_id = task.get("reply_task_id", "")
        sent_at_str = task.get("sent_at", "").strip()
        post_id = task.get("post_id", "")
        team_id = task.get("team_id", "").strip()
        current_member = task.get("assigned_member_name", "").strip()

        # Parse sent_at
        try:
            sent_at = datetime.fromisoformat(sent_at_str.replace("Z", "+00:00"))
            if sent_at.tzinfo is None:
                sent_at = sent_at.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue

        if now - sent_at < timeout_delta:
            continue  # Not timed out yet

        # Count previous reassignments for this task
        reassign_key = f"reassign_count_{task_id}"
        reassign_count = int(state.get(reassign_key, "0") or "0")

        if reassign_count >= ctx.config.max_reassign_attempts:
            # Max reassignments reached -> escalate to Alpha
            _escalate_to_alpha(
                ctx, teams_rows,
                "Reply task timed out (max reassignments reached)",
                f"Task {task_id} for post {post_id} has been reassigned "
                f"{reassign_count} time(s) but no one has replied.\n"
                f"Last assigned to: {current_member}\n"
                f"Comment URL: {task.get('comment_url', '')}\n\n"
                f"Suggested reply:\n{task.get('reply_suggestion', '')[:300]}",
            )
            if not ctx.config.dry_run:
                ctx.sheets.update_rows_by_id(
                    ctx.config.reply_queue_tab_name,
                    "reply_task_id", task_id,
                    {"status": "escalated"},
                )
            action_count += 1
            continue

        # Try to reassign to another team member
        if not team_id:
            # Look up team_id from the post
            posts_rows = ctx.sheets.read_rows(ctx.config.posts_tab_name)
            for p in posts_rows:
                if p.get("post_id", "").strip() == post_id:
                    team_id = p.get("team_id", "").strip()
                    break

        if not team_id or team_id not in team_members:
            _escalate_to_alpha(
                ctx, teams_rows,
                "Cannot reassign (team not found)",
                f"Task {task_id} timed out but team_id '{team_id}' not found.",
            )
            action_count += 1
            continue

        members = team_members.get(team_id, [])
        # Pick a different member
        new_member = None
        for m in members:
            if m.get("member_name", "").strip() != current_member:
                new_member = m
                break

        if not new_member:
            # Only one member in team -- escalate
            _escalate_to_alpha(
                ctx, teams_rows,
                "Cannot reassign (single-member team)",
                f"Task {task_id} timed out. Team {team_id} only has '{current_member}'. "
                f"No one else to reassign to.",
            )
            if not ctx.config.dry_run:
                ctx.sheets.update_rows_by_id(
                    ctx.config.reply_queue_tab_name,
                    "reply_task_id", task_id,
                    {"status": "escalated"},
                )
            action_count += 1
            continue

        new_member_name = new_member.get("member_name", "").strip()
        new_chat_id = new_member.get("telegram_user_id", "").strip()

        if not new_chat_id or not new_chat_id.isdigit():
            _escalate_to_alpha(
                ctx, teams_rows,
                "Cannot reassign (new member has no Telegram ID)",
                f"Task {task_id} timed out. Tried to reassign to '{new_member_name}' "
                f"but they have no Telegram ID.",
            )
            action_count += 1
            continue

        # Notify new member
        message = (
            f"[REASSIGNED] Reply task\n\n"
            f"This task was previously assigned to {current_member} but timed out.\n\n"
            f"Post ID: {post_id}\nAssigned to: {new_member_name}\n"
            f"Comment by u/{task.get('comment_author', '')}\n"
            f"Comment URL: {task.get('comment_url', '')}\n\n"
            f"Suggested reply:\n{task.get('reply_suggestion', '')}"
        )
        _send_or_print(ctx, chat_id=new_chat_id, text=message)

        # Notify original member
        old_member = member_lookup.get(current_member)
        if old_member:
            old_chat_id = old_member.get("telegram_user_id", "").strip()
            if old_chat_id and old_chat_id.isdigit():
                _send_or_print(
                    ctx, chat_id=old_chat_id,
                    text=f"Your reply task {task_id} has been reassigned to "
                         f"{new_member_name} due to timeout.",
                )

        # Update sheet
        if not ctx.config.dry_run:
            ctx.sheets.update_rows_by_id(
                ctx.config.reply_queue_tab_name,
                "reply_task_id", task_id,
                {
                    "assigned_member_name": new_member_name,
                    "status": "sent",
                    "sent_at": _now_utc().isoformat(),
                },
            )
            ctx.sheets.set_state(reassign_key, str(reassign_count + 1))

        logger.info("Reassigned task %s: %s -> %s (attempt %d)",
                     task_id, current_member, new_member_name, reassign_count + 1)
        action_count += 1

    return action_count


# ══════════════════════════════════════════════════════════════════════════
# Engagement metrics collection
# ══════════════════════════════════════════════════════════════════════════

def collect_engagement_metrics(ctx: RuntimeContext) -> int:
    """Collect engagement metrics: upvotes, response times, performance."""
    if ctx.reddit is None:
        return 0

    posts_rows = ctx.sheets.read_rows(ctx.config.posts_tab_name)
    reply_rows = ctx.sheets.read_rows(ctx.config.reply_queue_tab_name)
    teams_rows = ctx.sheets.read_rows(ctx.config.teams_tab_name)

    post_lookup = {row.get("post_id", ""): row for row in posts_rows if row.get("post_id")}
    team_lookup = {row.get("member_name", ""): row.get("team_id", "") for row in teams_rows}

    existing_metrics = ctx.sheets.read_rows(ctx.config.metrics_tab_name)
    tracked_task_ids = {row.get("reply_task_id", "") for row in existing_metrics if row.get("reply_task_id")}

    metrics_count = 0

    sent_replies = [
        row for row in reply_rows
        if row.get("status", "").strip().lower() in {"sent", "approved"}
        and row.get("reply_task_id", "") not in tracked_task_ids
    ]

    for reply_task in sent_replies:
        task_id = reply_task.get("reply_task_id", "")
        post_id = reply_task.get("post_id", "")
        comment_id = reply_task.get("reddit_comment_id", "")
        comment_url = reply_task.get("comment_url", "")
        member_name = reply_task.get("assigned_member_name", "")
        reply_posted_at = reply_task.get("reply_posted_at", "")

        if not post_id or not comment_url:
            continue

        post_info = post_lookup.get(post_id, {})
        post_url = post_info.get("reddit_post_url", "")
        team_id = team_lookup.get(member_name, "")

        if not post_url:
            continue

        try:
            post_metrics = ctx.reddit.get_post_metrics(post_url)
            if not post_metrics:
                continue

            comment_metrics = ctx.reddit.get_comment_score(comment_url, target_comment_id=comment_id)
            if not comment_metrics:
                continue

            response_time_hours = None
            if reply_posted_at:
                try:
                    comment_utc = float(comment_metrics.get("created_utc", 0))
                    reply_dt = datetime.fromisoformat(reply_posted_at.replace("Z", "+00:00"))
                    reply_utc = reply_dt.timestamp()
                    if comment_utc > 0 and reply_utc > comment_utc:
                        response_time_hours = round((reply_utc - comment_utc) / 3600.0, 2)
                except Exception:
                    pass

            metric_row = {
                "metric_id": str(uuid.uuid4()),
                "post_id": post_id,
                "reddit_post_url": post_url,
                "post_title": post_metrics.get("title", ""),
                "post_created_at": (
                    datetime.fromtimestamp(post_metrics.get("created_utc", 0), tz=timezone.utc).isoformat()
                    if post_metrics.get("created_utc") else ""
                ),
                "post_upvotes": str(post_metrics.get("score", 0)),
                "post_comments_count": str(post_metrics.get("num_comments", 0)),
                "comment_id": comment_id,
                "comment_author": reply_task.get("comment_author", ""),
                "comment_created_at": (
                    datetime.fromtimestamp(comment_metrics.get("created_utc", 0), tz=timezone.utc).isoformat()
                    if comment_metrics.get("created_utc") else ""
                ),
                "comment_upvotes": str(comment_metrics.get("score", 0)),
                "reply_task_id": task_id,
                "reply_author": member_name,
                "reply_posted_at": reply_posted_at or "",
                "reply_upvotes": "0",
                "response_time_hours": str(response_time_hours) if response_time_hours else "",
                "assigned_member_name": member_name,
                "team_id": team_id,
                "metric_date": _now_utc().date().isoformat(),
                "updated_at": _now_utc().isoformat(),
            }

            if not ctx.config.dry_run:
                ctx.sheets.append_metric(metric_row)
            metrics_count += 1

        except Exception as e:
            logger.error("Error collecting metrics for task %s: %s", task_id, e)
            continue

    return metrics_count


# ══════════════════════════════════════════════════════════════════════════
# Run modes
# ══════════════════════════════════════════════════════════════════════════

def run_once(ctx: RuntimeContext) -> None:
    """Single execution: telegram msgs + reminders + poll + approvals + timeouts + metrics + test."""
    tg_msgs = process_telegram_updates(ctx)
    reminders = send_daily_posting_reminders(ctx)
    replies = poll_comments_and_dispatch_replies(ctx)
    approved = process_pending_approvals(ctx)
    reassigned = check_reply_timeouts_and_reassign(ctx)
    metrics = collect_engagement_metrics(ctx)
    test_comments = poll_test_post_comments(ctx)
    print(
        f"Run complete: tg_updates={tg_msgs}, reminders={reminders}, reply_tasks={replies}, "
        f"approved_sent={approved}, reassigned={reassigned}, metrics={metrics}, "
        f"test_comments={test_comments}"
    )


def should_run_daily_reminders(last_run_date: str, config: BotConfig) -> bool:
    return last_run_date != _today_iso(config)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Telegram Reddit reply bot")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--mode", choices=["once", "daemon", "collect-ids", "timed-daemon"], default="once")
    parser.add_argument("--run-for-minutes", type=int, default=4,
                        help="For timed-daemon: how many minutes to run before exiting (default 4)")
    args = parser.parse_args()

    config = BotConfig.from_env(dry_run_override=args.dry_run)
    sheets = GoogleSheetsClient(config)
    sheets.ensure_default_schema()
    telegram = TelegramClient(config.telegram_bot_token)

    if args.mode == "collect-ids":
        ctx = RuntimeContext(config=config, sheets=sheets, reddit=None, telegram=telegram)
        process_telegram_updates(ctx)
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

    # ── Timed-daemon mode (ideal for GitHub Actions) ────────────────
    if args.mode == "timed-daemon":
        run_minutes = args.run_for_minutes
        cycle_sleep = 90  # seconds between cycles (1.5 min)
        deadline = time.time() + (run_minutes * 60)
        cycle_num = 0

        print(f"Timed-daemon: running for {run_minutes} minute(s), "
              f"polling every {cycle_sleep}s")

        while time.time() < deadline:
            cycle_num += 1
            cycle_start = time.time()
            print(f"\n--- Cycle {cycle_num} (remaining: "
                  f"{int(deadline - time.time())}s) ---")
            try:
                run_once(ctx)
            except Exception as exc:
                logger.error("Timed-daemon cycle %d error: %s", cycle_num, exc)
                traceback.print_exc()

            elapsed = time.time() - cycle_start
            remaining = deadline - time.time()
            sleep_time = min(cycle_sleep, max(remaining, 0))
            if sleep_time <= 5 or remaining <= 5:
                break  # Not enough time for another cycle
            print(f"Cycle {cycle_num} took {elapsed:.1f}s. "
                  f"Sleeping {sleep_time:.0f}s...")
            time.sleep(sleep_time)

        print(f"\nTimed-daemon finished after {cycle_num} cycle(s).")
        return

    # ── Daemon mode (infinite loop, for VPS hosting) ────────────────
    print(f"Daemon mode: poll every {config.poll_interval_minutes} minute(s)")
    last_daily_key = "last_daily_reminder_date"
    consecutive_errors = 0
    MAX_CONSECUTIVE_ERRORS = 5

    while True:
        try:
            state = sheets.get_state()
            last_daily = state.get(last_daily_key, "")
            now = _now_local(config)

            # Daily reminders
            if (
                should_run_daily_reminders(last_daily, config)
                and now.hour >= config.daily_hour
                and now.minute >= config.daily_minute
            ):
                reminders = send_daily_posting_reminders(ctx)
                print(f"Daily reminders sent: {reminders}")
                if not config.dry_run:
                    sheets.set_state(last_daily_key, date.today().isoformat())

            # Poll + dispatch
            replies = poll_comments_and_dispatch_replies(ctx)
            print(f"Reply tasks dispatched: {replies}")

            # Process approvals
            approved = process_pending_approvals(ctx)
            if approved:
                print(f"Approved tasks sent: {approved}")

            # Check timeouts + reassign
            reassigned = check_reply_timeouts_and_reassign(ctx)
            if reassigned:
                print(f"Timed-out tasks handled: {reassigned}")

            # Collect metrics (less frequently -- every 3rd cycle)
            metrics_cycle_key = "metrics_cycle_counter"
            cycle = int(state.get(metrics_cycle_key, "0") or "0")
            if cycle % 3 == 0:
                metrics = collect_engagement_metrics(ctx)
                if metrics:
                    print(f"Metrics collected: {metrics}")
            if not config.dry_run:
                sheets.set_state(metrics_cycle_key, str(cycle + 1))

            # Process all Telegram messages (URLs, /start, approvals, etc.)
            process_telegram_updates(ctx)

            # Poll test post comments (every cycle -- test mode needs fast feedback)
            test_sent = poll_test_post_comments(ctx)
            if test_sent:
                print(f"Test comments forwarded: {test_sent}")

            consecutive_errors = 0  # Reset on success

        except Exception as exc:
            consecutive_errors += 1
            logger.error("Bot loop error (#%d): %s", consecutive_errors, exc)
            traceback.print_exc()

            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                logger.critical(
                    "Hit %d consecutive errors. Sending emergency escalation.",
                    MAX_CONSECUTIVE_ERRORS,
                )
                try:
                    teams_rows = sheets.read_rows(config.teams_tab_name)
                    _escalate_to_alpha(
                        ctx, teams_rows,
                        "BOT CRITICAL: Repeated failures",
                        f"The bot has encountered {consecutive_errors} consecutive "
                        f"errors and may need manual intervention.\n\n"
                        f"Last error: {exc}",
                    )
                except Exception:
                    logger.error("Failed to send emergency escalation!")
                consecutive_errors = 0  # Reset to avoid spamming

        time.sleep(max(config.poll_interval_minutes, 1) * 60)


if __name__ == "__main__":
    main()
