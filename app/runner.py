from __future__ import annotations

import argparse
import logging
import time
import traceback
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional
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
# Telegram ID collection
# ══════════════════════════════════════════════════════════════════════════

def collect_telegram_ids_once(ctx: RuntimeContext) -> int:
    sheet_id = ctx.config.google_spreadsheet_id
    sheet_id_masked = sheet_id[:8] + "..." + sheet_id[-8:] if len(sheet_id) > 16 else "***"
    print(f"Reading Teams tab: '{ctx.config.teams_tab_name}' from sheet ID: {sheet_id_masked}")

    # Debug: List all worksheets
    try:
        all_worksheets = ctx.sheets._spreadsheet.worksheets()
        print(f"Available worksheets ({len(all_worksheets)}): "
              f"{[ws.title for ws in all_worksheets[:10]]}{'...' if len(all_worksheets) > 10 else ''}")

        teams_ws = None
        for ws in all_worksheets:
            if ws.title == ctx.config.teams_tab_name:
                teams_ws = ws
                break

        if teams_ws:
            all_values = teams_ws.get_all_values()
            print(f"Teams tab found: {len(all_values)} total rows (including header)")
            if all_values:
                print(f"  Header: {all_values[0]}")
                print(f"  Data rows: {len(all_values) - 1}")
        else:
            print(f"WARNING: Teams tab '{ctx.config.teams_tab_name}' NOT FOUND in worksheets!")
    except Exception as e:
        print(f"Error accessing worksheets: {e}")
        traceback.print_exc()

    teams_rows = ctx.sheets.read_rows(ctx.config.teams_tab_name)
    print(f"Raw rows from sheet: {len(teams_rows)} rows")

    username_to_member: Dict[str, str] = {}
    for idx, row in enumerate(teams_rows, start=1):
        username = _normalize_username(row.get("telegram_user_id", ""))
        member_name = row.get("member_name", "").strip()
        if username and member_name:
            username_to_member[username] = member_name

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

        # Handle approval commands from Alpha
        if text.startswith("/approve_") or text.startswith("/reject_"):
            _handle_approval_command(ctx, text, chat_id)
            continue

        if not text.startswith("/start") or not chat_id:
            continue

        print(f"Processing /start from: username=@{username}, first_name={first_name}, chat_id={chat_id}")

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
            _send_or_print(ctx, chat_id, f"Hi {member_name}, your Telegram ID is linked successfully.")
            processed += 1
        else:
            _send_or_print(
                ctx, chat_id,
                f"You are not mapped yet in the team sheet. "
                f"Your Telegram username is @{username or '(none)'}. "
                f"Please share your @username with the admin.",
            )

    if not ctx.config.dry_run:
        ctx.sheets.set_state(offset_key, str(max_update_id + 1))
    print(f"Collected/confirmed Telegram IDs for {processed} member(s).")
    return processed


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
            f"Posting reminder\n\nPost ID: {post_id}\nScheduled: {today} {scheduled_time}\n\n"
            f"Post content:\n{post_content}"
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
    """Single execution: reminders + poll + approvals + timeouts + metrics."""
    reminders = send_daily_posting_reminders(ctx)
    replies = poll_comments_and_dispatch_replies(ctx)
    approved = process_pending_approvals(ctx)
    reassigned = check_reply_timeouts_and_reassign(ctx)
    metrics = collect_engagement_metrics(ctx)
    print(
        f"Run complete: reminders={reminders}, reply_tasks={replies}, "
        f"approved_sent={approved}, reassigned={reassigned}, metrics={metrics}"
    )


def should_run_daily_reminders(last_run_date: str, config: BotConfig) -> bool:
    return last_run_date != _today_iso(config)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Telegram Reddit reply bot")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--mode", choices=["once", "daemon", "collect-ids"], default="once")
    args = parser.parse_args()

    config = BotConfig.from_env(dry_run_override=args.dry_run)
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

    # ── Daemon mode ─────────────────────────────────────────────────
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

            # Collect Telegram IDs (also runs in daemon to pick up new /start)
            collect_telegram_ids_once(ctx)

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
