from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from typing import Dict, List, Optional

from openai import OpenAI

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Content safety
# ---------------------------------------------------------------------------

# Words/patterns that should NEVER appear in generated replies
_BLOCKLIST_PATTERNS: List[re.Pattern] = [
    re.compile(r"\b(buy now|click here|use (my|this) (link|code))\b", re.I),
    re.compile(r"\b(discount code|promo code|affiliate)\b", re.I),
    re.compile(r"https?://\S+", re.I),  # No links in generated replies
    re.compile(r"\b(kys|kill yourself|neck yourself)\b", re.I),
    re.compile(r"\b(retard(ed)?|f[a@]gg?[o0]t|n[i1]gg?[e3a]r)\b", re.I),
    re.compile(r"\b(stfu|gtfo|go die)\b", re.I),
]

# Phrases that indicate the LLM broke character / leaked instructions
_INSTRUCTION_LEAK_PATTERNS: List[re.Pattern] = [
    re.compile(r"as an ai|as a language model|i'?m an ai", re.I),
    re.compile(r"i cannot|i can'?t .*(generate|help|assist)", re.I),
    re.compile(r"(openai|chatgpt|gpt-4|gpt-3)", re.I),
    re.compile(r"here'?s (a|the) (suggested|generated) (reply|response)", re.I),
    re.compile(r"^\s*sure[,!]?\s*(here|i)", re.I),
]

# Maximum allowed length for a reply (characters)
MAX_REPLY_LENGTH = 1500
# Minimum allowed length (too short = probably broken)
MIN_REPLY_LENGTH = 10


class ContentSafetyError(Exception):
    """Raised when generated content fails safety checks."""
    def __init__(self, reason: str, content: str):
        self.reason = reason
        self.content = content
        super().__init__(f"Content flagged: {reason}")


def check_content_safety(text: str) -> Optional[ContentSafetyError]:
    """Run content through safety filters.

    Returns None if content is safe, or a ContentSafetyError if flagged.
    """
    if not text or not text.strip():
        return ContentSafetyError("empty_reply", text)

    stripped = text.strip()

    if len(stripped) < MIN_REPLY_LENGTH:
        return ContentSafetyError("too_short", text)

    if len(stripped) > MAX_REPLY_LENGTH:
        return ContentSafetyError("too_long", text)

    for pattern in _BLOCKLIST_PATTERNS:
        if pattern.search(stripped):
            return ContentSafetyError(f"blocklist_match: {pattern.pattern}", text)

    for pattern in _INSTRUCTION_LEAK_PATTERNS:
        if pattern.search(stripped):
            return ContentSafetyError(f"instruction_leak: {pattern.pattern}", text)

    return None  # Safe


# ---------------------------------------------------------------------------
# Signature / dedup
# ---------------------------------------------------------------------------

def suggestion_signature(text: str) -> str:
    normalized = " ".join(text.lower().strip().split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def build_reply_prompt(
    post_context: Dict[str, str],
    comment_context: Dict[str, str],
    recent_suggestions: List[str],
) -> str:
    recent_block = "\n".join(f"- {item}" for item in recent_suggestions[-5:]) or "- (none)"
    return f"""
You are crafting a Reddit comment reply. Make it sound like a real Reddit user - casual, authentic, and conversational.

CRITICAL REQUIREMENTS:
- ALWAYS start with a lowercase letter (Reddit style)
- Vary the length: sometimes 1-2 sentences, sometimes 3-4, keep it dynamic
- Sound natural and conversational, not formal or corporate
- Use Reddit-typical phrases: "yeah", "honestly", "tbh", "imo", "that's fair", "good point", etc.
- Keep it short and punchy - avoid long paragraphs
- No sales language, no promotion, no marketing speak
- Don't repeat the comment text verbatim
- Vary wording compared to prior suggestions
- Match the tone: if comment is casual, be casual; if technical, be technical but still Reddit-style
- NEVER include URLs or links
- NEVER mention AI, ChatGPT, or language models
- NEVER use hateful, discriminatory, or violent language

Post context:
Title: {post_context.get("title", "")}
Body:
{post_context.get("body", "")}

Comment by u/{comment_context.get("author", "user")}:
{comment_context.get("body", "")}

Recent replies to avoid repeating:
{recent_block}

Generate ONE reply that sounds like a real Reddit user wrote it. Start with lowercase.
""".strip()


# ---------------------------------------------------------------------------
# OpenAI call with retry
# ---------------------------------------------------------------------------

def _call_openai(prompt: str, model: str, max_retries: int = 3) -> Optional[str]:
    """Call OpenAI with automatic retries on transient failures."""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        logger.warning("OPENAI_API_KEY not set, cannot generate reply.")
        return None

    client = OpenAI(api_key=api_key)
    last_exc: Optional[Exception] = None

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a Reddit user crafting authentic, casual replies. "
                            "Always start replies with lowercase letters. "
                            "Never include URLs, never mention AI or ChatGPT, "
                            "never use hateful language."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.8,
                max_tokens=200,
            )
            reply = response.choices[0].message.content
            if reply:
                return str(reply).strip()
            return None

        except Exception as e:
            last_exc = e
            error_str = str(e).lower()

            # Don't retry on auth / quota errors
            if any(kw in error_str for kw in ("invalid api key", "quota", "billing", "authentication")):
                logger.error("OpenAI non-retryable error: %s", e)
                return None

            wait = 2 ** attempt
            logger.warning("OpenAI API error (attempt %d/%d): %s. Retrying in %ds",
                           attempt + 1, max_retries, e, wait)
            time.sleep(wait)

    logger.error("OpenAI failed after %d retries. Last error: %s", max_retries, last_exc)
    return None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

FALLBACK_REPLY = (
    "yeah that's a good point. in my experience the practical impact "
    "shows up when you test it with real constraints. curious how you'd approach it?"
)

# Maximum regeneration attempts when content fails safety
MAX_GENERATION_ATTEMPTS = 3


def generate_reply_suggestion(
    llm_model: str,
    post_context: Dict[str, str],
    comment_context: Dict[str, str],
    recent_suggestions: List[str],
) -> str:
    """Generate a reply suggestion with content safety checks.

    Retries up to MAX_GENERATION_ATTEMPTS times if the LLM produces
    unsafe content. Falls back to a safe canned reply if all attempts fail.

    Returns a tuple-like str -- callers can also call
    ``check_content_safety()`` themselves for extra logging.
    """
    prompt = build_reply_prompt(post_context, comment_context, recent_suggestions)

    for attempt in range(MAX_GENERATION_ATTEMPTS):
        reply = _call_openai(prompt, llm_model)
        if not reply:
            logger.warning("OpenAI returned empty (attempt %d/%d)",
                           attempt + 1, MAX_GENERATION_ATTEMPTS)
            continue

        # Enforce lowercase start
        reply = reply.strip()
        if reply and reply[0].isupper():
            reply = reply[0].lower() + reply[1:]

        # Run safety filter
        safety_err = check_content_safety(reply)
        if safety_err is None:
            return reply

        logger.warning("Content safety failed (attempt %d/%d): %s -- reply: %s",
                       attempt + 1, MAX_GENERATION_ATTEMPTS,
                       safety_err.reason, reply[:120])
        # Add slight variation signal for next attempt
        prompt += f"\n\n(Previous attempt was rejected: {safety_err.reason}. Try again differently.)"

    logger.error("All %d generation attempts failed safety checks. Using fallback.",
                 MAX_GENERATION_ATTEMPTS)
    return FALLBACK_REPLY
