from __future__ import annotations

import hashlib
import os
from typing import Dict, List, Optional

from openai import OpenAI


def suggestion_signature(text: str) -> str:
    normalized = " ".join(text.lower().strip().split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def build_reply_prompt(
    post_context: Dict[str, str],
    comment_context: Dict[str, str],
    recent_suggestions: List[str],
) -> str:
    recent_block = "\n".join(f"- {item}" for item in recent_suggestions[-5:]) or "- (none)"
    return f"""
You are helping a real Reddit user craft a natural reply.

Requirements:
- Reply directly to the comment with useful substance.
- Keep it authentic, concise, and conversational.
- No promotion, no sales language.
- Do not copy the comment text verbatim.
- Vary wording compared to prior suggestions.
- Output only one reply draft.

Post context:
Title: {post_context.get("title", "")}
Body:
{post_context.get("body", "")}

Incoming comment by {comment_context.get("author", "user")}:
{comment_context.get("body", "")}

Recent reply suggestions to avoid repeating:
{recent_block}
""".strip()


def _call_openai(prompt: str, model: str) -> Optional[str]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None
    try:
        client = OpenAI(api_key=api_key)
        response = client.responses.create(
            model=model,
            input=prompt,
        )
        output_text = getattr(response, "output_text", None)
        if output_text:
            return str(output_text).strip()
    except Exception:
        return None
    return None


def generate_reply_suggestion(
    llm_model: str,
    post_context: Dict[str, str],
    comment_context: Dict[str, str],
    recent_suggestions: List[str],
) -> str:
    prompt = build_reply_prompt(post_context, comment_context, recent_suggestions)
    reply = _call_openai(prompt, llm_model)
    if reply:
        return reply
    return (
        "Thanks for sharing this perspective. I agree this is a key point, "
        "and in my experience the practical impact shows up when you test it "
        "with real constraints. Curious how you would approach it in your setup?"
    )


