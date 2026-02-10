from __future__ import annotations

from typing import Dict, List, Optional
from urllib.parse import urlparse

import praw

from app.config import BotConfig


class RedditClient:
    def __init__(self, config: BotConfig):
        self.reddit = praw.Reddit(
            client_id=config.reddit_client_id,
            client_secret=config.reddit_client_secret,
            user_agent=config.reddit_user_agent,
            check_for_async=False,
        )

    @staticmethod
    def _normalize_submission_url(url: str) -> str:
        parsed = urlparse(url)
        path = parsed.path.rstrip("/")
        if "/comments/" in path:
            return f"https://www.reddit.com{path}/"
        return url

    def get_submission_context(self, post_url: str) -> Dict[str, str]:
        submission = self.reddit.submission(url=self._normalize_submission_url(post_url))
        submission.comments.replace_more(limit=0)
        return {
            "post_id": submission.id,
            "title": submission.title or "",
            "body": submission.selftext or "",
            "subreddit": str(submission.subreddit),
            "url": f"https://www.reddit.com{submission.permalink}",
        }

    def fetch_new_comments(
        self,
        post_url: str,
        known_comment_ids: Optional[set[str]] = None,
        min_created_utc: Optional[float] = None,
    ) -> List[Dict[str, str]]:
        known_comment_ids = known_comment_ids or set()
        submission = self.reddit.submission(url=self._normalize_submission_url(post_url))
        submission.comments.replace_more(limit=0)

        comments: List[Dict[str, str]] = []
        for comment in submission.comments.list():
            if comment.id in known_comment_ids:
                continue
            if min_created_utc is not None and float(comment.created_utc) <= min_created_utc:
                continue
            comments.append(
                {
                    "comment_id": comment.id,
                    "author": str(comment.author) if comment.author else "[deleted]",
                    "body": comment.body or "",
                    "created_utc": str(comment.created_utc),
                    "comment_url": f"https://www.reddit.com{comment.permalink}",
                    "parent_id": comment.parent_id or "",
                    "post_id": submission.id,
                }
            )
        comments.sort(key=lambda c: float(c.get("created_utc", "0")))
        return comments


