from __future__ import annotations

import time
import logging
from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests

from app.config import BotConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Retry / resilience helpers
# ---------------------------------------------------------------------------

class RedditPostDeleted(Exception):
    """Raised when a Reddit post is confirmed deleted or removed."""
    pass


class RedditRateLimited(Exception):
    """Raised when Reddit returns 429 Too Many Requests."""
    def __init__(self, retry_after: int = 60):
        self.retry_after = retry_after
        super().__init__(f"Rate limited, retry after {retry_after}s")


def _retry_request(session: requests.Session, url: str, max_retries: int = 3,
                   backoff_base: float = 2.0, timeout: int = 15) -> requests.Response:
    """Execute a GET request with exponential backoff retries.

    Handles:
      - 429 (rate limit) -- respects Retry-After header
      - 5xx server errors -- retries with backoff
      - Connection / timeout errors -- retries with backoff
    Raises immediately for:
      - 404 -- wraps as RedditPostDeleted
      - 403 -- logs warning, raises
      - Other 4xx -- raises
    """
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            resp = session.get(url, timeout=timeout)

            if resp.status_code == 404:
                raise RedditPostDeleted(f"Reddit returned 404 for {url}")

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "60"))
                logger.warning("Reddit rate-limited (429). Sleeping %ds (attempt %d/%d)",
                               retry_after, attempt + 1, max_retries)
                time.sleep(retry_after)
                continue

            if resp.status_code == 403:
                logger.warning("Reddit returned 403 Forbidden for %s", url)
                resp.raise_for_status()

            if resp.status_code >= 500:
                wait = backoff_base ** attempt
                logger.warning("Reddit server error %d. Retrying in %.1fs (attempt %d/%d)",
                               resp.status_code, wait, attempt + 1, max_retries)
                time.sleep(wait)
                last_exc = requests.HTTPError(response=resp)
                continue

            resp.raise_for_status()
            return resp

        except (requests.ConnectionError, requests.Timeout) as exc:
            wait = backoff_base ** attempt
            logger.warning("Network error fetching %s: %s. Retrying in %.1fs (attempt %d/%d)",
                           url, exc, wait, attempt + 1, max_retries)
            time.sleep(wait)
            last_exc = exc

    raise last_exc or RuntimeError(f"All {max_retries} retries failed for {url}")


class RedditClient:
    def __init__(self, config: BotConfig):
        self.user_agent = config.reddit_user_agent or "rt-cert-program-utils/telegram-reply-bot"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "application/json",
        })

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_submission_url(url: str) -> str:
        """Normalize Reddit URL to ensure it ends with / for JSON endpoint."""
        parsed = urlparse(url)
        path = parsed.path.rstrip("/")
        if "/comments/" in path:
            return f"https://www.reddit.com{path}/"
        return url

    # ------------------------------------------------------------------
    # Core fetch (with retry + deletion detection)
    # ------------------------------------------------------------------

    def _fetch_json(self, url: str) -> Dict:
        """Fetch JSON data from Reddit's public .json endpoint.

        Raises RedditPostDeleted if the post/page returns 404.
        Retries on transient errors automatically.
        """
        json_url = url.rstrip("/") + ".json"
        resp = _retry_request(self.session, json_url)
        data = resp.json()

        # Detect soft-deleted posts (Reddit sometimes returns 200 but
        # the post body is "[removed]" / "[deleted]")
        if isinstance(data, list) and len(data) >= 1:
            children = data[0].get("data", {}).get("children", [])
            if children:
                post_data = children[0].get("data", {})
                selftext = post_data.get("selftext", "")
                removed_by = post_data.get("removed_by_category", "")
                if selftext in ("[removed]", "[deleted]") or removed_by:
                    raise RedditPostDeleted(
                        f"Post appears deleted/removed (selftext={selftext!r}, "
                        f"removed_by={removed_by!r}) for {url}"
                    )

        return data

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def _flatten_comments(self, data: Dict, post_id: str) -> List[Dict[str, str]]:
        """Recursively flatten Reddit comment tree into a flat list."""
        comments: List[Dict[str, str]] = []

        if not isinstance(data, dict):
            return comments

        kind = data.get("kind")
        if kind != "t1":
            return comments

        comment_data = data.get("data", {})
        if not comment_data:
            return comments

        comment_id = comment_data.get("id", "")
        author = comment_data.get("author", "[deleted]")
        body = comment_data.get("body", "")
        created_utc = comment_data.get("created_utc", 0)
        permalink = comment_data.get("permalink", "")
        parent_id = comment_data.get("parent_id", "")

        comments.append({
            "comment_id": comment_id,
            "author": author if author else "[deleted]",
            "body": body or "",
            "created_utc": str(created_utc),
            "comment_url": f"https://www.reddit.com{permalink}" if permalink else "",
            "parent_id": parent_id or "",
            "post_id": post_id,
        })

        replies = comment_data.get("replies", {})
        if replies and isinstance(replies, dict):
            children = replies.get("data", {}).get("children", [])
            for child in children:
                comments.extend(self._flatten_comments(child, post_id))

        return comments

    def is_post_alive(self, post_url: str) -> bool:
        """Quick health-check: returns True if the post is still accessible
        and not deleted/removed. Returns False otherwise."""
        try:
            self._fetch_json(self._normalize_submission_url(post_url))
            return True
        except RedditPostDeleted:
            return False
        except Exception as exc:
            logger.warning("Could not verify post health for %s: %s", post_url, exc)
            return True  # Assume alive if we can't tell (network blip)

    def get_submission_context(self, post_url: str) -> Dict[str, str]:
        """Fetch post context (title, body, subreddit) from Reddit.

        Raises RedditPostDeleted if the post is gone.
        """
        normalized_url = self._normalize_submission_url(post_url)
        json_data = self._fetch_json(normalized_url)

        if not json_data or len(json_data) < 1:
            raise ValueError(f"Invalid Reddit JSON response for {post_url}")

        post_data = json_data[0].get("data", {}).get("children", [])
        if not post_data:
            raise ValueError(f"No post data found for {post_url}")

        post = post_data[0].get("data", {})
        return {
            "post_id": post.get("id", ""),
            "title": post.get("title", ""),
            "body": post.get("selftext", ""),
            "subreddit": post.get("subreddit", ""),
            "url": f"https://www.reddit.com{post.get('permalink', '')}" if post.get("permalink") else normalized_url,
        }

    def fetch_new_comments(
        self,
        post_url: str,
        known_comment_ids: Optional[set[str]] = None,
        min_created_utc: Optional[float] = None,
    ) -> List[Dict[str, str]]:
        """Fetch new comments from a Reddit post, filtering out known ones.

        Returns empty list (instead of crashing) if the post is deleted.
        """
        known_comment_ids = known_comment_ids or set()
        normalized_url = self._normalize_submission_url(post_url)

        try:
            json_data = self._fetch_json(normalized_url)
        except RedditPostDeleted:
            logger.warning("Post deleted, returning empty comments: %s", post_url)
            return []

        if not json_data or len(json_data) < 2:
            return []

        post_data = json_data[0].get("data", {}).get("children", [])
        post_id = post_data[0].get("data", {}).get("id", "") if post_data else ""

        comments_data = json_data[1].get("data", {}).get("children", [])
        all_comments: List[Dict[str, str]] = []
        for child in comments_data:
            all_comments.extend(self._flatten_comments(child, post_id))

        filtered_comments: List[Dict[str, str]] = []
        for comment in all_comments:
            cid = comment.get("comment_id", "")
            if cid in known_comment_ids:
                continue
            if min_created_utc is not None:
                created_utc = float(comment.get("created_utc", "0"))
                if created_utc <= min_created_utc:
                    continue
            filtered_comments.append(comment)

        filtered_comments.sort(key=lambda c: float(c.get("created_utc", "0")))
        return filtered_comments

    def get_comment_score(self, comment_url: str, target_comment_id: Optional[str] = None) -> Optional[Dict]:
        """Fetch a specific comment's score (upvotes) and metadata.
        Returns None gracefully on any error."""
        try:
            json_data = self._fetch_json(comment_url)

            if not json_data or len(json_data) < 2:
                return None

            if not target_comment_id and "/comments/" in comment_url:
                parts = comment_url.split("/")
                for i, part in enumerate(parts):
                    if part == "comments" and i + 3 < len(parts):
                        target_comment_id = parts[i + 3]
                        break

            comments_data = json_data[1].get("data", {}).get("children", [])
            for child in comments_data:
                info = self._extract_comment_score_recursive(child, target_comment_id)
                if info:
                    return info
            return None

        except RedditPostDeleted:
            logger.warning("Comment/post deleted when fetching score: %s", comment_url)
            return None
        except Exception as e:
            logger.warning("Error fetching comment score for %s: %s", comment_url, e)
            return None

    def _extract_comment_score_recursive(self, data: Dict, target_id: Optional[str] = None) -> Optional[Dict]:
        """Recursively search for a comment and extract its score."""
        if not isinstance(data, dict):
            return None
        if data.get("kind") != "t1":
            return None

        comment_data = data.get("data", {})
        if not comment_data:
            return None

        comment_id = comment_data.get("id", "")

        if target_id and comment_id != target_id:
            replies = comment_data.get("replies", {})
            if replies and isinstance(replies, dict):
                children = replies.get("data", {}).get("children", [])
                for child in children:
                    result = self._extract_comment_score_recursive(child, target_id)
                    if result:
                        return result
            return None

        return {
            "comment_id": comment_id,
            "score": comment_data.get("score", 0),
            "upvotes": comment_data.get("ups", 0),
            "downvotes": comment_data.get("downs", 0),
            "created_utc": comment_data.get("created_utc", 0),
            "author": comment_data.get("author", "[deleted]"),
            "permalink": comment_data.get("permalink", ""),
        }

    def get_post_metrics(self, post_url: str) -> Optional[Dict]:
        """Fetch post metrics: upvotes, comment count, etc.
        Returns None gracefully if post is deleted or unreachable."""
        try:
            normalized_url = self._normalize_submission_url(post_url)
            json_data = self._fetch_json(normalized_url)

            if not json_data or len(json_data) < 1:
                return None

            post_data = json_data[0].get("data", {}).get("children", [])
            if not post_data:
                return None

            post = post_data[0].get("data", {})
            return {
                "post_id": post.get("id", ""),
                "title": post.get("title", ""),
                "score": post.get("score", 0),
                "upvotes": post.get("ups", 0),
                "num_comments": post.get("num_comments", 0),
                "created_utc": post.get("created_utc", 0),
                "permalink": post.get("permalink", ""),
            }
        except RedditPostDeleted:
            logger.warning("Post deleted when fetching metrics: %s", post_url)
            return None
        except Exception as e:
            logger.warning("Error fetching post metrics for %s: %s", post_url, e)
            return None
