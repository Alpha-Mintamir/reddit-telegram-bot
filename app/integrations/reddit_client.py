from __future__ import annotations

from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests

from app.config import BotConfig


class RedditClient:
    def __init__(self, config: BotConfig):
        # Ensure User-Agent is set - Reddit requires a proper User-Agent
        self.user_agent = config.reddit_user_agent or "rt-cert-program-utils/telegram-reply-bot"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "application/json",
        })

    @staticmethod
    def _normalize_submission_url(url: str) -> str:
        """Normalize Reddit URL to ensure it ends with / for JSON endpoint."""
        parsed = urlparse(url)
        path = parsed.path.rstrip("/")
        if "/comments/" in path:
            return f"https://www.reddit.com{path}/"
        return url

    def _fetch_json(self, url: str) -> Dict:
        """Fetch JSON data from Reddit's public .json endpoint."""
        json_url = url.rstrip("/") + ".json"
        response = self.session.get(json_url, timeout=10)
        response.raise_for_status()
        return response.json()

    def _flatten_comments(self, data: Dict, post_id: str) -> List[Dict[str, str]]:
        """Recursively flatten Reddit comment tree into a flat list."""
        comments: List[Dict[str, str]] = []
        
        if not isinstance(data, dict):
            return comments
            
        kind = data.get("kind")
        if kind != "t1":  # t1 = comment, t3 = post
            return comments
            
        comment_data = data.get("data", {})
        if not comment_data:
            return comments
            
        # Extract comment info
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
        
        # Recursively process replies
        replies = comment_data.get("replies", {})
        if replies and isinstance(replies, dict):
            children = replies.get("data", {}).get("children", [])
            for child in children:
                comments.extend(self._flatten_comments(child, post_id))
        
        return comments

    def get_submission_context(self, post_url: str) -> Dict[str, str]:
        """Fetch post context (title, body, subreddit) from Reddit."""
        normalized_url = self._normalize_submission_url(post_url)
        json_data = self._fetch_json(normalized_url)
        
        # Reddit JSON structure: [post_data, comments_data]
        if not json_data or len(json_data) < 1:
            raise ValueError(f"Invalid Reddit JSON response for {post_url}")
        
        post_data = json_data[0].get("data", {}).get("children", [])
        if not post_data:
            raise ValueError(f"No post data found for {post_url}")
        
        post = post_data[0].get("data", {})
        post_id = post.get("id", "")
        title = post.get("title", "")
        body = post.get("selftext", "")
        subreddit = post.get("subreddit", "")
        permalink = post.get("permalink", "")
        
        return {
            "post_id": post_id,
            "title": title or "",
            "body": body or "",
            "subreddit": subreddit or "",
            "url": f"https://www.reddit.com{permalink}" if permalink else normalized_url,
        }

    def fetch_new_comments(
        self,
        post_url: str,
        known_comment_ids: Optional[set[str]] = None,
        min_created_utc: Optional[float] = None,
    ) -> List[Dict[str, str]]:
        """Fetch new comments from a Reddit post, filtering out known ones."""
        known_comment_ids = known_comment_ids or set()
        normalized_url = self._normalize_submission_url(post_url)
        json_data = self._fetch_json(normalized_url)
        
        # Reddit JSON structure: [post_data, comments_data]
        if not json_data or len(json_data) < 2:
            return []
        
        # Get post ID for comment context
        post_data = json_data[0].get("data", {}).get("children", [])
        post_id = ""
        if post_data:
            post_id = post_data[0].get("data", {}).get("id", "")
        
        # Extract all comments from the comments tree
        comments_data = json_data[1].get("data", {}).get("children", [])
        all_comments: List[Dict[str, str]] = []
        for child in comments_data:
            all_comments.extend(self._flatten_comments(child, post_id))
        
        # Filter out known comments and apply time filter
        filtered_comments: List[Dict[str, str]] = []
        for comment in all_comments:
            comment_id = comment.get("comment_id", "")
            if comment_id in known_comment_ids:
                continue
            if min_created_utc is not None:
                created_utc = float(comment.get("created_utc", "0"))
                if created_utc <= min_created_utc:
                    continue
            filtered_comments.append(comment)
        
        # Sort by creation time
        filtered_comments.sort(key=lambda c: float(c.get("created_utc", "0")))
        return filtered_comments




