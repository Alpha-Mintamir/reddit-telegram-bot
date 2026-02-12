# Reddit Scraping Setup Guide

## No API Credentials Needed!

This bot uses Reddit's public JSON endpoints for read-only access. **No Reddit API registration or credentials are required.**

## How It Works

Reddit exposes public JSON data for any URL by appending `.json`. For example:
- `https://www.reddit.com/r/MachineLearning/comments/abc123/` 
- Becomes: `https://www.reddit.com/r/MachineLearning/comments/abc123/.json`

The bot uses this to fetch:
- Post context (title, body, subreddit)
- Comments (author, body, timestamps)

## Configuration

### User-Agent Header (Optional)

Set a polite User-Agent string for HTTP requests. This is optional but recommended:

```bash
REDDIT_USER_AGENT=rt-cert-program-utils/1.0 by u/yourusername
```

Or in GitHub Secrets:
- `REDDIT_USER_AGENT` = `rt-cert-program-utils/1.0 by u/yourusername`

**Default:** If not set, defaults to `rt-cert-program-utils/telegram-reply-bot`

## Testing

Run the API check:
```bash
python run_api_checks.py --step reddit
```

Or use GitHub Action: `API Setup Checks` with `step=reddit`

## Benefits

- ✅ No API registration wait time
- ✅ No API approval process
- ✅ No rate limit concerns for read-only access
- ✅ Works immediately
- ✅ No credentials to manage

## Limitations

- Read-only access (which is all this bot needs)
- Subject to Reddit's public rate limits (very generous for read access)
- No authentication means no private subreddit access (not needed for this bot)
