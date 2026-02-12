# Telegram Reddit Reply Bot

Standalone bot package for:
- Google Sheets-driven team/post schedule
- Telegram DM notifications
- Reddit comment polling
- Round-robin reply assignment
- LLM-assisted reply drafting

## Quick Start

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Configure environment:

```bash
copy env.example .env
```

3. Validate APIs:

```bash
python run_api_checks.py --step all
```

4. Collect Telegram numeric IDs (after members send `/start`):

```bash
python run_bot.py --mode collect-ids
```

5. Run bot:

```bash
python run_bot.py --mode daemon
```

## Render Deploy

This folder includes `render.yaml` for worker deployment.

## GitHub Actions (Free Hosting Path)

This repo includes workflows:

- `.github/workflows/bot-runner.yml`
  - Scheduled every 10 minutes
  - Runs: `python run_bot.py --mode once`
  - Supports manual run with mode `once` or `collect-ids`
- `.github/workflows/api-checks.yml`
  - Manual API health checks (`telegram`, `sheets`, `reddit`, `all`)

### Required GitHub Secrets

Set these in `Settings -> Secrets and variables -> Actions`:

- `TELEGRAM_BOT_TOKEN`
- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `REDDIT_USER_AGENT` (optional, defaults to `rt-cert-program-utils/telegram-reply-bot`)
- `OPENAI_API_KEY`

**Note:** Reddit API credentials are no longer needed. The bot uses Reddit's public JSON endpoints for scraping (read-only access).

### Optional GitHub Secrets

- `BOT_REPLY_MODEL` (default `gpt-4o-mini`)
- `BOT_TIMEZONE` (default `Africa/Addis_Ababa`)
- `BOT_DAILY_HOUR` (default `8`)
- `BOT_DAILY_MINUTE` (default `0`)
- `BOT_POLL_INTERVAL_MINUTES` (default `10`)
- `BOT_TEAMS_TAB` (default `Teams`)
- `BOT_POSTING_TAB` (default `PostingPlan`)
- `BOT_REPLY_QUEUE_TAB` (default `ReplyQueue`)
- `BOT_STATE_TAB` (default `State`)



