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


