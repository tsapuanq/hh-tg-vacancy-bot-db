# Telegram Vacancy Ingestor Prototype

This folder is an isolated test area for the Telegram-channel vacancy source.
It does not change the current HH scraper or publisher pipeline.

## Target Flow

```text
telegram channel post
  -> save raw text and source url
  -> send whole post to GPT
  -> GPT returns strict JSON
  -> skip if not a job or not relevant
  -> publish generated message later
```

## GPT Contract

The Telegram post is treated as unstructured input. The code does not require
fields like title, company, salary, or location before calling GPT.

Expected JSON:

```json
{
  "is_job": true,
  "is_relevant": true,
  "reason": "short explanation",
  "message": "ready Telegram post text",
  "source_url": "https://t.me/source_channel/123"
}
```

Rules:

- `is_job=false` for news, courses, memes, generic programming posts, or events.
- `is_relevant=false` for jobs outside Data, ML, Analytics, BI, Data Engineering, DevOps, MLOps, AI/NLP/CV, or System Analysis.
- `is_relevant=false` for jobs that are clearly outside Kazakhstan.
- If the country is unclear, keep the job instead of rejecting it.
- `message=null` when the post must not be published.
- Missing details should be written as `Не указано`; GPT must not invent facts.
- The final message should be rewritten, not copied verbatim from the source.

## Current Test File

`telegram_gpt_contract.py` contains a dry-run prototype:

- builds the GPT prompt from raw Telegram text;
- parses strict JSON from a model response;
- decides whether the result is publishable;
- runs local sample posts without network or DB access.

Run:

```bash
python3 test/telegram_gpt_contract.py
```

## Read Telegram Channel

Install the read-only test dependency:

```bash
python3 -m pip install telethon
```

Local `.env` values:

```env
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_SOURCE_CHANNEL=@source_channel
TELEGRAM_PHONE=+77001234567
TELEGRAM_PASSWORD=your_telegram_2fa_password
```

`TELEGRAM_PHONE` is optional. If it is missing, Telethon asks for the phone
number in the terminal. The first login also asks for the Telegram login code
and 2FA password if enabled. `TELEGRAM_PASSWORD` is optional and is only used
for local 2FA login.

Read recent posts without GPT, DB writes, or publishing:

```bash
python3 test/telegram_read_channel.py --limit 5
```

Telethon stores the local login session under `test/.sessions/`; this folder is
ignored by git.

## GPT + Test Publish

The end-to-end test reads Telegram posts, asks GPT for a publish/skip decision,
and sends only approved messages to `CHANNEL_USERNAME_TEST`.

Required `.env` values:

```env
TELEGRAM_BOT_TOKEN=your_bot_token
CHANNEL_USERNAME_TEST=@your_test_channel
OPENAI_API_KEY=your_openai_key
```

Run a safe dry-run without sending:

```bash
python3 test/telegram_gpt_publish_test.py --limit 5 --dry-run
```

Send approved messages to the test channel:

```bash
python3 test/telegram_gpt_publish_test.py --limit 5 --send
```

Processed Telegram source message ids are stored under `test/.state/`, which is
ignored by git. Use `--reprocess` to ignore the local state for another test.
