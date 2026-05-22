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
