from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telegram import Bot

ROOT_DIR = Path(__file__).resolve().parents[1]
TEST_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(TEST_DIR) not in sys.path:
    sys.path.insert(0, str(TEST_DIR))

from src.llm_summary import openai_api_call
from telegram_gpt_contract import (
    TelegramPost,
    build_gpt_prompt,
    is_publishable,
    parse_gpt_result,
)
from telegram_read_channel import SESSION_NAME, normalize_channel

try:
    from telethon import TelegramClient
except ImportError as exc:
    raise SystemExit(
        "Telethon is not installed. Install it with: python3 -m pip install telethon"
    ) from exc


STATE_DIR = ROOT_DIR / "test" / ".state"
STATE_FILE = STATE_DIR / "telegram_ingest_state.json"
MAX_TELEGRAM_MESSAGE_LEN = 3900


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read source channel posts, classify with GPT, and optionally publish to CHANNEL_USERNAME_TEST."
    )
    parser.add_argument("--limit", type=int, default=5, help="How many recent source posts to inspect.")
    parser.add_argument("--send", action="store_true", help="Send approved GPT messages to CHANNEL_USERNAME_TEST.")
    parser.add_argument("--dry-run", action="store_true", help="Print decisions without sending messages.")
    parser.add_argument("--reprocess", action="store_true", help="Ignore local processed-message state.")
    return parser.parse_args()


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required env var: {name}")
    return value.strip()


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {"processed_source_urls": []}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"processed_source_urls": []}


def save_state(state: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def mark_processed(state: dict[str, Any], source_url: str) -> None:
    processed = set(state.get("processed_source_urls") or [])
    processed.add(source_url)
    state["processed_source_urls"] = sorted(processed)


def clean_gpt_json(raw: str) -> str:
    text = raw.strip()
    if text.startswith("```"):
        text = text.removeprefix("```json").removeprefix("```").strip()
        text = text.removesuffix("```").strip()
    return text


def ensure_source_line(message: str, source_url: str) -> str:
    text = message.replace(source_url, "")
    text = re.sub(r"(?im)^\s*источник\s*:.*$", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


async def read_source_posts(
    api_id: int,
    api_hash: str,
    channel: str,
    limit: int,
    phone: str | None,
    password: str | None,
) -> list[TelegramPost]:
    client = TelegramClient(str(SESSION_NAME), api_id, api_hash)
    await client.start(
        phone=phone.strip() if phone else None,
        password=password.strip() if password else None,
    )
    try:
        entity = await client.get_entity(channel)
        posts: list[TelegramPost] = []
        async for message in client.iter_messages(entity, limit=limit):
            text = (message.message or "").strip()
            if not text:
                continue
            posts.append(
                TelegramPost(
                    channel_username=channel,
                    message_id=message.id,
                    raw_text=text,
                )
            )
        return list(reversed(posts))
    finally:
        await client.disconnect()


async def publish_to_test_channel(bot_token: str, channel: str, message: str) -> None:
    if len(message) > MAX_TELEGRAM_MESSAGE_LEN:
        message = message[:MAX_TELEGRAM_MESSAGE_LEN].rstrip() + "\n\n... <truncated>"
    bot = Bot(token=bot_token)
    await bot.send_message(chat_id=channel, text=message)


async def main() -> None:
    load_dotenv(ROOT_DIR / ".env")
    args = parse_args()
    if args.send and args.dry_run:
        raise SystemExit("Choose either --send or --dry-run, not both.")
    if not args.send and not args.dry_run:
        raise SystemExit("Use --dry-run first, or pass --send to publish to CHANNEL_USERNAME_TEST.")

    api_id = int(get_required_env("TELEGRAM_API_ID"))
    api_hash = get_required_env("TELEGRAM_API_HASH")
    source_channel = normalize_channel(get_required_env("TELEGRAM_SOURCE_CHANNEL"))
    phone = os.getenv("TELEGRAM_PHONE")
    password = os.getenv("TELEGRAM_PASSWORD")

    bot_token = get_required_env("TELEGRAM_BOT_TOKEN")
    test_channel = get_required_env("CHANNEL_USERNAME_TEST")

    state = load_state()
    processed = set(state.get("processed_source_urls") or [])
    posts = await read_source_posts(
        api_id=api_id,
        api_hash=api_hash,
        channel=source_channel,
        limit=args.limit,
        phone=phone,
        password=password,
    )

    print(f"Loaded {len(posts)} source posts from {source_channel}")
    for post in posts:
        if post.source_url in processed and not args.reprocess:
            print(f"SKIP already processed: {post.source_url}")
            continue

        print("=" * 80)
        print(f"Source: {post.source_url}")
        raw_response = openai_api_call(build_gpt_prompt(post))
        if not raw_response:
            print("SKIP empty GPT response")
            continue

        try:
            result = parse_gpt_result(clean_gpt_json(raw_response))
        except ValueError as exc:
            print(f"SKIP invalid GPT JSON: {exc}")
            print(raw_response)
            continue

        print(
            "Decision:",
            f"is_job={result['is_job']}",
            f"is_relevant={result['is_relevant']}",
            f"reason={result['reason']}",
        )

        if not is_publishable(result):
            mark_processed(state, post.source_url)
            save_state(state)
            print("SKIP not publishable")
            continue

        message = ensure_source_line(str(result["message"]), post.source_url)
        print("Message preview:")
        print(message[:1200])

        if args.send:
            await publish_to_test_channel(bot_token, test_channel, message)
            print(f"SENT to {test_channel}")
        else:
            print(f"DRY-RUN would send to {test_channel}")

        mark_processed(state, post.source_url)
        save_state(state)


if __name__ == "__main__":
    asyncio.run(main())
