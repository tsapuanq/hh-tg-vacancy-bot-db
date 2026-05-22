from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

try:
    from telethon import TelegramClient
except ImportError as exc:
    raise SystemExit(
        "Telethon is not installed. Install it with: python3 -m pip install telethon"
    ) from exc


ROOT_DIR = Path(__file__).resolve().parents[1]
SESSION_DIR = ROOT_DIR / "test" / ".sessions"
SESSION_NAME = SESSION_DIR / "telegram_source_reader"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read recent posts from TELEGRAM_SOURCE_CHANNEL without GPT, DB, or publishing."
    )
    parser.add_argument("--limit", type=int, default=10, help="How many posts to read.")
    return parser.parse_args()


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required env var: {name}")
    return value.strip()


def normalize_channel(value: str) -> str:
    value = value.strip()
    if value.startswith("https://t.me/"):
        value = value.removeprefix("https://t.me/").split("/", 1)[0]
    return value


async def main() -> None:
    load_dotenv(ROOT_DIR / ".env")
    args = parse_args()

    api_id = int(get_required_env("TELEGRAM_API_ID"))
    api_hash = get_required_env("TELEGRAM_API_HASH")
    channel = normalize_channel(get_required_env("TELEGRAM_SOURCE_CHANNEL"))
    phone = os.getenv("TELEGRAM_PHONE")
    password = os.getenv("TELEGRAM_PASSWORD")

    SESSION_DIR.mkdir(parents=True, exist_ok=True)

    client = TelegramClient(str(SESSION_NAME), api_id, api_hash)
    await client.start(
        phone=phone.strip() if phone else None,
        password=password.strip() if password else None,
    )
    try:
        entity = await client.get_entity(channel)
        print(f"Reading last {args.limit} posts from: {channel}")
        print(f"Resolved title: {getattr(entity, 'title', 'unknown')}")
        print("=" * 80)

        async for message in client.iter_messages(entity, limit=args.limit):
            text = (message.message or "").strip()
            if not text:
                continue

            public_name = channel.lstrip("@")
            source_url = f"https://t.me/{public_name}/{message.id}"
            preview = text[:1000]

            print(f"id: {message.id}")
            print(f"date: {message.date}")
            print(f"url: {source_url}")
            print("text:")
            print(preview)
            if len(text) > len(preview):
                print("... <truncated>")
            print("-" * 80)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
