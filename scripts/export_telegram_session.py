from __future__ import annotations

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SESSION_FILE = ROOT_DIR / ".telegram_source_reader"


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required env var: {name}")
    return value.strip()


async def main() -> None:
    load_dotenv(ROOT_DIR / ".env")
    api_id = int(get_required_env("TELEGRAM_API_ID"))
    api_hash = get_required_env("TELEGRAM_API_HASH")
    phone = os.getenv("TELEGRAM_PHONE")
    password = os.getenv("TELEGRAM_PASSWORD")
    session_file = Path(os.getenv("TELEGRAM_SESSION_FILE") or DEFAULT_SESSION_FILE)

    client = TelegramClient(str(session_file), api_id, api_hash)
    await client.start(
        phone=phone.strip() if phone else None,
        password=password.strip() if password else None,
    )
    try:
        print(StringSession.save(client.session))
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
