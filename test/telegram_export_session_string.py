from __future__ import annotations

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession


ROOT_DIR = Path(__file__).resolve().parents[1]
SESSION_FILE = ROOT_DIR / "test" / ".sessions" / "telegram_source_reader"


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required env var: {name}")
    return value.strip()


async def main() -> None:
    load_dotenv(ROOT_DIR / ".env")
    api_id = int(get_required_env("TELEGRAM_API_ID"))
    api_hash = get_required_env("TELEGRAM_API_HASH")

    file_client = TelegramClient(str(SESSION_FILE), api_id, api_hash)
    await file_client.connect()
    try:
        if not await file_client.is_user_authorized():
            raise SystemExit(
                "Local file session is not authorized. Run telegram_read_channel.py first."
            )
        session_string = StringSession.save(file_client.session)
    finally:
        await file_client.disconnect()

    print(session_string)


if __name__ == "__main__":
    asyncio.run(main())
