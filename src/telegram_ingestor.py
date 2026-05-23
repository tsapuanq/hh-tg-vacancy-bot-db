from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg2.extras
from psycopg2.extras import Json
from telegram import Bot
from telegram.error import TelegramError

from database import Database
from src.config import (
    CHANNEL_USERNAME,
    TELEGRAM_API_HASH,
    TELEGRAM_API_ID,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_DELAY_SECONDS,
    TELEGRAM_LOOKBACK_DAYS,
    TELEGRAM_MAX_DELAY_SECONDS,
    TELEGRAM_PROCESS_LIMIT,
    TELEGRAM_PASSWORD,
    TELEGRAM_PHONE,
    TELEGRAM_SESSION_FILE,
    TELEGRAM_SESSION_STRING,
    TELEGRAM_SOURCE_CHANNEL,
    TELEGRAM_SOURCE_LIMIT,
)
from src.llm_summary import openai_api_call
from src.utils import setup_logger

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except ImportError as exc:
    raise RuntimeError(
        "Telethon is required for Telegram source ingestion. Install: python3 -m pip install telethon"
    ) from exc


MAX_TELEGRAM_MESSAGE_LEN = 3900
ROOT_DIR = Path(__file__).resolve().parents[1]

GPT_PROMPT_TEMPLATE = """\
Ты фильтруешь Telegram-посты для канала с Data/ML/Analytics/BI/Data Engineering/DevOps/MLOps/AI вакансиями.

Тебе дают один сырой пост целиком. Не требуй заранее обязательные поля.

Задача:
1. Определи, является ли пост вакансией.
2. Определи, подходит ли вакансия для канала.
3. Если подходит, перепиши пост в аккуратную карточку для Telegram.
4. Не копируй исходный текст дословно.
5. Не выдумывай факты. Если поля нет, пиши "Не указано".
6. Не добавляй ссылку на исходный Telegram-канал и не упоминай, откуда взят пост.

Подходящие направления:
- Data Scientist
- ML Engineer / AI Engineer / NLP / CV
- Data Analyst / BI Analyst
- Data Engineer / Big Data / DWH / ETL
- DevOps / MLOps
- System Analyst

Неподходящие посты:
- новости, статьи, курсы, митапы, мемы;
- общие программистские посты без вакансии;
- вакансии вне целевых направлений.

География:
- Публикуй вакансии по Казахстану.
- Если явно указана другая страна, например Узбекистан, Россия, Европа, США, и нет Казахстана или remote для Казахстана, ставь is_relevant=false.
- Если страна или город непонятны, не отсекай вакансию только из-за географии.
- Если указано remote/global без ограничений по стране, оставляй вакансию.

Верни только JSON без markdown:
{{
  "is_job": true,
  "is_relevant": true,
  "reason": "коротко почему публикуем или пропускаем",
  "message": "готовый текст публикации без ссылки на источник или null",
  "source_url": "{source_url}"
}}

Источник для внутренней дедупликации, не добавлять в публикацию: {source_url}

Сырой пост:
{raw_text}
"""


@dataclass(frozen=True)
class TelegramSourcePost:
    source_channel: str
    message_id: int
    raw_text: str
    published_at: datetime | None

    @property
    def source_url(self) -> str:
        public_name = self.source_channel.lstrip("@")
        return f"https://t.me/{public_name}/{self.message_id}"


def normalize_channel(value: str) -> str:
    value = value.strip()
    if value.startswith("https://t.me/"):
        value = value.removeprefix("https://t.me/").split("/", 1)[0]
    return value


def resolve_file_session_name() -> str:
    if TELEGRAM_SESSION_FILE:
        return TELEGRAM_SESSION_FILE
    test_session = ROOT_DIR / "test" / ".sessions" / "telegram_source_reader"
    if test_session.with_suffix(".session").exists():
        return str(test_session)
    return str(ROOT_DIR / ".telegram_source_reader")


def build_gpt_prompt(post: TelegramSourcePost | dict[str, Any]) -> str:
    if isinstance(post, TelegramSourcePost):
        source_url = post.source_url
        raw_text = post.raw_text
    else:
        source_url = str(post["source_url"])
        raw_text = str(post["raw_text"])
    return GPT_PROMPT_TEMPLATE.format(source_url=source_url, raw_text=raw_text.strip())


def clean_gpt_json(raw: str) -> str:
    text = raw.strip()
    if text.startswith("```"):
        text = text.removeprefix("```json").removeprefix("```").strip()
        text = text.removesuffix("```").strip()
    return text


def parse_gpt_result(raw_response: str) -> dict[str, Any]:
    payload = json.loads(clean_gpt_json(raw_response))
    if not isinstance(payload, dict):
        raise ValueError("GPT response must be a JSON object")
    for key in ("is_job", "is_relevant", "reason", "message", "source_url"):
        if key not in payload:
            raise ValueError(f"GPT response missing key: {key}")
    if not isinstance(payload["is_job"], bool):
        raise ValueError("is_job must be boolean")
    if not isinstance(payload["is_relevant"], bool):
        raise ValueError("is_relevant must be boolean")
    if payload["message"] is not None and not isinstance(payload["message"], str):
        raise ValueError("message must be string or null")
    return payload


def is_publishable(result: dict[str, Any]) -> bool:
    return bool(result["is_job"] and result["is_relevant"] and result["message"])


def strip_source_links(message: str, source_url: str) -> str:
    text = message.replace(source_url, "")
    text = re.sub(r"(?im)^\s*источник\s*:.*$", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def ensure_schema(db: Database) -> None:
    conn = None
    cursor = None
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_posts (
                id BIGSERIAL PRIMARY KEY,
                source_channel TEXT NOT NULL,
                message_id BIGINT NOT NULL,
                source_url TEXT NOT NULL UNIQUE,
                raw_text TEXT NOT NULL,
                published_at TIMESTAMPTZ,
                gpt_result_json JSONB,
                is_job BOOLEAN,
                is_relevant BOOLEAN,
                reason TEXT,
                publish_text TEXT,
                processed_at TIMESTAMPTZ,
                sent_to_telegram BOOLEAN NOT NULL DEFAULT FALSE,
                sent_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (source_channel, message_id)
            );
            """
        )
        conn.commit()
    finally:
        if cursor:
            cursor.close()
        if conn:
            db.return_connection(conn)


async def read_source_posts(
    source_channel: str,
    limit: int,
    lookback_days: int,
    session_string: str | None,
    phone: str | None,
    password: str | None,
) -> list[TelegramSourcePost]:
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        raise RuntimeError("TELEGRAM_API_ID and TELEGRAM_API_HASH are required")
    if not session_string and os.getenv("GITHUB_ACTIONS") == "true":
        raise RuntimeError("TELEGRAM_SESSION_STRING is required in GitHub Actions")

    channel = normalize_channel(source_channel)
    session = StringSession(session_string) if session_string else resolve_file_session_name()
    client = TelegramClient(session, int(TELEGRAM_API_ID), TELEGRAM_API_HASH)
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    posts: list[TelegramSourcePost] = []

    await client.start(
        phone=phone.strip() if phone else None,
        password=password.strip() if password else None,
    )
    try:
        entity = await client.get_entity(channel)
        async for message in client.iter_messages(entity, limit=limit):
            if message.date and message.date < cutoff:
                continue
            text = (message.message or "").strip()
            if not text:
                continue
            posts.append(
                TelegramSourcePost(
                    source_channel=channel,
                    message_id=message.id,
                    raw_text=text,
                    published_at=message.date,
                )
            )
        return list(reversed(posts))
    finally:
        await client.disconnect()


def save_raw_posts(db: Database, posts: list[TelegramSourcePost]) -> int:
    if not posts:
        return 0
    conn = None
    cursor = None
    inserted = 0
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        for post in posts:
            cursor.execute(
                """
                INSERT INTO telegram_posts (
                    source_channel, message_id, source_url, raw_text, published_at
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (source_url) DO UPDATE
                SET raw_text = EXCLUDED.raw_text,
                    published_at = EXCLUDED.published_at
                WHERE telegram_posts.gpt_result_json IS NULL
                """,
                (
                    post.source_channel,
                    post.message_id,
                    post.source_url,
                    post.raw_text,
                    post.published_at,
                ),
            )
            inserted += max(cursor.rowcount, 0)
        conn.commit()
        return inserted
    finally:
        if cursor:
            cursor.close()
        if conn:
            db.return_connection(conn)


def process_unprocessed_posts(db: Database, limit: int) -> int:
    conn = None
    cursor = None
    processed = 0
    try:
        conn = db.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(
            """
            SELECT id, source_url, raw_text
            FROM telegram_posts
            WHERE gpt_result_json IS NULL
            ORDER BY published_at NULLS LAST, id
            LIMIT %s
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        for row in rows:
            raw_response = openai_api_call(build_gpt_prompt(dict(row)))
            if not raw_response:
                logging.warning("[Telegram GPT] Empty response for %s", row["source_url"])
                continue
            try:
                result = parse_gpt_result(raw_response)
            except Exception as exc:
                logging.warning(
                    "[Telegram GPT] Invalid JSON for %s: %s\n%s",
                    row["source_url"],
                    exc,
                    raw_response,
                )
                continue

            publish_text = None
            if is_publishable(result):
                publish_text = strip_source_links(str(result["message"]), row["source_url"])

            cursor.execute(
                """
                UPDATE telegram_posts
                SET gpt_result_json = %s,
                    is_job = %s,
                    is_relevant = %s,
                    reason = %s,
                    publish_text = %s,
                    processed_at = NOW()
                WHERE id = %s
                """,
                (
                    Json(result),
                    result["is_job"],
                    result["is_relevant"],
                    str(result.get("reason") or ""),
                    publish_text,
                    row["id"],
                ),
            )
            conn.commit()
            processed += 1
        return processed
    finally:
        if cursor:
            cursor.close()
        if conn:
            db.return_connection(conn)


async def publish_approved_posts(db: Database, channel_username: str, bot_token: str) -> int:
    conn = None
    cursor = None
    sent = 0
    try:
        conn = db.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(
            """
            SELECT id, publish_text
            FROM telegram_posts
            WHERE is_job = TRUE
            AND is_relevant = TRUE
            AND sent_to_telegram = FALSE
            AND publish_text IS NOT NULL
            ORDER BY processed_at NULLS LAST, id
            """
        )
        rows = cursor.fetchall()
        if not rows:
            return 0

        bot = Bot(token=bot_token)
        for idx, row in enumerate(rows, 1):
            text = str(row["publish_text"]).strip()
            if len(text) > MAX_TELEGRAM_MESSAGE_LEN:
                text = text[:MAX_TELEGRAM_MESSAGE_LEN].rstrip() + "\n\n... <truncated>"
            try:
                await bot.send_message(chat_id=channel_username, text=text)
            except TelegramError as exc:
                logging.error("[Telegram Publish] Failed for telegram_posts.id=%s: %s", row["id"], exc)
                continue

            cursor.execute(
                """
                UPDATE telegram_posts
                SET sent_to_telegram = TRUE,
                    sent_at = NOW()
                WHERE id = %s
                """,
                (row["id"],),
            )
            conn.commit()
            sent += 1

            if idx < len(rows):
                await asyncio.sleep(random.uniform(TELEGRAM_DELAY_SECONDS, TELEGRAM_MAX_DELAY_SECONDS))
        return sent
    finally:
        if cursor:
            cursor.close()
        if conn:
            db.return_connection(conn)


async def run_telegram_ingestor(db: Database, publish: bool = True) -> None:
    setup_logger()
    if not TELEGRAM_SOURCE_CHANNEL:
        logging.info("[Telegram Source] TELEGRAM_SOURCE_CHANNEL is not set, skipping.")
        return
    if not TELEGRAM_BOT_TOKEN or not CHANNEL_USERNAME:
        logging.info("[Telegram Source] TELEGRAM_BOT_TOKEN or CHANNEL_USERNAME is not set, skipping publish.")
        return

    ensure_schema(db)
    posts = await read_source_posts(
        source_channel=TELEGRAM_SOURCE_CHANNEL,
        limit=TELEGRAM_SOURCE_LIMIT,
        lookback_days=TELEGRAM_LOOKBACK_DAYS,
        session_string=TELEGRAM_SESSION_STRING,
        phone=TELEGRAM_PHONE,
        password=TELEGRAM_PASSWORD,
    )
    logging.info("[Telegram Source] Read %s posts from source channel.", len(posts))
    saved = save_raw_posts(db, posts)
    logging.info("[Telegram Source] Inserted/updated %s raw posts.", saved)
    processed = process_unprocessed_posts(db, TELEGRAM_PROCESS_LIMIT)
    logging.info("[Telegram Source] GPT-processed %s posts.", processed)
    if not publish:
        logging.info("[Telegram Source] Publish disabled, leaving approved posts unsent.")
        return

    sent = await publish_approved_posts(db, CHANNEL_USERNAME, TELEGRAM_BOT_TOKEN)
    logging.info("[Telegram Source] Published %s approved posts.", sent)
