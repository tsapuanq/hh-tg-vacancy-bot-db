from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


PROMPT_TEMPLATE = """\
Ты фильтруешь Telegram-посты для канала с Data/ML/Analytics/BI/Data Engineering/DevOps/MLOps/AI вакансиями.

Тебе дают один сырой пост целиком. Не требуй заранее обязательные поля.

Задача:
1. Определи, является ли пост вакансией.
2. Определи, подходит ли вакансия для канала.
3. Если подходит, перепиши пост в аккуратную карточку для Telegram.
4. Не копируй исходный текст дословно.
5. Не выдумывай факты. Если поля нет, пиши "Не указано".

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

Верни только JSON без markdown:
{{
  "is_job": true,
  "is_relevant": true,
  "reason": "коротко почему публикуем или пропускаем",
  "message": "готовый текст публикации или null",
  "source_url": "{source_url}"
}}

Источник: {source_url}

Сырой пост:
{raw_text}
"""


@dataclass(frozen=True)
class TelegramPost:
    channel_username: str
    message_id: int
    raw_text: str

    @property
    def source_url(self) -> str:
        return f"https://t.me/{self.channel_username.lstrip('@')}/{self.message_id}"


def build_gpt_prompt(post: TelegramPost) -> str:
    return PROMPT_TEMPLATE.format(
        source_url=post.source_url,
        raw_text=post.raw_text.strip(),
    )


def parse_gpt_result(raw_response: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw_response)
    except json.JSONDecodeError as exc:
        raise ValueError(f"GPT response is not valid JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError("GPT response must be a JSON object")

    for key in ("is_job", "is_relevant", "reason", "message", "source_url"):
        if key not in payload:
            raise ValueError(f"GPT response missing required key: {key}")

    if not isinstance(payload["is_job"], bool):
        raise ValueError("is_job must be boolean")

    if not isinstance(payload["is_relevant"], bool):
        raise ValueError("is_relevant must be boolean")

    if payload["message"] is not None and not isinstance(payload["message"], str):
        raise ValueError("message must be string or null")

    return payload


def is_publishable(result: dict[str, Any]) -> bool:
    return bool(result["is_job"] and result["is_relevant"] and result["message"])


def run_dry_demo() -> None:
    samples = [
        TelegramPost(
            channel_username="@source_channel",
            message_id=101,
            raw_text=(
                "We are hiring a Junior Data Analyst. SQL, Python, Power BI. "
                "Remote, Kazakhstan. Send CV to hr@example.com"
            ),
        ),
        TelegramPost(
            channel_username="@source_channel",
            message_id=102,
            raw_text="New article: how to prepare for Python interviews in 2026.",
        ),
    ]

    for post in samples:
        print("=" * 80)
        print(build_gpt_prompt(post))


if __name__ == "__main__":
    run_dry_demo()
