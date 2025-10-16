# publisher.py
import asyncio
import random
import logging
import re
from telegram import Bot
from telegram.error import TelegramError
from src.config import (
    TELEGRAM_BOT_TOKEN,
    CHANNEL_USERNAME,
    TELEGRAM_DELAY_SECONDS,
    TELEGRAM_MAX_DELAY_SECONDS,
)
from src.llm_summary import summarize_description_llm, filter_vacancy_llm
from src.cleaning import clean_text_safe
from database import Database
from datetime import date
from src.utils import is_potentially_relevant
import psycopg2.extras

def escape_markdown_v2(text: str) -> str:
    """Экранирует специальные символы MarkdownV2 в строке."""
    if not isinstance(text, str):
        text = str(text if text is not None else "")

    chars_to_escape = r"_*[]()~`>#+-=|{}.!"

    pattern = r"(" + "|".join(re.escape(char) for char in chars_to_escape) + r")"

    escaped_text = re.sub(pattern, r"\\\1", text)

    return escaped_text


def _to_bullets(x) -> str:
    """Преобразует список или строку с переносами строк в форматированный список Markdown."""
    if not x or (isinstance(x, str) and x.strip().lower() == "не указано"):
        return "Не указано"

    if isinstance(x, list):
        lines = x
    else:
        lines = str(x).split("\n")

    bullets = []
    for item in lines:
        s = str(item).strip().strip("'\"")
        if s and s.lower() != "не указано":
            escaped_item = escape_markdown_v2(s)
            bullets.append(f"• {escaped_item}")

    return "\n".join(bullets) if bullets else "Не указано"


def format_message(data: dict, summary: dict) -> str:
    """
    Форматирует данные о вакансии и summary для отправки в Telegram в MarkdownV2.
    Применяет экранирование для всех текстовых полей.
    """
    title = escape_markdown_v2(clean_text_safe(data.get("title", "Не указано")))
    company = escape_markdown_v2(clean_text_safe(data.get("company", "Не указано")))
    location = escape_markdown_v2(clean_text_safe(data.get("location", "Не указано")))

    salary_raw_val = data.get("salary_range") or data.get("salary")
    salary_raw_str = str(salary_raw_val) if salary_raw_val is not None else "Не указано"
    salary_info = escape_markdown_v2(clean_text_safe(salary_raw_str))

    experience = escape_markdown_v2(
        clean_text_safe(data.get("experience", "Не указано"))
    )
    employment_type = escape_markdown_v2(
        clean_text_safe(data.get("employment_type", "Не указано"))
    )
    schedule = escape_markdown_v2(clean_text_safe(data.get("schedule", "Не указано")))
    working_hours = escape_markdown_v2(
        clean_text_safe(data.get("working_hours", "Не указано"))
    )
    work_format = escape_markdown_v2(
        clean_text_safe(data.get("work_format", "Не указано"))
    )

    pub_date_obj = data.get("published_at")
    pub_date_str_formatted = "Не указано"
    if isinstance(pub_date_obj, date):
        try:
            pub_date_str_formatted = pub_date_obj.strftime("%d.%m.%Y")
        except Exception:
            pub_date_str_formatted = str(pub_date_obj)
    pub_date_str_formatted = escape_markdown_v2(pub_date_str_formatted)

    resp = _to_bullets(summary.get("responsibilities", "Не указано"))
    reqs = _to_bullets(summary.get("requirements", "Не указано"))

    about_raw = summary.get("about_company", "Не указано")
    about = escape_markdown_v2(clean_text_safe(about_raw))

    url = data.get("url", "#")
    link_text = escape_markdown_v2("Подробнее на hh")

    message_text = f"""\
🌐 Город: {location}
📅 Должность: **{title}**
💼 Компания: {company}
💰 ЗП: {salary_info}

🎓 Опыт: {experience}
📂 Тип занятости: {employment_type}
📆 График: {schedule}
🕒 Рабочие часы: {working_hours}
🏠 Формат работы: {work_format}
📅 Дата публикации: {pub_date_str_formatted}

🧾 Обязанности:
{resp}

🎯 Требования:
{reqs}

🏢 О компании:
{about}

🔎 [{link_text}]({url})
"""

    return message_text


async def main(db: Database):
    """
    Основная функция пайплайна публикации: фильтрация, суммаризация, отправка в Telegram.
    """
    conn = None
    cursor = None

    try:
        conn = db.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        logging.info("🕵️‍♂️ Шаг 1: Проверка релевантности новых вакансий...")
        cursor.execute(
            """
            SELECT id, title, description
            FROM vacancies
            WHERE is_relevant IS NULL 
            AND published_at >= CURRENT_DATE - INTERVAL '1 day'
            """
        )
        rows_to_filter = cursor.fetchall()
        logging.info(
            f"📊 Найдено {len(rows_to_filter)} вакансий для фильтрации релевантности."
        )

        for idx, row in enumerate(rows_to_filter, 1):
            vacancy_id = row["id"]
            title = row["title"]
            description = row["description"]
            if not is_potentially_relevant(title, description):
                logging.info(f"[PreFilter] Пропуск без LLM: {title}")
                cursor.execute(
                    "UPDATE vacancies SET is_relevant = FALSE WHERE id = %s",
                    (vacancy_id,),
                )
                continue
            logging.info(
                f"[Gemini Filter] [{idx}/{len(rows_to_filter)}] Проверяем релевантность для: {title}"
            )
            try:
                is_relevant = filter_vacancy_llm(title, description)
                logging.info(
                    f"[Gemini Filter] {title} → {'✅ Релевантно' if is_relevant else '❌ Не релевантно'}"
                )

                cursor.execute(
                    "UPDATE vacancies SET is_relevant = %s WHERE id = %s",
                    (
                        is_relevant,
                        vacancy_id,
                    ),
                )

                await asyncio.sleep(4.5)  

            except Exception as e:
                logging.error(
                    f"❌ Ошибка при фильтрации вакансии {vacancy_id} ('{title}'): {e}",
                    exc_info=True,
                )

        conn.commit()

        logging.info("📝 Шаг 2: Генерация суммари для релевантных вакансий...")
        cursor.execute(
            """
            SELECT id, description
            FROM vacancies
            WHERE is_relevant = TRUE
            AND (summary_duties IS NULL OR summary_requirements IS NULL OR summary_company IS NULL)
            AND published_at >= CURRENT_DATE - INTERVAL '1 day';
            """
        )
        rows_to_summarize = cursor.fetchall()
        logging.info(f"📊 Найдено {len(rows_to_summarize)} вакансий для суммаризации.")

        for idx, row in enumerate(rows_to_summarize, 1):
            vacancy_id = row["id"]
            description = row["description"]

            logging.info(
                f"[Gemini Summary] [{idx}/{len(rows_to_summarize)}] Генерируем суммари для вакансии {vacancy_id}..."
            )
            try:
                summary = summarize_description_llm(
                    description
                ) 
 
                responsibilities_list = summary.get("responsibilities", ["Не указано"])
                requirements_list = summary.get("requirements", ["Не указано"])
                about_company_string = summary.get(
                    "about_company", "Не указано"
                )  

                responsibilities_string = "\n".join(
                    map(str, responsibilities_list)
                )  
                requirements_string = "\n".join(map(str, requirements_list))

                cursor.execute(
                    """
                    UPDATE vacancies
                    SET summary_duties = %s, summary_requirements = %s, summary_company = %s
                    WHERE id = %s
                    """,
                    (
                        responsibilities_string,  
                        requirements_string,  
                        about_company_string,
                        vacancy_id,
                    ),
                )
                await asyncio.sleep(4.5)

            except Exception as e:
                logging.error(
                    f"❌ Ошибка при суммаризации вакансии {vacancy_id}: {e}",
                    exc_info=True,
                )

        conn.commit()

        logging.info("📬 Шаг 3: Публикация в Telegram...")
        cursor.execute(
            """
                SELECT id, title, company, location, salary, salary_range, experience, employment_type, schedule,
                    working_hours, work_format, published_at, summary_duties, summary_requirements,
                    summary_company, url
                FROM vacancies
                WHERE is_relevant = TRUE
                AND published_at >= CURRENT_DATE - INTERVAL '1 day'
                AND sent_to_telegram = FALSE
                AND summary_duties IS NOT NULL
                AND summary_requirements IS NOT NULL
                AND summary_company IS NOT NULL;
            """
        )
        rows_to_publish = cursor.fetchall()

        if not rows_to_publish:
            logging.info("ℹ️ Сегодня нет новых релевантных вакансий для публикации.")
        else:
            logging.info(
                f"📊 Найдено {len(rows_to_publish)} релевантных и неотправленных вакансий для публикации."
            )

            if not TELEGRAM_BOT_TOKEN or not CHANNEL_USERNAME:
                logging.error(
                    "❌ Отсутствует токен бота или имя канала Telegram в конфигурации. Пропускаем отправку."
                )
            else:
                bot = Bot(token=TELEGRAM_BOT_TOKEN)

                for idx, row in enumerate(rows_to_publish, 1):
                    data = dict(row)

                    summary = {
                        "responsibilities": data.get("summary_duties"),
                        "requirements": data.get("summary_requirements"),
                        "about_company": data.get("summary_company"),
                    }
                    text = format_message(data, summary)

                    try:
                        await bot.send_message(
                            chat_id=CHANNEL_USERNAME,
                            text=text,
                            parse_mode="MarkdownV2",
                        )
                        logging.info(
                            f"✅ [{idx}/{len(rows_to_publish)}] Вакансия {data.get('id', 'N/A')} отправлена в Telegram."
                        )

                        cursor.execute(
                            "UPDATE vacancies SET sent_to_telegram = TRUE WHERE id = %s",
                            (data["id"],),
                        )
                        conn.commit()  

                    except TelegramError as e:
                        logging.error(
                            f"❌ Ошибка Telegram API при отправке вакансии {data.get('id', 'N/A')} ('{data.get('title', 'Без названия')}'): {e}"
                        )
                        continue  

                    except Exception as e:
                        logging.error(
                            f"❌ Неожиданная ошибка при отправке вакансии {data.get('id', 'N/A')} ('{data.get('title', 'Без названия')}'): {e}",
                            exc_info=True,
                        )
                        continue  

                    if idx < len(rows_to_publish):
                        delay = random.uniform(
                            TELEGRAM_DELAY_SECONDS, TELEGRAM_MAX_DELAY_SECONDS
                        )
                        logging.info(
                            f"⏱️ Задержка перед следующей отправкой: {delay:.2f} сек."
                        )
                        await asyncio.sleep(delay)

        logging.info("🧹 Запуск безопасной очистки старых и нерелевантных вакансий...")
        try:
            cursor.execute("DELETE FROM vacancies WHERE is_relevant = FALSE OR is_relevant IS NULL;")
            deleted_irrelevant_count = cursor.rowcount
            if deleted_irrelevant_count > 0:
                logging.info(
                    f"🗑️ Удалено {deleted_irrelevant_count} нерелевантных вакансий."
                )
            else:
                logging.info("✅ Нет нерелевантных вакансий для удаления.")

            cursor.execute(
                "DELETE FROM vacancies WHERE sent_to_telegram = FALSE AND processed_at < CURRENT_DATE;"
            )
            deleted_old_not_sent_count = cursor.rowcount
            if deleted_old_not_sent_count > 0:
                logging.info(
                    f"🗑️ Удалено {deleted_old_not_sent_count} старых неотправленных вакансий (обработаны до сегодня)."
                )
            else:
                logging.info("✅ Нет старых неотправленных вакансий для удаления.")

            conn.commit()  

        except Exception as e:
            logging.error(
                f"❌ Ошибка при выполнении безопасной очистки вакансий: {e}",
                exc_info=True,
            )
            if conn:
                conn.rollback()  

    except Exception as e:
        logging.error(
            f"❌ Критическая ошибка в процессе публикации: {e}", exc_info=True
        )

    finally:
        if cursor:
            cursor.close()
        if conn:
            db.return_connection(conn)

def run_publisher(db):
    return asyncio.run(main(db))
