# publisher.py
import asyncio
import random
import logging
from telegram import Bot
from telegram.error import TelegramError # Импортируем специфическое исключение
# Импортируем настройки из config
from src.config import TELEGRAM_BOT_TOKEN, CHANNEL_USERNAME, TELEGRAM_DELAY_SECONDS, TELEGRAM_MAX_DELAY_SECONDS
# Импортируем LLM функции
from src.llm_summary import summarize_description_llm, filter_vacancy_llm
from src.cleaning import clean_text_safe # Используем clean_text_safe из cleaning
# Импортируем класс Database
from database import Database
import re
from datetime import date # Импортируем date для проверки типа
import psycopg2.extras

def escape_markdown_v2(text: str) -> str:
    """Экранирует специальные символы MarkdownV2 в строке."""
    if not isinstance(text, str):
        # Преобразуем в строку, если вход не строка (напр. None или число)
        text = str(text if text is not None else '') # Преобразуем None в пустую строку перед str()

    # Список символов, которые нужно экранировать
    chars_to_escape = r'_*[]()~`>#+-=|{}.!'
    
    # Создаем паттерн для поиска всех этих символов
    pattern = r'(' + '|'.join(re.escape(char) for char in chars_to_escape) + r')'
    
    # Заменяем каждый найденный символ на экранированный (\ + символ)
    escaped_text = re.sub(pattern, r'\\\1', text)
    
    return escaped_text


# ——— Вспомогательная конверсия LLM-результатов в Markdown-буллеты ———
def _to_bullets(x) -> str:
    """Преобразует список или строку с переносами строк в форматированный список Markdown."""
    if not x or (isinstance(x, str) and x.strip().lower() == "не указано"):
        return "Не указано"

    if isinstance(x, list):
        lines = x
    else:
        # Обрабатываем как строку, разделяя по переносам строк
        lines = str(x).split("\n")

    bullets = []
    for item in lines:
        s = str(item).strip().strip("'\"") # Убираем пробелы и кавычки
        if s and s.lower() != "не указано":
            # Экранирование символов MarkdownV2 в элементах списка
            # Экранируем каждый пункт отдельно, чтобы не ломать форматирование внутри пункта,
            # но при этом безопасно отображать текст.
            escaped_item = escape_markdown_v2(s)
            # Используем жирную точку или другую маркировку
            # Точка маркировки • не должна экранироваться сама по себе.
            bullets.append(f"• {escaped_item}")

    return "\n".join(bullets) if bullets else "Не указано"


# ——— Форматирование вакансии для Telegram ———
def format_message(data: dict, summary: dict) -> str:
    """
    Форматирует данные о вакансии и summary для отправки в Telegram в MarkdownV2.
    Применяет экранирование для всех текстовых полей.
    """
    # Применяем clean_text_safe (если не применили раньше) и escape_markdown_v2
    # Экранируем все текстовые поля ДО помещения их в f-string
    title = escape_markdown_v2(clean_text_safe(data.get('title','Не указано')))
    company = escape_markdown_v2(clean_text_safe(data.get('company', 'Не указано')))
    location = escape_markdown_v2(clean_text_safe(data.get('location', 'Не указано')))
    
    # Для зарплаты используем salary_range, если есть, иначе salary
    salary_raw = data.get('salary_range') or data.get('salary')
    salary_info = escape_markdown_v2(clean_text_safe(salary_raw if salary_raw is not None else 'Не указано'))

    experience = escape_markdown_v2(clean_text_safe(data.get('experience', 'Не указано')))
    employment_type = escape_markdown_v2(clean_text_safe(data.get('employment_type', 'Не указано')))
    schedule = escape_markdown_v2(clean_text_safe(data.get('schedule', 'Не указано')))
    working_hours = escape_markdown_v2(clean_text_safe(data.get('working_hours', 'Не указано')))
    work_format = escape_markdown_v2(clean_text_safe(data.get('work_format', 'Не указано')))

    # Форматируем дату, если она есть и это объект date
    pub_date_obj = data.get('published_at')
    pub_date_str_formatted = "Не указано"
    if isinstance(pub_date_obj, date):
         try:
             pub_date_str_formatted = pub_date_obj.strftime("%d.%m.%Y") # Формат "день.месяц.год"
         except Exception:
              # В случае ошибки форматирования даты, преобразуем ее в строку
              pub_date_str_formatted = str(pub_date_obj)
    # Экранируем форматированную строку даты
    pub_date_str_formatted = escape_markdown_v2(pub_date_str_formatted)

    # Summary поля обрабатываются в _to_bullets, которая сама вызывает escape_markdown_v2 для каждого пункта
    resp = _to_bullets(summary.get("responsibilities", "Не указано"))
    reqs = _to_bullets(summary.get("requirements", "Не указано"))
    
    # Summary 'О компании' - это одна строка/пара предложений, экранируем ее полностью
    about = escape_markdown_v2(clean_text_safe(summary.get("about_company", "Не указано")))

    # URL для ссылки - сам URL не экранируется, только текст ссылки
    url = data.get('url', '#') # URL должен быть всегда, но добавим дефолт
    link_text = escape_markdown_v2("Подробнее на hh") # Экранируем текст ссылки

    # Используем MarkdownV2 синтаксис. Все переменные уже содержат экранированные строки.
    # Используем жирный шрифт для заголовка
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
    # Настройка логирования - лучше делать один раз в run_all.py
    # if not logging.getLogger().handlers:
    #      logging.basicConfig(level=logging.INFO)


    conn = None # Инициализируем None
    cursor = None

    try:
        conn = db.get_connection()
        # Используем DictCursor, чтобы получать строки как словари, а не кортежи.
        # Это делает доступ к данным по именам колонок более удобным и надежным.
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor) # Нужен import psycopg2.extras


        # Шаг 1: Проверка релевантности с помощью LLM
        logging.info("🕵️‍♂️ Шаг 1: Проверка релевантности новых вакансий...")
        cursor.execute(
            """
            SELECT id, title, description
            FROM vacancies
            -- Выбираем только вакансии, которые были обработаны сегодня и еще не были отфильтрованы
            -- is_relevant = NULL после первой вставки в run_scraper.py
            WHERE is_relevant IS NULL AND processed_at >= CURRENT_DATE
            """
        )
        rows_to_filter = cursor.fetchall()
        logging.info(f"📊 Найдено {len(rows_to_filter)} вакансий для фильтрации релевантности.")

        for idx, row in enumerate(rows_to_filter, 1):
            # Получаем данные из DictCursor row по именам колонок
            vacancy_id = row['id']
            title = row['title']
            description = row['description']

            logging.info(f"[Gemini Filter] [{idx}/{len(rows_to_filter)}] Проверяем релевантность для: {title}")
            try:
                is_relevant = filter_vacancy_llm(title, description)
                logging.info(f"[Gemini Filter] {title} → {'✅ Релевантно' if is_relevant else '❌ Не релевантно'}")

                if is_relevant:
                    # Помечаем как релевантную
                    cursor.execute(
                        "UPDATE vacancies SET is_relevant = TRUE WHERE id = %s", (vacancy_id,)
                    )
                else:
                    # Удаляем нерелевантную вакансию
                    # Удаление сразу может быть рискованным, можно вместо этого установить is_relevant = FALSE
                    # cursor.execute("DELETE FROM vacancies WHERE id = %s", (vacancy_id,))
                    cursor.execute(
                         "UPDATE vacancies SET is_relevant = FALSE WHERE id = %s", (vacancy_id,)
                    )


                # Задержка между вызовами LLM для соблюдения rate limit
                await asyncio.sleep(4.5)

            except Exception as e:
                 # Логируем ошибку фильтрации, но не останавливаем процесс
                 logging.error(f"❌ Ошибка при фильтрации вакансии {vacancy_id} ('{title}'): {e}", exc_info=True) # Логируем traceback
                 # Возможно, стоит пометить эту вакансию, чтобы не пытаться фильтровать ее снова?
                 # Например, добавить поле llm_filter_failed BOOLEAN и установить его в TRUE


        conn.commit() # Коммитим результаты фильтрации

        # Шаг 2: Генерация суммари для релевантных вакансий с помощью LLM
        logging.info("📝 Шаг 2: Генерация суммари для релевантных вакансий...")
        cursor.execute(
            """
            SELECT id, description
            FROM vacancies
            -- Выбираем только релевантные вакансии, у которых еще нет полного суммари
            WHERE is_relevant = TRUE
              AND (summary_duties IS NULL OR summary_requirements IS NULL OR summary_company IS NULL)
            """
        )
        rows_to_summarize = cursor.fetchall()
        logging.info(f"📊 Найдено {len(rows_to_summarize)} вакансий для суммаризации.")

        for idx, row in enumerate(rows_to_summarize, 1):
            vacancy_id = row['id']
            description = row['description']

            logging.info(f"[Gemini Summary] [{idx}/{len(rows_to_summarize)}] Генерируем суммари для вакансии {vacancy_id}...")
            try:
                summary = summarize_description_llm(description)
                cursor.execute(
                    """
                    UPDATE vacancies
                    SET summary_duties = %s, summary_requirements = %s, summary_company = %s
                    WHERE id = %s
                    """,
                    (
                        summary.get("responsibilities"), # Если get вернул None, так и запишется в БД
                        summary.get("requirements"),
                        summary.get("about_company"),
                        vacancy_id,
                    ),
                )
                # Задержка между вызовами LLM
                await asyncio.sleep(4.5)

            except Exception as e:
                # Логируем ошибку суммаризации
                 logging.error(f"❌ Ошибка при суммаризации вакансии {vacancy_id}: {e}", exc_info=True)
                 # Аналогично, можно пометить вакансию, чтобы не пытаться суммаризировать снова


        conn.commit() # Коммитим результаты суммаризации


        # Шаг 3: Публикация релевантных и неотправленных вакансий в Telegram
        logging.info("📬 Шаг 3: Публикация в Telegram...")
        cursor.execute(
            """
            SELECT id, title, company, location, salary, salary_range, experience, employment_type, schedule,
                   working_hours, work_format, published_at, summary_duties, summary_requirements,
                   summary_company, url
            FROM vacancies
            -- Выбираем релевантные вакансии, обработанные сегодня (или позже?) и еще не отправленные,
            -- и убеждаемся, что у них есть суммари (или прошел шаг суммаризации)
            WHERE is_relevant = TRUE
              AND processed_at >= CURRENT_DATE -- Обработано сегодня или позже
              AND sent_to_telegram = FALSE -- Еще не отправлено
              AND summary_duties IS NOT NULL -- Убедимся, что суммаризация прошла успешно
              -- Условие AND summary_requirements IS NOT NULL И summary_company IS NOT NULL
              -- может быть слишком строгим, если LLM вернул "Не указано" для одного из полей.
              -- Можно убрать эти проверки, если NULL допустим, или проверять на 'Не указано' в Python
              -- но для консистентности с WHERE выше, оставим проверку на NULL

            """
        )
        rows_to_publish = cursor.fetchall()

        if not rows_to_publish:
            logging.info("ℹ️ Сегодня нет новых релевантных вакансий для публикации.")
            # Нет вакансий для отправки, просто выходим после возврата соединения
            # return # Убираем return, чтобы гарантировать finally блок

        logging.info(f"📊 Найдено {len(rows_to_publish)} релевантных и неотправленных вакансий для публикации.")

        # Инициализируем бота
        if not TELEGRAM_BOT_TOKEN or not CHANNEL_USERNAME:
             logging.error("❌ Отсутствует токен бота или имя канала Telegram в конфигурации. Пропускаем отправку.")
             # return # Убираем return


        bot = Bot(token=TELEGRAM_BOT_TOKEN) # Инициализируем бота только если есть токены

        for idx, row in enumerate(rows_to_publish, 1):
            # Получаем данные из DictCursor row по именам колонок
            # Это удобнее и надежнее, чем по индексу
            data = dict(row) # Преобразуем DictRow в обычный словарь, если нужно, хотя можно работать и с DictRow

            # Формируем словарь summary из полей данных
            summary = {
                "responsibilities": data.get("summary_duties"), # Используем .get()
                "requirements": data.get("summary_requirements"),
                "about_company": data.get("summary_company"),
            }
            # Форматируем сообщение для Telegram
            text = format_message(data, summary)

            # Проверяем, инициализирован ли бот, прежде чем пытаться отправлять
            if not bot:
                logging.warning(f"[{idx}/{len(rows_to_publish)}] Пропуск отправки вакансии {data.get('id', 'N/A')} из-за отсутствия токена бота/канала.")
                continue # Переходим к следующей вакансии

            try:
                # Отправляем сообщение
                await bot.send_message(
                    chat_id=CHANNEL_USERNAME,
                    text=text,
                    parse_mode="MarkdownV2" # Используем MarkdownV2 для лучшей совместимости с экранированием
                    # link_preview_options=LinkPreviewOptions(is_disabled=True) # Отключить превью, если мешает
                )
                logging.info(f"✅ [{idx}/{len(rows_to_publish)}] Вакансия {data.get('id', 'N/A')} отправлена в Telegram.")

                # Помечаем вакансию как отправленную
                cursor.execute(
                    "UPDATE vacancies SET sent_to_telegram = TRUE WHERE id = %s",
                    (data["id"],), # Используем id из словаря data
                )
                conn.commit() # Коммитим после каждой успешной отправки

            except TelegramError as e:
                # Логируем ошибки отправки через Telegram API
                logging.error(f"❌ Ошибка Telegram API при отправке вакансии {data.get('id', 'N/A')} ('{data.get('title', 'Без названия')}'): {e}")
                # Вакансия не помечается как sent_to_telegram=TRUE, будет повторная попытка в след. раз
                # conn.rollback() # Не нужен, т.к. коммит идет после успешной отправки
                continue # Пропускаем задержку и переходим к следующей вакансии

            except Exception as e:
                # Логируем любые другие неожиданные ошибки при отправке
                logging.error(f"❌ Неожиданная ошибка при отправке вакансии {data.get('id', 'N/A')} ('{data.get('title', 'Без названия')}'): {e}", exc_info=True)
                # conn.rollback()
                continue # Пропускаем задержку

            # Задержка между отправками в Telegram
            if idx < len(rows_to_publish):
                delay = random.uniform(TELEGRAM_DELAY_SECONDS, TELEGRAM_MAX_DELAY_SECONDS)
                logging.info(f"⏱️ Задержка перед следующей отправкой: {delay:.2f} сек.")
                await asyncio.sleep(delay)

        # Удаление неотправленных вакансий - УДАЛЕНО как потенциально опасное.
        # Логика очистки старых или нерелевантных вакансий должна быть в другом месте
        # и быть более осторожной.

    except Exception as e:
        logging.error(f"❌ Критическая ошибка в процессе публикации: {e}", exc_info=True)


    finally:
        # Гарантированное возвращение соединения в пул
        if cursor:
             cursor.close()
        if conn:
            db.return_connection(conn)


    logging.info(f"📬 Всего обработано для отправки в этом запуске: {len(rows_to_publish)} вакансий.")


# run_publisher остается асинхронной оберткой для main
def run_publisher(db):
    # Необходимо импортировать psycopg2.extras для DictCursor
    import psycopg2.extras
    return asyncio.run(main(db))