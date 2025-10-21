# run_scraper.py
import asyncio
import logging
import random
from playwright.async_api import (
    async_playwright
)
from src.config import SEARCH_KEYWORDS, MAX_CONCURRENT_TASKS
from src.scraper import get_vacancy_details
from src.parser import get_vacancy_links
from src.utils import setup_logger, canonical_link
from database import Database


async def scrape_single(link, semaphore, context, results, idx, total):
    """
    Обрабатывает одну ссылку на вакансию асинхронно.
    """
    async with semaphore:  
        page = None  
        try:
            page = await context.new_page()
            logging.info(f"[{idx}/{total}] Обрабатываем: {link}")
            data = await get_vacancy_details(
                link, page
            )  
            if data:
                results.append(data)  
            delay = random.uniform(1, 3)  
            logging.info(f"⏱️ Задержка перед следующим запросом: {delay:.2f} сек.")
            await asyncio.sleep(delay)

        except Exception as e:
            logging.warning(f"[Scrape Single Error] Ошибка при обработке {link}: {e}")
        finally:
            if page:
                await page.close()  


async def run_scraper(db: Database, mode: str = "daily"):
    """
    Основная функция для запуска процесса скрейпинга.
    """
    setup_logger()  

    logging.info(f"🔍 Режим запуска скрейпера: {mode.upper()}")
    logging.info("🔎 Загрузка уже существующих ссылок из БД...")

    conn = None  
    cursor = None
    existing_links = set()
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT url FROM vacancies")
        existing_links = {canonical_link(row[0]) for row in cursor.fetchall()}
        logging.info(f"📊 В БД найдено {len(existing_links)} существующих ссылок.")
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке существующих ссылок из БД: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            db.return_connection(conn)  

    logging.info("🔎 Загрузка ссылок по ключевым словам с HH...")

    all_links = set()
    for keyword in SEARCH_KEYWORDS:
        max_pages = 100 if mode == "full" else 1
        raw_links = await get_vacancy_links(keyword, max_pages=max_pages)
        for raw in raw_links:
            canonical = canonical_link(raw)
            if canonical:  
                all_links.add(canonical)

    new_links = list(all_links - existing_links)
    logging.info(f"🔗 Найдено {len(all_links)} ссылок всего по ключевым словам.")
    logging.info(f"🆕 Новых ссылок для обработки: {len(new_links)}")

    if not new_links:
        logging.info("✅ Нет новых ссылок для скрейпинга.")
        return

    results = []  
    semaphore = asyncio.Semaphore(
        MAX_CONCURRENT_TASKS
    )  

    logging.info(
        f"🚀 Запускаем скрейпинг деталей для {len(new_links)} новых вакансий (до {MAX_CONCURRENT_TASKS} одновременно)..."
    )

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True
        )  
        try:
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"  
                ),
                locale="ru-RU",  
            )
            tasks = [
                scrape_single(link, semaphore, context, results, idx, len(new_links))
                for idx, link in enumerate(new_links, 1)
            ]
            await asyncio.gather(*tasks)  

        finally:
            await browser.close()  

    successful_results = [r for r in results if r is not None]
    logging.info(
        f"✅ Успешно обработано деталей для {len(successful_results)} вакансий."
    )

    if successful_results:
        conn = None
        cursor = None
        try:
            conn = db.get_connection()
            cursor = conn.cursor()

            for data in successful_results:
                cursor.execute(
                    """
                    INSERT INTO vacancies (
                        title, description, url, published_date, company, location, salary, salary_range, experience,
                        employment_type, schedule, working_hours, work_format, skills,
                        is_relevant, processed_at, sent_to_telegram, published_at -- Добавлены поля
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        NULL, NOW(), FALSE, %s -- NULL для is_relevant (будет определен LLM), NOW() для processed_at, FALSE для sent_to_telegram. published_at из спарсенных данных
                    ) ON CONFLICT (url) DO NOTHING; -- Точка с запятой в конце
                """,
                    (
                        data["title"],
                        data["description"],
                        data["url"],
                        data["published_date"],
                        data["company"],
                        data["location"],
                        data["salary"],
                        data["salary_range"],
                        data["experience"],
                        data["employment_type"],
                        data["schedule"],
                        data["working_hours"],
                        data["work_format"],
                        data["skills"],
                        data[
                            "published_at"
                        ],  
                    ),
                )

            conn.commit()  
            logging.info(
                f"✅ Сохранено {len(successful_results)} новых/обновленных вакансий в БД."
            )
        except Exception as e:
            logging.error(f"❌ Ошибка при сохранении данных в БД: {e}")
            if conn:
                conn.rollback()  
        finally:
            if cursor:
                cursor.close()
            if conn:
                db.return_connection(conn)  
    else:
        logging.info("❌ Нет успешно обработанных данных для сохранения.")
