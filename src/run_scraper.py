import asyncio
import logging
import random
from playwright.async_api import async_playwright
from src.config import SEARCH_KEYWORDS
from src.parser import get_vacancy_links
from src.scraper import get_vacancy_details
from src.utils import setup_logger, canonical_link
from database import Database
import os

MAX_CONCURRENT_TASKS = 10

async def scrape_single(link, semaphore, context, results, idx, total):
    async with semaphore:
        try:
            page = await context.new_page()
            logging.info(f"[{idx}/{total}] Обрабатываем: {link}")
            data = await get_vacancy_details(link, page)
            await page.close()
            if data:
                results.append(data)

            delay = random.uniform(1, 2)
            logging.info(f"⏱️ Задержка перед следующим запросом: {delay:.2f} сек.")
            await asyncio.sleep(delay)

        except Exception as e:
            logging.warning(f"[Scrape Error] {link}: {e}")

async def run_scraper(db, mode: str = "daily"):
    setup_logger()

    # Получаем уже существующие ссылки из БД
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM vacancies")
    existing_links = {canonical_link(row[0]) for row in cursor.fetchall()}
    db.return_connection(conn)

    logging.info(f"🔍 Режим запуска: {mode.upper()}")
    logging.info("🔎 Загрузка ссылок по ключевым словам...")

    all_links = set()
    for keyword in SEARCH_KEYWORDS:
        max_pages = 100 if mode == "full" else 1
        raw_links = await get_vacancy_links(keyword, max_pages=max_pages)
        for raw in raw_links:
            all_links.add(canonical_link(raw))

    new_links = list(all_links - existing_links)
    logging.info(f"🔗 Новых ссылок для обработки: {len(new_links)}")

    results = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            locale="ru-RU"
        )

        tasks = [
            scrape_single(link, semaphore, context, results, idx, len(new_links))
            for idx, link in enumerate(new_links, 1)
        ]

        await asyncio.gather(*tasks)
        await browser.close()

    results = [r for r in results if r is not None]

    if results:
        conn = db.get_connection()
        cursor = conn.cursor()
        for data in results:
            cursor.execute("""
                INSERT INTO vacancies (
                    title, description, url, published_at, company, location, salary, experience,
                    employment_type, schedule, working_hours, work_format, skills
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (url) DO NOTHING
            """, (
                data['title'], data['description'], data['link'], data['published_at'],
                data['company'], data['location'], data['salary'], data['experience'],
                data['employment_type'], data['schedule'], data['working_hours'],
                data['work_format'], data['skills']
            ))
        conn.commit()
        db.return_connection(conn)
        logging.info(f"✅ Сохранено {len(results)} новых вакансий в БД")
    else:
        logging.info("❌ Нет новых данных для сохранения")

if __name__ == "__main__":
    db = Database(os.getenv("DATABASE_URL"))
    asyncio.run(run_scraper(db))