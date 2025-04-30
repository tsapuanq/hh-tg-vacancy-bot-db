import asyncio 
import logging
import pandas as pd
import random
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from src.config import SEARCH_KEYWORDS, CSV_MAIN, CSV_RAW_DAILY
from src.parser import get_vacancy_links
from src.scraper import get_vacancy_details
from src.utils import setup_logger, save_to_csv, load_existing_links, save_raw_data

import argparse

MAX_CONCURRENT_TASKS = 10

# ——— Обработка одной вакансии ———
async def scrape_single(link_data, semaphore, context, results, idx, total):
    async with semaphore:
        try:
            page = await context.new_page()
            logging.info(f"[{idx}/{total}] Обрабатываем: {link_data['link']}")
            data = await get_vacancy_details(link_data, page)
            await page.close()
            if data:
                results.append(data)

            delay = random.uniform(1, 2)
            logging.info(f"⏱️ Задержка перед следующим запросом: {delay:.2f} сек.")
            await asyncio.sleep(delay)

        except Exception as e:
            logging.warning(f"[Scrape Error] {link_data['link']}: {e}")

# ——— Главная функция скрапинга ———
async def run_scraper(mode: str = "daily") -> pd.DataFrame:
    setup_logger()
    all_links = []
    existing_links = load_existing_links(CSV_MAIN)

    logging.info(f"🔍 Режим запуска: {mode.upper()}")
    logging.info("🔎 Загрузка ссылок по ключевым словам...")

    for keyword in SEARCH_KEYWORDS:
        max_pages = 100 if mode == "full" else 1
        links = await get_vacancy_links(keyword, max_pages=max_pages)
        all_links.extend(links)

    # Оставляем только те, которых нет в базе
    existing_urls = set(existing_links)
    new_links = [item for item in all_links if item["link"] not in existing_urls]

    logging.info(f"🔗 Новых ссылок для обработки: {len(new_links)}")

    results = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            locale="ru-RU"
        )

        tasks = [
            scrape_single(link_data, semaphore, context, results, idx, len(new_links))
            for idx, link_data in enumerate(new_links, 1)
        ]

        await asyncio.gather(*tasks)
        await browser.close()

    results = [r for r in results if r is not None]

    if results:
        df = pd.DataFrame(results)
        save_to_csv(results, CSV_MAIN)
        save_raw_data(df, CSV_RAW_DAILY)
        logging.info(f"✅ Сохранено {len(df)} новых вакансий")
        return df
    else:
        logging.info("❌ Нет новых данных для сохранения")
        return pd.DataFrame()

# ——— CLI запуск ———
if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["full", "daily"], default="daily")
    args = parser.parse_args()

    asyncio.run(run_scraper(mode=args.mode))
