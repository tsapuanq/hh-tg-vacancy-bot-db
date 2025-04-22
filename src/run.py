import os
import asyncio
import logging
import pandas as pd
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from src.config import SEARCH_KEYWORDS, CSV_MAIN, CSV_RAW_DAILY
from src.parser import get_vacancy_links
from src.scraper import get_vacancy_details
from src.utils import setup_logger, save_to_csv, load_existing_links, save_raw_data

import argparse

MAX_CONCURRENT_TASKS = 30

async def scrape_single(link, semaphore, context, results, idx, total):
    async with semaphore:
        try:
            page = await context.new_page()
            logging.info(f"[{idx}/{total}] Обрабатываем: {link}")
            data = await get_vacancy_details(link, page)
            results.append(data)
            await page.close()
        except Exception as e:
            logging.warning(f"Ошибка при обработке {link}: {e}")

async def run_scraper(mode: str = "daily"):
    setup_logger()
    all_links = set()
    existing_links = load_existing_links(CSV_MAIN)

    logging.info(f"🔍 Режим запуска: {mode.upper()}")
    logging.info("Загрузка вакансий по ключевым словам...")

    for keyword in SEARCH_KEYWORDS:
        max_pages = 100 if mode == "full" else 1
        links = await get_vacancy_links(keyword, max_pages=max_pages)
        all_links.update(links)

    new_links = list(set(all_links) - existing_links)
    logging.info(f"Новых ссылок для обработки: {len(new_links)}")

    results = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        tasks = [
            scrape_single(link, semaphore, context, results, idx, len(new_links))
            for idx, link in enumerate(new_links, 1)
        ]

        await asyncio.gather(*tasks)
        await browser.close()

    if results:
        df = pd.DataFrame(results)
        save_to_csv(results, CSV_MAIN)
        save_raw_data(df, CSV_RAW_DAILY)
        logging.info(f"✅ Сохранено {len(results)} новых вакансий")
    else:
        logging.info("❌ Нет новых данных для сохранения")

if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["full", "daily"], default="daily")
    args = parser.parse_args()

    asyncio.run(run_scraper(mode=args.mode))
