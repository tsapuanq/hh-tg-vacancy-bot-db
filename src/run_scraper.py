import asyncio
import logging
import pandas as pd
import random
from playwright.async_api import async_playwright
from src.config import SEARCH_KEYWORDS, CSV_MAIN, CSV_RAW_DAILY
from src.parser import get_vacancy_links
from src.scraper import get_vacancy_details
from src.utils import setup_logger, save_to_main_csv, load_existing_links, save_raw_data
from src.utils import canonical_link  # 🟢 импорт утилиты для обрезки query

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

async def run_scraper(mode: str = "daily") -> pd.DataFrame:
    setup_logger()

    # 📌 Вместо «сырых» ссылок — сразу берём уже канонизированные
    existing_links = {
        canonical_link(l)            # 🟢 обрезаем всё после '?'
        for l in load_existing_links(CSV_MAIN)
    }

    logging.info(f"🔍 Режим запуска: {mode.upper()}")
    logging.info("🔎 Загрузка ссылок по ключевым словам...")

    all_links = set()
    for keyword in SEARCH_KEYWORDS:
        max_pages = 100 if mode == "full" else 1
        raw_links = await get_vacancy_links(keyword, max_pages=max_pages)
        for raw in raw_links:
            # 🟢 сразу канонизируем и сохраняем без query-параметров
            all_links.add(canonical_link(raw))

    # 📌 Вычисляем разницу уже по каноническим URL
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

        # new_links уже чистые URL — можно прямо итерировать
        tasks = [
            scrape_single(link, semaphore, context, results, idx, len(new_links))
            for idx, link in enumerate(new_links, 1)
        ]

        await asyncio.gather(*tasks)
        await browser.close()

    results = [r for r in results if r is not None]

    if results:
        df = pd.DataFrame(results)
        save_to_main_csv(results, CSV_MAIN)  # здесь тоже util-функция должна канонизировать
        save_raw_data(df, CSV_RAW_DAILY)
        logging.info(f"✅ Сохранено {len(df)} новых вакансий")
        return df
    else:
        logging.info("❌ Нет новых данных для сохранения")
        return pd.DataFrame()
