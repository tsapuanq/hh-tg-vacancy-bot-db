#!/usr/bin/env python3
# File: scrape_and_clean.py

import asyncio
import logging
from playwright.async_api import async_playwright

from src.parser import get_vacancy_links
from src.scraper import get_vacancy_details

from src.cleaning import extract_city, normalize_city_name

async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # 1) Получаем до 10 ссылок по запросу
    keyword = "data scientist"
    links = await get_vacancy_links(keyword, max_pages=1)
    links = links[:10]
    logging.info(f"Будем тестировать город на {len(links)} ссылках")

    # 2) Открываем браузер один раз
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 3) Для каждой ссылки: вытягиваем location_raw и сразу city
        for i, link in enumerate(links, start=1):
            logging.info(f"[{i}/{len(links)}] Загружаем {link}")
            raw = await get_vacancy_details(link, page)
            if not raw or "location" not in raw:
                logging.warning(f"[{i}] Нет поля location_raw")
                continue

            loc_raw = raw["location"]
            city = extract_city(loc_raw)
            city = normalize_city_name(city)

            print(f"\n[{i}] {link}")
            print(f"  raw location: {loc_raw!r}")
            print(f"  extracted city: {city!r}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())