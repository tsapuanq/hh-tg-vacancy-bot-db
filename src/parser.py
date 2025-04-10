# src/parser.py

import asyncio
from playwright.async_api import async_playwright
from src.config import SEARCH_KEYWORDS, BASE_URL
import logging


async def get_vacancy_links(keyword: str, max_pages: int = 1) -> list[str]:
    links = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for page_number in range(max_pages):
            params = f"?text={keyword}&page={page_number}"
            url = BASE_URL + params
            logging.info(f"🔍 Открываем: {url}")
            await page.goto(url)

            # Ждём пока появятся элементы
            await page.wait_for_selector('a[data-qa="serp-item__title"]')

            items = await page.query_selector_all('a[data-qa="serp-item__title"]')
            for item in items:
                link = await item.get_attribute("href")
                if link:
                    links.append(link)

        await browser.close()
    return links
