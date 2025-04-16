# src/parser.py

from playwright.async_api import async_playwright
from src.config import BASE_URL, REGION_ID
import logging

async def get_vacancy_links(keyword: str, max_pages: int = 10) -> list[str]:
    links = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for page_number in range(max_pages):
            url = f"{BASE_URL}?text={keyword}&area={REGION_ID}&page={page_number}"
            logging.info(f"🔎 Парсим: {url}")
            await page.goto(url)
            try:
                await page.wait_for_selector('a[data-qa="serp-item__title"]', timeout=5000)
            except:
                logging.warning(f"Нет вакансий на странице {page_number}")
                break

            items = await page.query_selector_all('a[data-qa="serp-item__title"]')
            page_links = [await item.get_attribute("href") for item in items if await item.get_attribute("href")]
            links.extend(page_links)

        await browser.close()

    return links
