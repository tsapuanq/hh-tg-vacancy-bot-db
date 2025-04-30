# src/parser.py
from playwright.async_api import async_playwright
from src.config import BASE_URL, REGION_ID
import logging

async def get_vacancy_links(keyword: str, max_pages: int = 10) -> list[dict]:
    links = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for page_number in range(max_pages):
            url = f"{BASE_URL}?text={keyword}&area={REGION_ID}&page={page_number}"
            logging.info(f"Парсим: {url}")
            await page.goto(url)
            try:
                await page.wait_for_selector('div[data-qa="vacancy-serp__vacancy"]', timeout=5000)
            except:
                logging.warning(f"Нет вакансий на странице {page_number}")
                break

            items = await page.query_selector_all('div[data-qa="vacancy-serp__vacancy"]')
            for item in items:
                href_el = await item.query_selector('a[data-qa="serp-item__title"]')
                city_el = await item.query_selector('div[data-qa="vacancy-serp__vacancy-address"]')

                link = await href_el.get_attribute("href") if href_el else None
                city = await city_el.inner_text() if city_el else "Не указано"

                if link:
                    links.append({"link": link, "city": city.strip()})

        await browser.close()

    return links
