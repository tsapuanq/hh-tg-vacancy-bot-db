from playwright.async_api import async_playwright
import logging

async def get_vacancy_details(link: str) -> dict:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(link)

        await page.wait_for_selector('h1[data-qa="vacancy-title"]')

        title = await page.inner_text('h1[data-qa="vacancy-title"]')
        company = await page.inner_text('a[data-qa="vacancy-company-name"]', timeout=3000) if await page.query_selector('a[data-qa="vacancy-company-name"]') else "Не указано"
        location = await page.inner_text('span[data-qa="vacancy-view-raw-address"]', timeout=3000) if await page.query_selector('span[data-qa="vacancy-view-raw-address"]') else "Не указано"
        salary = await page.inner_text('span[data-qa="vacancy-salary"]', timeout=3000) if await page.query_selector('span[data-qa="vacancy-salary"]') else "Не указано"
        description = await page.inner_text('div[data-qa="vacancy-description"]')

        await browser.close()

        return {
            "title": title,
            "company": company,
            "location": location,
            "salary": salary,
            "description": description,
            "link": link
        }
