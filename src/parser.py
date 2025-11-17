# parser.py
from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)
from src.config import BASE_URL, REGION_ID, SCRAPER_TIMEOUT_SELECTOR
import logging


async def get_vacancy_links(keyword: str, max_pages: int = 10) -> list[str]:
    """
    Собирает ссылки на вакансии по ключевому слову с нескольких страниц поиска.
    """
    links = []
    logging.info(
        f"🔗 Собираем ссылки по ключевому слову: '{keyword}' до {max_pages} страниц."
    )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            page = await browser.new_page()

            for page_number in range(max_pages):
                url = f"{BASE_URL}?text={keyword}&area={REGION_ID}&page={page_number}&order_by=publication_time&search_period=3"
                logging.info(
                    f"Парсим страницу {page_number + 1}/{max_pages}: {url}"
                )  

                try:
                    await page.goto(
                        url, timeout=20000
                    )  
                    await page.wait_for_selector(
                        'a[data-qa="serp-item__title"]',
                        timeout=SCRAPER_TIMEOUT_SELECTOR,
                    )  
                except PlaywrightTimeoutError:
                    logging.warning(
                        f"⏳ Таймаут ожидания селектора на странице {page_number + 1} для {keyword}. Возможно, нет вакансий или загрузка занимает слишком много времени."
                    )
                    break  
                except Exception as e:
                    logging.warning(
                        f"❌ Ошибка при загрузке страницы {page_number + 1} для {keyword}: {e}"
                    )
                    continue 

                try:
                    items = await page.query_selector_all(
                        'a[data-qa="serp-item__title"]'
                    )
                    page_links = [
                        await item.get_attribute("href")
                        for item in items
                        if await item.get_attribute("href")
                    ]
                    logging.info(
                        f"⚡ Найдено {len(page_links)} ссылок на странице {page_number + 1}"
                    )
                    links.extend(page_links)

                    if not page_links and page_number > 0:
                        logging.info(
                            f"ℹ️ Нет ссылок на странице {page_number + 1}, завершаем парсинг для '{keyword}'."
                        )
                        break

                except Exception as e:
                    logging.warning(
                        f"❌ Ошибка при извлечении ссылок на странице {page_number + 1} для {keyword}: {e}"
                    )
                    continue  

        finally:
            await browser.close()

    logging.info(f"✅ Всего найдено ссылок для '{keyword}': {len(links)}")
    return links
