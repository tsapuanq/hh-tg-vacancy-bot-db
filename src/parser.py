# #parser.py
# from playwright.async_api import async_playwright
# from src.config import BASE_URL, REGION_ID
# import logging

# async def get_vacancy_links(keyword: str, max_pages: int = 10) -> list[str]:
#     links = []

#     async with async_playwright() as p:
#         browser = await p.chromium.launch(headless=True)
#         page = await browser.new_page()

#         for page_number in range(max_pages):
#             url = f"{BASE_URL}?text={keyword}&area={REGION_ID}&page={page_number}&order_by=publication_time&search_period=1"
#             logging.info(f"Парсим: {url}")
#             await page.goto(url)
#             try:
#                 await page.wait_for_selector('a[data-qa="serp-item__title"]', timeout=5000)
#             except:
#                 logging.warning(f"Нет вакансий на странице {page_number}")
#                 break

#             items = await page.query_selector_all('a[data-qa="serp-item__title"]')
#             page_links = [await item.get_attribute("href") for item in items if await item.get_attribute("href")]
#             links.extend(page_links)

#         await browser.close()

#     return links

# parser.py
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from src.config import BASE_URL, REGION_ID, SCRAPER_TIMEOUT_SELECTOR
import logging

async def get_vacancy_links(keyword: str, max_pages: int = 10) -> list[str]:
    """
    Собирает ссылки на вакансии по ключевому слову с нескольких страниц поиска.
    """
    links = []
    logging.info(f"🔗 Собираем ссылки по ключевому слову: '{keyword}' до {max_pages} страниц.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # Используем try...finally для гарантии закрытия браузера
        try:
            page = await browser.new_page()

            for page_number in range(max_pages):
                url = f"{BASE_URL}?text={keyword}&area={REGION_ID}&page={page_number}&order_by=publication_time&search_period=1"
                logging.info(f"Парсим страницу {page_number + 1}/{max_pages}: {url}") # Логируем номер страницы

                try:
                    await page.goto(url, timeout=20000) # Используем таймаут по умолчанию или из config
                    await page.wait_for_selector('a[data-qa="serp-item__title"]', timeout=SCRAPER_TIMEOUT_SELECTOR) # Таймаут для селектора
                except PlaywrightTimeoutError:
                     logging.warning(f"⏳ Таймаут ожидания селектора на странице {page_number + 1} для {keyword}. Возможно, нет вакансий или загрузка занимает слишком много времени.")
                     # Если нет селектора, вероятно, страницы закончились или ошибка. Можно попробовать break.
                     break # Заканчиваем, если нет вакансий или селектор не найден
                except Exception as e:
                    logging.warning(f"❌ Ошибка при загрузке страницы {page_number + 1} для {keyword}: {e}")
                    continue # Пропускаем эту страницу и пробуем следующую

                try:
                    items = await page.query_selector_all('a[data-qa="serp-item__title"]')
                    page_links = [await item.get_attribute("href") for item in items if await item.get_attribute("href")]
                    logging.info(f"⚡ Найдено {len(page_links)} ссылок на странице {page_number + 1}")
                    links.extend(page_links)
                    
                    if not page_links and page_number > 0:
                         # Если на текущей странице (кроме первой) нет ссылок, это, вероятно, конец
                         logging.info(f"ℹ️ Нет ссылок на странице {page_number + 1}, завершаем парсинг для '{keyword}'.")
                         break


                except Exception as e:
                     logging.warning(f"❌ Ошибка при извлечении ссылок на странице {page_number + 1} для {keyword}: {e}")
                     continue # Пропускаем извлечение на этой странице и пробуем следующую

        finally:
            await browser.close()

    logging.info(f"✅ Всего найдено ссылок для '{keyword}': {len(links)}")
    return links