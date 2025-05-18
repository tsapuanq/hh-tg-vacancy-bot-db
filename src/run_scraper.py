# #run_scraper.py
# import asyncio
# import logging
# import random
# from playwright.async_api import async_playwright
# from src.config import SEARCH_KEYWORDS
# from src.parser import get_vacancy_links
# from src.scraper import get_vacancy_details
# from src.utils import setup_logger, canonical_link
# from database import Database
# import os

# MAX_CONCURRENT_TASKS = 10

# async def scrape_single(link, semaphore, context, results, idx, total):
#     async with semaphore:
#         try:
#             page = await context.new_page()
#             logging.info(f"[{idx}/{total}] Обрабатываем: {link}")
#             data = await get_vacancy_details(link, page)
#             await page.close()
#             if data:
#                 results.append(data)

#             delay = random.uniform(1, 2)
#             logging.info(f"⏱️ Задержка перед следующим запросом: {delay:.2f} сек.")
#             await asyncio.sleep(delay)

#         except Exception as e:
#             logging.warning(f"[Scrape Error] {link}: {e}")

# async def run_scraper(db, mode: str = "daily"):
#     setup_logger()

#     # Получаем уже существующие ссылки из БД
#     conn = db.get_connection()
#     cursor = conn.cursor()
#     cursor.execute("SELECT url FROM vacancies")
#     existing_links = {canonical_link(row[0]) for row in cursor.fetchall()}
#     db.return_connection(conn)

#     logging.info(f"🔍 Режим запуска: {mode.upper()}")
#     logging.info("🔎 Загрузка ссылок по ключевым словам...")

#     all_links = set()
#     for keyword in SEARCH_KEYWORDS:
#         max_pages = 100 if mode == "full" else 1
#         raw_links = await get_vacancy_links(keyword, max_pages=max_pages)
#         for raw in raw_links:
#             all_links.add(canonical_link(raw))

#     new_links = list(all_links - existing_links)
#     logging.info(f"🔗 Новых ссылок для обработки: {len(new_links)}")

#     results = []
#     semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

#     async with async_playwright() as p:
#         browser = await p.chromium.launch(headless=True)
#         context = await browser.new_context(
#             user_agent=(
#                 "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#                 "AppleWebKit/537.36 (KHTML, like Gecko) "
#                 "Chrome/123.0.0.0 Safari/537.36"
#             ),
#             locale="ru-RU"
#         )

#         tasks = [
#             scrape_single(link, semaphore, context, results, idx, len(new_links))
#             for idx, link in enumerate(new_links, 1)
#         ]

#         await asyncio.gather(*tasks)
#         await browser.close()

#     results = [r for r in results if r is not None]

#     if results:
#         conn = db.get_connection()
#         cursor = conn.cursor()
#         for data in results:
#             cursor.execute("""
#                 INSERT INTO vacancies (
#                     title, description, url, published_date, company, location, salary, experience,
#                     employment_type, schedule, working_hours, work_format, skills
#                 ) VALUES (
#                     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
#                 ) ON CONFLICT (url) DO NOTHING
#             """, (
#                 data['title'], data['description'], data['link'], data['published_date'],
#                 data['company'], data['location'], data['salary'], data['experience'],
#                 data['employment_type'], data['schedule'], data['working_hours'],
#                 data['work_format'], data['skills']
#             ))
#         conn.commit()
#         db.return_connection(conn)
#         logging.info(f"✅ Сохранено {len(results)} новых вакансий в БД")
#     else:
#         logging.info("❌ Нет новых данных для сохранения")

# if __name__ == "__main__":
#     db = Database(os.getenv("DATABASE_URL"))
#     asyncio.run(run_scraper(db))



# run_scraper.py
import asyncio
import logging
import random
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
# Импортируем нужные части из config
from src.config import SEARCH_KEYWORDS, MAX_CONCURRENT_TASKS
# Импортируем get_vacancy_details из scraper (он теперь включает чистку)
from src.scraper import get_vacancy_details
from src.parser import get_vacancy_links
# Импортируем утилиты
from src.utils import setup_logger, canonical_link
# Импортируем класс Database
from database import Database
import os
from datetime import datetime # Нужен для NOW() или CURRENT_TIMESTAMP, но лучше использовать SQL функцию


# MAX_CONCURRENT_TASKS перенесен в config.py

async def scrape_single(link, semaphore, context, results, idx, total):
    """
    Обрабатывает одну ссылку на вакансию асинхронно.
    """
    async with semaphore: # Ограничивает количество одновременно выполняемых задач
        page = None # Инициализируем None на случай ошибки до page = await context.new_page()
        try:
            page = await context.new_page()
            logging.info(f"[{idx}/{total}] Обрабатываем: {link}")
            data = await get_vacancy_details(link, page) # get_vacancy_details теперь парсит и чистит
            if data:
                results.append(data) # Добавляем результат, если парсинг успешен

            # Случайная задержка между запросами для снижения нагрузки
            delay = random.uniform(1, 3) # Увеличил верхнюю границу задержки
            logging.info(f"⏱️ Задержка перед следующим запросом: {delay:.2f} сек.")
            await asyncio.sleep(delay)

        except Exception as e:
            # Логируем ошибки при обработке отдельной ссылки
            logging.warning(f"[Scrape Single Error] Ошибка при обработке {link}: {e}")
        finally:
            if page:
                await page.close() # Гарантированное закрытие страницы Playwright


async def run_scraper(db: Database, mode: str = "daily"):
    """
    Основная функция для запуска процесса скрейпинга.
    """
    setup_logger() # Настройка логирования, можно вызвать один раз в run_all.py

    logging.info(f"🔍 Режим запуска скрейпера: {mode.upper()}")
    logging.info("🔎 Загрузка уже существующих ссылок из БД...")

    conn = None # Инициализируем None
    cursor = None
    existing_links = set()
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        # Получаем только URL, т.к. сравниваем только их
        cursor.execute("SELECT url FROM vacancies")
        existing_links = {canonical_link(row[0]) for row in cursor.fetchall()}
        logging.info(f"📊 В БД найдено {len(existing_links)} существующих ссылок.")
    except Exception as e:
         logging.error(f"❌ Ошибка при загрузке существующих ссылок из БД: {e}")
         # В случае ошибки загрузки ссылок, existing_links останется пустым, что приведет к повторному скрейпингу всего.
         # Возможно, стоит прервать выполнение или добавить более строгую обработку.
         # Пока оставим так, чтобы попытаться продолжить.
    finally:
        if cursor:
             cursor.close()
        if conn:
            db.return_connection(conn) # Гарантированное возвращение соединения


    logging.info("🔎 Загрузка ссылок по ключевым словам с HH...")

    all_links = set()
    for keyword in SEARCH_KEYWORDS:
        # Определяем количество страниц для парсинга в зависимости от режима
        max_pages = 100 if mode == "full" else 1
        # get_vacancy_links теперь более устойчива к ошибкам
        raw_links = await get_vacancy_links(keyword, max_pages=max_pages)
        for raw in raw_links:
            canonical = canonical_link(raw)
            if canonical: # Убедимся, что нормализация успешна
                all_links.add(canonical)

    # Находим новые ссылки, которых еще нет в базе данных
    new_links = list(all_links - existing_links)
    logging.info(f"🔗 Найдено {len(all_links)} ссылок всего по ключевым словам.")
    logging.info(f"🆕 Новых ссылок для обработки: {len(new_links)}")

    # Если нет новых ссылок, завершаем работу скрейпера
    if not new_links:
        logging.info("✅ Нет новых ссылок для скрейпинга.")
        return

    results = [] # Список для хранения результатов парсинга деталей
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS) # Семафор для ограничения параллельных задач

    logging.info(f"🚀 Запускаем скрейпинг деталей для {len(new_links)} новых вакансий (до {MAX_CONCURRENT_TASKS} одновременно)...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True) # Запускаем браузер в фоновом режиме
        try:
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36" # Актуальный User-Agent
                ),
                locale="ru-RU" # Установка локали
            )

            # Создаем и запускаем задачи для каждой новой ссылки
            tasks = [
                scrape_single(link, semaphore, context, results, idx, len(new_links))
                for idx, link in enumerate(new_links, 1)
            ]
            await asyncio.gather(*tasks) # Ожидаем завершения всех задач

        finally:
             await browser.close() # Гарантированное закрытие браузера Playwright

    # Фильтруем None результаты (вакансии, которые не удалось обработать)
    successful_results = [r for r in results if r is not None]
    logging.info(f"✅ Успешно обработано деталей для {len(successful_results)} вакансий.")

    # Сохранение успешно обработанных данных в базу данных
    if successful_results:
        conn = None
        cursor = None
        try:
            conn = db.get_connection()
            cursor = conn.cursor()
            # Начало транзакции для более эффективной вставки
            # conn.autocommit = False # Можно установить autocommit в False и использовать conn.commit()/rollback()

            for data in successful_results:
                # Вставляем новые данные. Используем ON CONFLICT (url) DO NOTHING,
                # чтобы избежать ошибок, если ссылка вдруг уже оказалась в БД (хотя мы фильтруем выше).
                # Добавлены поля is_relevant, processed_at, sent_to_telegram с начальными значениями.
                cursor.execute("""
                    INSERT INTO vacancies (
                        title, description, url, published_date, company, location, salary, salary_range, experience,
                        employment_type, schedule, working_hours, work_format, skills,
                        is_relevant, processed_at, sent_to_telegram, published_at -- Добавлены поля
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        NULL, NOW(), FALSE, %s -- NULL для is_relevant (будет определен LLM), NOW() для processed_at, FALSE для sent_to_telegram. published_at из спарсенных данных
                    ) ON CONFLICT (url) DO NOTHING; -- Точка с запятой в конце
                """, (
                    data['title'], data['description'], data['url'], data['published_date'],
                    data['company'], data['location'], data['salary'], data['salary_range'], data['experience'],
                    data['employment_type'], data['schedule'], data['working_hours'],
                    data['work_format'], data['skills'],
                    data['published_at'] # Значение для published_at (date object or None)
                ))

            conn.commit() # Подтверждаем все вставки
            logging.info(f"✅ Сохранено {len(successful_results)} новых/обновленных вакансий в БД.")
        except Exception as e:
            logging.error(f"❌ Ошибка при сохранении данных в БД: {e}")
            if conn:
                conn.rollback() # Откатываем транзакцию в случае ошибки
        finally:
            if cursor:
                 cursor.close()
            if conn:
                db.return_connection(conn) # Гарантированное возвращение соединения
    else:
        logging.info("❌ Нет успешно обработанных данных для сохранения.")

# if __name__ == "__main__": удален