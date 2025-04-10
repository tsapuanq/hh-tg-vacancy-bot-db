import asyncio
from src.config import SEARCH_KEYWORDS, CSV_PATH
from src.parser import get_vacancy_links
from src.scraper import get_vacancy_details
from src.utils import setup_logger, save_to_csv, load_existing_links
import logging

async def main():
    setup_logger()
    all_links = set()
    existing_links = load_existing_links(CSV_PATH)

    logging.info("📌 Загрузка вакансий по ключевым словам...")

    for keyword in SEARCH_KEYWORDS:
        links = await get_vacancy_links(keyword, max_pages=10)
        all_links.update(links)

    new_links = list(set(all_links) - existing_links)
    logging.info(f"🆕 Новых ссылок для обработки: {len(new_links)}")

    all_data = []
    for i, link in enumerate(new_links, 1):
        logging.info(f"📄 [{i}/{len(new_links)}] Обрабатываем: {link}")
        try:
            vacancy_data = await get_vacancy_details(link)
            all_data.append(vacancy_data)
        except Exception as e:
            logging.warning(f"⚠️ Ошибка при обработке {link}: {e}")

    if all_data:
        save_to_csv(all_data, CSV_PATH)
        logging.info(f"✅ Сохранено {len(all_data)} новых вакансий в {CSV_PATH}")
    else:
        logging.info("⚠️ Нет новых данных для сохранения")

if __name__ == "__main__":
    asyncio.run(main())
