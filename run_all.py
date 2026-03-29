# run_all.py
import asyncio
import logging
from src.run_scraper import run_scraper
from src.publisher import run_publisher
from src.utils import setup_logger
from database import Database
import os

if __name__ == "__main__":
    setup_logger()

    db = None
    scraper_ok = False
    try:
        logging.info("🚀 Запуск основного пайплайна...")
        db = Database(os.getenv("DATABASE_URL"))

        logging.info("[STEP 1/2] Scraping raw data...")
        try:
            asyncio.run(run_scraper(db))
            scraper_ok = True
        except Exception as e:
            logging.critical(f"❌ Ошибка скрейпера: {e}", exc_info=True)

        if scraper_ok:
            logging.info("[STEP 2/2] Publishing to Telegram...")
            try:
                run_publisher(db)
            except Exception as e:
                logging.critical(f"❌ Ошибка публикации: {e}", exc_info=True)

        logging.info("✅ Pipeline completed.")

    except Exception as e:
        logging.critical(f"❌ Критическая ошибка инициализации: {e}", exc_info=True)
    finally:
        if db:
            db.close_all()
            logging.info("Соединения БД закрыты.")
