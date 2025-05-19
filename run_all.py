# run_all.py
import asyncio
import logging
from src.run_scraper import run_scraper
from src.publisher import run_publisher
from src.utils import setup_logger, determine_mode
from database import Database
import os

if __name__ == "__main__":
    setup_logger()  

    db = None  
    try:
        logging.info("🚀 Запуск основного пайплайна...")
        db = Database(os.getenv("DATABASE_URL"))

        mode = determine_mode()  
        logging.info(f"[MODE] Выбран режим: {mode.upper()}")

        logging.info("[STEP 1/2] Scraping raw data...")
        asyncio.run(run_scraper(db, mode=mode))

        logging.info("[STEP 2/2] Publishing to Telegram...")
        run_publisher(db)

        logging.info("✅ Pipeline completed successfully!")

    except Exception as e:
        logging.critical(
            f"❌ Критическая ошибка выполнения пайплайна: {e}", exc_info=True
        )  
    finally:
        if db:
            db.close_all()
            logging.info("🔌 Соединения БД закрыты.")
