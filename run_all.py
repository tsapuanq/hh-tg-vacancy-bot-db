#run_all.py
import asyncio
import logging
from src.run_scraper import run_scraper
from src.cleaning import run_cleaning_pipeline
from src.publisher import run_publisher
from src.utils import setup_logger, determine_mode
from database import Database
import os

if __name__ == "__main__":
    setup_logger()

    db = Database(os.getenv("DATABASE_URL"))
    mode = determine_mode()
    logging.info(f"[MODE] Выбран режим: {mode.upper()}")

    logging.info("[STEP 1] Scraping raw data...")
    asyncio.run(run_scraper(db, mode=mode))

    logging.info("[STEP 2] Cleaning data...")
    run_cleaning_pipeline(db)

    logging.info("[STEP 3] Publishing to Telegram...")
    run_publisher(db)

    logging.info("✅ Pipeline completed successfully!")