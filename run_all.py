import asyncio
import logging
from src.run_scraper import run_scraper
from src.cleaning import run_cleaning_pipeline
from src.publisher import run_publisher
from src.utils import setup_logger
from src.utils import determine_mode
from src.utils import save_daily_clean_csv

if __name__ == "__main__":
    setup_logger()

    mode = determine_mode()
    logging.info(f"[MODE] Выбран режим: {mode.upper()}")

    logging.info("[STEP 1] Scraping raw data...")
    raw = asyncio.run(run_scraper(mode=mode))

    logging.info("[STEP 2] Cleaning data...")
    cleaned = run_cleaning_pipeline(raw)

    logging.info("[STEP 3] Saving to CSV...")
    save_daily_clean_csv(cleaned)

    logging.info("[STEP 4] Publishing to Telegram...")
    asyncio.run(run_publisher())

    logging.info("✅ Pipeline completed successfully!")
