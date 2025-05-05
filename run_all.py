import os
import asyncio
import pandas as pd
import logging
from datetime import datetime
from src.run import run_scraper
from src.cleaning import run_cleaning_pipeline
from src.publisher import run_publisher
from src.config import CSV_MAIN, PROCESSED_DIR
from src.utils import setup_logger
def determine_mode() -> str:
    if not os.path.exists(CSV_MAIN):
        return "full"
    try:
        df = pd.read_csv(CSV_MAIN)
        return "full" if df.empty else "daily"
    except Exception:
        return "full"

def save_to_csv(df: pd.DataFrame):
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    filename = f"vacancies_clean_{datetime.now().strftime('%Y-%m-%d')}.csv"
    full_path = os.path.join(PROCESSED_DIR, filename)
    df.to_csv(full_path, index=False)
    print(f"[INFO] Saved cleaned data to: {full_path}")

if __name__ == "__main__":
    setup_logger()
    
    mode = determine_mode()
    logging.info(f"[MODE] Выбран режим: {mode.upper()}")

    logging.info("[STEP 1] Scraping raw data...")
    raw = asyncio.run(run_scraper(mode=mode))

    logging.info("[STEP 2] Cleaning data...")
    cleaned = run_cleaning_pipeline(raw)

    logging.info("[STEP 3] Saving to CSV...")
    save_to_csv(cleaned)

    logging.info("[STEP 4] Publishing to Telegram...")
    asyncio.run(run_publisher())

    logging.info("✅ Pipeline completed successfully!")
