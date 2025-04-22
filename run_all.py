import argparse
import asyncio
import pandas as pd
from src.run import run_scraper  # async
from src.cleaning import run_cleaning_pipeline  # sync
from src.publisher_test import run_publisher  # returns coroutine
from src.config import PROCESSED_DIR
from datetime import datetime
import os

def save_to_csv(df: pd.DataFrame):
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    filename = f"vacancies_clean_{datetime.now().strftime('%Y-%m-%d')}.csv"
    full_path = os.path.join(PROCESSED_DIR, filename)
    df.to_csv(full_path, index=False)
    print(f"[INFO] Saved cleaned data to: {full_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["full", "daily"], default="daily")
    args = parser.parse_args()

    print("[STEP 1] Scraping raw data...")
    raw = asyncio.run(run_scraper(mode=args.mode))

    print("\n[STEP 2] Cleaning data...")
    cleaned = run_cleaning_pipeline(raw)

    print("\n[STEP 3] Saving to CSV...")
    save_to_csv(cleaned)

    print("\n[STEP 4] Publishing to Telegram...")
    asyncio.run(run_publisher())  # ✅ теперь всё корректно

    print("\n✅ Pipeline completed successfully!")
