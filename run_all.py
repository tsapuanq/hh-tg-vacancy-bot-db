import argparse
import asyncio
import pandas as pd
from src.run import run_scraper
from src.cleaning import run_cleaning_pipeline
from src.publisher_test import run_publisher
from src.crud import insert_if_not_exists
from src.db import SessionLocal

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["full", "daily"], default="daily")
    args = parser.parse_args()

    print("[STEP 1] Scraping raw data...")
    raw = asyncio.run(run_scraper(mode=args.mode))

    print("\n[STEP 2] Cleaning data...")
    cleaned = run_cleaning_pipeline(raw)  # cleaned — это DataFrame

    print("\n[STEP 3] Saving to DB...")
    session = SessionLocal()
    for row in cleaned.to_dict(orient="records"):
        insert_if_not_exists(session, row)

    print("\n[STEP 4] Publishing to Telegram...")
    asyncio.run(run_publisher())

    print("\n✅ Pipeline completed successfully!")
