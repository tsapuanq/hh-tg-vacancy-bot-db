import argparse
import asyncio
from src.run import run_scraper  # async
from src.cleaning_runner import run_cleaning  # sync
from src.publisher_test import run_publisher  # async

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["full", "daily"],
        default="daily",
        help="Scraper mode: full or daily"
    )
    args = parser.parse_args()

    print("[STEP 1] Scraping raw data...")
    asyncio.run(run_scraper(mode=args.mode))

    print("\n[STEP 2] Cleaning data...")
    run_cleaning()

    # print("\n[STEP 3] Publishing to Telegram...")
    # asyncio.run(run_publisher())

    print("\n✅ Pipeline completed successfully!")
