from src.run import run_scraper
from src.cleaning_runner import run_cleaning
from src.publisher_test import run_publisher

if __name__ == "__main__":
    print("[STEP 1] Scraping raw data...")
    run_scraper()

    print("\n[STEP 2] Cleaning data...")
    run_cleaning()

    print("\n[STEP 3] Publishing to Telegram...")
    run_publisher()

    print("\n✅ Pipeline completed successfully!")
