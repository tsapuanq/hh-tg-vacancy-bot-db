from dotenv import load_dotenv
load_dotenv()

import os
import pandas as pd
from datetime import datetime
from src.cleaning import run_cleaning_pipeline
from src.config import RAW_DIR, PROCESSED_DIR

def get_latest_csv(path: str = RAW_DIR) -> str:
    csvs = [f for f in os.listdir(path) if f.endswith(".csv")]
    if not csvs:
        raise FileNotFoundError("[ERROR] No CSV files found in raw folder.")
    latest = sorted(csvs)[-1]
    return os.path.join(path, latest)

def save_processed_data(df: pd.DataFrame, path: str = PROCESSED_DIR):
    os.makedirs(path, exist_ok=True)
    filename = f"vacancies_clean_{datetime.now().strftime('%Y-%m-%d')}.csv"
    full_path = os.path.join(path, filename)
    df.to_csv(full_path, index=False)
    print(f"[INFO] Processed data saved to {full_path}")

def main():
    print("[INFO] Starting data cleaning pipeline...")
    latest_raw_path = get_latest_csv()
    print(f"[INFO] Found latest raw file: {latest_raw_path}")

    try:
        df = pd.read_csv(latest_raw_path, encoding="utf-8", errors="ignore")
    except Exception as e:
        print(f"[ERROR] Failed to read raw CSV: {e}")
        return

    cleaned_df = run_cleaning_pipeline(df)

    if cleaned_df is None or cleaned_df.empty:
        print("[WARNING] Cleaned data is empty. Skipping save.")
        return

    save_processed_data(cleaned_df)
    print("[INFO] Cleaning complete.")

def run_cleaning():
    main()

if __name__ == "__main__":
    main()
