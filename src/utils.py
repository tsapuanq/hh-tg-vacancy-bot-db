# utils.py
import logging
import pandas as pd
import os
from src.config import CSV_MAIN, PROCESSED_DIR
from pathlib import Path
from datetime import datetime

def setup_logger():
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.INFO
    )

def load_existing_links(csv_path: str) -> set:
    if Path(csv_path).exists():
        df = pd.read_csv(csv_path)
        return set(df['link'].unique())
    return set()

def save_to_csv(data: list[dict], csv_path: str):
    Path(csv_path).parent.mkdir(parents=True, exist_ok=True)

    if Path(csv_path).exists():
        df_old = pd.read_csv(csv_path)
        df_new = pd.DataFrame(data)
        df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=["link"])
        df_combined.to_csv(csv_path, index=False, encoding="utf-8-sig")
    else:
        pd.DataFrame(data).to_csv(csv_path, index=False, encoding="utf-8-sig")

def save_raw_data(df: pd.DataFrame, file_path: str):
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(file_path, index=False, encoding="utf-8-sig")
    logging.info(f"[INFO] Raw data saved to: {file_path}")


def clean_text_safe(text):
    if not isinstance(text, str):
        return ""
    return text.replace("\xa0", " ").strip()

def determine_mode() -> str:
    if not os.path.exists(CSV_MAIN):
        return "full"
    try:
        df = pd.read_csv(CSV_MAIN)
        return "full" if df.empty else "daily"
    except Exception:
        return "full"


def save_daily_clean_csv(df: pd.DataFrame):
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    filename = f"vacancies_clean_{datetime.now().strftime('%Y-%m-%d')}.csv"
    full_path = os.path.join(PROCESSED_DIR, filename)
    df.to_csv(full_path, index=False)
    print(f"[INFO] Saved cleaned data to: {full_path}")