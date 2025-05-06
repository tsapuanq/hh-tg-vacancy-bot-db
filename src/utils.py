# utils.py
import logging
import pandas as pd
import os
from src.config import CSV_MAIN, PROCESSED_DIR
from pathlib import Path
from datetime import datetime
from src.config import SENT_LINKS_PATH, SENT_IDS_PATH

def setup_logger():
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.INFO
    )

def load_today_rows() -> pd.DataFrame:
    csv_path = get_today_processed_csv()
    if not os.path.exists(csv_path):
        print(f"❌ CSV не найден: {csv_path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(csv_path)
        today_str = datetime.now().strftime("%Y-%m-%d")
        filtered_df = df[df["published_date_dt"] == today_str]
        
        print(f"🔎 Найдено {len(filtered_df)} вакансий за {today_str}")
        return filtered_df

    except Exception as e:
        print(f"❌ Ошибка чтения CSV: {e}")
        return pd.DataFrame()
    

def append_sent_links(links: list, path: str = SENT_LINKS_PATH):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for link in links:
            f.write(link + "\n")
    
def load_sent_links(path: str = SENT_LINKS_PATH) -> set:
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f)

def load_sent_ids(path: str = SENT_IDS_PATH) -> set:
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f)

def append_sent_ids(ids: list, path: str = SENT_IDS_PATH):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for vacancy_id in ids:
            f.write(vacancy_id + "\n")

def extract_vacancy_id(link: str) -> str:
    try:
        return link.split('/vacancy/')[1].split('?')[0]
    except (IndexError, AttributeError):
        return None

def get_today_processed_csv():
    today = datetime.now().strftime("%Y-%m-%d")
    return f"data/processed/vacancies_clean_{today}.csv"

def load_existing_links(csv_path: str) -> set:
    if Path(csv_path).exists():
        df = pd.read_csv(csv_path)
        return set(df['link'].unique())
    return set()

def save_to_main_csv(data: list[dict], csv_path: str):
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