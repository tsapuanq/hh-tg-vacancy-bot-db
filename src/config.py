# src/config.py
from datetime import datetime
from dotenv import load_dotenv
import os
load_dotenv()

# === Поисковые ключи ===
SEARCH_KEYWORDS = [
    "Data Scientist", "Data Analyst", "NLP Engineer",
    "Senior Data Scientist", "Machine Learning Engineer",
    "ML Engineer","Junior Data Scientist", "Senior Data Analyst",
    "Data Engineer", "Big Data Engineer",
    "Data Architect", "Business Intelligence Analyst", "BI Analyst",
    "Business Intelligence Developer", "Statistician", "Quantitative Analyst",
    "Computer Vision Engineer", "Deep Learning Engineer",
    "Artificial Intelligence Engineer", "AI Researcher", "Data Researcher",
    "Predictive Analytics Specialist", "Data Science Manager",
    "Analytics Consultant", "Data Miner", "Data Specialist"
]

# === HH settings ===
BASE_URL = "https://hh.kz/search/vacancy"
REGION_ID = 40  # Казахстан

# === Пути к данным ===
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

# Автоматически создаваемый файл при каждом скрапе
TODAY_STR = datetime.now().strftime("%Y-%m-%d")
CSV_RAW_DAILY = f"{RAW_DIR}/vacancies_{TODAY_STR}.csv"

# Результат после очистки
CSV_CLEANED_DAILY = f"{PROCESSED_DIR}/vacancies_clean_{TODAY_STR}.csv"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")


def get_today_processed_csv():
    today = datetime.now().strftime("%Y-%m-%d")
    return f"data/processed/vacancies_clean_{today}.csv"


CSV_MAIN = "data/main.csv"


SENT_LINKS_PATH = "data/sent_links.txt"
SENT_IDS_PATH = "data/sent_ids.txt"