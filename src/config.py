# src/config.py
from datetime import datetime
from dotenv import load_dotenv
import os
load_dotenv()

# === Поисковые ключи ===
SEARCH_KEYWORDS = [
    # Data Scientist
    "Data Science",
    "Data Scientist",
    "Intern Data Scientist",
    "Junior Data Scientist",
    "Middle Data Scientist",
    "Senior Data Scientist",

    # Data Analyst
    "Аналитик данных",
    "Системный аналитик",
    "Data Analyst",
    "Intern Data Analyst",
    "Junior Data Analyst",
    "Middle Data Analyst",
    "Senior Data Analyst",

    # NLP Engineer
    "NLP",
    "NLP Engineer",
    "Intern NLP Engineer",
    "Junior NLP Engineer",
    "Middle NLP Engineer",
    "Senior NLP Engineer",

    # Machine Learning Engineer / ML Engineer
    "Machine Learning Engineer",
    "Intern Machine Learning Engineer",
    "Junior Machine Learning Engineer",
    "Middle Machine Learning Engineer",
    "Senior Machine Learning Engineer",
    "ML Engineer",
    "Intern ML Engineer",
    "Junior ML Engineer",
    "Middle ML Engineer",
    "Senior ML Engineer",

    # Data Engineer
    "Data Engineer",
    "Intern Data Engineer",
    "Junior Data Engineer",
    "Middle Data Engineer",
    "Senior Data Engineer",

    # Big Data Engineer
    "Big Data",
    "Big Data Engineer",
    "Intern Big Data Engineer",
    "Junior Big Data Engineer",
    "Middle Big Data Engineer",
    "Senior Big Data Engineer",

    # Data Architect
    "Data Architect",

    # Business Intelligence Analyst
    "BI Analyst",
    "Business Intelligence Analyst",
    "Intern Business Intelligence Analyst",
    "Junior Business Intelligence Analyst",
    "Middle Business Intelligence Analyst",
    "Senior Business Intelligence Analyst",

    # Statistician
    "Statistician",
    "Intern Statistician",
    "Junior Statistician",
    "Middle Statistician",
    "Senior Statistician",

    # Quantitative Analyst
    "Quantitative Analyst"

    # Computer Vision Engineer
    "Computer Vision Engineer",
    "Intern Computer Vision Engineer",
    "Junior Computer Vision Engineer",
    "Middle Computer Vision Engineer",
    "Senior Computer Vision Engineer",

    # Deep Learning Engineer
    "Deep Learning Engineer",
    "Intern Deep Learning Engineer",
    "Junior Deep Learning Engineer",
    "Middle Deep Learning Engineer",
    "Senior Deep Learning Engineer",

    # Artificial Intelligence Engineer
    "Artificial Intelligence Engineer",
    "Intern Artificial Intelligence Engineer",
    "Junior Artificial Intelligence Engineer",
    "Middle Artificial Intelligence Engineer",
    "Senior Artificial Intelligence Engineer",

    # AI Researcher
    "AI Researcher",
    "Intern AI Researcher",
    "Junior AI Researcher",
    "Middle AI Researcher",
    "Senior AI Researcher",

    # Data Researcher
    "Data Researcher",
    "Intern Data Researcher",
    "Junior Data Researcher",
    "Middle Data Researcher",
    "Senior Data Researcher",

    # Predictive Analytics Specialist
    "Predictive Analytics Specialist"

    # Data Science Manager
    "Data Science Manager"

    # Analytics Consultant
    "Analytics Consultant",

    # Data Miner
    "Data Miner"

    # Data Specialist
    "Data Specialist",

    # DevOps Engineer
    "DevOps",
    "DevOps Engineer",
    "Intern DevOps Engineer",
    "Junior DevOps Engineer",
    "Middle DevOps Engineer",
    "Senior DevOps Engineer",

    # MLOps Engineer
    "MlOps",
    "MLOps Engineer",
    "Intern MLOps Engineer",
    "Junior MLOps Engineer",
    "Middle MLOps Engineer",
    "Senior MLOps Engineer",

    # System Analyst
    "System Analyst",
    "Intern System Analyst",
    "Junior System Analyst",
    "Middle System Analyst",
    "Senior System Analyst",

    # Data Platform Engineer
    "Data Platform Engineer",

    # Analytics Engineer
    "Analytics Engineer",

    # Data Visualization Engineer
    "Data Visualization Engineer",

    "Финансовый аналитик"
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

CSV_MAIN = "data/main.csv"


SENT_LINKS_PATH = "data/sent_links.txt"
SENT_IDS_PATH = "data/sent_ids.txt"