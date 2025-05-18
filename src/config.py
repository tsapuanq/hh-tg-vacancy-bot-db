# config.py
from dotenv import load_dotenv
import os
load_dotenv()

# === Поисковые ключи ===
# Активирован более полный список, если он требуется
SEARCH_KEYWORDS = [
    "Data Science"
]
#  "Data Analyst", "ML Engineer", "Data Scientist", "Intern Data Scientist", "Junior Data Scientist",
#     "Middle Data Scientist", "Senior Data Scientist", "Аналитик данных", "Системный аналитик",
#     "Intern Data Analyst", "Junior Data Analyst", "Middle Data Analyst",
#     "Senior Data Analyst", "NLP", "NLP Engineer", "Intern NLP Engineer", "Junior NLP Engineer",
#     "Middle NLP Engineer", "Senior NLP Engineer", "Machine Learning Engineer",
#     "Intern Machine Learning Engineer", "Junior Machine Learning Engineer",
#     "Middle Machine Learning Engineer", "Senior Machine Learning Engineer", "ML Engineer",
#     "Intern ML Engineer", "Junior ML Engineer", "Middle ML Engineer", "Senior ML Engineer",
#     "Data Engineer", "Intern Data Engineer", "Junior Data Engineer", "Middle Data Engineer",
#     "Senior Data Engineer", "Big Data", "Big Data Engineer", "Data Architect", "BI Analyst",
#     "Business Intelligence Analyst", "Intern Business Intelligence Analyst",
#     "Junior Business Intelligence Analyst", "Middle Business Intelligence Analyst",
#     "Senior Business Intelligence Analyst", "Computer Vision Engineer",
#     "Intern Computer Vision Engineer", "Junior Computer Vision Engineer",
#     "Middle Computer Vision Engineer", "Senior Computer Vision Engineer",
#     "Deep Learning Engineer", "Intern Deep Learning Engineer", "Junior Deep Learning Engineer",
#     "Middle Deep Learning Engineer", "Senior Deep Learning Engineer",
#     "Artificial Intelligence Engineer", "Intern Artificial Intelligence Engineer",
#     "Junior Artificial Intelligence Engineer", "Middle Artificial Intelligence Engineer",
#     "Senior Artificial Intelligence Engineer", "AI Researcher", "Data Science Manager",
#     "Analytics Consultant", "Data Miner", "Data Specialist", "DevOps", "DevOps Engineer",
#     "Intern DevOps Engineer", "Junior DevOps Engineer", "Middle DevOps Engineer",
#     "Senior DevOps Engineer", "MlOps", "MLOps Engineer", "Intern MLOps Engineer",
#     "Junior MLOps Engineer", "Middle MLOps Engineer", "Senior MLOps Engineer",
#     "System Analyst", "Analytics Engineer", "Data Visualization Engineer", "Финансовый аналитик"
# === HH settings ===
BASE_URL = "https://hh.kz/search/vacancy"
REGION_ID = 40  # Казахстан

# === Telegram settings ===
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")

# === Database settings ===
# Добавляем сюда для явности, хотя используется через os.getenv в database.py
DATABASE_URL = os.getenv("DATABASE_URL")

# === LLM settings ===
GEMINI_API_KEY = os.getenv("GEM_API_TOKEN")

# Переносим из llm_summary.py, т.к. это конфигурация
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
HEADERS = {"Content-Type": "application/json"} # Тоже можно оставить здесь

# === Scraper settings ===
MAX_CONCURRENT_TASKS = 10 # Переносим из run_scraper.py
SCRAPER_TIMEOUT_GOTO = 20000 # ms, переносим из scraper.py
SCRAPER_TIMEOUT_SELECTOR = 15000 # ms, переносим из scraper.py

# === Publisher settings ===
TELEGRAM_DELAY_SECONDS = 5 # Минимальная задержка между отправками в ТГ
TELEGRAM_MAX_DELAY_SECONDS = 10 # Максимальная задержка

# === LLM API settings ===
LLM_API_RETRIES = 3 # Количество повторных попыток вызова LLM
LLM_API_DELAY = 2.0 # Задержка между повторными попытками LLM
LLM_API_TIMEOUT = 30 # Таймаут запроса к LLM