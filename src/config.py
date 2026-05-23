# config.py
from dotenv import load_dotenv
import os

load_dotenv()

SEARCH_KEYWORDS = ["Data Science", "Data Analyst", "Data Scientist", 
    "ML Engineer", "Intern Data Scientist", "Junior Data Scientist",
    "Middle Data Scientist", "Senior Data Scientist", "Аналитик данных",
    "Intern Data Analyst", "Junior Data Analyst", "Middle Data Analyst",
    "Senior Data Analyst", "NLP","CV", "NLP Engineer",  "Machine Learning Engineer", "ML Engineer",
    "Intern ML Engineer", "Junior ML Engineer", "Middle ML Engineer", "Senior ML Engineer",
    "Data Engineer", "Intern Data Engineer", "Junior Data Engineer", "Middle Data Engineer",
    "Senior Data Engineer", "Big Data", "Big Data Engineer", "Data Architect", "BI Analyst",
    "Business Intelligence Analyst", "Computer Vision Engineer","Deep Learning Engineer",
    "Artificial Intelligence Engineer", "AI Researcher", "Data Science Manager",
    "Analytics Consultant", "Data Miner", "Data Specialist", "DevOps", "DevOps Engineer",
    "MlOps", "MLOps Engineer"]

BASE_URL = "https://hh.kz/search/vacancy"
REGION_ID = 40  

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")
CHANNEL_USERNAME_TEST = os.getenv("CHANNEL_USERNAME_TEST")
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
TELEGRAM_SOURCE_CHANNEL = os.getenv("TELEGRAM_SOURCE_CHANNEL")
TELEGRAM_SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING")
TELEGRAM_LOOKBACK_DAYS = int(os.getenv("TELEGRAM_LOOKBACK_DAYS", "2"))
TELEGRAM_SOURCE_LIMIT = int(os.getenv("TELEGRAM_SOURCE_LIMIT", "50"))
TELEGRAM_PROCESS_LIMIT = int(os.getenv("TELEGRAM_PROCESS_LIMIT", "20"))

DATABASE_URL = os.getenv("DATABASE_URL")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_URL = os.getenv("OPENAI_API_URL", "https://api.openai.com/v1/responses")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_HEADERS = {"Content-Type": "application/json"}

MAX_CONCURRENT_TASKS = 10
SCRAPER_MAX_PAGES_DAILY = 1
SCRAPER_MAX_PAGES_FULL = 100
SCRAPER_TIMEOUT_GOTO = 20000
SCRAPER_TIMEOUT_SELECTOR = 15000
SAFE_TIMEOUT_GOTO = 45000

TELEGRAM_DELAY_SECONDS = 5 
TELEGRAM_MAX_DELAY_SECONDS = 10  

LLM_API_RETRIES = 3  
LLM_API_DELAY = 2.0  
LLM_API_TIMEOUT = 30  

LLM_API_MIN_INTERVAL = 6.0    
LLM_API_MAX_PER_MIN = 9
LLM_API_BACKOFF_BASE = 2.0
LLM_API_BACKOFF_CAP = 30.0

OTHER_LABEL = "Other"

DESCRIPTION_PROMPT_MAX_CHARS = 4000
NORMALIZE_BATCH_LIMIT = 2000
