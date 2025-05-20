# config.py
from dotenv import load_dotenv
import os

load_dotenv()

SEARCH_KEYWORDS = ["Data Science", "Data Analyst", "Data Scientist", 
    "ML Engineer", "Intern Data Scientist", "Junior Data Scientist",
    "Middle Data Scientist", "Senior Data Scientist", "Аналитик данных", "Системный аналитик",
    "Intern Data Analyst", "Junior Data Analyst", "Middle Data Analyst",
    "Senior Data Analyst", "NLP","CV", "NLP Engineer",  "Machine Learning Engineer", "ML Engineer",
    "Intern ML Engineer", "Junior ML Engineer", "Middle ML Engineer", "Senior ML Engineer",
    "Data Engineer", "Intern Data Engineer", "Junior Data Engineer", "Middle Data Engineer",
    "Senior Data Engineer", "Big Data", "Big Data Engineer", "Data Architect", "BI Analyst",
    "Business Intelligence Analyst", "Computer Vision Engineer","Deep Learning Engineer",
    "Artificial Intelligence Engineer", "AI Researcher", "Data Science Manager",
    "Analytics Consultant", "Data Miner", "Data Specialist", "DevOps", "DevOps Engineer",
    "MlOps", "MLOps Engineer",
    "System Analyst", "Финансовый аналитик"]

BASE_URL = "https://hh.kz/search/vacancy"
REGION_ID = 40  

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")

DATABASE_URL = os.getenv("DATABASE_URL")

GEMINI_API_KEY = os.getenv("GEM_API_TOKEN")

GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
HEADERS = {"Content-Type": "application/json"}  

MAX_CONCURRENT_TASKS = 10  
SCRAPER_TIMEOUT_GOTO = 20000
SCRAPER_TIMEOUT_SELECTOR = 15000  

TELEGRAM_DELAY_SECONDS = 5 
TELEGRAM_MAX_DELAY_SECONDS = 10  

LLM_API_RETRIES = 3  
LLM_API_DELAY = 2.0  
LLM_API_TIMEOUT = 30  
