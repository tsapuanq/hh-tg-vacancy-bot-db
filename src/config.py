from dotenv import load_dotenv
import os
load_dotenv()

# === Поисковые ключи ===
SEARCH_KEYWORDS = [
    "Data Science"
]
# , "Data Scientist", "Intern Data Scientist", "Junior Data Scientist",
#     "Middle Data Scientist", "Senior Data Scientist", "Аналитик данных", "Системный аналитик",
#     "Data Analyst", "Intern Data Analyst", "Junior Data Analyst", "Middle Data Analyst",
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