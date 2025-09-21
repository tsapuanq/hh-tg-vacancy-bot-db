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

LLM_API_MIN_INTERVAL = 6.0     # ~9 запросов в минуту
LLM_API_MAX_PER_MIN = 9
LLM_API_BACKOFF_BASE = 2.0
LLM_API_BACKOFF_CAP = 30.0

OTHER_LABEL = "Other"

ROLE_KEYWORDS = {
    
    "DevOps": [
        r"\bdevsecops\b", r"\bdevops\b", r"devops\s*engineer", r"devops/cloud",
        r"\bsite\s*reliability\s*engineer\b", r"\bsre\b"
    ],
    "MLOps": [
        r"\bmlops\b", r"\bml\s*ops\b", r"\bml\s*platform\b",
        r"\bml\s*infra\b", r"ml\s*инфра", r"ml\s*платформ",
        r"\bmlflow\b", r"\bfeature\s*store\b", r"\bsagemaker\b"
    ],
    "ML Engineer": [
        r"\bml[\s\-]*engineer\b", r"\bml[\s\-]*инженер\b",
        r"\bai[\s\-]*engineer\b", r"machine\s*learning\s*engineer",
        r"(инженер|разработчик|специалист)\s+машинн\w*\s+обуч",
        r"\bnlp[\s/\-]*(engineer|разработ|инженер|специалист)\b",
        r"\bcv[\s/\-]*(engineer|инженер|специалист)\b", r"\bcomputer\s*vision\b",
        r"\brecommendation[s]?\b|\brecsys\b", r"\bdeep\s*learning\b",
        r"\bai\s*developer\b|\bпрограммист\s*ai\b|\bai[\s/\-]*ml\s*разработчик\b",
        r"\bprompt\s*engineer\b",
        r"(lead|head|team\s*lead|руководител[ья]|эксперт|senior).*(\bml\b|machine\s*learning|ai)"
    ],
    "Data Engineer": [
        r"\bdata\s*engineer\b", r"\bdata[\s\-]*инженер\b", r"\bдата[\s\-]*инженер\b",
        r"(инженер|разработчик)\s+данн", r"\betl\b", r"\bdwh\b",
        r"\bdata\s*pipeline\b", r"\bbig\s*data\b", r"\bbig\s*data\s*engineer\b",
        r"\bdata\s*model(?:er|ing)\b|\bdata\s*modeler/engineer\b",
        r"\bdata\s*fabric\s*engineer\b", r"\bdata\s*&\s*infrastructure\s*engineer\b",
        r"\bdata\s*governance\b|\bdata\s*quality\b|\bdg\s*&\s*dq\b",
        r"\bdataops\b",
        r"(lead|head|team\s*lead|руководител[ья]|эксперт|senior).*data\s*(engineer|инженер|engineering)|head\s*of\s*data\s*engineering"
    ],
    "Analytics Engineer": [
        r"\banalytics\s*engineer\b", r"\bdata\s*analytics\s*engineering\b", r"\bdbt\b"
    ],
    "Data Architect": [
        r"\bdata\s*architect\b", r"архитектор\s*данн"
    ],
    "DBA": [
        r"\b(dba|database\s*administrator)\b", r"администратор\s*баз\s*данн"
    ],

    "BI Analyst": [
        r"\bbi[\s\-]*analyst\b", r"\bbi[\s\-]*аналитик\b", r"\bbusiness\s*intelligence\s*analyst\b",
        r"\bpower\s*bi\b", r"\btableau\b", r"\bqlik\b|\bqlick\b",
        r"\bbi[\s\-]*developer\b|\bbi[\s\-]*разработчик\b|разработк\w*\s*bi\s*систем",
        r"консультант\s*внедрени[яе]\s*bi|bi\s*&\s*reporting",
        r"\bbi[\s\-]*engineer\b|\bинженер\s*по\s*бизнес[\s\-]*аналитике\b",
        r"(lead|head|team\s*lead|руководител[ья]).*\bbi\b|\bbi\b.*(lead|head|team\s*lead)"
    ],
    "System Analyst": [
        r"\bsystems?\s*analyst\b", r"системн\w*\s*аналитик\b", r"\bsa\b(?!\w)"
    ],
    "Business Analyst": [
        r"\bbusiness\s*analyst\b", r"бизнес[\s\-]*аналитик\b|\bba\b(?!\w)",
        r"(head|lead|руководител[ья]).*business\s*analyt"
    ],
    "Product Analyst": [
        r"\bproduct\s*analyst\b", r"продуктов\w*\s*аналитик",
        r"\bsenior\s*product\s*analytic[s]?\b"
    ],
    "Marketing Analyst": [
        r"\bmarketing\s*analyst\b", r"маркетинг\w*\s*аналитик",
        r"\bcvm\b|\bcustomer\s*value\s*management\b|\bchurn\b"
    ],
    "Financial Analyst": [
        r"\bfinancial\s*analyst\b", r"финанс\w*\s*аналитик"
    ],
    "Risk/Fraud Analyst": [
        r"\brisk\s*analyst\b|\bfraud\s*analyst\b|\banti[\s\-]*fraud\b",
        r"кредитн\w*\s*рис(к|ков)|рисков\w*\s*аналитик|риск[\s\-]?модел",
        r"мошенничеств\w*"
    ],

    "Data Scientist": [
        r"\bdata\s*scientist\b|\bds\b(?!\w)", r"уч[её]н\w*\s+данн",
        r"машинн\w*\s+обучен\w*(?!\s*инженер)", r"\bresearch\s*scientist\b",
        r"\beconometric\w+\b|эконометр\w+",
        r"(lead|head|team\s*lead|руководител[ья]).*\bdata\s*science\b|\bhead\s*of\s*data\s*science\b|\blead\s*data\s*science\b"
    ],

    "Data Analyst": [
        r"\bdata\s*analyst\b", r"аналитик\s*данн", r"\banalyst\b\b", r"\bаналитик\b\b",
        r"специалист\s+по\s+работе\s+с\s+данными",
        r"менеджер\s+по\s+аналитик\w*|менеджер\s+по\s+операционной\s+аналитике",
        r"\bdata\s*analytics\s*specialist\b|\bintern\s*to\s*data\s*analytics\s*team\b",
        r"специалист\s+по\s+аналитике(?!\s*и\s*внедрени)|\bспециалист\s+аналитики\b",
        r"специалист\s+по\s+обработке\s+информации",
        r"\bруководител[ья]\s+отдел[ау]?\s+аналитик\w*|\bруководител[ья]\s+центра\s*data[\s\-]*аналитик\w*",
        r"\bруководител[ья]\s+групп\w*\s*(b2b\s*)?аналитик\w*|\bhead\s*of\s*business\s*analytics\b",
        r"\bsta(zh|ж)[её]р\w*\s+аналитик\w*|\bстажер\w*\s+в\s+управлени\w*\s+по\s+аналитике",
        r"\bdata\s*science\s*intern\b|\bdata\s*science\s*trainee\b",     
        r"\bdata\s*engineering\s*trainee\b|\bdata\s*engineering\s*intern\b" 
    ],
}