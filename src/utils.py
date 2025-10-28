# utils.py
import logging
import re  
from typing import Optional

def setup_logger():
    if not logging.getLogger().handlers:
        logging.basicConfig(
            format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
        )
        logging.info("✅ Логгер настроен.")
    else:
        logging.debug("Логгер уже настроен, пропускаем.")


def determine_mode() -> str:
    return "daily"  


def canonical_link(link: str) -> str | None:
    if not isinstance(link, str) or not link.strip():
        return None
    try:
        return link.split("?", 1)[0].strip()  
    except Exception:
        return (
            link.strip() if isinstance(link, str) else None
        )  


def extract_vacancy_id(link: str) -> str | None:
    """Извлекает ID вакансии из URL."""
    if not isinstance(link, str) or not link.strip():
        return None
    try:
        match = re.search(r"/vacancy/(\d+)", link)
        if match:
            return match.group(1)
        return None
    except Exception:
        return None



TARGET_KEYWORDS_FINAL = [
    "data sci", "data scientist", "data analyst", "data science", "analyt", 
    "аналитик дан", "analyst", "business analyst", "system analyst",
    "bi analyst", "business intelligence", "датасаент", 
    "дата сайнс", "аналитич конс", "data miner", "data specialist",

    "machine learning", "ml engineer", "ml engin", "ml разраб", "nlp", 
    "computer vision", "cv engin", "deep learn", "ai engineer", "ai researcher", 
    "машинн обуч", "мл инженер", "мл специалист", "cv", 
    
    "data engin", "data engineer", "инженер дан", "big data", "big data engineer", 
    "data architect", "etl", "dwh", "мониторинг данных", "биот аналитик",
    "cloud engineer", "data analytics", "data analysis",
    "power bi", "powerbi", "junior analyst",

    "mlops", "mlops engineer", "devops", "devops engineer"
]

BASE_KEYWORDS_ROOT = [
    "data", "аналит", "sql", "python", "ml", "etl", "dwh",
    "postgres", "spark", "airflow", "cloud", "aws", "gcp", "azure", 
    "обработк", "статистик", "моделирован"
]

DENY_WORDS_ROOT = [
    "курьер", "продав", "официант", "администратор", "водител", "охран",
    "повар", "учител", "менеджер по продаж", "копирайт", "hr",
    "техподдержк", "кассир", "сварщ", "бухгалтер"
]

def normalize_text(text: str) -> str:
    if not text:
        return ""
    
    text = re.sub(r'[^a-zа-я0-9]+', ' ', text.lower())

    return re.sub(r'\s+', ' ', text).strip()

def is_relevant_soft(title: str, description: Optional[str]) -> bool:

    text = normalize_text(title + " " + (description or ""))

    if any(word in text for word in DENY_WORDS_ROOT):
        return False

    if any(word in text for word in TARGET_KEYWORDS_FINAL):
        return True

    base_matches = sum(1 for word in BASE_KEYWORDS_ROOT if word in text)

    return base_matches >= 2
