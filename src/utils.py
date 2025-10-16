# utils.py
import logging
import re  

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



TARGET_KEYWORDS = [
    # английские
    "data science", "data scientist", "data analyst", "data engineer",
    "analytics engineer", "machine learning", "ml engineer",
    "nlp", "computer vision", "cv", "deep learning", "ai engineer",
    "mlops", "bi analyst", "big data",

    # русские
    "аналитик дан", "машинн обуч", "инженер дан", "биот аналитик",
    "data sci", "data analyt", "датасаент", "дата сайнс",
    "data engineer", "data scient", "мониторинг данных",
    "ml специалист", "ml разработчик", "ml инженер"
]

BASE_KEYWORDS = [
    "data", "аналит", "sql", "python", "ml", "машинн", "etl",
    "dwh", "postgres", "developer", "разработ"
]

DENY_WORDS = [
    "курьер", "продав", "официант", "администратор", "водител", "охран",
    "повар", "учитель", "менеджер по продаж", "smm", "копирайт", "hr",
    "строител", "уборщ", "кладовщ", "медицин", "секретар"
]

def is_potentially_relevant(title: str, description: str | None) -> bool:
    text = (title + " " + (description or "")).lower()

    # 1. Жёсткий отсев (мусорные профессии)
    if any(word in text for word in DENY_WORDS):
        return False

    # 2. Целевые вакансии — сразу пропускаем в LLM
    if any(word in text for word in TARGET_KEYWORDS):
        return True

    # 3. Остальные проверяем по базовым словам
    return any(word in text for word in BASE_KEYWORDS)