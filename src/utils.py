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



# for pre-filter

BASE_KEYWORDS = [
    "data", "аналит", "sql", "python", "ml", "машинн", "etl",
    "dwh", "postgres", "backend", "developer", "разработ"
]

DENY_WORDS = [
    "курьер", "продав", "официант", "администратор", "водител", "охран",
    "повар", "учитель", "менеджер по продаж", "smm", "копирайт", "hr",
    "строител", "уборщ", "кладовщ", "медицин", "секретар"
]


def is_potentially_relevant(title: str, description: str | None) -> bool:
    """
    Возвращает True, если вакансию можно отправлять на LLM.
    """
    text = (title + " " + (description or "")).lower()

    # 1. Явный треш — сразу отсекаем
    if any(word in text for word in DENY_WORDS):
        return False

    # 2. Должны быть ключевые признаки "датовой" профессии
    return any(word in text for word in BASE_KEYWORDS)