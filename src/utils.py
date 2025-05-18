#utils.py
import logging
from datetime import datetime

def setup_logger():
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.INFO
    )

def clean_text_safe(text):
    if not isinstance(text, str):
        return ""
    return text.replace("\xa0", " ").strip()

def determine_mode() -> str:
    # Больше не проверяем CSV, но оставим функцию для совместимости
    return "daily"  # По умолчанию ежедневный режим

def canonical_link(link: str) -> str:
    """
    Обрезает все параметры после '?' в URL вакансии, оставляя базовый путь.
    """
    try:
        return link.split("?", 1)[0]
    except Exception:
        return link

def extract_vacancy_id(link: str) -> str:
    try:
        return link.split('/vacancy/')[1].split('?')[0]
    except (IndexError, AttributeError):
        return None