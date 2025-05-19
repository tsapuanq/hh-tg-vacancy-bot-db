# utils.py
import logging
from datetime import datetime, timedelta  
import re  



def setup_logger():
    """Настраивает базовый логгер."""
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
