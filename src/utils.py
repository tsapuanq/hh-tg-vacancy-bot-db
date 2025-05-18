# #utils.py
# import logging
# from datetime import datetime

# def setup_logger():
#     logging.basicConfig(
#         format="%(asctime)s - %(levelname)s - %(message)s",
#         level=logging.INFO
#     )

# def determine_mode() -> str:
#     return "daily"  # По умолчанию ежедневный режим

# def canonical_link(link: str) -> str:
#     """
#     Обрезает все параметры после '?' в URL вакансии, оставляя базовый путь.
#     """
#     try:
#         return link.split("?", 1)[0]
#     except Exception:
#         return link

# def extract_vacancy_id(link: str) -> str:
#     try:
#         return link.split('/vacancy/')[1].split('?')[0]
#     except (IndexError, AttributeError):
#         return None


# utils.py
import logging
from datetime import datetime, timedelta # Добавил timedelta
import re # Добавил для extract_vacancy_id, если он останется

# clean_text_safe перенесена в cleaning.py

def setup_logger():
    """Настраивает базовый логгер."""
    # Проверяем, настроен ли уже логгер, чтобы избежать дублирования хендлеров
    if not logging.getLogger().handlers:
        logging.basicConfig(
            format="%(asctime)s - %(levelname)s - %(message)s",
            level=logging.INFO
        )
        logging.info("✅ Логгер настроен.")
    else:
         logging.debug("Логгер уже настроен, пропускаем.")


def determine_mode() -> str:
    """
    Определяет режим работы (daily или full).
    В текущей версии всегда возвращает 'daily'.
    Может быть расширена для чтения из ENV или аргументов.
    """
    # Реализация для выбора режима (например, из переменных окружения)
    # mode_env = os.getenv("SCRAPER_MODE", "daily").lower()
    # if mode_env in ["full", "daily"]:
    #     return mode_env
    return "daily" # По умолчанию ежедневный режим


def canonical_link(link: str) -> str | None:
    """
    Обрезает все параметры после '?' в URL вакансии, оставляя базовый путь.
    Возвращает нормализованную ссылку или None, если ссылка недействительна.
    """
    if not isinstance(link, str) or not link.strip():
        return None
    try:
        # split() без maxsplit разделяет по всем '?', split(?, 1) разделяет только по первому
        return link.split("?", 1)[0].strip() # Добавил strip() на случай пробелов
    except Exception:
        return link.strip() if isinstance(link, str) else None # Если ошибка, возвращаем исходную ссылку (обрезав пробелы) или None

# extract_vacancy_id есть, но нигде не используется в представленном коде.
# Если он не нужен, его можно удалить. Оставим пока на случай, если он нужен для чего-то еще.
def extract_vacancy_id(link: str) -> str | None:
    """Извлекает ID вакансии из URL."""
    if not isinstance(link, str) or not link.strip():
        return None
    try:
        # Ищем паттерн /vacancy/число/
        match = re.search(r'/vacancy/(\d+)', link)
        if match:
             return match.group(1)
        return None
    except Exception:
        return None

# if __name__ == "__main__": удален