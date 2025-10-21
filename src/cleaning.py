# cleaning.py
import re

def clean_schedule(text):
    if isinstance(text, str):
        if "График:" in text:
            return text.split("График:")[1].strip()
        elif "Не указано" in text:
            return "Не указано"
    return "Не указано"


def clean_text_safe(text):
    if not isinstance(text, str):
        return ""
    return text.replace("\xa0", " ").strip()


def normalize_city_name(city: str) -> str:
    corrections = {
        "Алматы": "Алматы",
        "Астане": "Астана",
        "Атырау": "Атырау",
        "Шымкенте": "Шымкент",
        "Костанае": "Костанай",
        "Актобе": "Актобе",
        "Караганде": "Караганда",
        "Таразе": "Тараз",
        "Щучинске": "Щучинск",
        "Усть-Каменогорске": "Усть-Каменогорск",
        "Семее": "Семей",
        "Павлодаре": "Павлодар",
        "Кокшетауе": "Кокшетау",
        "Талдыкоргане": "Талдыкорган",
        "Уральске": "Уральск",
    }
    return corrections.get(city, city)


def extract_city_from_block(info_block: str):
    if not info_block:
        return None

    parts = [p.strip() for p in info_block.split("\n") if p.strip()]
    if not parts:
        return None

    for i, p in enumerate(parts):
        if p.lower() == "в" and i + 1 < len(parts):
            return parts[i + 1]

    last = parts[-1]
    return last if not any(ch.isdigit() for ch in last) else None


def clean_working_hours(hours_str: str) -> str:
    if not hours_str:
        return "Не указано"
    s = str(hours_str).strip()
    result = re.sub(r"(?i)^.*?рабочие часы[:：]?\s*", "", s)
    return result if result else "Не указано"


def extract_salary_range_with_currency(salary_str):
    if not salary_str or "не указано" in str(salary_str).lower():
        return "Не указано"

    text = str(salary_str).lower().replace("\xa0", " ").strip()
    if "₸" in text:
        currency = "₸"
    elif "₽" in text:
        currency = "₽"
    elif "$" in text or "usd" in text:
        currency = "$"
    elif "€" in text or "eur" in text:
        currency = "€"
    else:
        currency = ""

    m = re.search(
        r"от\s+([\d\s]+)\s*(?:₸|₽|\$|€)?\s*до\s+([\d\s]+)\s*(?:₸|₽|\$|€)?", text
    )
    if m:
        lo = m.group(1).replace(" ", "")
        hi = m.group(2).replace(" ", "")
        return f"{lo}-{hi} {currency}"

    m = re.search(r"до\s+([\d\s]+)\s*(?:₸|₽|\$|€)?", text)
    if m:
        hi = m.group(1).replace(" ", "")
        return f"до {hi} {currency}"

    m = re.search(r"от\s+([\d\s]+)\s*(?:₸|₽|\$|€)?", text)
    if m:
        lo = m.group(1).replace(" ", "")
        return f"от {lo} {currency}"

    m = re.search(r"([\d\s]+)\s*(₸|₽|\$|€)", text)
    if m:
        amount = m.group(1).replace(" ", "")
        cur = m.group(2)
        return f"{amount} {cur}"

    m = re.search(r"([\d\s]+)", text)
    if m:
        amount = m.group(1).replace(" ", "")
        return f"{amount} {currency}"

    return "Не указано"


def clean_skills(skills_str):
    if not skills_str or skills_str == "Не указано":
        return "Не указано"
    try:
        skills = skills_str.split(",")
        skills = [s.strip() for s in skills if s.strip()]
        return ", ".join(skills) if skills else "Не указано"
    except:
        return "Не указано"

MONTH_MAP = {
    "января": "01",
    "февраля": "02",
    "марта": "03",
    "апреля": "04",
    "мая": "05",
    "июня": "06",
    "июля": "07",
    "августа": "08",
    "сентября": "09",
    "октября": "10",
    "ноября": "11",
    "декабря": "12",
}

def parse_russian_date(date_str: str | None) -> str | None:
    """
    Преобразует '6 октября 2025' → '2025-10-06'
    Возвращает None, если не удалось распарсить.
    """
    if not date_str:
        return None
    try:
        date_str = date_str.strip().lower()
        match = re.search(r"(\d{1,2})\s+([а-я]+)\s+(\d{4})", date_str)
        if not match:
            return None
        day, month_rus, year = match.groups()
        month = MONTH_MAP.get(month_rus)
        if not month:
            return None
        return f"{year}-{month}-{day.zfill(2)}"
    except Exception:
        return None


def clean_work_format(text: str) -> str:
    if not isinstance(text, str):
        return "Не указано"
    return text.replace("Формат работы:", "").strip().capitalize() or "Не указано"