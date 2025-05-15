import logging
import re
from database import Database
import os
from datetime import datetime

def normalize_city_name(city: str) -> str:
    corrections = {
        'Алматы': 'Алматы', "Астане": "Астана", "Атырау": "Атырау", "Шымкенте": "Шымкент",
        "Костанае": "Костанай", "Актобе": "Актобе", "Караганде": "Караганда", "Таразе": "Тараз",
        "Щучинске": "Щучинск", "Усть-Каменогорске": "Усть-Каменогорск", "Семее": "Семей",
        "Павлодаре": "Павлодар", "Кокшетауе": "Кокшетау", "Талдыкоргане": "Талдыкорган",
        "Уральске": "Уральск"
    }
    return corrections.get(city, city)

def extract_city(pub_text: str) -> str:
    if not pub_text or ' в ' not in pub_text:
        return "Не указано"
    city = pub_text.split(' в ')[-1].strip()
    city = re.sub(r'[.,\s]+$', '', city)
    return city.capitalize()

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

    m = re.search(r"от\s+([\d\s]+)\s*(?:₸|₽|\$|€)?\s*до\s+([\d\s]+)\s*(?:₸|₽|\$|€)?", text)
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

month_map = {
    "января": "01", "февраля": "02", "марта": "03", "апреля": "04",
    "мая": "05", "июня": "06", "июля": "07", "августа": "08",
    "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12",
}

def parse_russian_date(date_str: str) -> str:
    try:
        parts = date_str.strip().split()
        if len(parts) != 3:
            return "Не указано"
        day, month_rus, year = parts
        month = month_map.get(month_rus.lower())
        return f"{year}-{month}-{day.zfill(2)}" if month else "Не указано"
    except:
        return "Не указано"

def clean_work_format(text: str) -> str:
    if not isinstance(text, str):
        return "Не указано"
    return text.replace("Формат работы:", "").strip().capitalize() or "Не указано"

def run_cleaning_pipeline(db):
    
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM vacancies WHERE COALESCE(sent_to_telegram, FALSE) = FALSE")
        count = cursor.fetchone()[0]
        logging.info(f"🕵️‍♂️ Найдено {count} неотправленных вакансий для удаления")

        if count > 0:
            logging.info("⏳ Удаление всех вакансий, не отправленных в Telegram...")
            cursor.execute("DELETE FROM vacancies WHERE COALESCE(sent_to_telegram, FALSE) = FALSE")
            deleted_count = cursor.rowcount
            conn.commit()
            logging.info(f"🗑️ Удалено {deleted_count} неотправленных вакансий")
        else:
            logging.info("✅ Нет вакансий для удаления")

    except Exception as e:
        logging.error(f"❌ Ошибка при удалении неотправленных вакансий: {e}")
        conn.rollback()

    cursor.execute("""
        SELECT id, location, salary, skills, work_format, working_hours, published_date
        FROM vacancies
        WHERE summary_duties IS NULL OR summary_requirements IS NULL OR summary_company IS NULL
    """)
    rows = cursor.fetchall()
    for row in rows:
        vacancy_id, location, salary, skills, work_format, working_hours, published_date = row
        # Обработка даты
        if published_date and published_date.strip() != "Не указано":
            parsed_date_str = parse_russian_date(published_date)
            if parsed_date_str != "Не указано":
                try:
                    published_at = datetime.strptime(parsed_date_str, "%Y-%m-%d").date()
                except Exception:
                    published_at = None
            else:
                published_at = None
        else:
            published_at = None
        cleaned = {
            "location": normalize_city_name(extract_city(location)) if location else "Не указано",
            "salary_range": extract_salary_range_with_currency(salary) if salary else "Не указано",
            "skills": clean_skills(skills) if skills else "Не указано",
            "work_format": clean_work_format(work_format) if work_format else "Не указано",
            "working_hours": clean_working_hours(working_hours) if working_hours else "Не указано"
        }
        cursor.execute("""
            UPDATE vacancies
            SET location = %s, salary_range = %s, skills = %s,
                work_format = %s, working_hours = %s, published_at = %s
            WHERE id = %s
        """, (
            cleaned['location'], cleaned['salary_range'], cleaned['skills'],
            cleaned['work_format'], cleaned['working_hours'],
            published_at,
            vacancy_id
        ))

    conn.commit()
    db.return_connection(conn)
    logging.info(f"✅ Очищено {len(rows)} записей")

if __name__ == "__main__":
    db = Database(os.getenv("DATABASE_URL"))
    run_cleaning_pipeline(db)