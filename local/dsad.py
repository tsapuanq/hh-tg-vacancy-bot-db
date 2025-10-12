# local/debug_scrap_db.py
import os
import psycopg2
import requests
from bs4 import BeautifulSoup
import pandas as pd
from dotenv import load_dotenv
import os
import sys

# --- добавляем путь к проекту ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(project_root)
from src.cleaning import (
    clean_text_safe,
    extract_city,
    normalize_city_name,
    extract_salary_range_with_currency,
    clean_skills,
    clean_schedule,
    clean_work_format,
    parse_russian_date,
)

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

# -------------------------------
# 1️⃣ Подключение к БД
# -------------------------------
def connect():
    conn = psycopg2.connect(DB_URL, sslmode="require")
    conn.autocommit = True
    return conn


# -------------------------------
# 2️⃣ Простейший парсер hh.kz (одна страница)
# -------------------------------
def scrape_vacancy(url):
    print(f"🔍 Scraping: {url}")
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(resp.text, "html.parser")

    title = clean_text_safe(soup.find("h1").get_text() if soup.find("h1") else "")
    company = clean_text_safe(
        soup.find("a", {"data-qa": "vacancy-company-name"}).get_text()
        if soup.find("a", {"data-qa": "vacancy-company-name"})
        else ""
    )
    salary = extract_salary_range_with_currency(
        clean_text_safe(
            soup.find("span", {"data-qa": "vacancy-salary-compensation-type-net"})
            or soup.find("span", {"data-qa": "vacancy-salary"})
        )
    )
    desc_block = soup.find("div", {"data-qa": "vacancy-description"})
    description = clean_text_safe(desc_block.get_text() if desc_block else "")

    pub_date_tag = soup.find("p", {"class": "vacancy-creation-time-redesigned"})
    published_at = None
    if pub_date_tag:
        raw_date = clean_text_safe(pub_date_tag.get_text())
        parsed = parse_russian_date(raw_date.replace("Опубликовано ", ""))
        published_at = parsed or raw_date

    city_tag = soup.find("p", {"data-qa": "vacancy-view-location"})
    city = normalize_city_name(extract_city(city_tag.get_text())) if city_tag else "Не указано"

    skills_list = soup.select("div.bloko-tag__section_text")
    skills = (
        clean_skills(", ".join([clean_text_safe(s.get_text()) for s in skills_list]))
        if skills_list
        else "Не указано"
    )

    schedule = clean_schedule(
        clean_text_safe(
            soup.find("p", {"data-qa": "vacancy-view-employment-mode"})
            or soup.find("p", {"data-qa": "vacancy-view-employment"})
        )
    )
    work_format = clean_work_format(
        clean_text_safe(
            soup.find("p", {"data-qa": "vacancy-view-employment-format"})
        )
    )

    return {
        "url": url,
        "title": title,
        "company": company,
        "salary": salary,
        "description": description,
        "published_at": published_at,
        "city": city,
        "skills": skills,
        "schedule": schedule,
        "work_format": work_format,
    }


# -------------------------------
# 3️⃣ Сохранение в БД
# -------------------------------
def save_to_db(data):
    conn = connect()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vacancies_debug (
            id SERIAL PRIMARY KEY,
            url TEXT,
            title TEXT,
            company TEXT,
            salary TEXT,
            description TEXT,
            published_at TEXT,
            city TEXT,
            skills TEXT,
            schedule TEXT,
            work_format TEXT
        )
    """)

    insert_query = """
        INSERT INTO vacancies_debug (
            url, title, company, salary, description,
            published_at, city, skills, schedule, work_format
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cur.execute(
        insert_query,
        (
            data["url"],
            data["title"],
            data["company"],
            data["salary"],
            data["description"],
            data["published_at"],
            data["city"],
            data["skills"],
            data["schedule"],
            data["work_format"],
        ),
    )
    print("✅ Вакансия сохранена в БД.")
    conn.close()


# -------------------------------
# 4️⃣ Проверка содержимого
# -------------------------------
def check_db():
    conn = connect()
    df = pd.read_sql("SELECT * FROM vacancies_debug ORDER BY id DESC LIMIT 5", conn)
    conn.close()
    print("\n📊 Последние записи в таблице:")
    print(df.to_string(index=False))


# -------------------------------
# MAIN
# -------------------------------
if __name__ == "__main__":
    urls = [
        "https://hh.kz/vacancy/126346195",  # Консультант по оценке бизнеса
        "https://hh.kz/vacancy/125330131",  # Аналитик Colvir
    ]
    for u in urls:
        data = scrape_vacancy(u)
        print("🧩 Результат парсинга:", data)
        save_to_db(data)

    check_db()