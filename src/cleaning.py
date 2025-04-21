import pandas as pd
import re
import ast
from datetime import datetime
from src.llm_summary import filter_vacancy_llm, summarize_description_llm
import logging

# ======= Keywords фильтрация =======
from src.config import SEARCH_KEYWORDS

whitelist = [kw.lower() for kw in SEARCH_KEYWORDS] + [
    "data", "данных", "ml", "machine learning", "analyst", "scientist", "ai", "аналитик", 'devops', 'data engineer'
]

blacklist = [
    "маркет", "продаж", "java", "smm", "frontend", "backend", "developer", "qa", "тестировщик",
    "финансист", "консультант", "1с", ".net", "technical director", "repair controller", "hr", "human resources"
]

def is_relevant(title: str) -> bool:
    if not isinstance(title, str):
        return False

    t = title.lower()

    if any(bad in t for bad in blacklist):
        return False

    if not any(kw in t for kw in whitelist):
        return False

    return True

# ======= salary =======
def extract_salary_range_with_currency(salary_str):
    if pd.isna(salary_str) or "Не указано" in salary_str.lower():
        return "Не указано"

    text = salary_str.lower().replace('\xa0', ' ')

    if '₸' in text:
        currency = '₸'
    elif '$' in text:
        currency = '$'
    elif '€' in text or 'eur' in text:
        currency = '€'
    else:
        currency = ''

    match = re.search(r'от\s+([\d\s]+)\s+до\s+([\d\s]+)', text)
    if match:
        min_val = match.group(1).replace(" ", "")
        max_val = match.group(2).replace(" ", "")
        return f"{min_val}–{max_val} {currency}"

    match = re.search(r'от\s+([\d\s]+)', text)
    if match:
        min_val = match.group(1).replace(" ", "")
        return f"от {min_val} {currency}"

    match = re.search(r'до\s+([\d\s]+)', text)
    if match:
        max_val = match.group(1).replace(" ", "")
        return f"до {max_val} {currency}"

    return "Не указано"

# ======= skills =======
def clean_skills(skills_str):
    try:
        skills = ast.literal_eval(skills_str)
        skills = [s.strip() for s in skills if isinstance(s, str) and s.strip()]
        return skills if skills else "Не указано"

    except:
        return "Не указано"

# ======= date =======
month_map = {
    "января": "01", "февраля": "02", "марта": "03", "апреля": "04",
    "мая": "05", "июня": "06", "июля": "07", "августа": "08",
    "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12"
}

def parse_russian_date(date_str: str) -> pd.Timestamp:
    try:
        parts = date_str.strip().split()
        if len(parts) != 3:
            return pd.NaT
        day, month_rus, year = parts
        month = month_map.get(month_rus.lower())
        if not month:
            return pd.NaT
        return pd.to_datetime(f"{day.zfill(2)}.{month}.{year}", format="%d.%m.%Y", errors='coerce')
    except Exception:
        return pd.NaT

# ======= description splitter (регулярками) =======
def split_description_blocks(description: str) -> dict:
    blocks = {
        "about_company": "Не указано",
        "responsibilities": "Не указано",
        "requirements": "Не указано"
    }

    section_patterns = {
        "about_company": [
            r"(мы\s—|о\sкомпании|о\sнас|о\sгруппе|about|join us|we are|компания\s)",
        ],
        "responsibilities": [
            r"(обязанности|что\sбудешь\sсделать|тебе\sпредстоит|ключевые\sзадачи|responsibilities|roles\sand\sresponsibilities|чем\sпредстоит\sзаниматься)"
        ],
        "requirements": [
            r"(требования|что\sмы\sожидаем|skills|requirements|qualifications|qualifications/experience|будет\sпреимуществом|технические\sнавыки|nice\sto\shave)"
        ]
    }

    positions = {}

    for section, patterns in section_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, description, flags=re.IGNORECASE)
            if match:
                positions[section] = match.start()
                break  # Берем первый подходящий

    if not positions:
        blocks["about_company"] = description.strip() if description.strip() else "Не указано"
        return blocks

    sorted_pos = sorted(positions.items(), key=lambda x: x[1])
    sorted_pos.append(("end", len(description)))

    for i in range(len(sorted_pos) - 1):
        key = sorted_pos[i][0]
        start = sorted_pos[i][1]
        end = sorted_pos[i + 1][1]
        block_text = description[start:end].strip()
        blocks[key] = block_text if block_text else "Не указано"

    return blocks

# ======= пайплайн =======

# def run_cleaning_pipeline(df: pd.DataFrame) -> pd.DataFrame:
#     # 1. LLM‑фильтр
#     df = df[
#         df.apply(
#             lambda r: filter_vacancy_llm(r.get("title", ""), r.get("description", "")),
#             axis=1,
#         )
#     ].copy()

#     # 2. Твои текущие очистки
#     df["salary"] = df["salary"].apply(extract_salary_range_with_currency)
#     df["skills"] = df["skills"].apply(clean_skills)
#     df["published_date_dt"] = df["published_date"].apply(parse_russian_date)

#     # 3. Gemini‑summary
#     blocks = df["description"].apply(summarize_description_llm)
#     df["about_company"] = blocks.map(lambda x: x["about_company"])
#     df["responsibilities"] = blocks.map(lambda x: x["responsibilities"])
#     df["requirements"] = blocks.map(lambda x: x["requirements"])

#     return df


def run_cleaning_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame):
        logging.warning("[Pipeline] Input is not a DataFrame.")
        return pd.DataFrame()

    if df.empty:
        logging.warning("[Pipeline] Пустой DataFrame — пропускаем очистку.")
        return df

    # дальше всё, как было

    # 1. LLM‑фильтр через Gemini
    df = df[
        df.apply(
            lambda r: filter_vacancy_llm(r.get("title", ""), r.get("description", "")),
            axis=1,
        )
    ].copy()

    if df.empty:
        print("[WARNING] После LLM-фильтрации DataFrame стал пустым.")
        return df

    # 2. Классическая очистка
    df["salary"] = df["salary"].apply(extract_salary_range_with_currency)
    df["skills"] = df["skills"].apply(clean_skills)
    df["published_date_dt"] = df["published_date"].apply(parse_russian_date)

    # 3. Summary от Gemini
    blocks = df["description"].apply(summarize_description_llm)
    df["about_company"] = blocks.map(lambda x: x.get("about_company", "Не указано"))
    df["responsibilities"] = blocks.map(lambda x: x.get("responsibilities", "Не указано"))
    df["requirements"] = blocks.map(lambda x: x.get("requirements", "Не указано"))

    return df

def run_cleaning_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame):
        logging.warning("[Pipeline] Input is not a DataFrame.")
        return pd.DataFrame()

    if df.empty:
        logging.warning("[Pipeline] Пустой DataFrame — пропускаем очистку.")
        return df

    # дальше всё, как было
