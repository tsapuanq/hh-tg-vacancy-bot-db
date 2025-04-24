import pandas as pd
import ast
import logging

# ======= salary =======
def extract_salary_range_with_currency(salary_str):
    if pd.isna(salary_str) or "Не указано" in str(salary_str).lower():
        return "Не указано"

    text = str(salary_str).lower().replace('\xa0', ' ')
    import re
    currency = '₸' if '₸' in text else ('$' if '$' in text else ('€' if '€' in text or 'eur' in text else ''))

    match = re.search(r'от\s+([\d\s]+)\s+до\s+([\d\s]+)', text)
    if match:
        return f"{match.group(1).replace(' ', '')}–{match.group(2).replace(' ', '')} {currency}"
    match = re.search(r'от\s+([\d\s]+)', text)
    if match:
        return f"от {match.group(1).replace(' ', '')} {currency}"
    match = re.search(r'до\s+([\d\s]+)', text)
    if match:
        return f"до {match.group(1).replace(' ', '')} {currency}"
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
        return pd.to_datetime(f"{day.zfill(2)}.{month}.{year}", format="%d.%m.%Y", errors='coerce') if month else pd.NaT
    except:
        return pd.NaT

# ======= Основной pipeline =======
def run_cleaning_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame):
        logging.warning("[Pipeline] Input is not a DataFrame.")
        return pd.DataFrame()

    if df.empty:
        logging.warning("[Pipeline] Пустой DataFrame — пропускаем очистку.")
        return df

    print(f"[DEBUG] Исходная форма: {df.shape}")
    print(f"[DEBUG] Колонки: {list(df.columns)}")

    # Удаляем пустые description
    df = df[df["description"].notna()].copy()
    print(f"[DEBUG] После фильтра по description: {df.shape}")

    # Очистка классических полей
    df["salary_range"] = df["salary"].apply(extract_salary_range_with_currency)
    df["skills"] = df["skills"].apply(clean_skills)
    df["published_date_dt"] = df["published_date"].apply(parse_russian_date)

    print(f"[DEBUG] Финальный DataFrame: {df.shape}")
    return df
