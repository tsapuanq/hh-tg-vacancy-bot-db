# # import pandas as pd
# # import ast
# # import logging
# # from datetime import datetime
# # from src.llm_summary import summarize_description_llm

# # # ======= salary =======
# # def extract_salary_range_with_currency(salary_str):
# #     if pd.isna(salary_str) or "Не указано" in str(salary_str).lower():
# #         return "Не указано"

# #     text = str(salary_str).lower().replace('\xa0', ' ')

# #     if '₸' in text:
# #         currency = '₸'
# #     elif '$' in text:
# #         currency = '$'
# #     elif '€' in text or 'eur' in text:
# #         currency = '€'
# #     else:
# #         currency = ''

# #     import re
# #     match = re.search(r'от\s+([\d\s]+)\s+до\s+([\d\s]+)', text)
# #     if match:
# #         min_val = match.group(1).replace(" ", "")
# #         max_val = match.group(2).replace(" ", "")
# #         return f"{min_val}–{max_val} {currency}"

# #     match = re.search(r'от\s+([\d\s]+)', text)
# #     if match:
# #         min_val = match.group(1).replace(" ", "")
# #         return f"от {min_val} {currency}"

# #     match = re.search(r'до\s+([\d\s]+)', text)
# #     if match:
# #         max_val = match.group(1).replace(" ", "")
# #         return f"до {max_val} {currency}"

# #     return "Не указано"

# # # ======= skills =======
# # def clean_skills(skills_str):
# #     try:
# #         skills = ast.literal_eval(skills_str)
# #         skills = [s.strip() for s in skills if isinstance(s, str) and s.strip()]
# #         return skills if skills else "Не указано"
# #     except:
# #         return "Не указано"

# # # ======= date =======
# # month_map = {
# #     "января": "01", "февраля": "02", "марта": "03", "апреля": "04",
# #     "мая": "05", "июня": "06", "июля": "07", "августа": "08",
# #     "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12"
# # }

# # def parse_russian_date(date_str: str) -> pd.Timestamp:
# #     try:
# #         parts = date_str.strip().split()
# #         if len(parts) != 3:
# #             return pd.NaT
# #         day, month_rus, year = parts
# #         month = month_map.get(month_rus.lower())
# #         if not month:
# #             return pd.NaT
# #         return pd.to_datetime(f"{day.zfill(2)}.{month}.{year}", format="%d.%m.%Y", errors='coerce')
# #     except Exception:
# #         return pd.NaT

# # # ======= Final pipeline =======
# # # def run_cleaning_pipeline(df: pd.DataFrame) -> pd.DataFrame:
# # #     if df is None or not isinstance(df, pd.DataFrame):
# # #         logging.warning("[Pipeline] Input is not a DataFrame.")
# # #         return pd.DataFrame()

# # #     if df.empty:
# # #         logging.warning("[Pipeline] Пустой DataFrame — пропускаем очистку.")
# # #         return df

# # #     df = df[df["description"].notna()].copy()

# # #     # 1. Классическая очистка
# # #     df["salary_range"] = df["salary"].apply(extract_salary_range_with_currency)
# # #     df["skills"] = df["skills"].apply(clean_skills)
# # #     df["published_date_dt"] = df["published_date"].apply(parse_russian_date)

# # #     # 2. Summary от Gemini
# # #     #blocks = df["description"].apply(summarize_description_llm)
# # #     df["about_company"] = 'Не указано' #blocks.map(lambda x: x.get("about_company", "Не указано"))
# # #     df["responsibilities"] = 'Не указано' #blocks.map(lambda x: x.get("responsibilities", "Не указано"))
# # #     df["requirements"] = 'Не указано' #blocks.map(lambda x: x.get("requirements", "Не указано"))

# # #     return df

# # def run_cleaning_pipeline(df: pd.DataFrame) -> pd.DataFrame:
# #     if df is None or not isinstance(df, pd.DataFrame):
# #         logging.warning("[Pipeline] Input is not a DataFrame.")
# #         return pd.DataFrame()

# #     if df.empty:
# #         logging.warning("[Pipeline] Пустой DataFrame — пропускаем очистку.")
# #         return df

# #     print(f"[DEBUG] Raw shape: {df.shape}")
# #     print(f"[DEBUG] Columns: {list(df.columns)}")

# #     df = df[df["description"].notna()].copy()
# #     print(f"[DEBUG] After description filter: {df.shape}")

# #     df["salary_range"] = df["salary"].apply(extract_salary_range_with_currency)
# #     df["skills"] = df["skills"].apply(clean_skills)
# #     df["published_date_dt"] = df["published_date"].apply(parse_russian_date)

# #     df["about_company"] = "Не указано"
# #     df["responsibilities"] = "Не указано"
# #     df["requirements"] = "Не указано"

# #     print(f"[DEBUG] Final shape: {df.shape}")
# #     return df


# import pandas as pd
# import ast
# import logging
# from datetime import datetime
# from src.llm_summary import summarize_description_llm, filter_vacancy_llm

# # ======= salary =======
# def extract_salary_range_with_currency(salary_str):
#     if pd.isna(salary_str) or "Не указано" in str(salary_str).lower():
#         return "Не указано"

#     text = str(salary_str).lower().replace('\xa0', ' ')
#     import re
#     currency = '₸' if '₸' in text else ('$' if '$' in text else ('€' if '€' in text or 'eur' in text else ''))

#     match = re.search(r'от\s+([\d\s]+)\s+до\s+([\d\s]+)', text)
#     if match:
#         return f"{match.group(1).replace(' ', '')}–{match.group(2).replace(' ', '')} {currency}"
#     match = re.search(r'от\s+([\d\s]+)', text)
#     if match:
#         return f"от {match.group(1).replace(' ', '')} {currency}"
#     match = re.search(r'до\s+([\d\s]+)', text)
#     if match:
#         return f"до {match.group(1).replace(' ', '')} {currency}"
#     return "Не указано"

# # ======= skills =======
# def clean_skills(skills_str):
#     try:
#         skills = ast.literal_eval(skills_str)
#         skills = [s.strip() for s in skills if isinstance(s, str) and s.strip()]
#         return skills if skills else "Не указано"
#     except:
#         return "Не указано"

# # ======= date =======
# month_map = {
#     "января": "01", "февраля": "02", "марта": "03", "апреля": "04",
#     "мая": "05", "июня": "06", "июля": "07", "августа": "08",
#     "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12"
# }

# def parse_russian_date(date_str: str) -> pd.Timestamp:
#     try:
#         parts = date_str.strip().split()
#         if len(parts) != 3:
#             return pd.NaT
#         day, month_rus, year = parts
#         month = month_map.get(month_rus.lower())
#         return pd.to_datetime(f"{day.zfill(2)}.{month}.{year}", format="%d.%m.%Y", errors='coerce') if month else pd.NaT
#     except:
#         return pd.NaT

# # ======= Основной pipeline =======
# def run_cleaning_pipeline(df: pd.DataFrame, use_llm: bool = True) -> pd.DataFrame:
#     if df is None or not isinstance(df, pd.DataFrame):
#         logging.warning("[Pipeline] Input is not a DataFrame.")
#         return pd.DataFrame()

#     if df.empty:
#         logging.warning("[Pipeline] Пустой DataFrame — пропускаем очистку.")
#         return df

#     print(f"[DEBUG] Исходная форма: {df.shape}")
#     print(f"[DEBUG] Колонки: {list(df.columns)}")

#     # Удаляем пустые description
#     df = df[df["description"].notna()].copy()
#     print(f"[DEBUG] После фильтра по description: {df.shape}")

#     # Фильтрация по релевантности (через Gemini)
#     df = df[df.apply(lambda row: filter_vacancy_llm(row["title"], row["description"]), axis=1)]
#     print(f"[DEBUG] После LLM-фильтра: {df.shape}")

#     # Очистка классических полей
#     df["salary_range"] = df["salary"].apply(extract_salary_range_with_currency)
#     df["skills"] = df["skills"].apply(clean_skills)
#     df["published_date_dt"] = df["published_date"].apply(parse_russian_date)

#     # Генерация блоков summary (через Gemini)
#     if use_llm:
#         print("[INFO] Генерация блоков summary через Gemini...")
#         summaries = df["description"].apply(summarize_description_llm)
#         df["about_company"] = summaries.map(lambda x: x.get("about_company", "Не указано"))
#         df["responsibilities"] = summaries.map(lambda x: x.get("responsibilities", "Не указано"))
#         df["requirements"] = summaries.map(lambda x: x.get("requirements", "Не указано"))
#     else:
#         df["about_company"] = "Не указано"
#         df["responsibilities"] = "Не указано"
#         df["requirements"] = "Не указано"

#     print(f"[DEBUG] Финальный DataFrame: {df.shape}")
#     return df

import pandas as pd
import ast
import logging
from datetime import datetime

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

    # Статические поля (для совместимости)
    # df["about_company"] = "Не указано"
    # df["responsibilities"] = "Не указано"
    # df["requirements"] = "Не указано"

    print(f"[DEBUG] Финальный DataFrame: {df.shape}")
    return df
