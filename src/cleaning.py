import pandas as pd
import re
import ast
from datetime import datetime

# ======= salary =======
def clean_salary(s: str) -> str:
    if not isinstance(s, str) or "не указано" in s.lower():
        return "не указано"
    s = re.sub(r"[а-яА-ЯёЁ]+", "", s.lower())
    s = re.sub(r"[^\d\–\-– $€₸]", "", s).strip()
    return s if s else "не указано"

# ======= skills =======
def clean_skills(skills_str):
    try:
        skills = ast.literal_eval(skills_str)
        return [s.strip() for s in skills if isinstance(s, str) and s.strip()]
    except:
        return []

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
        "about_company": "",
        "responsibilities": "",
        "requirements": ""
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
        blocks["about_company"] = description
        return blocks

    sorted_pos = sorted(positions.items(), key=lambda x: x[1])
    sorted_pos.append(("end", len(description)))

    for i in range(len(sorted_pos) - 1):
        key = sorted_pos[i][0]
        start = sorted_pos[i][1]
        end = sorted_pos[i + 1][1]
        blocks[key] = description[start:end].strip()

    return blocks

# ======= пайплайн =======
def run_cleaning_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df["salary_range"] = df["salary"].apply(clean_salary)
    df["skills_cleaned"] = df["skills"].apply(clean_skills)
    df["published_date_dt"] = df["published_date"].apply(parse_russian_date)

    blocks = df["description"].dropna().apply(split_description_blocks)
    df["about_company"] = blocks.apply(lambda x: x.get("about_company", ""))
    df["responsibilities"] = blocks.apply(lambda x: x.get("responsibilities", ""))
    df["requirements"] = blocks.apply(lambda x: x.get("requirements", ""))

    return df
