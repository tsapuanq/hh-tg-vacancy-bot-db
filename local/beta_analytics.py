import os
import psycopg2
import pandas as pd
from datetime import date, timedelta
import re
from dotenv import load_dotenv
from src.config import ROLE_KEYWORDS, OTHER_LABEL

load_dotenv()

def connect_to_db():
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        raise ValueError("Url of DB is not set")
    return psycopg2.connect(db_url, sslmode='require')

def pre_normalize(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    s = re.sub(r"\b(trainee|стаж[её]р|интерн)\b", " intern ", s, flags=re.I)
    s = re.sub(r"\b(head of|руководител[ья]|team\s*lead|lead|лид)\b", " lead ", s, flags=re.I)
    s = s.replace("-", "-").replace("–", "-").replace("—", "-").replace("ё", "е")
    return s

def normalize_text(s: str) -> str:
    s = pre_normalize(s)
    s = s.lower()
    s = re.sub(r"[^\w\s+/\-]+", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s

ROLE_PATTERNS = [
    (role, [re.compile(pat, re.IGNORECASE) for pat in pats])
    for role, pats in ROLE_KEYWORDS.items()
]

def map_role_by_keywords(title: str) -> str:
    t = normalize_text(title)
    for role, patterns in ROLE_PATTERNS:
        for pat in patterns:
            if pat.search(t):
                return role
    return OTHER_LABEL

today = date.today()
week = today - timedelta(days=7)
print(f"from {week} to {today}")

conn = connect_to_db()
query = "SELECT * FROM vacancies"
df = pd.read_sql(query, conn, params=(week, today))
conn.close()

df["canonical_role"] = df["title"].astype(str).apply(map_role_by_keywords)

total = int(df.shape[0])
counts = (
    df["canonical_role"]
    .value_counts(dropna=False)
    .rename_axis("role")
    .reset_index(name="count")
    .sort_values(["count", "role"], ascending=[False, True])
)
counts["percent"] = (counts["count"] / total * 100).round(1) if total else 0.0

print(f"Вакансии за эту неделю: {total}")
print("Профили:")
for _, row in counts.iterrows():
    print(f"- {row['role']} — {row['percent']}%")

print("\nПримеры (первые 20):")
print(df[["title", "canonical_role"]].head(20).to_string(index=False))

df[df['canonical_role'] == 'DevOps'][['title', 'canonical_role']].to_csv('local/roles.csv', index=False)
df.to_csv('local/vacancies_with_roles.csv', index=False)