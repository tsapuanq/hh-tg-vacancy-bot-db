import os
import psycopg2
import pandas as pd
from datetime import date, timedelta
import re
from dotenv import load_dotenv

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

ROLE_KEYWORDS = {
    "DevOps": [
        r"\bdevsecops\b", r"\bdevops\b", r"devops\s*engineer", r"devops/cloud",
        r"\bsite\s*reliability\s*engineer\b", r"\bsre\b"
    ],
    "MLOps": [
        r"\bmlops\b", r"\bml\s*ops\b", r"\bml\s*platform\b",
        r"\bml\s*infra\b", r"ml\s*инфра", r"ml\s*платформ",
        r"\bmlflow\b", r"\bfeature\s*store\b", r"\bsagemaker\b"
    ],
    "ML Engineer": [
        r"\bml[\s-]*engineer\b", r"\bml[-\s]*инженер\b",
        r"machine\s*learning\s*engineer", r"\bai[\s-]*engineer\b",
        r"(инженер|разработчик|специалист)\s+машинн\w*\s+обуч",
        r"\bnlp[\s/\-]*(engineer|разработ|инженер|специалист)\b",
        r"\bcv[\s/\-]*(engineer|инженер|специалист)\b", r"\bcomputer\s*vision\b",
        r"\brecommendation[s]?\b|\brecsys\b", r"\bdeep\s*learning\b",
        r"\bai\s*developer\b|\bпрограммист\s*ai\b|\bai\/ml\s*разработчик\b",
        r"prompt\s*engineer",
        r"lead .* (ml|machine\s*learning|ai)| (ml|machine\s*learning|ai) .* lead"
    ],
    "Data Engineer": [
        r"\bdata\s*engineer\b", r"\bdata[-\s]*инженер\b", r"\bдата[-\s]*инженер\b",
        r"(инженер|разработчик)\s+данн", r"\betl\b", r"\bdwh\b",
        r"\bdata\s*pipeline\b", r"\bbig\s*data\b", r"\bbig\s*data\s*engineer\b",
        r"\bdata\s*model(er|ing)\b|\bdata\s*modeler/engineer\b",
        r"\bdata\s*fabric\s*engineer\b",
        r"\bdata\s*&\s*infrastructure\s*engineer\b",
        r"\bdata\s*governance\b|\bdata\s*quality\b|\bdg\s*&\s*dq\b",
        r"lead .* data\s*engineer|data\s*engineer .* lead|head of data engineering"
    ],
    "Analytics Engineer": [
        r"\banalytics\s*engineer\b", r"\bdata\s*analytics\s*engineering\b", r"\bdbt\b"
    ],
    "Data Architect": [
        r"\bdata\s*architect\b", r"архитектор\s*данн"
    ],
    "DBA": [
        r"\b(dba|database\s*administrator)\b", r"администратор\s*баз\s*данн"
    ],
    "BI Analyst": [
        r"\bbi[-\s]*analyst\b", r"\bbi[-\s]*аналитик\b", r"\bbusiness\s*intelligence\s*analyst\b",
        r"\bpower\s*bi\b", r"\btableau\b", r"\bqlik\b|\bqlick\b",
        r"bi[-\s]*developer|bi[-\s]*разработчик|разработке\s*bi\s*систем",
        r"консультант\s*внедрени[яе]\s*bi|bi\s*&\s*reporting"
    ],
    "System Analyst": [
        r"\bsystems?\s*analyst\b", r"системн\w*\s*аналитик", r"\bsa\b(?!\w)"
    ],
    "Business Analyst": [
        r"\bbusiness\s*analyst\b", r"бизнес[-\s]*аналитик\b|\bba\b(?!\w)",
        r"head of business analytics|lead .* business analytics"
    ],
    "Product Analyst": [
        r"\bproduct\s*analyst\b", r"продуктов\w*\s*аналитик"
    ],
    "Marketing Analyst": [
        r"\bmarketing\s*analyst\b", r"маркетинг\w*\s*аналитик"
    ],
    "Financial Analyst": [
        r"\bfinancial\s*analyst\b", r"финанс\w*\s*аналитик"
    ],
    "Risk/Fraud Analyst": [
        r"\brisk\s*analyst\b|\bfraud\s*analyst\b|\banti[-\s]*fraud\b",
        r"кредитн\w*\s*рис(к|ков)|рисков\w*\s*аналитик|риск-?модел",
        r"мошенничеств\w*"
    ],
    "Data Scientist": [
        r"\bdata\s*scientist\b|\bds\b(?!\w)", r"уч[её]н\w*\s+данн",
        r"машинн\w*\s+обучен\w*(?!\s*инженер)",
        r"\bresearch\s*scientist\b|\beconometric\w+\b|эконометр",
        r"project\s*manager\s*\(data\s*science/analytics\)"
    ],
    "Data Analyst": [
        r"\bdata\s*analyst\b", r"аналитик\s*данн", r"\banalyst\b\b", r"\bаналитик\b\b",
        r"специалист\s+по\s+работе\s+с\s+данными",
        r"менеджер\s+по\s+аналитике|менеджер\s+по\s+операционной\s+аналитике",
        r"data\s*analytics\s*specialist|intern\s*to\s*data\s*analytics\s*team",
        r"специалист\s+по\s+аналитике(?!\s*и\s*внедрени)|специалист\s+аналитики",
        r"специалист\s+по\s+обработке\s+информации",
        r"data\s*science\s*intern|data\s*science\s*trainee",
        r"data\s*engineering\s*intern|data\s*engineering\s*trainee"
    ],
}

OTHER_LABEL = "Other"

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

df[df['canonical_role'] == 'Other'][['title', 'canonical_role']].to_csv('roles.csv', index=False)
