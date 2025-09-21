# weekly_report_creative.py
import os
import re
import math
import requests
import psycopg2
import pandas as pd
import argparse
from datetime import date, timedelta
from dotenv import load_dotenv

# ---- роли из проекта ----
# Убедитесь, что этот импорт работает в вашей структуре проекта
# from src.config import ROLE_KEYWORDS, OTHER_LABEL
# Mock-up для запуска скрипта как есть:
ROLE_KEYWORDS = {
    "Data Analyst": [r"data\sanalyst", r"аналитик\sданных"],
    "Data Scientist": [r"data\sscientist", r"data\sscience"],
    "ML Engineer": [r"ml\sengineer", r"machine\slearning"],
    "Data Engineer": [r"data\sengineer", r"инженер\sданных"],
    "MLOps": [r"mlops"],
    "DevOps": [r"devops"],
    "BI Analyst": [r"bi\sanalyst", r"bi\sdeveloper"],
    "Business Analyst": [r"business\sanalyst", r"бизнес-аналитик"],
    "System Analyst": [r"system\sanalyst", r"системный\sаналитик"],
}
OTHER_LABEL = "Other"

load_dotenv()

# =========================
# Telegram
# =========================
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN1")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID")
TG_API   = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage" if TG_TOKEN else None
TG_MAX   = 4096

def tg_send(text: str, parse_mode: str = "HTML"):
    if not TG_TOKEN or not TG_CHAT:
        print("[WARN] Telegram creds not set; printing:\n", text)
        return
    
    # Режем на куски, если сообщение слишком длинное
    chunks = []
    current_chunk = ""
    for line in text.split('\n'):
        if len(current_chunk) + len(line) + 1 > TG_MAX:
            chunks.append(current_chunk)
            current_chunk = line
        else:
            current_chunk += '\n' + line
    chunks.append(current_chunk.strip())

    for chunk in chunks:
        if chunk:
            _post(chunk, parse_mode)

def _post(payload: str, parse_mode: str):
    r = requests.post(
        TG_API,
        data={"chat_id": TG_CHAT, "text": payload, "parse_mode": parse_mode, "disable_web_page_preview": True},
        timeout=20,
    )
    if not r.ok:
        print(f"[TG ERROR] {r.status_code}: {r.text}")

# =========================
# Подключение к БД
# =========================
def connect_to_db():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL is not set")
    return psycopg2.connect(db_url, sslmode="require")

# =========================
# Нормализация и маппинг ролей (без изменений)
# =========================
def _pre_norm(s: str) -> str:
    if not isinstance(s, str): return ""
    s = s.strip()
    s = re.sub(r"\b(trainee|стаж[её]р|интерн)\b", " intern ", s, flags=re.I)
    s = re.sub(r"\b(head of|руководител[ья]|team\s*lead|lead|лид)\b", " lead ", s, flags=re.I)
    s = s.replace("–", "-").replace("—", "-").replace("ё", "е")
    return s

def _norm_title(s: str) -> str:
    s = _pre_norm(s).lower()
    s = re.sub(r"[^\w\s+/\-]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()

ROLE_PATTERNS = [(role, [re.compile(p, re.I) for p in pats]) for role, pats in ROLE_KEYWORDS.items()]

def map_role_by_keywords(title: str) -> str:
    t = _norm_title(title)
    for role, pats in ROLE_PATTERNS:
        if any(p.search(t) for p in pats):
            return role
    return OTHER_LABEL

# =========================
# Навыки — нормализация (без изменений)
# =========================
SKILL_SYNONYMS = {
    "amazon web services":"aws","aws":"aws","microsoft azure":"azure","azure":"azure",
    "google cloud":"gcp","google cloud platform":"gcp","gcp":"gcp",
    "power bi":"powerbi","ms power bi":"powerbi","microsoft power bi":"powerbi","power-bi":"powerbi",
    "qlik sense":"qlik","qlikview":"qlik","qlick":"qlik","looker studio":"looker",
    "google data studio":"looker","superset":"superset","apache superset":"superset",
    "power query":"powerquery", "clickhouse":"clickhouse","greenplum":"greenplum","snowflake":"snowflake","redshift":"redshift",
    "bigquery":"bigquery","data warehouse":"dwh","кхд":"dwh","dbt":"dbt",
    "apache airflow":"airflow","airflow":"airflow","apache kafka":"kafka","kafka":"kafka",
    "apache nifi":"nifi","nifi":"nifi","spark":"spark","apache spark":"spark","pyspark":"pyspark",
    "docker":"docker","kubernetes":"kubernetes","k8s":"kubernetes","helm":"helm","terraform":"terraform",
    "ansible":"ansible","jenkins":"jenkins","zabbix":"zabbix","gitlab ci":"gitlabci",
    "ci/cd":"cicd","ci cd":"cicd","ci-cd":"cicd","ci–cd":"cicd","ci—cd":"cicd",
    "prometheus":"prometheus","grafana":"grafana","elk":"elk","efk":"elk","linux":"linux","openshift":"openshift",
    "python":"python","java":"java","c#":"csharp","csharp":"csharp",
    "javascript":"javascript","js":"javascript","sql":"sql","pandas":"pandas","numpy":"numpy",
    "scikit-learn":"sklearn","sklearn":"sklearn","pytorch":"pytorch","tensorflow":"tensorflow",
    "xgboost":"xgboost","lightgbm":"lightgbm","matplotlib":"matplotlib",
    "nlp":"nlp","llm":"llm","rag":"rag","openai":"openai","langchain":"langchain",
    "computer vision":"computer vision","cv":"computer vision",
}
SQL_ALIASES = [r"\bms\s*sql\b", r"\bmssql\b", r"\bsql\s*server\b", r"\bt[\-\s]?sql\b", r"\btsql\b", r"\bpl[\-\s]?sql\b", r"\bplsql\b", r"\bpostgres(?:ql)?\b", r"\bpostgres\s*sql\b", r"\bmy\s*sql\b", r"\bmysql\b", r"\bmaria\s*db\b", r"\bmariadb\b"]
SQL_ALIAS_COMPILED = [re.compile(p, re.I) for p in SQL_ALIASES]
EXCLUDE_FROM_OVERALL = {"анализ данных", "data analysis", "devops"}
LANG_PAT  = re.compile(r"\b(английск\w*|english|russian|русск\w*|kazakh|казахск\w*)\b", re.I)
LEVEL_PAT = re.compile(r"\b(a1|a2|b1|b2|c1|c2|beginner|intermediate|advanced)\b", re.I)
SKILL_TRASH = {"не указано","none","nan","—","-","soft skills","аналитическое мышление","аналитика", "деловая переписка","работа в команде","умение анализировать","ms office","microsoft windows", "бизнес-анализ","system analysis","business analysis","бизнес-аналитика"}
NBSP_RE = re.compile(r"[\u00A0\u2007\u202F]")
DASH_RE = re.compile(r"[–—]")
RANGE_DASHES = r"[–—-]"

def _clean_sep(x: str) -> str: return DASH_RE.sub("-", NBSP_RE.sub(" ", x))
def _strip_quotes(x: str) -> str: return re.sub(r"\s+", " ", re.sub(r"[\"'’«»“”]", " ", x)).strip()
def _cut_lang(x: str) -> str: return re.split(RANGE_DASHES, x.lower())[0].strip() if LANG_PAT.search(x.lower()) else x
def _norm_skill(s: str) -> str:
    x = _cut_lang(_strip_quotes(_clean_sep(s))).lower()
    x = re.sub(r"\s*/\s*", "/", x); x = re.sub(r"\s*-\s*", "-", x); x = re.sub(r"\s+", " ", x).strip()
    if any(p.search(x) for p in SQL_ALIAS_COMPILED): return "sql"
    return SKILL_SYNONYMS.get(x, x)
def _is_trash(x: str) -> bool:
    if not x or x in SKILL_TRASH: return True
    if LANG_PAT.search(x) or LEVEL_PAT.search(x): return True
    if len(x) <= 1 or x.isdigit(): return True
    wl = {"machine learning","deep learning","data warehouse","business intelligence","rest api","soap api","user story"}
    if len(x.split()) > 3 and x not in wl: return True
    return False

def explode_skills(df: pd.DataFrame, skills_col="skills") -> pd.DataFrame:
    if skills_col not in df.columns or df[skills_col].isna().all():
        return pd.DataFrame(columns=["row_id","skill"])
    rows = []
    for rid, raw in enumerate(df[skills_col].astype(str).tolist()):
        if not raw or raw.lower().strip() in {"не указано","nan","none",""}: continue
        parts = re.split(r"[,\;\|•\n\r]+", raw)
        seen = set()
        for p in parts:
            subs = re.split(rf"\s{RANGE_DASHES}\s", p) if re.search(rf"\s{RANGE_DASHES}\s", p) else [p]
            for sub in subs:
                tok = _norm_skill(sub)
                if _is_trash(tok) or tok in seen: continue
                rows.append({"row_id": rid, "skill": tok}); seen.add(tok)
    return pd.DataFrame(rows)

# =========================
# Подсчёты (без изменений)
# =========================
def _pct(n, d): return 0 if not d else round(n/d*100)

def top_overall(df, skills_col="skills", topn=10):
    s = explode_skills(df, skills_col)
    if s.empty: return []
    vc = (s.groupby("skill")["row_id"].nunique().rename("cnt").reset_index())
    vc = vc[~vc["skill"].isin(EXCLUDE_FROM_OVERALL)].sort_values("cnt", ascending=False).head(topn)
    return [(r["skill"], int(r["cnt"])) for _, r in vc.iterrows()], int(s.row_id.nunique())

def top_by_role(df, skills_col="skills", role_col="canonical_role", per_role_top=3):
    if role_col not in df.columns: return []
    s = explode_skills(df, skills_col)
    if s.empty: return []
    s = s.merge(df[[role_col]].reset_index(drop=True).rename(columns={role_col:"role"}),
                left_on="row_id", right_index=True, how="left")
    out = []
    roles_order = ["Data Analyst","Data Scientist","ML Engineer","Data Engineer", "MLOps","DevOps","BI Analyst","Business Analyst","System Analyst"]
    for role in roles_order:
        g = s[s["role"] == role]
        if g.empty: continue
        denom = int(g["row_id"].nunique())
        vc = (g.groupby("skill")["row_id"].nunique().sort_values(ascending=False).head(per_role_top))
        pairs = [(k, f"{_pct(int(v), max(denom,1))}%") for k, v in vc.items()]
        out.append((role, pairs))
    return out

def top_companies(df, topn=10):
    col_guess = next((c for c in ["company","employer","company_name","company_title"] if c in df.columns), None)
    if not col_guess: return []
    vc = (df[col_guess].astype(str).str.strip().replace({"nan":"", "None":""}).pipe(lambda s: s[s!=""]).value_counts().head(topn))
    return [(k, int(v)) for k, v in vc.items()]

# =========================
# НОВЫЙ БЛОК: Форматирование секций
# =========================
# Константы для эмодзи и оформления
HEADER_EMOJI = "📊"
PROFILES_EMOJI = "🎯"
SKILLS_EMOJI = "🛠️"
BY_ROLE_EMOJI = "📈"
COMPANIES_EMOJI = "🏢"
SEPARATOR = "\n---\n"
MEDALS = ["🥇", "🥈", "🥉"]
ROLE_EMOJIS = {
    "Data Analyst": "👨‍💻", "Data Scientist": "🤖", "ML Engineer": "🧠",
    "Data Engineer": "🏗️", "MLOps": "⚙️", "DevOps": "☁️",
    "BI Analyst": "📊", "Business Analyst": "👔", "System Analyst": "📝",
}

def format_header(total_vacancies: int, period_days: int) -> str:
    if period_days >= 3650: # ~10 лет, считаем что это "с запуска"
        period_str = "4 месяца" # Как в оригинале, можно сделать динамическим
    elif period_days > 30:
        period_str = f"за {period_days // 30} мес."
    elif period_days > 1:
        period_str = f"за {period_days} дней"
    else:
        period_str = "за сегодня"
        
    return (
        f"<b>{HEADER_EMOJI} Аналитика рынка за {period_str}: {total_vacancies} вакансий</b>\n\n"
        f"<i>Свежая статистика по главным трендам рынка. Следите за обновлениями!</i>"
    )

def format_list_with_medals(title: str, items: list, total_denom: int) -> str:
    lines = [f"<b>{title}</b>"]
    for i, (name, count) in enumerate(items):
        percent = _pct(count, total_denom)
        prefix = MEDALS[i] if i < len(MEDALS) else "•"
        lines.append(f"{prefix} {name} — {count} ({percent}%)")
    return "\n".join(lines)

def format_top_skills(title: str, items: list, total_denom: int) -> str:
    lines = [f"<b>{title}</b>"]
    for i, (name, count) in enumerate(items):
        percent = _pct(count, total_denom)
        prefix = MEDALS[i] if i < len(MEDALS) else "•"
        lines.append(f"{prefix} <b>{name}</b> — {count} ({percent}%)")
    return "\n".join(lines)

def format_skills_by_role(title: str, items: list) -> str:
    lines = [f"<b>{title}</b>"]
    for role, skills in items:
        emoji = ROLE_EMOJIS.get(role, "🔹")
        skills_str = " • ".join([f"<code>{name}</code> ({percent})" for name, percent in skills])
        lines.append(f"{emoji} <b>{role}</b> — {skills_str}")
    return "\n".join(lines)

# =========================
# Сборка сообщения (переработанная)
# =========================
def build_message(df: pd.DataFrame, days: int) -> str:
    total = len(df)
    
    # 1. Профили
    role_col = "canonical_role"
    if role_col not in df.columns:
        df[role_col] = df["title"].astype(str).apply(map_role_by_keywords)
    role_counts = df[role_col].value_counts()
    roles_pairs = list(role_counts.items())
    profiles_block = format_list_with_medals(f"{PROFILES_EMOJI} Профили", roles_pairs[:10], total)

    # 2. Общий топ навыков
    overall_pairs, posts_with_sk = top_overall(df, skills_col="skills", topn=10)
    skills_block = format_top_skills(f"{SKILLS_EMOJI} Топ навыков", overall_pairs, posts_with_sk)
    
    # 3. Навыки по направлениям
    by_role_pairs = top_by_role(df, skills_col="skills", role_col=role_col, per_role_top=3)
    by_role_block = format_skills_by_role(f"{BY_ROLE_EMOJI} По направлениям (топ-3 навыка)", by_role_pairs)

    # 4. Компании
    companies_pairs = top_companies(df, topn=10)
    companies_block = format_list_with_medals(f"{COMPANIES_EMOJI} Топ компаний", companies_pairs, total)

    # 5. Сборка всего сообщения
    parts = [
        format_header(total, days),
        profiles_block,
        skills_block,
        by_role_block,
        companies_block,
        "#вакансии #skills #аналитика"
    ]
    return SEPARATOR.join(parts)

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate and send weekly Telegram report.")
    parser.add_argument(
        "--days",
        type=int,
        default=9999, # По умолчанию берем все данные, как в оригинале
        help="Number of past days to include in the report. Default is all-time.",
    )
    args = parser.parse_args()

    start_date = date.today() - timedelta(days=args.days)
    
    try:
        conn = connect_to_db()
        # Добавляем фильтр по дате в запрос (предполагаем, что есть колонка created_at)
        query = "SELECT * FROM vacancies"
        df = pd.read_sql(query, conn, params=(start_date,))
        conn.close()

        if df.empty:
            print(f"No new vacancies found for the last {args.days} days.")
        else:
            msg = build_message(df, args.days)
            tg_send(msg)
            print("✅ Report sent.")
            
    except Exception as e:
        print(f"An error occurred: {e}")