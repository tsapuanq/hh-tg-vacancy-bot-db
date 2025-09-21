# weekly_report.py — компактный TG-отчёт (навыки + сводка)

import os
import re
import math
import requests
import pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv
from urllib.parse import quote_plus
from collections import Counter

# SQLAlchemy (надёжнее, чем прямой psycopg2)
from sqlalchemy import create_engine, text

# роли из проекта
from src.config import ROLE_KEYWORDS, OTHER_LABEL

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
TG_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage" if TELEGRAM_BOT_TOKEN else None
TG_MAX = 4096

def tg_send(text: str, parse_mode: str = "HTML"):
    """Бережно режем на куски <=4096 и шлём."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARN] TG creds not set; printing message:\n", text)
        return
    chunks, cur, ln = [], [], 0
    for line in text.split("\n"):
        add = len(line) + 1
        if ln + add > TG_MAX:
            chunks.append("\n".join(cur)); cur, ln = [line], add
        else:
            cur.append(line); ln += add
    if cur: chunks.append("\n".join(cur))
    for ch in chunks:
        r = requests.post(
            TG_API,
            data={"chat_id": TELEGRAM_CHAT_ID, "text": ch, "parse_mode": parse_mode, "disable_web_page_preview": True},
            timeout=20,
        )
        if not r.ok:
            print(f"[TG ERROR] {r.status_code}: {r.text}")

# =========================
# DB engine
# =========================
def get_engine():
    url = os.getenv("DATABASE_URL")
    if url:
        if "sslmode=" not in url:
            url += ("&" if "?" in url else "?") + "sslmode=require"
        return create_engine(url, pool_pre_ping=True)
    host = os.getenv("PGHOST") or "aws-0-ap-northeast-1.pooler.supabase.com"
    port = os.getenv("PGPORT", "6543")
    db   = os.getenv("PGDATABASE", "postgres")
    user = os.getenv("PGUSER", "postgres")
    pwd  = os.getenv("PGPASSWORD")
    if not pwd:
        raise RuntimeError("DATABASE_URL or PGPASSWORD must be set")
    uri = f"postgresql://{user}:{quote_plus(pwd)}@{host}:{port}/{db}?sslmode=require"
    return create_engine(uri, pool_pre_ping=True)

def fetch_vacancies(engine, dfrom=None, dto=None) -> pd.DataFrame:
    if dfrom and dto:
        q = text("SELECT * FROM vacancies WHERE published_at BETWEEN :dfrom AND :dto")
        return pd.read_sql(q, engine, params={"dfrom": dfrom, "dto": dto})
    return pd.read_sql(text("SELECT * FROM vacancies"), engine)

# =========================
# Роли
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

def map_role(title: str) -> str:
    t = _norm_title(title)
    for role, pats in ROLE_PATTERNS:
        if any(p.search(t) for p in pats):
            return role
    return OTHER_LABEL

# =========================
# Навыки: нормализация
# =========================
SKILL_SYNONYMS = {
    # облака
    "amazon web services": "aws", "aws": "aws",
    "microsoft azure": "azure", "azure": "azure",
    "google cloud": "gcp", "google cloud platform": "gcp", "gcp": "gcp",
    # BI
    "power bi": "powerbi","ms power bi":"powerbi","microsoft power bi":"powerbi","power-bi":"powerbi",
    "qlik sense":"qlik","qlikview":"qlik","qlick":"qlik","qlik":"qlik",
    "looker studio":"looker","google data studio":"looker","looker":"looker",
    "metabase":"metabase","apache superset":"superset","superset":"superset","tableau":"tableau",
    "power query":"powerquery","powerquery":"powerquery","dax":"dax",
    # офис
    "ms excel":"excel","microsoft excel":"excel","excel":"excel",
    # платформы
    "clickhouse":"clickhouse","greenplum":"greenplum","snowflake":"snowflake","redshift":"redshift",
    "bigquery":"bigquery","data warehouse":"dwh","кхд":"dwh","dwh":"dwh","dbt":"dbt",
    "apache airflow":"airflow","airflow":"airflow","apache kafka":"kafka","kafka":"kafka",
    "apache nifi":"nifi","nifi":"nifi","spark":"spark","apache spark":"spark","pyspark":"pyspark",
    # devops
    "docker":"docker","kubernetes":"kubernetes","k8s":"kubernetes","helm":"helm","terraform":"terraform",
    "ansible":"ansible","jenkins":"jenkins","zabbix":"zabbix","gitlab ci":"gitlabci","gitlab-ci":"gitlabci",
    "ci/cd":"cicd","ci cd":"cicd","ci-cd":"cicd","ci–cd":"cicd","ci—cd":"cicd",
    "prometheus":"prometheus","grafana":"grafana","elk":"elk","efk":"elk","linux":"linux","openshift":"openshift",
    # языки/ML
    "python":"python","java":"java","c#":"csharp","csharp":"csharp","javascript":"javascript","js":"javascript",
    "sql":"sql","pandas":"pandas","numpy":"numpy","scipy":"scipy",
    "scikit-learn":"sklearn","sklearn":"sklearn","pytorch":"pytorch","tensorflow":"tensorflow",
    "xgboost":"xgboost","lightgbm":"lightgbm","matplotlib":"matplotlib",
    # llm/nlp/ai
    "nlp":"nlp","llm":"llm","rag":"rag","openai":"openai","langchain":"langchain","computer vision":"computer vision","cv":"computer vision",
}

SQL_ALIASES = [
    r"\bms\s*sql\b", r"\bmssql\b", r"\bsql\s*server\b",
    r"\bt[\-\s]?sql\b", r"\btsql\b", r"\bpl[\-\s]?sql\b", r"\bplsql\b",
    r"\bpostgres(?:ql)?\b", r"\bpostgres\s*sql\b", r"\bmy\s*sql\b", r"\bmysql\b",
    r"\bmaria\s*db\b", r"\bmariadb\b",
]
SQL_ALIAS_COMPILED = [re.compile(p, re.I) for p in SQL_ALIASES]

EXCLUDE_FROM_OVERALL = {"анализ данных", "data analysis", "devops"}

LANG_PAT  = re.compile(r"\b(английск\w*|english|русск\w*|russian|казахск\w*|kazakh)\b", re.I)
LEVEL_PAT = re.compile(r"\b(a1|a2|b1|b2|c1|c2)\b", re.I)

SKILL_TRASH = {
    "не указано","none","nan","—","-","soft skills","коммуникабельность","ответственность","аналитическое мышление",
    "аналитика","деловая переписка","умение работать в коллективе","деловая коммуникация","пунктуальность",
    "внимательность","точность и внимательность к деталям","навыки продаж","работа в команде","умение анализировать",
    "системное мышление","критическое мышление","стратегическое планирование","управление командой",
    "управление проектами","project management","ms office","ms powerpoint","microsoft windows","навыки презентаций",
    "подготовка презентаций","бизнес-анализ","system analysis","системный анализ","business analysis","бизнес-аналитика",
    "начальный","средний","продвинутый","средне-продвинутый","beginner","elementary","intermediate","upper-intermediate","advanced",
}

NBSP_RE = re.compile(r"[\u00A0\u2007\u202F]")
DASH_RE = re.compile(r"[–—]")
RANGE_DASHES = r"[–—-]"

def _clean_sep(x: str) -> str:
    return DASH_RE.sub("-", NBSP_RE.sub(" ", x))

def _strip_quotes(x: str) -> str:
    x = re.sub(r"[\"'’«»“”]", " ", x)
    return re.sub(r"\s+", " ", x).strip()

def _cut_lang_levels(x: str) -> str:
    xl = x.lower()
    if LANG_PAT.search(xl):
        return re.split(RANGE_DASHES, xl)[0].strip()
    return x

def _norm_skill(s: str) -> str:
    x = _cut_lang_levels(_strip_quotes(_clean_sep(s))).lower()
    x = re.sub(r"\s*/\s*", "/", x)
    x = re.sub(r"\s*-\s*", "-", x)
    x = re.sub(r"\s+", " ", x).strip()
    if any(p.search(x) for p in SQL_ALIAS_COMPILED):
        return "sql"
    return SKILL_SYNONYMS.get(x, x)

def _is_trash(x: str) -> bool:
    if not x or x in SKILL_TRASH: return True
    if LANG_PAT.search(x) or LEVEL_PAT.search(x): return True
    if len(x) <= 1 or x.isdigit(): return True
    # длинные фразы выкидываем, кроме whitelisted
    WL = {
        "machine learning","deep learning","data analysis","data mining",
        "business intelligence","data warehouse","rest api","soap api","user story",
        "моделирование бизнес процессов","модель данных","a/b тесты","а/b тесты","а/а тесты"
    }
    if len(x.split()) > 3 and x not in WL: return True
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
            if re.search(rf"\s{RANGE_DASHES}\s", p):
                subs = re.split(rf"\s{RANGE_DASHES}\s", p)
            else:
                subs = [p]
            for sub in subs:
                tok = _norm_skill(sub)
                if _is_trash(tok) or tok in seen: continue
                rows.append({"row_id": rid, "skill": tok}); seen.add(tok)
    return pd.DataFrame(rows)

# =========================
# Вспомогательное форматирование
# =========================
ROLE_EMOJI = {
    "Data Analyst":"📉","Data Scientist":"🧪","ML Engineer":"🤖",
    "Data Engineer":"🛠","MLOps":"⚙️","DevOps":"🧰",
    "BI Analyst":"📊","Business Analyst":"💼","System Analyst":"🧩",
}

def _pct(n, d): return 0 if not d else round(n/d*100)

def _grid(items, cols=2, sep="  │  "):
    """
    items: list[tuple(name:str, count:int)]
    Возвращает компактный моноширинный грид в <code>...</code>.
    """
    if not items: return "<code>—</code>"
    # вычисляем ширины колонок
    col_heights = math.ceil(len(items)/cols)
    columns = [items[i*col_heights:(i+1)*col_heights] for i in range(cols)]
    name_w = max((len(n) for n,_ in items), default=1)
    cnt_w  = max((len(str(c)) for _,c in items), default=1)

    def fmt_cell(n, c):
        return f"{n.ljust(name_w)} {str(c).rjust(cnt_w)}"

    lines = []
    for r in range(col_heights):
        cells = []
        for col in columns:
            if r < len(col):
                n,c = col[r]; cells.append(fmt_cell(n,c))
        lines.append(sep.join(cells))
    return "<code>" + "\n".join(lines) + "</code>"

def _inline_list(pairs, max_items=6):
    return " • ".join([f"{k} {v}" for k,v in pairs[:max_items]]) if pairs else "—"

# =========================
# Топы
# =========================
def top_overall(df, skills_col="skills", topn=12):
    s = explode_skills(df, skills_col)
    if s.empty: return []
    vc = (s.groupby("skill")["row_id"].nunique().rename("cnt").reset_index())
    vc = vc[~vc["skill"].isin(EXCLUDE_FROM_OVERALL)].sort_values("cnt", ascending=False).head(topn)
    return [(r["skill"], int(r["cnt"])) for _, r in vc.iterrows()]

def top_by_role(df, skills_col="skills", role_col="canonical_role", topn=4, roles_order=None):
    if role_col not in df.columns: return []
    s = explode_skills(df, skills_col)
    if s.empty: return []
    s = s.merge(df[[role_col]].reset_index(drop=True).rename(columns={role_col:"role"}),
                left_on="row_id", right_index=True, how="left")
    if not roles_order:
        roles_order = list(ROLE_EMOJI.keys())
    out = []
    for role in roles_order:
        g = s[s["role"]==role]
        if g.empty: continue
        vc = (g.groupby("skill")["row_id"].nunique().rename("cnt").sort_values(ascending=False).head(topn))
        pairs = [(k,int(v)) for k,v in vc.items()]
        out.append((role, pairs, int(g["row_id"].nunique())))
    return out

# =========================
# Блок «навыки»
# =========================
def build_skills_block(df, dfrom, dto, skills_col="skills", role_col="canonical_role"):
    total = len(df)
    s = explode_skills(df, skills_col)
    with_sk = int(s.row_id.nunique()) if not s.empty else 0

    overall = top_overall(df, skills_col=skills_col, topn=12)
    grid = _grid(overall, cols=2)

    role_lines = []
    for role, pairs, denom in top_by_role(df, skills_col=skills_col, role_col=role_col, topn=4):
        emoji = ROLE_EMOJI.get(role, "•")
        line = _inline_list([ (k, f"{v} ({_pct(v, max(denom,1))}%)") for k,v in pairs ], max_items=4)
        role_lines.append(f"{emoji} <b>{role}</b>: {line}")
    roles_txt = "\n".join(role_lines) if role_lines else "—"

    return (
        f"🏆 <b>Топ навыков (неделя)</b>\n{grid}\n\n"
        f"📌 <b>По направлениям</b>\n{roles_txt}\n\n"
        f"Всего: <b>{total}</b> • Со скиллами: <b>{with_sk}</b> (<b>{_pct(with_sk, max(total,1))}%</b>)\n"
        "#skills #вакансии"
    )

# =========================
# Еженедельная сводка (доп. аналитика)
# =========================
def _vc(df, col, topn=None, normalize=False):
    if col not in df.columns: return []
    s = df[col].astype(str).str.strip()
    if normalize:
        s = s.str.lower().str.replace("ё","е")
    cnt = Counter([x for x in s if x and x.lower() not in {"nan","none","не указано"}])
    items = cnt.most_common(topn)
    return items

def build_summary_block(df, dfrom, dto, role_col="canonical_role"):
    total = len(df)
    period = f"{dfrom.strftime('%d.%m')}–{dto.strftime('%d.%m')}"

    # профили
    if role_col not in df.columns:
        roles = []
    else:
        roles = df[role_col].value_counts().items()
        roles = [(k, int(v)) for k,v in roles]

    # опыт
    exp = _vc(df, "experience")
    # локации
    loc = _vc(df, "city", topn=5)
    # зарплата указана?
    salary_cols = [c for c in df.columns if c.lower() in {"salary", "salary_from", "salary_to"}]
    with_salary = 0
    if salary_cols:
        if "salary" in df.columns:
            with_salary = int(df["salary"].notna().sum())
        else:
            with_salary = int((df.get("salary_from").notna() | df.get("salary_to").notna()).sum())
    # формат работы/часы/компании/график/занятость
    fmt = _vc(df, "work_format", topn=3)
    hours = _vc(df, "working_hours", topn=3)
    companies = _vc(df, "company", topn=5)
    schedule = _vc(df, "schedule", topn=3)
    emp_type = _vc(df, "employment_type", topn=3)

    def bullet(pairs, title):
        if not pairs: return ""
        line = "\n".join([f"• {k}: {v}" for k,v in pairs])
        return f"\n{title}\n{line}\n"

    roles_txt = "\n".join([f"• {k} – {v} ({_pct(v,total)} %)" for k,v in roles]) if roles else "—"

    salary_line = ""
    if total:
        salary_line = f"\n💰 <b>Указание зарплаты:</b>\n• Зарплата указана в {with_salary} из {total} вакансий ({round(with_salary/total*100,1)}%)\n"

    parts = [
        f"🔔 <b>Еженедельный отчёт по вакансиям</b> ({period})",
        f"\nНовых вакансий за период: <b>{total}</b>\n",
        f"👥 <b>Профили:</b>\n{roles_txt}\n",
        bullet(exp, "📊 <b>Опыт:</b>"),
        bullet(loc, "🌍 <b>Локации (Топ 5):</b>"),
        salary_line,
        bullet(fmt, "🏢 <b>Формат работы (Топ 3):</b>"),
        bullet(hours, "🕒 <b>Рабочие часы (Топ 3):</b>"),
        bullet(companies, "🏢 <b>Топ компаний (Топ 5):</b>"),
        bullet(schedule, "🗓️ <b>График (Топ 3):</b>"),
        bullet(emp_type, "⌚️ <b>Тип занятости (Топ 3):</b>"),
        "#вакансии",
    ]
    return "\n".join([p for p in parts if p])

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    today = date.today()
    week_ago = today - timedelta(days=7)
    print(f"range: {week_ago} .. {today}")

    engine = get_engine()
    # рекомендуемый режим — брать неделю
    df = fetch_vacancies(engine, week_ago, today)
    engine.dispose()

    if "canonical_role" not in df.columns:
        df["canonical_role"] = df["title"].astype(str).apply(map_role)

    # 1) навыки
    skills_msg = build_skills_block(df, week_ago, today, skills_col="skills", role_col="canonical_role")
    tg_send(skills_msg)

    # 2) сводка (только реальные доступные блоки)
    summary_msg = build_summary_block(df, week_ago, today, role_col="canonical_role")
    tg_send(summary_msg)

    print("✅ done.")