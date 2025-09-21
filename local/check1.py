# weekly_report_minimal.py
import os
import re
import math
import requests
import psycopg2
import pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv

# ---- роли из проекта ----
from src.config import ROLE_KEYWORDS, OTHER_LABEL

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
    # режем на куски <=4096
    chunk, cur_len = [], 0
    for line in text.split("\n"):
        add = len(line) + 1
        if cur_len + add > TG_MAX:
            _post("\n".join(chunk), parse_mode)
            chunk, cur_len = [line], add
        else:
            chunk.append(line); cur_len += add
    if chunk:
        _post("\n".join(chunk), parse_mode)

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
# Нормализация и маппинг ролей
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
# Навыки — нормализация
# =========================
SKILL_SYNONYMS = {
    # облака
    "amazon web services":"aws","aws":"aws","microsoft azure":"azure","azure":"azure",
    "google cloud":"gcp","google cloud platform":"gcp","gcp":"gcp",
    # BI
    "power bi":"powerbi","ms power bi":"powerbi","microsoft power bi":"powerbi","power-bi":"powerbi",
    "qlik sense":"qlik","qlikview":"qlik","qlick":"qlik","looker studio":"looker",
    "google data studio":"looker","superset":"superset","apache superset":"superset",
    "power query":"powerquery",
    # платформы / DWH / оркестрация
    "clickhouse":"clickhouse","greenplum":"greenplum","snowflake":"snowflake","redshift":"redshift",
    "bigquery":"bigquery","data warehouse":"dwh","кхд":"dwh","dbt":"dbt",
    "apache airflow":"airflow","airflow":"airflow","apache kafka":"kafka","kafka":"kafka",
    "apache nifi":"nifi","nifi":"nifi","spark":"spark","apache spark":"spark","pyspark":"pyspark",
    # devops
    "docker":"docker","kubernetes":"kubernetes","k8s":"kubernetes","helm":"helm","terraform":"terraform",
    "ansible":"ansible","jenkins":"jenkins","zabbix":"zabbix","gitlab ci":"gitlabci",
    "ci/cd":"cicd","ci cd":"cicd","ci-cd":"cicd","ci–cd":"cicd","ci—cd":"cicd",
    "prometheus":"prometheus","grafana":"grafana","elk":"elk","efk":"elk","linux":"linux","openshift":"openshift",
    # языки/ML
    "python":"python","java":"java","c#":"csharp","csharp":"csharp",
    "javascript":"javascript","js":"javascript","sql":"sql","pandas":"pandas","numpy":"numpy",
    "scikit-learn":"sklearn","sklearn":"sklearn","pytorch":"pytorch","tensorflow":"tensorflow",
    "xgboost":"xgboost","lightgbm":"lightgbm","matplotlib":"matplotlib",
    # ai
    "nlp":"nlp","llm":"llm","rag":"rag","openai":"openai","langchain":"langchain",
    "computer vision":"computer vision","cv":"computer vision",
}

SQL_ALIASES = [
    r"\bms\s*sql\b", r"\bmssql\b", r"\bsql\s*server\b", r"\bt[\-\s]?sql\b", r"\btsql\b",
    r"\bpl[\-\s]?sql\b", r"\bplsql\b", r"\bpostgres(?:ql)?\b", r"\bpostgres\s*sql\b",
    r"\bmy\s*sql\b", r"\bmysql\b", r"\bmaria\s*db\b", r"\bmariadb\b",
]
SQL_ALIAS_COMPILED = [re.compile(p, re.I) for p in SQL_ALIASES]

# не даём попасть в общий топ
EXCLUDE_FROM_OVERALL = {"анализ данных", "data analysis", "devops"}  # devops как «скилл» — в бан

LANG_PAT  = re.compile(r"\b(английск\w*|english|russian|русск\w*|kazakh|казахск\w*)\b", re.I)
LEVEL_PAT = re.compile(r"\b(a1|a2|b1|b2|c1|c2|beginner|intermediate|advanced)\b", re.I)

SKILL_TRASH = {
    "не указано","none","nan","—","-","soft skills","аналитическое мышление","аналитика",
    "деловая переписка","работа в команде","умение анализировать","ms office","microsoft windows",
    "бизнес-анализ","system analysis","business analysis","бизнес-аналитика",
}

NBSP_RE = re.compile(r"[\u00A0\u2007\u202F]")
DASH_RE = re.compile(r"[–—]")
RANGE_DASHES = r"[–—-]"

def _clean_sep(x: str) -> str:
    return DASH_RE.sub("-", NBSP_RE.sub(" ", x))

def _strip_quotes(x: str) -> str:
    x = re.sub(r"[\"'’«»“”]", " ", x)
    return re.sub(r"\s+", " ", x).strip()

def _cut_lang(x: str) -> str:
    xl = x.lower()
    if LANG_PAT.search(xl):
        return re.split(RANGE_DASHES, xl)[0].strip()
    return x

def _norm_skill(s: str) -> str:
    x = _cut_lang(_strip_quotes(_clean_sep(s))).lower()
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
# Подсчёты
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
    roles_order = [
        "Data Analyst","Data Scientist","ML Engineer","Data Engineer",
        "MLOps","DevOps","BI Analyst","Business Analyst","System Analyst"
    ]
    for role in roles_order:
        g = s[s["role"] == role]
        if g.empty: continue
        denom = int(g["row_id"].nunique())
        vc = (g.groupby("skill")["row_id"].nunique().sort_values(ascending=False).head(per_role_top))
        pairs = [(k, f"{_pct(int(v), max(denom,1))}%") for k, v in vc.items()]
        out.append((role, pairs))
    return out

def top_companies(df, topn=10):
    col_guess = None
    for c in ["company","employer","company_name","company_title"]:
        if c in df.columns: col_guess = c; break
    if not col_guess: return []
    vc = (df[col_guess].astype(str).str.strip()
          .replace({"nan":"", "None":""})
          .pipe(lambda s: s[s!=""])
          .value_counts()
          .head(topn))
    total = len(df)
    return [(k, f"{int(v)} ({_pct(int(v), total)}%)") for k, v in vc.items()]

# =========================
# Форматирование секций (вертикально, под мобилу)
# =========================
def lines_list(pairs, denom=None):
    if not pairs: return "—"
    out = []
    for k, v in pairs:
        if isinstance(v, int) and denom is not None:
            out.append(f"• {k} — {v} ({_pct(v, denom)}%)")
        else:
            out.append(f"• {k} — {v}")
    return "\n".join(out)

def build_message(df: pd.DataFrame) -> str:
    total = len(df)
    # профили
    role_col = "canonical_role"
    if role_col not in df.columns:
        df[role_col] = df["title"].astype(str).apply(map_role_by_keywords)
    role_counts = df[role_col].value_counts()
    roles_pairs = [(k, f"{int(v)} ({_pct(int(v), total)}%)") for k, v in role_counts.items()]

    # общий топ навыков
    overall_pairs, posts_with_sk = top_overall(df, skills_col="skills", topn=10)

    # по направлениям (топ‑3 навыка)
    role_lines = []
    for role, pairs in top_by_role(df, skills_col="skills", role_col=role_col, per_role_top=3):
        triplet = " • ".join([f"{k} ({v})" for k, v in pairs])
        role_lines.append(f"• {role} — {triplet}")
    roles_block = "\n".join(role_lines) if role_lines else "—"

    # компании
    companies_pairs = top_companies(df, topn=10)

    # заголовок (фиксированный текст)
    header = (
        "<b>🔔 Аналитика с запуска</b>\n"
        "<i>4 месяца, 780 вакансий — и это только начало!\n"
        "Да, в данных могут быть погрешности, но теперь свежая статистика будет каждую неделю. "
        "Следите, чтобы не пропустить главные тренды рынка.</i>\n"
    )

    parts = [
        header,
        "<b>📌 Профили</b>",
        lines_list(roles_pairs),
        "",
        "<b>🛠 Топ навыков</b>",
        lines_list([(k, v) for k, v in overall_pairs], denom=posts_with_sk),
        "",
        "<b>📈 По направлениям (топ‑3 навыка)</b>",
        roles_block,
        "",
        "<b>🏢 Топ компаний</b>",
        lines_list(companies_pairs),
        "",
        f"Итого: <b>{total}</b>",
        "#вакансии #skills",
    ]
    return "\n".join(parts)

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    # период берём весь (аналитика «с запуска»); при желании можно ограничить датой
    conn = connect_to_db()
    df = pd.read_sql("SELECT * FROM vacancies", conn)
    conn.close()

    msg = build_message(df)
    tg_send(msg)
    print("✅ Report sent.")