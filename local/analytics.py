# weekly_report.py
import os
import re
import psycopg2
import pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv
import sys 
# --- добавляем корень проекта, чтобы импортировать src/*
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(project_root)
# ---- твои роли (берём из проекта) ----
from src.config import ROLE_KEYWORDS, OTHER_LABEL

load_dotenv()

# =========================
# Подключение к БД
# =========================
def connect_to_db():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("Url of DB is not set")
    return psycopg2.connect(db_url, sslmode="require")

# =========================
# Нормализация и маппинг ролей
# =========================
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

# =========================
# Блок по SKILLS (нормализация и аналитика)
# =========================
SKILL_SYNONYMS = {
    # облака
    "amazon web services": "aws", "aws": "aws",
    "microsoft azure": "azure", "azure": "azure",
    "google cloud": "gcp", "google cloud platform": "gcp", "gcp": "gcp",

    # BI / аналитика
    "power bi": "powerbi", "ms power bi": "powerbi", "microsoft power bi": "powerbi",
    "powerbi": "powerbi", "power-bi": "powerbi",
    "qlik sense": "qlik", "qlikview": "qlik", "qlick": "qlik", "qlik": "qlik",
    "looker studio": "looker", "google data studio": "looker", "looker": "looker",
    "metabase": "metabase", "apache superset": "superset", "superset": "superset",
    "tableau": "tableau",
    "power query": "powerquery", "powerquery": "powerquery", "dax": "dax",

    # офис
    "ms excel": "excel", "microsoft excel": "excel", "excel": "excel",

    # платформы/оркестрация/хранилища (без SQL — схлопаем отдельно)
    "clickhouse": "clickhouse", "greenplum": "greenplum",
    "snowflake": "snowflake", "redshift": "redshift", "bigquery": "bigquery",
    "data warehouse": "dwh", "кхд": "dwh", "dwh": "dwh",
    "dbt": "dbt",
    "apache airflow": "airflow", "airflow": "airflow",
    "apache kafka": "kafka", "kafka": "kafka",
    "apache nifi": "nifi", "nifi": "nifi",
    "spark": "spark", "apache spark": "spark", "pyspark": "pyspark",

    # devops
    "docker": "docker",
    "kubernetes": "kubernetes", "k8s": "kubernetes",
    "helm": "helm", "terraform": "terraform", "ansible": "ansible",
    "jenkins": "jenkins", "zabbix": "zabbix",
    "gitlab ci": "gitlabci", "gitlab-ci": "gitlabci", "gitlabci": "gitlabci",
    "ci/cd": "cicd", "ci cd": "cicd", "ci-cd": "cicd", "ci–cd": "cicd", "ci—cd": "cicd",
    "prometheus": "prometheus", "grafana": "grafana",
    "elk": "elk", "efk": "elk",
    "linux": "linux", "openshift": "openshift",

    # языки/ML‑стек
    "python": "python", "java": "java", "c#": "csharp", "csharp": "csharp",
    "javascript": "javascript", "js": "javascript",
    "sql": "sql",
    "pandas": "pandas", "numpy": "numpy", "scipy": "scipy",
    "scikit-learn": "sklearn", "sklearn": "sklearn",
    "pytorch": "pytorch", "tensorflow": "tensorflow",
    "xgboost": "xgboost", "lightgbm": "lightgbm",
    "matplotlib": "matplotlib",

    # llm/nlp/ai
    "nlp": "nlp", "llm": "llm", "rag": "rag", "openai": "openai", "langchain": "langchain",
    "computer vision": "computer vision", "cv": "computer vision",

    # трекинг/маркетинг
    "appmetrica": "appmetrica", "app metrica": "appmetrica",
    "adjust": "adjust", "adjust (fraud)": "adjust", "adjust.": "adjust", "adjust ": "adjust",
    "fraudscore": "fraudscore", "mytracker": "mytracker",
    "amplitude": "amplitude", "appsflyer": "appsflyer",
}

# любые варианты ниже превращаем в "sql"
SQL_ALIASES = [
    r"\bms\s*sql\b", r"\bmssql\b", r"\bsql\s*server\b",
    r"\bt[\-\s]?sql\b", r"\btsql\b",
    r"\bpl[\-\s]?sql\b", r"\bplsql\b",
    r"\bpostgres(?:ql)?\b", r"\bpostgres\s*sql\b",
    r"\bmy\s*sql\b", r"\bmysql\b",
    r"\bmaria\s*db\b", r"\bmariadb\b",
]
SQL_ALIAS_COMPILED = [re.compile(p, re.I) for p in SQL_ALIASES]

# «контейнеры», которые не показываем в общем топе (оставим для разрезов)
EXCLUDE_FROM_OVERALL = {"анализ данных", "data analysis", "devops"}

LANG_PAT  = re.compile(r"\b(английск\w*|english|русск\w*|russian|казахск\w*|kazakh)\b", re.IGNORECASE)
LEVEL_PAT = re.compile(r"\b(a1|a2|b1|b2|c1|c2)\b", re.IGNORECASE)

SKILL_TRASH = {
    "не указано","none","nan","—","-",
    "soft skills","коммуникабельность","ответственность","аналитическое мышление","аналитика",
    "работа с большим объемом информации","деловая переписка","умение работать в коллективе",
    "деловая коммуникация","пунктуальность","внимательность","точность и внимательность к деталям",
    "навыки продаж","работа в команде","умение анализировать","системное мышление","критическое мышление",
    "стратегическое планирование","управление командой","управление проектами","project management",
    "ms office","ms powerpoint","microsoft windows","навыки презентаций","подготовка презентаций",
    "бизнес-анализ","system analysis","системный анализ","business analysis","бизнес-аналитика",
    "начальный","средний","продвинутый","средне-продвинутый",
    "beginner","elementary","intermediate","upper-intermediate","advanced",
}

NBSP_RE = re.compile(r"[\u00A0\u2007\u202F]")
DASH_RE = re.compile(r"[–—]")
RANGE_DASHES = r"[–—-]"

def _clean_separators(x: str) -> str:
    x = NBSP_RE.sub(" ", x)
    x = DASH_RE.sub("-", x)
    return x

def _strip_quotes_spaces(x: str) -> str:
    x = re.sub(r"[\"'’«»“”]", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

def _cut_language_levels(x: str) -> str:
    x_low = x.lower()
    if LANG_PAT.search(x_low):
        x_low = re.split(RANGE_DASHES, x_low)[0].strip()
        return x_low
    return x

def _norm_token(s: str) -> str:
    x = _clean_separators(s)
    x = _strip_quotes_spaces(x)
    x = _cut_language_levels(x)
    x = x.lower()
    x = x.replace(" / ", "/")
    x = re.sub(r"\s*/\s*", "/", x)
    x = re.sub(r"\s*-\s*", "-", x)
    x = re.sub(r"\s+", " ", x).strip()

    # SQL: если любой алиас совпал — сворачиваем в "sql"
    if any(p.search(x) for p in SQL_ALIAS_COMPILED):
        return "sql"

    # обычные синонимы
    x = SKILL_SYNONYMS.get(x, x)
    return x

def _is_trash(x: str) -> bool:
    if not x or x in SKILL_TRASH: return True
    if LANG_PAT.search(x): return True
    if LEVEL_PAT.search(x): return True
    if len(x) <= 1 or x.isdigit(): return True
    words = x.split()
    if len(words) > 3 and x not in {
        "machine learning", "deep learning", "data analysis", "data mining",
        "data warehouse", "site reliability engineer", "feature store",
        "business intelligence", "rest api", "soap api", "user story",
        "a/b тесты", "а/b тесты", "а/а тесты",
        "модель данных", "моделирование бизнес процессов",
    }:
        return True
    return False

def explode_skills(df: pd.DataFrame, skills_col: str = "skills") -> pd.DataFrame:
    rows = []
    if skills_col not in df.columns or df[skills_col].isna().all():
        return pd.DataFrame(columns=["row_id","skill"])

    for rid, raw in enumerate(df[skills_col].astype(str).tolist()):
        if not raw or raw.lower().strip() in {"не указано","nan","none",""}:
            continue
        parts = re.split(r"[,\;\|•\n\r]+", raw)
        seen = set()

        for p in parts:
            # если "SQL — Power BI" -> делим по тире на 2 токена
            if re.search(rf"\s{RANGE_DASHES}\s", p):
                for sub in re.split(rf"\s{RANGE_DASHES}\s", p):
                    tok = _norm_token(sub)
                    if not _is_trash(tok) and tok not in seen:
                        rows.append({"row_id": rid, "skill": tok})
                        seen.add(tok)
                continue

            tok = _norm_token(p)
            if _is_trash(tok):
                continue
            if tok not in seen:
                rows.append({"row_id": rid, "skill": tok})
                seen.add(tok)

    return pd.DataFrame(rows)

def _percent(n, total):
    return 0 if not total else round(n / total * 100)

def _format_items(df_items, total_posts):
    # пример: "sql — 57 (25%) • python — 49 (21%)"
    return " • ".join([f"{r.skill} — {int(r.count)} ({_percent(r.count, total_posts)}%)"
                       for r in df_items.itertuples(index=False)])

def top_overall(df: pd.DataFrame, skills_col="skills", topn=12) -> pd.DataFrame:
    s = explode_skills(df, skills_col)
    if s.empty:
        return pd.DataFrame(columns=["skill","count"])
    # считаем по числу вакансий, где встречается навык (а не по общим упоминаниям)
    vc = (s.groupby("skill")["row_id"].nunique().rename("count").reset_index())
    vc = vc[~vc["skill"].isin(EXCLUDE_FROM_OVERALL)]
    vc = vc.sort_values("count", ascending=False).head(topn)
    return vc

def top_by_role(df: pd.DataFrame, skills_col="skills", role_col="canonical_role", topn=6, roles_order=None):
    if role_col not in df.columns:
        return []
    s = explode_skills(df, skills_col)
    if s.empty:
        return []
    s = s.merge(
        df[[role_col]].reset_index(drop=True).rename(columns={role_col: "role"}),
        left_on="row_id", right_index=True, how="left"
    )

    out = []
    if not roles_order:
        roles_order = [
            "Data Analyst","Data Scientist","ML Engineer","Data Engineer",
            "MLOps","DevOps","BI Analyst","Business Analyst","System Analyst"
        ]
    for role in roles_order:
        g = s[s["role"] == role]
        if g.empty:
            continue
        vc = (g.groupby("skill")["row_id"].nunique()
                .sort_values(ascending=False)
                .head(topn)
                .rename("count")
                .reset_index())
        out.append((role, vc, int(g["row_id"].nunique())))  # добавим покрытие по роли
    return out

def universal_skills_by_roles(df: pd.DataFrame, skills_col="skills", role_col="canonical_role", min_roles=3):
    if role_col not in df.columns:
        return pd.DataFrame(columns=["skill","roles"])
    s = explode_skills(df, skills_col)
    if s.empty:
        return pd.DataFrame(columns=["skill","roles"])
    s = s.merge(
        df[[role_col]].reset_index(drop=True).rename(columns={role_col: "role"}),
        left_on="row_id", right_index=True, how="left"
    ).dropna(subset=["role"])

    role_sets = s.groupby("skill")["role"].apply(lambda x: set(x.dropna()))
    out = (role_sets[role_sets.apply(len) >= min_roles]
           .reset_index().rename(columns={"role":"roles"}))
    out["roles_count"] = out["roles"].apply(len)
    out = out.sort_values(["roles_count","skill"], ascending=[False,True]).reset_index(drop=True)
    return out

def week_trend(df: pd.DataFrame, skills_col="skills", date_col="published_at", topn=6):
    if date_col not in df.columns:
        return None

    today = pd.Timestamp(date.today())
    start_this = today - pd.Timedelta(days=6)      # последние 7 дней
    start_prev = start_this - pd.Timedelta(days=7) # предыдущая неделя
    end_prev   = start_this - pd.Timedelta(days=1)

    dfx = df.copy()
    dfx[date_col] = pd.to_datetime(dfx[date_col], errors="coerce")
    cur = dfx[(dfx[date_col] >= start_this) & (dfx[date_col] <= today)]
    prv = dfx[(dfx[date_col] >= start_prev) & (dfx[date_col] <= end_prev)]
    if cur.empty or prv.empty:
        return None

    s_cur = explode_skills(cur, skills_col)
    s_prv = explode_skills(prv, skills_col)
    if s_cur.empty or s_prv.empty:
        return None

    cur_vc = s_cur.groupby("skill")["row_id"].nunique().rename("cur")
    prv_vc = s_prv.groupby("skill")["row_id"].nunique().rename("prv")
    merged = (pd.concat([cur_vc, prv_vc], axis=1).fillna(0).astype(int))
    merged["delta"] = merged["cur"] - merged["prv"]

    gainers = (merged.sort_values("delta", ascending=False)
                      .head(topn)
                      .reset_index().rename(columns={"index":"skill"}))
    losers  = (merged.sort_values("delta", ascending=True)
                      .head(topn)
                      .reset_index().rename(columns={"index":"skill"}))
    return {"gainers": gainers, "losers": losers}

# =========================
# ЕДИНЫЙ ОТЧЁТ (Профили + Скиллы)
# =========================
def make_full_report(df: pd.DataFrame,
                     skills_col: str = "skills",
                     role_col: str = "canonical_role",
                     date_col: str = "published_at",
                     top_overall_n: int = 12,
                     top_role_n: int = 6,
                     min_roles_universal: int = 3) -> str:
    # ---- Профили (доли по ролям)
    total = int(df.shape[0])
    counts = (
        df[role_col]
        .value_counts(dropna=False)
        .rename_axis("role")
        .reset_index(name="count")
        .sort_values(["count", "role"], ascending=[False, True])
    )
    counts["percent"] = (counts["count"] / total * 100).round(1) if total else 0.0
    roles_lines = [f"- {row['role']} — {row['percent']}%" for _, row in counts.iterrows()]
    profiles_block = "📊 Профили (неделя)\n" + ("\n".join(roles_lines) if roles_lines else "—")

    # ---- Скиллы (общий топ)
    overall = top_overall(df, skills_col=skills_col, topn=top_overall_n)
    s_exp = explode_skills(df, skills_col)
    posts_with_sk = int(s_exp.row_id.nunique()) if not s_exp.empty else 0
    share = f"{(posts_with_sk/total*100):.0f}%" if total else "0%"
    overall_txt = _format_items(overall, max(posts_with_sk, 1)) if not overall.empty else "—"
    skills_top_block = f"🏆 Топ навыков (неделя)\n{overall_txt}"

    # ---- По ролям
    role_blocks = []
    cover_lines = []
    ROLES_EMOJI = {
        "Data Analyst":"📈","Data Scientist":"🧪","ML Engineer":"🤖",
        "Data Engineer":"🛠","MLOps":"⚙️","DevOps":"🧰",
        "BI Analyst":"📊","Business Analyst":"💼","System Analyst":"🧩",
    }
    for role, tab, role_posts_with_sk in top_by_role(df, skills_col=skills_col, role_col=role_col, topn=top_role_n):
        emoji = ROLES_EMOJI.get(role, "•")
        line = " • ".join([f"{r.skill} — {int(r.count)} ({_percent(r.count, max(role_posts_with_sk,1))}%)"
                           for r in tab.itertuples(index=False)])
        role_blocks.append(f"{emoji} {role}: {line}")
        role_total = int((df[role_col] == role).sum())
        cover_lines.append(f"{role}: {role_posts_with_sk}/{role_total} ({_percent(role_posts_with_sk, max(role_total,1))}%)")
    roles_txt = "\n".join(role_blocks) if role_blocks else "—"
    covers_txt = " • ".join(cover_lines) if cover_lines else "—"
    skills_roles_block = f"📌 По направлениям\n{roles_txt}\n\n🧩 Покрытие по ролям (со скиллами / все)\n{covers_txt}"

    # ---- Универсальные навыки
    uni = universal_skills_by_roles(df, skills_col=skills_col, role_col=role_col, min_roles=min_roles_universal)
    uni_txt = " • ".join([f"{r.skill} ({len(r.roles)})" for r in uni.itertuples(index=False)]) if not uni.empty else "—"
    skills_uni_block = f"🧭 Универсальные навыки (в ≥{min_roles_universal} ролях)\n{uni_txt}"

    # ---- Тренды недели (если есть дата)
    trend_txt = ""
    tr = week_trend(df, skills_col=skills_col, date_col=date_col, topn=6)
    if tr:
        up_line = " • ".join([f"{r.skill} (▲ {int(r.delta)})" for r in tr["gainers"].itertuples(index=False)])
        dn_line = " • ".join([f"{r.skill} (▼ {abs(int(r.delta))})" for r in tr["losers"].itertuples(index=False)])
        trend_txt = f"\n📈 Растут: {up_line}\n📉 Падают: {dn_line}"

    # ---- Итог и сводка
    header = "Еженедельный отчёт по вакансиям\n"
    footer = f"\nВсего вакансий: {total} • Со скиллами: {posts_with_sk} ({share})\n#вакансии #skills"
    return (
        header
        + profiles_block + "\n\n"
        + skills_top_block + "\n\n"
        + skills_roles_block + "\n\n"
        + skills_uni_block
        + trend_txt
        + footer
    )

# =========================
# MAIN
# =========================
# =========================
# MAIN (Markdown-отчёт)
# =========================
if __name__ == "__main__":
    today = date.today()
    week = today - timedelta(days=7)
    print(f"from {week} to {today}")

    conn = connect_to_db()
    query = "SELECT * FROM vacancies"
    df = pd.read_sql(query, conn)
    conn.close()

    # --- добавляем роли
    df["canonical_role"] = df["title"].astype(str).apply(map_role_by_keywords)

    # --- генерируем отчёт (сырой текст)
    raw_text = make_full_report(
        df,
        skills_col="skills",
        role_col="canonical_role",
        date_col="published_at",
        top_overall_n=12,
        top_role_n=6,
        min_roles_universal=3,
    )

    # =========================
    # КРАСИВОЕ Markdown-оформление для Telegram / Notion
    # =========================
    md_report = f"""\
📅 **Еженедельный отчёт по вакансиям**  
_({week.strftime('%d.%m.%Y')} — {today.strftime('%d.%m.%Y')})_

---

### 📊 **Профили**
{raw_text.split("🏆")[0].split("неделя")[1].strip()}

---

### 🏆 **Топ навыков**
{raw_text.split("🏆 Топ навыков (неделя)")[1].split("📌")[0].strip()}

---

### 📌 **По направлениям**
{raw_text.split("📌 По направлениям")[1].split("🧭")[0].strip()}

---

### 🧭 **Универсальные навыки (в ≥3 ролях)**
{raw_text.split("🧭 Универсальные навыки (в ≥3 ролях)")[1].split("📈")[0].strip()}

---

### 📈 **Тренды недели**
{raw_text.split("📈")[1].strip() if "📈" in raw_text else "Нет данных"}

---

**{raw_text.split('Всего вакансий:')[1].strip()}**
"""

    # --- сохраняем в файл
    output_path = os.path.join(current_dir, "weekly_report.md")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(md_report)

    print("\n✅ Markdown-отчёт сохранён:")
    print(output_path)
    print("\n--- ПРЕВЬЮ ---\n")
    print(md_report)
