import os, requests, json

GEMINI_API_KEY = os.getenv("GEM_API_TOKEN")
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

HEADERS = {"Content-Type": "application/json"}


def gemini_api_call(prompt: str) -> str:
    resp = requests.post(
        f"{GEMINI_API_URL}?key={GEMINI_API_KEY}",
        headers=HEADERS,
        json={"contents": [{"parts": [{"text": prompt}]}]},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["candidates"][0]["content"]["parts"][0]["text"]


# ——— LLM‑фильтр ———
# --- LLM‑фильтр ---
FILTER_PROMPT_TEMPLATE = """
Ты ассистент, который определяет релевантность вакансии по описанию и названию.

Профессия считается релевантной, если она относится к одной из следующих областей:
- Data Science
- Machine Learning
- Artificial Intelligence (AI)
- Анализ данных (Data Analytics)
- Business Intelligence (BI)
- DevOps

❗ Не учитывай вакансии из других сфер, даже если в тексте встречаются слова вроде “data” или “модель”.

Профессия: "{title}"
Описание: "{description}"

Ответь строго одним словом: yes или no.
"""



def filter_vacancy_llm(title: str, description: str) -> bool:
    prompt = FILTER_PROMPT_TEMPLATE.format(title=title, description=description)
    try:
        return "yes" in gemini_api_call(prompt).lower()
    except Exception:
        return False


# ——— LLM‑summary ———
SUMMARY_PROMPT_TEMPLATE = """
Разбей текст описания вакансии на три блока:
1. О компании
2. Обязанности
3. Требования

Если блока нет — напиши "Не указано".
Верни ЧИСТЫЙ JSON с ключами: about_company, responsibilities, requirements.

Описание:
{description}
"""


def summarize_description_llm(description: str) -> dict:
    prompt = SUMMARY_PROMPT_TEMPLATE.format(description=description)
    try:
        raw = gemini_api_call(prompt).strip()
        data = json.loads(raw) if raw.startswith("{") else {}
    except Exception:
        data = {}

    # fallback
    return {
        "about_company": data.get("about_company", "Не указано"),
        "responsibilities": data.get("responsibilities", "Не указано"),
        "requirements": data.get("requirements", "Не указано"),
    }
