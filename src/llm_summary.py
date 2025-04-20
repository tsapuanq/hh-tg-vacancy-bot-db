# ✅ llm_summary.py (src/llm_summary.py)
import os
import requests

GEMINI_API_KEY = os.getenv("GEM_API_TOKEN")
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent"

headers = {
    "Content-Type": "application/json"
}


def gemini_api_call(prompt: str) -> str:
    payload = {
        "contents": [{"parts": [{"text": prompt}]}]
    }
    response = requests.post(
        f"{GEMINI_API_URL}?key={GEMINI_API_KEY}", 
        headers=headers, 
        json=payload
    )
    response.raise_for_status()
    return response.json()['candidates'][0]['content']['parts'][0]['text']


# 🔍 ФИЛЬТРАЦИЯ вакансий
FILTER_PROMPT_TEMPLATE = """
Ты ассистент, который определяет релевантность вакансии по описанию и названию.

Профессия считается релевантной, если она относится к одной из следующих областей:
- Data Science
- Machine Learning
- Artificial Intelligence (AI)
- Анализ данных (Data Analytics)
- Business Intelligence (BI)
- DevOps

НЕ учитывай вакансии из других сфер, даже если в описании есть слова вроде "data" или "модель".

Профессия: "{title}"
Описание: "{description}"

Ответь строго одним словом: yes или no.
"""

def filter_vacancy_llm(title: str, description: str) -> bool:
    prompt = FILTER_PROMPT_TEMPLATE.format(title=title, description=description)
    try:
        result = gemini_api_call(prompt).lower()
        return "yes" in result
    except Exception as e:
        return False


# 📄 САММАРИ по описанию
SUMMARY_PROMPT_TEMPLATE = """
Разбей текст описания вакансии на три смысловых блока:
1. О компании
2. Обязанности
3. Требования

Если какой-то блок не указан, напиши "Не указано". Ответ верни строго в JSON формате с ключами:
"about_company", "responsibilities", "requirements".

Описание:
{description}
"""


def summarize_description_llm(description: str) -> dict:
    prompt = SUMMARY_PROMPT_TEMPLATE.format(description=description)
    try:
        response = gemini_api_call(prompt)
        return eval(response) if response.strip().startswith("{") else {
            "about_company": "Не указано",
            "responsibilities": "Не указано",
            "requirements": "Не указано",
        }
    except:
        return {
            "about_company": "Не указано",
            "responsibilities": "Не указано",
            "requirements": "Не указано",
        }

