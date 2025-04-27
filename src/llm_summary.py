import os
import time
import json
import re
import requests
import logging

# === Конфигурация ===
GEMINI_API_KEY = os.getenv("GEM_API_TOKEN")
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
HEADERS = {"Content-Type": "application/json"}

# === Вызов Gemini API с повторными попытками ===
def gemini_api_call(prompt: str, timeout: int = 30, retries: int = 3, delay: float = 2.0) -> str:
    if not GEMINI_API_KEY:
        logging.warning("[Gemini] ❌ Токен не найден в переменной окружения GEM_API_TOKEN")
        return ""

    for attempt in range(1, retries + 1):
        try:
            response = requests.post(
                f"{GEMINI_API_URL}?key={GEMINI_API_KEY}",
                headers=HEADERS,
                json={"contents": [{"parts": [{"text": prompt}]}]},
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json()["candidates"][0]["content"]["parts"][0]["text"]
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                logging.warning(f"[Gemini] 429 Too Many Requests — попытка {attempt}/{retries}")
                time.sleep(delay * attempt)
                continue
            else:
                logging.warning(f"[Gemini] HTTP ошибка: {e}")
                break
        except Exception as e:
            logging.warning(f"[Gemini] Общая ошибка: {e}")
            break

    return ""

# === Чистка и безопасный парсинг JSON Gemini ответа ===
def clean_gemini_response(raw: str) -> dict:
    try:
        cleaned = re.sub(r"^```(?:json)?\n?|```$", "", raw.strip(), flags=re.IGNORECASE | re.MULTILINE).strip()
        cleaned = cleaned.replace('\u200b', '').replace('\ufeff', '')
        cleaned = cleaned.replace('\\n', ' ').replace('\\', '')

        if not (cleaned.startswith('{') and cleaned.endswith('}')):
            logging.warning(f"[Gemini-summary] ❌ Невалидный JSON: {cleaned}")
            return {"about_company": "Не указано", "responsibilities": "Не указано", "requirements": "Не указано"}

        parsed = json.loads(cleaned)

        if not isinstance(parsed, dict):
            logging.warning(f"[Gemini-summary] ❌ Ожидался dict, но пришло {type(parsed)}: {parsed}")
            return {"about_company": "Не указано", "responsibilities": "Не указано", "requirements": "Не указано"}

        def process_field(value):
            if isinstance(value, list):
                return " ".join(item.strip() for item in value if isinstance(item, str))
            return str(value).strip()

        about_company = process_field(parsed.get("about_company", "Не указано"))
        responsibilities = process_field(parsed.get("responsibilities", "Не указано"))
        requirements = process_field(parsed.get("requirements", "Не указано"))

        return {
            "about_company": about_company if about_company else "Не указано",
            "responsibilities": responsibilities if responsibilities else "Не указано",
            "requirements": requirements if requirements else "Не указано",
        }

    except Exception as e:
        logging.warning(f"[Gemini-summary] ❌ Ошибка парсинга JSON: {e}")
        return {"about_company": "Не указано", "responsibilities": "Не указано", "requirements": "Не указано"}

# === Промпт для генерации краткого summary ===
SUMMARY_PROMPT_TEMPLATE = """
Разбей текст описания вакансии на три кратких блока:

1. 📌 *О компании* — в 1–2 предложениях.
2. 🧾 *Обязанности* — только ключевые пункты, кратко, по делу (до 3–5 пунктов).
3. 🎯 *Требования* — самые важные навыки и условия, кратко (до 3–5 пунктов).

📢 Не пиши вводных фраз, не добавляй лишние слова. Используй маркированный список (•), если возможно.
🚫 Если для какого-либо блока нет информации — укажи "Не указано".
🔍 Не придумывай ничего нового — работай только с тем, что есть в описании.

Верни чистый JSON:
```json
{
  "about_company": "...",
  "responsibilities": "...",
  "requirements": "..."
}
```

Описание вакансии:
{description}
"""

# === Финальный summary вызов ===
def summarize_description_llm(description: str) -> dict:
    if not description or len(description.strip()) < 50:
        logging.warning("[Gemini-summary] ❗ Пропущено из-за короткого описания")
        return {"about_company": "Не указано", "responsibilities": "Не указано", "requirements": "Не указано"}

    try:
        prompt = SUMMARY_PROMPT_TEMPLATE.format(description=description)
    except Exception as e:
        logging.warning(f"[Gemini-summary] ❌ Ошибка форматирования промпта: {e}")
        return {"about_company": "Не указано", "responsibilities": "Не указано", "requirements": "Не указано"}

    raw = gemini_api_call(prompt)
    if not raw:
        logging.warning("[Gemini-summary] ❌ Пустой ответ от Gemini")
        return {"about_company": "Не указано", "responsibilities": "Не указано", "requirements": "Не указано"}

    logging.info("[Gemini-summary] Сырый ответ:\n" + raw.strip())
    return clean_gemini_response(raw)

# === Промпт для фильтрации релевантных вакансий ===
FILTER_PROMPT = """
Ты ассистент, который определяет релевантность вакансии по названию и описанию.

Профессия считается релевантной, если она относится к одной из следующих:
- Data Scientist
- Senior Data Scientist
- Junior Data Scientist
- Machine Learning Engineer
- ML Engineer
- Data Analyst
- Senior Data Analyst
- Data Engineer
- Big Data Engineer
- Data Architect
- Business Intelligence Analyst
- BI Analyst
- Business Intelligence Developer
- Statistician
- Quantitative Analyst
- NLP Engineer
- Computer Vision Engineer
- Deep Learning Engineer
- Artificial Intelligence Engineer
- AI Researcher
- Data Researcher
- Predictive Analytics Specialist
- Data Science Manager
- Analytics Consultant
- Data Miner
- Data Specialist
- Data Modeler

❗️Игнорируй вакансии из других сфер, даже если в тексте встречаются слова типа "data", "AI", "model" и т.п., но они не относятся к профессиям из списка.

Профессия: "{title}"
Описание: "{description}"

Ответь строго одним словом: yes или no.
"""

# === Фильтрация вакансии ===
def filter_vacancy_llm(title: str, description: str) -> bool:
    try:
        prompt = FILTER_PROMPT.format(title=title, description=description)
    except Exception as e:
        logging.warning(f"[Gemini-filter] ❌ Ошибка форматирования промпта: {e}")
        return False

    for attempt in range(2):
        raw = gemini_api_call(prompt)
        if raw:
            logging.info("[Gemini-filter] Сырый ответ:\n" + raw.strip())
            return raw.strip().lower() == "yes"
        else:
            logging.warning(f"[Gemini-filter] ❌ Пустой ответ от Gemini. Попытка {attempt+1}")
            time.sleep(2)

    return False
