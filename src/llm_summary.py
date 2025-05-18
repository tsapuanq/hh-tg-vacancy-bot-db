# llm_summary.py
import os
import time
import json
import re
import requests
import logging
from src.config import GEMINI_API_KEY, GEMINI_API_URL, HEADERS, LLM_API_RETRIES, LLM_API_DELAY, LLM_API_TIMEOUT

def gemini_api_call(prompt: str) -> str:
    """
    Выполняет запрос к Gemini API с повторными попытками.
    """
    if not GEMINI_API_KEY:
        logging.warning("[Gemini] ❌ Токен не найден в переменной окружения GEM_API_TOKEN")
        return ""

    for attempt in range(1, LLM_API_RETRIES + 1):
        try:
            response = requests.post(
                f"{GEMINI_API_URL}?key={GEMINI_API_KEY}",
                headers=HEADERS,
                json={"contents": [{"parts": [{"text": prompt}]}]},
                timeout=LLM_API_TIMEOUT,
            )
            response.raise_for_status() # Проверяем HTTP ошибки (4xx, 5xx)
            response_json = response.json()
            if response_json and response_json.get("candidates") and response_json["candidates"][0].get("content") and response_json["candidates"][0]["content"].get("parts"):
                 return response_json["candidates"][0]["content"]["parts"][0]["text"]
            else:
                 logging.warning(f"[Gemini] Ответ API не содержит ожидаемой структуры: {response.text}")
                 return "" # Возвращаем пустую строку, если структура неверна

        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                logging.warning(f"[Gemini] 429 Too Many Requests — попытка {attempt}/{LLM_API_RETRIES}")
                time.sleep(LLM_API_DELAY * attempt) # Увеличиваем задержку при 429
                continue 
            else:
                logging.warning(f"[Gemini] HTTP ошибка: {e.response.status_code} - {e.response.text}")
                break 
        except requests.exceptions.RequestException as e:
             logging.warning(f"[Gemini] Сетевая ошибка: {e} — попытка {attempt}/{LLM_API_RETRIES}")
             if attempt < LLM_API_RETRIES:
                 time.sleep(LLM_API_DELAY * attempt)
                 continue 
             else:
                 break 
        except Exception as e:
            logging.warning(f"[Gemini] Общая ошибка при запросе: {e}")
            break # Не повторять при неожиданной ошибке

    return "" # Возвращаем пустую строку после всех неудачных попыток

SUMMARY_PROMPT_TEMPLATE = """
Разбей текст описания вакансии на три кратких блока:

1. 📌 *О компании* — напиши о компании самой в 1-2 предложениях.
2. 🧾 *Обязанности* — только ключевые пункты, кратко, по делу (до 3–5 пунктов).
3. 🎯 *Требования* — самые важные навыки и условия, кратко (до 3–5 пунктов).

📢 Не пиши вводных фраз, не добавляй лишние слова. Используй маркированный список (•), если возможно.
🚫 Если для какого-либо блока нет информации — укажи "Не указано".
🔍 Не придумывай ничего нового — работай только с тем, что есть в описании.

Верни **ЧИСТЫЙ JSON** с ключами: `about_company`, `responsibilities`, `requirements`.

Описание:
{description}
"""

def clean_gemini_response(raw: str) -> dict:
    """
    Чистит сырой ответ LLM (удаляет ```json) и парсит JSON.
    """
    try:
        # Более надежное удаление маркеров JSON
        cleaned = re.sub(r"^\s*```json\s*\n|\n\s*```\s*$", "", raw, flags=re.IGNORECASE|re.DOTALL).strip()
        parsed = json.loads(cleaned)
        return {
            "about_company": str(parsed.get("about_company", "Не указано")).strip(),
            "responsibilities": parsed.get("responsibilities", "Не указано"), # Может быть списком или строкой
            "requirements": parsed.get("requirements", "Не указано"), # Может быть списком или строкой
        }
    except json.JSONDecodeError as e:
        logging.warning(f"[Gemini‑summary] ❌ Ошибка парсинга JSON ответа LLM: {e}")
        logging.warning("[Gemini‑summary] Сырой ответ, вызвавший ошибку:\n" + raw)
        return {
            "about_company": "Не указано",
            "responsibilities": "Не указано",
            "requirements": "Не указано",
        }
    except Exception as e:
        logging.warning(f"[Gemini‑summary] ❌ Неожиданная ошибка при чистке/парсинге LLM ответа: {e}")
        logging.warning("[Gemini‑summary] Сырой ответ, вызвавший ошибку:\n" + raw)
        return {
            "about_company": "Не указано",
            "responsibilities": "Не указано",
            "requirements": "Не указано",
        }

def summarize_description_llm(description: str) -> dict:
    """
    Получает summary описания вакансии с помощью LLM.
    """
    if not description or description.strip() == "Не указано":
        return {
            "about_company": "Не указано",
            "responsibilities": "Не указано",
            "requirements": "Не указано",
        }

    prompt = SUMMARY_PROMPT_TEMPLATE.format(description=description)
    raw = gemini_api_call(prompt) # Убрал .strip(), т.к. clean_gemini_response делает strip
    logging.info("[Gemini‑summary] Сырый ответ:\n" + raw)
    return clean_gemini_response(raw)

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
- DevOps Engineer
- MLOps Engineer
- System Analyst
- Analytics Engineer
- Data Platform Engineer
- Data Visualization Engineer
- Data Modeler
❗️Игнорируй вакансии из других сфер, даже если в тексте встречаются слова типа “data”, “AI”, “model” и т.п., но они не относятся к профессиям из списка.

Профессия: "{title}"
Описание: "{description}"

Ответь строго одним словом: yes или no.
"""

def filter_vacancy_llm(title: str, description: str) -> bool:
    """
    Определяет релевантность вакансии с помощью LLM.
    """
    if not title or not description:
         return False # Не можем определить релевантность без названия или описания

    prompt = FILTER_PROMPT.format(title=title, description=description)
    raw = gemini_api_call(prompt).strip()
    logging.info("[Gemini‑filter] Сырый ответ:\n" + raw)
    # Более строгая проверка ответа
    return raw.lower() == "yes"

