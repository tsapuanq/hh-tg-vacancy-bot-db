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

# === Промпт для генерации краткого summary ===
SUMMARY_PROMPT_TEMPLATE = """
Разбей текст описания вакансии на три кратких блока:

1. 📌 *О компании* — в 1-2 предложениях.
2. 🧾 *Обязанности* — только ключевые пункты, кратко, по делу (до 3–5 пунктов).
3. 🎯 *Требования* — самые важные навыки и условия, кратко (до 3–5 пунктов).

📢 Не пиши вводных фраз, не добавляй лишние слова. Используй маркированный список (•) если возможно.
Верни **ЧИСТЫЙ JSON** с ключами: `about_company`, `responsibilities`, `requirements`.

Описание:
{description}
"""

# === Чистка и парсинг JSON Gemini ответа ===
def clean_gemini_response(raw: str) -> dict:
    try:
        cleaned = re.sub(r"^```json\n?|```$", "", raw.strip(), flags=re.IGNORECASE).strip()
        parsed = json.loads(cleaned)
        return {
            "about_company": parsed.get("about_company", "Не указано"),
            "responsibilities": parsed.get("responsibilities", "Не указано"),
            "requirements": parsed.get("requirements", "Не указано"),
        }
    except Exception as e:
        logging.warning(f"[Gemini‑summary] ❌ Ошибка парсинга JSON: {e}")
        return {
            "about_company": "Не указано",
            "responsibilities": "Не указано",
            "requirements": "Не указано",
        }

# === Финальный summary вызов ===
def summarize_description_llm(description: str) -> dict:
    prompt = SUMMARY_PROMPT_TEMPLATE.format(description=description)
    raw = gemini_api_call(prompt).strip()
    logging.info("[Gemini‑summary] Сырый ответ:\n" + raw)
    return clean_gemini_response(raw)
