# llm_summary.py
import time
import json
import re
import random
import logging
import requests
from collections import deque

from src.config import (
    GEMINI_API_KEY,
    GEMINI_API_URL,
    HEADERS,
    LLM_API_RETRIES,
    LLM_API_TIMEOUT,
    LLM_API_MIN_INTERVAL,
    LLM_API_MAX_PER_MIN,
    LLM_API_BACKOFF_BASE,
    LLM_API_BACKOFF_CAP,
)

_session = requests.Session()
_call_timestamps = deque()
_last_call_mono = 0.0

def _rate_limit_wait():
    now = time.monotonic()

    if LLM_API_MIN_INTERVAL > 0:
        gap = now - _last_call_mono
        if gap < LLM_API_MIN_INTERVAL:
            time.sleep(LLM_API_MIN_INTERVAL - gap)

    now = time.monotonic()
    while _call_timestamps and (now - _call_timestamps[0]) > 60:
        _call_timestamps.popleft()

    if LLM_API_MAX_PER_MIN and len(_call_timestamps) >= LLM_API_MAX_PER_MIN:
        wait_s = 60 - (now - _call_timestamps[0])
        if wait_s > 0:
            time.sleep(wait_s)

def _mark_call():
    global _last_call_mono
    _last_call_mono = time.monotonic()
    _call_timestamps.append(_last_call_mono)

def _sleep_with_jitter(base_seconds: float):
    time.sleep(base_seconds + random.uniform(0, 0.5))

def gemini_api_call(prompt: str) -> str:
    if not GEMINI_API_KEY:
        logging.warning("[Gemini] ❌ Не найден GEMINI_API_KEY")
        return ""

    headers = dict(HEADERS or {})
    headers.setdefault("Content-Type", "application/json; charset=utf-8")

    for attempt in range(1, LLM_API_RETRIES + 1):
        _rate_limit_wait()
        try:
            resp = _session.post(
                f"{GEMINI_API_URL}?key={GEMINI_API_KEY}",
                headers=headers,
                json={"contents": [{"parts": [{"text": prompt}]}]},
                timeout=LLM_API_TIMEOUT,
            )
            _mark_call()
            resp.raise_for_status()

            data = resp.json()
            if (
                data
                and data.get("candidates")
                and data["candidates"][0].get("content")
                and data["candidates"][0]["content"].get("parts")
                and data["candidates"][0]["content"]["parts"][0].get("text")
            ):
                return data["candidates"][0]["content"]["parts"][0]["text"]

            logging.warning(f"[Gemini] Пустой/нетипичный ответ API: {resp.text}")
            return ""

        except requests.exceptions.HTTPError as e:
            r = e.response
            code = r.status_code if r is not None else None

            if code == 429:
                retry_after = 0.0
                if r is not None:
                    ra = r.headers.get("Retry-After")
                    if ra and ra.isdigit():
                        retry_after = float(ra)

                if attempt < LLM_API_RETRIES:
                    wait = retry_after if retry_after > 0 else min(
                        LLM_API_BACKOFF_CAP,
                        LLM_API_BACKOFF_BASE * (2 ** (attempt - 1))
                    )
                    logging.warning(f"[Gemini] 429 Too Many Requests — попытка {attempt}/{LLM_API_RETRIES}, пауза {wait:.2f}с")
                    _sleep_with_jitter(wait)
                    continue

                logging.warning("[Gemini] 429: исчерпаны попытки")
                return ""

            if code and 500 <= code < 600 and attempt < LLM_API_RETRIES:
                wait = min(LLM_API_BACKOFF_CAP, LLM_API_BACKOFF_BASE * (2 ** (attempt - 1)))
                logging.warning(f"[Gemini] {code} серверная ошибка — попытка {attempt}/{LLM_API_RETRIES}, пауза {wait:.2f}с")
                _sleep_with_jitter(wait)
                continue

            body = r.text if r is not None else ""
            logging.warning(f"[Gemini] HTTP ошибка: {code} — {body[:500]}")
            return ""

        except requests.exceptions.RequestException as e:
            if attempt < LLM_API_RETRIES:
                wait = min(LLM_API_BACKOFF_CAP, LLM_API_BACKOFF_BASE * (2 ** (attempt - 1)))
                logging.warning(f"[Gemini] Сетевая ошибка: {e} — попытка {attempt}/{LLM_API_RETRIES}, пауза {wait:.2f}с")
                _sleep_with_jitter(wait)
                continue

            logging.warning(f"[Gemini] Сетевая ошибка, попытки исчерпаны: {e}")
            return ""

        except Exception as e:
            logging.warning(f"[Gemini] Общая ошибка при запросе: {e}", exc_info=True)
            return ""

    return ""

SUMMARY_PROMPT_TEMPLATE = """
Разбей текст описания вакансии на три кратких блока:
1. О компании — 1–2 предложения.
2. Обязанности — 3–5 пунктов, по делу.
3. Требования — 3–5 ключевых навыков/условий.
Если блока нет — верни "Не указано".
Верни ЧИСТЫЙ JSON с ключами: about_company (string), responsibilities (array of strings), requirements (array of strings).

Описание:
{description}
"""

def clean_gemini_response(raw: str) -> dict:
    try:
        cleaned = re.sub(r"^\s*```json\s*\n|\n\s*```\s*$", "", raw, flags=re.I | re.S).strip()
        obj = json.loads(cleaned)

        def to_list(x):
            if isinstance(x, list):
                return [str(s).strip() for s in x if str(s).strip()]
            if isinstance(x, str):
                xs = x.strip()
                if not xs or xs.lower() == "не указано":
                    return []
                try:
                    arr = json.loads(xs)
                    if isinstance(arr, list):
                        return [str(s).strip() for s in arr if str(s).strip()]
                except json.JSONDecodeError:
                    pass
                if "\n" in xs:
                    return [ln.strip().strip('"') for ln in xs.splitlines() if ln.strip().strip('"')]
                return [xs.strip('"')]
            return [str(x).strip()] if x is not None and str(x).strip() else []

        about = str(obj.get("about_company", "Не указано")).strip() or "Не указано"
        resp = to_list(obj.get("responsibilities"))
        reqs = to_list(obj.get("requirements"))

        return {
            "about_company": about,
            "responsibilities": resp or ["Не указано"],
            "requirements": reqs or ["Не указано"],
        }

    except json.JSONDecodeError as e:
        logging.warning(f"[Gemini-summary] ❌ Ошибка парсинга JSON: {e}")
        logging.warning("[Gemini-summary] Сырой ответ:\n" + (raw or ""))
        return {"about_company": "Не указано", "responsibilities": ["Не указано"], "requirements": ["Не указано"]}
    except Exception as e:
        logging.warning(f"[Gemini-summary] ❌ Неожиданная ошибка: {e}", exc_info=True)
        logging.warning("[Gemini-summary] Сырой ответ:\n" + (raw or ""))
        return {"about_company": "Не указано", "responsibilities": ["Не указано"], "requirements": ["Не указано"]}

def summarize_description_llm(description: str) -> dict:
    if not description or description.strip() == "Не указано":
        return {"about_company": "Не указано", "responsibilities": ["Не указано"], "requirements": ["Не указано"]}
    prompt = SUMMARY_PROMPT_TEMPLATE.format(description=description)
    raw = gemini_api_call(prompt) or ""
    logging.info("[Gemini-summary] Сырый ответ:\n" + raw)
    return clean_gemini_response(raw)

FILTER_PROMPT = """
Определи, относится ли вакансия К СТРОГО следующему списку профессий:
Data Scientist, Machine Learning Engineer, Data Analyst, Data Engineer,
Big Data Engineer, Data Architect, BI Analyst/Developer, DevOps Engineer,
MLOps Engineer, System Analyst, AI/ML/NLP/CV Engineer, Researcher в области данных/ML.

Если профессия НЕ относится — ответь "no".

⚠️ Формат ответа:
- Только одно слово, либо "yes", либо "no".
- Без пояснений, без дополнительных слов.

Профессия: "{title}"
Описание: "{description}"

Ответь строго: yes или no.
"""

def filter_vacancy_llm(title: str, description: str) -> bool:
    if not title or not description:
        return False
    prompt = FILTER_PROMPT.format(title=title, description=description)
    raw = (gemini_api_call(prompt) or "").strip().lower()
    logging.info("[Gemini-filter] Сырый ответ:\n" + raw)
    return raw == "yes"