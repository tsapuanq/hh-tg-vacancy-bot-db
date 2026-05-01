# llm_summary.py
import time
import json
import re
import random
import logging
import requests
from collections import deque

from src.config import (
    OPENAI_API_KEY,
    OPENAI_API_URL,
    OPENAI_MODEL,
    OPENAI_HEADERS,
    LLM_API_RETRIES,
    LLM_API_TIMEOUT,
    LLM_API_MIN_INTERVAL,
    LLM_API_MAX_PER_MIN,
    LLM_API_BACKOFF_BASE,
    LLM_API_BACKOFF_CAP,
    DESCRIPTION_PROMPT_MAX_CHARS,
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

def _collect_output_text(obj):
    if obj is None:
        return []
    if isinstance(obj, str):
        return [obj]
    if isinstance(obj, dict):
        collected = []
        text = obj.get("text")
        if isinstance(text, str):
            collected.append(text)
        for child_key in ("content", "message", "output", "choices"):
            if child_key in obj:
                collected.extend(_collect_output_text(obj[child_key]))
        return collected
    if isinstance(obj, list):
        collected = []
        for item in obj:
            collected.extend(_collect_output_text(item))
        return collected
    return []


def _extract_openai_text(response_data: dict) -> str:
    if not response_data:
        return ""
    texts = _collect_output_text(response_data.get("output"))
    if not texts:
        texts = _collect_output_text(response_data.get("choices"))
    cleaned = [text.strip() for text in texts if isinstance(text, str) and text.strip()]
    return "\n".join(cleaned).strip()


def openai_api_call(prompt: str) -> str:
    if not OPENAI_API_KEY:
        logging.warning("[OpenAI] ❌ Не найден OPENAI_API_KEY")
        return ""

    headers = dict(OPENAI_HEADERS or {})
    headers.setdefault("Content-Type", "application/json; charset=utf-8")
    headers.setdefault("Authorization", f"Bearer {OPENAI_API_KEY}")

    payload = {
        "model": OPENAI_MODEL,
        "input": prompt,
    }

    for attempt in range(1, LLM_API_RETRIES + 1):
        _rate_limit_wait()
        try:
            resp = _session.post(
                OPENAI_API_URL,
                headers=headers,
                json=payload,
                timeout=LLM_API_TIMEOUT,
            )
            _mark_call()
            resp.raise_for_status()

            data = resp.json()
            text = _extract_openai_text(data)
            if text:
                return text

            logging.warning(f"[OpenAI] Пустой/нетипичный ответ API: {resp.text}")
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
                    logging.warning(f"[OpenAI] 429 Too Many Requests — попытка {attempt}/{LLM_API_RETRIES}, пауза {wait:.2f}с")
                    _sleep_with_jitter(wait)
                    continue

                logging.warning("[OpenAI] 429: исчерпаны попытки")
                return ""

            if code and 500 <= code < 600 and attempt < LLM_API_RETRIES:
                wait = min(LLM_API_BACKOFF_CAP, LLM_API_BACKOFF_BASE * (2 ** (attempt - 1)))
                logging.warning(f"[OpenAI] {code} серверная ошибка — попытка {attempt}/{LLM_API_RETRIES}, пауза {wait:.2f}с")
                _sleep_with_jitter(wait)
                continue

            body = r.text if r is not None else ""
            logging.warning(f"[OpenAI] HTTP ошибка: {code} — {body[:500]}")
            return ""

        except requests.exceptions.RequestException as e:
            if attempt < LLM_API_RETRIES:
                wait = min(LLM_API_BACKOFF_CAP, LLM_API_BACKOFF_BASE * (2 ** (attempt - 1)))
                logging.warning(f"[OpenAI] Сетевая ошибка: {e} — попытка {attempt}/{LLM_API_RETRIES}, пауза {wait:.2f}с")
                _sleep_with_jitter(wait)
                continue

            logging.warning(f"[OpenAI] Сетевая ошибка, попытки исчерпаны: {e}")
            return ""

        except Exception as e:
            logging.warning(f"[OpenAI] Общая ошибка при запросе: {e}", exc_info=True)
            return ""

    return ""

SUMMARY_PROMPT_TEMPLATE = """
Ты — строгий JSON-парсер.  
Единственный допустимый вывод — ЧИСТЫЙ валидный JSON-объект.  
Никакого текста до, после или внутри. Никаких комментариев. Только JSON.

Структура JSON:
{
  "about_company": "string",
  "responsibilities": ["string", "string", ...],
  "requirements": ["string", "string", ...]
}

Правила:
1. Все три ключа ОБЯЗАТЕЛЬНЫ.
2. "about_company" — ровно 1–2 предложения из текста, которые описывают саму компанию (её сферу деятельности, продукт, услуги или масштаб).  
   Если в тексте нет такой информации или присутствуют только данные про трудоустройство, график, соцпакет, коллектив и т.п. → "Не указано".
3. "responsibilities" — массив из 3–5 коротких пунктов, отражающих задачи или обязанности. Если нет → ["Не указано"].
4. "requirements" — массив из 3–5 ключевых навыков или требований. Если нет → ["Не указано"].
5. Все значения должны строго соответствовать исходному тексту. Ничего не придумывать, не интерпретировать, не дополнять.
6. Формат ответа нельзя изменять. Допустим только объект с ключами: "about_company", "responsibilities", "requirements".
7. Ответ должен быть строго валидным JSON (без комментариев, Markdown, текста до/после, переносов или форматирования, которое мешает парсингу).

Входные данные (описание вакансии):
{description}
"""

def _prepare_description_for_prompt(description: str) -> str:
    if not description:
        return ""
    clean = re.sub(r"\s+", " ", description).strip()
    if len(clean) <= DESCRIPTION_PROMPT_MAX_CHARS:
        return clean
    truncated = clean[:DESCRIPTION_PROMPT_MAX_CHARS]
    boundary = truncated.rfind(" ")
    if boundary > DESCRIPTION_PROMPT_MAX_CHARS - 100 and boundary > 0:
        truncated = truncated[:boundary]
    return truncated.strip()


def clean_openai_response(raw: str) -> dict:
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
        logging.warning(f"[OpenAI-summary] ❌ Ошибка парсинга JSON: {e}")
        logging.warning("[OpenAI-summary] Сырой ответ:\n" + (raw or ""))
        return {"about_company": "Не указано", "responsibilities": ["Не указано"], "requirements": ["Не указано"]}
    except Exception as e:
        logging.warning(f"[OpenAI-summary] ❌ Неожиданная ошибка: {e}", exc_info=True)
        logging.warning("[OpenAI-summary] Сырой ответ:\n" + (raw or ""))
        return {"about_company": "Не указано", "responsibilities": ["Не указано"], "requirements": ["Не указано"]}

def summarize_description_llm(description: str) -> dict:
    if not description or description.strip() == "Не указано":
        return {"about_company": "Не указано", "responsibilities": ["Не указано"], "requirements": ["Не указано"]}
    prompt_description = _prepare_description_for_prompt(description)
    prompt = SUMMARY_PROMPT_TEMPLATE.replace("{description}", prompt_description)
    raw = openai_api_call(prompt) or ""
    logging.info("[OpenAI-summary] Сырый ответ:\n" + raw)
    return clean_openai_response(raw)

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

def filter_vacancy_llm(title: str, description: str) -> bool | None:
    if not title or not description:
        return False
    prompt_description = _prepare_description_for_prompt(description)
    prompt = FILTER_PROMPT.format(title=title, description=prompt_description)
    raw = (openai_api_call(prompt) or "").strip().lower()
    logging.info("[OpenAI-filter] Сырый ответ:\n" + raw)
    if not raw:
        return None
    return raw == "yes"
