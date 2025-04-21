import os
import requests

GEMINI_API_KEY = os.getenv("GEM_API_TOKEN")  # убедись, что переменная задана в .env или окружении
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"


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
    print("[STATUS]", response.status_code)
    print("[RESPONSE]", response.text)

    response.raise_for_status()
    content = response.json()
    return content['candidates'][0]['content']['parts'][0]['text']


if __name__ == "__main__":
    prompt = """
Скажи, относится ли профессия к одной из следующих областей: Data Science, Machine Learning, AI, Анализ данных, BI, DevOps.

Профессия: "Data Scientist"
Описание: "Мы ищем специалиста по анализу данных для разработки моделей и аналитических решений."

Ответь только "yes" или "no"
"""
    print("[RESPONSE TEXT]:")
    print(gemini_api_call(prompt))
