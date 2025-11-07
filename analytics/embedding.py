import pandas as pd
import numpy as np
import torch
from sentence_transformers import SentenceTransformer, util
from deep_translator import GoogleTranslator
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# ======== Настройки ========
MODEL_NAME = "BAAI/bge-m3"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
model = SentenceTransformer(MODEL_NAME, device=DEVICE)

# Основные направления
CATEGORIES = {
    "data science": "Работа с данными, построение моделей, машинное обучение, Python, статистика, Jupyter",
    "machine learning": "ML, обучение моделей, регрессия, классификация, pytorch, tensorflow",
    "ai engineer": "искусственный интеллект, нейросети, LLM, генеративные модели",
    "data analytics": "анализ данных, SQL, Power BI, Tableau, визуализация, аналитика",
    "bi analytics": "business intelligence, BI аналитик, отчеты, Power BI, дашборды",
    "business analytics": "бизнес аналитик, требования, документация, Jira, Confluence",
    "data engineer": "ETL, pipelines, Spark, Airflow, базы данных, хранилища данных",
    "devops": "CI/CD, Docker, Kubernetes, Jenkins, автоматизация, инфраструктура",
    "system analyst": "технические требования, UML, документация, взаимодействие с разработкой",
    "computer vision": "CV, обработка изображений, OpenCV, детекция объектов, нейросети"
}

# ======== Эмбеддинги направлений ========
logging.info("Создаем эмбеддинги категорий...")
category_names = list(CATEGORIES.keys())
category_embeddings = model.encode(list(CATEGORIES.values()), normalize_embeddings=True)
logging.info("✅ Эмбеддинги категорий готовы!")

translator = GoogleTranslator(source='auto', target='en')

# ======== Классификатор ========
def categorize_vacancy(text, threshold=0.35):
    if not text or text.strip() == "":
        return "other", 0.0

    try:
        # Переводим на английский, если текст не на нём
        translated = translator.translate(text)
    except Exception:
        translated = text  # если не перевелось, оставляем как есть

    # Создаём эмбеддинг
    embedding = model.encode([translated], normalize_embeddings=True)

    # Считаем косинусное сходство
    similarities = util.cos_sim(embedding, category_embeddings)[0].cpu().numpy()

    best_idx = np.argmax(similarities)
    best_score = similarities[best_idx]
    best_category = category_names[best_idx]

    if best_score < threshold:
        return "other", best_score

    return best_category, float(best_score)


# ======== Тест ========
test_vacancies = [
    "ML Engineer — разработка и внедрение моделей машинного обучения",
    "Data Analyst, SQL, Power BI, визуализация данных",
    "DevOps Engineer — CI/CD, Docker, Kubernetes",
    "Системный аналитик — UML, Jira, взаимодействие с командой",
    "Computer Vision Engineer, детекция объектов на видео",
]

for text in test_vacancies:
    cat, score = categorize_vacancy(text)
    print(f"{text}\n → {cat.upper()} ({score:.3f})\n")