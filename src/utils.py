# utils.py
import logging
import re
from typing import Optional, Tuple, List

def setup_logger():
    if not logging.getLogger().handlers:
        logging.basicConfig(
            format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
        )
        logging.info("✅ Логгер настроен.")
    else:
        logging.debug("Логгер уже настроен, пропускаем.")


def canonical_link(link: str) -> str | None:
    if not isinstance(link, str) or not link.strip():
        return None
    try:
        return link.split("?", 1)[0].strip()  
    except Exception:
        return (
            link.strip() if isinstance(link, str) else None
        )  


def extract_vacancy_id(link: str) -> str | None:
    """Извлекает ID вакансии из URL."""
    if not isinstance(link, str) or not link.strip():
        return None
    try:
        match = re.search(r"/vacancy/(\d+)", link)
        if match:
            return match.group(1)
        return None
    except Exception:
        return None



TARGET_KEYWORDS_FINAL = [
    "data sci", "data scientist", "data analyst", "data science", "analyt", 
    "аналитик дан", "analyst", "business analyst", "system analyst",
    "bi analyst", "business intelligence", "датасаент", 
    "дата сайнс", "аналитич конс", "data miner", "data specialist",

    "machine learning", "ml engineer", "ml engin", "ml разраб", "nlp", 
    "computer vision", "cv engin", "deep learn", "ai engineer", "ai researcher", 
    "машинн обуч", "мл инженер", "мл специалист", "cv", 
    
    "data engin", "data engineer", "инженер дан", "big data", "big data engineer", 
    "data architect", "etl", "dwh", "мониторинг данных", "биот аналитик",
    "cloud engineer", "data analytics", "data analysis",
    "power bi", "powerbi", "junior analyst",

    "mlops", "mlops engineer", "devops", "devops engineer"
]

BASE_KEYWORDS_ROOT = [
    "data", "аналит", "sql", "python", "ml", "etl", "dwh",
    "postgres", "spark", "airflow", "cloud", "aws", "gcp", "azure", 
    "обработк", "статистик", "моделирован"
]

DENY_WORDS_ROOT = [
    "курьер", "продав", "официант", "администратор", "водител", "охран",
    "повар", "учител", "менеджер по продаж", "копирайт", "hr",
    "техподдержк", "кассир", "сварщ", "бухгалтер"
]

KEYWORD_WEIGHTS = {
    "data scientist": 4,
    "data scientist senior": 4,
    "data engineer": 4,
    "machine learning engineer": 4,
    "mlops engineer": 4,
    "data architect": 3,
    "ai engineer": 3,
    "machine learning": 3,
    "data science": 3,
    "data science engineer": 3,
    "data science manager": 3,
    "data analytics engineer": 3,
    "business intelligence": 3,
    "business intelligence analyst": 3,
    "business analyst": 3,
    "data analyst": 3,
    "data analysis": 3,
    "data visualization": 2,
    "analytics engineer": 3,
    "business analytics": 2,
    "ml engineer": 3,
    "mlops": 3,
    "nlp engineer": 3,
    "nlp specialist": 3,
    "natural language processing": 3,
    "computer vision": 3,
    "deep learning": 3,
    "artificial intelligence": 3,
    "prompt engineer": 2,
    "research scientist": 3,
    "statistician": 2,
    "data platform": 2,
    "data pipeline": 2,
    "data automation": 2,
    "automation engineer": 2,
    "model deployment": 2,
    "analytics": 2,
    "data": 1,
    "engineer": 1,
    "analyst": 1,
    "devops": 2,
    "ml": 2,
    "ai": 2,
    "big data": 3,
}

BONUS_PHRASE_COMBOS: List[Tuple[Tuple[str, ...], int]] = [
    (("data", "engineer"), 2),
    (("machine", "learning"), 2),
    (("ml", "engineer"), 2),
    (("production", "ml"), 2),
    (("data", "science"), 2),
    (("deep", "learning"), 2),
    (("research", "ml"), 1),
    (("business", "intelligence"), 2),
    (("data", "analytics"), 2),
    (("data", "pipeline"), 2),
    (("mlops", "engineer"), 2),
]

RELEVANCE_SCORE_THRESHOLD = 4

def normalize_text(text: str) -> str:
    if not text:
        return ""
    
    text = re.sub(r'[^a-zа-я0-9]+', ' ', text.lower())

    return re.sub(r'\s+', ' ', text).strip()


def _calculate_relevance_score(text: str) -> Tuple[int, List[str]]:
    """
    Возвращает суммарный вес ключевых слов и список включённых сигналов.
    """
    score = 0
    signals: List[str] = []

    for keyword, weight in KEYWORD_WEIGHTS.items():
        if keyword in text:
            score += weight
            signals.append(f"{keyword}:{weight}")

    for combo, bonus in BONUS_PHRASE_COMBOS:
        if all(term in text for term in combo):
            score += bonus
            signals.append(f"{'+'.join(combo)}:{bonus}")

    return score, signals

def is_relevant_soft(title: str, description: Optional[str]) -> bool:
    text = normalize_text(title + " " + (description or ""))

    if any(word in text for word in DENY_WORDS_ROOT):
        return False

    score, signals = _calculate_relevance_score(text)
    if score >= RELEVANCE_SCORE_THRESHOLD:
        logging.debug(
            f"[SoftFilter] {title} score={score} signals={signals}"
        )
        return True

    if any(word in text for word in TARGET_KEYWORDS_FINAL):
        logging.debug(f"[SoftFilter] target keyword match for {title}")
        return True

    base_matches = sum(
        1 for word in BASE_KEYWORDS_ROOT
        if word in text
    )

    return base_matches >= 2
