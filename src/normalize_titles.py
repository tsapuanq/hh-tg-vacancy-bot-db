# normalize_titles.py

from typing import Dict, Optional
import logging
import psycopg2.extras
from database import Database
from src.config import NORMALIZE_BATCH_LIMIT

class JobTitleNormalizer:
    """Нормализатор названий вакансий"""

    def __init__(self):
        self.categories = {
            'Data Analyst': ['data analyst', 'аналитик данных', 'дата аналитик', 'data аналитик', 'аналитик по данным', 'датa-аналитик', 'data analytics specialist', 'data analytics engineer', 'data analytics team', 'data & analytics specialist'],
            'Business Analyst': ['business analyst', 'бизнес-аналитик', 'бизнес аналитик', 'ba analyst', 'бизнес - аналитик'],
            'System Analyst': ['system analyst', 'системный аналитик', 'system аналитик', 'жүйелік талдаушы'],
            'Data Engineer': ['data engineer', 'дата инженер', 'инженер данных', 'dwh', 'etl', 'data fabric', 'developer data инженер', 'data modeler/engineer', 'dataops engineer', 'data & infrastructure engineer', 'дата-инженер', 'dataops/mlops инженер', 'dataops инженер', 'dataops', 'data analytics engineer', 'data analytics engineering', 'эксперт data инженер'],
            'Data Scientist': ['data scientist', 'дата сайентист', 'data science', 'ml analyst', 'datascientist'],
            'ML Engineer': ['ml engineer', 'ml-инженер', 'machine learning', 'ml developer', 'nlp engineer', 'cv/ml', 'computer vision engineer', "эксперт ml nlp", 'эксперт по машинному обучению', 'ds/ml инженер', 'ml-разработчик', 'ai/ml разработчик', 'ai/ml инженер', 'nlp/ml специалист', 'computer vision lead engineer', 'mlops engineer'],
            'AI Engineer': ['ai engineer', 'ai-инженер', 'llm', 'prompt engineer', 'нейросет', 'искусственный интеллект', 'workflow developer', 'ai specialist', 'senior ai developer', 'ai data group lead', 'senior ai-специалист', 'generative ai'],
            'DevOps Engineer': ['devops', 'dev ops', 'sre', 'девопс', 'cloud engineer', 'platform engineer'],
            'BI Analyst': ['bi analyst', 'bi аналитик', 'business intelligence', 'power bi', 'tableau', 'bi developer', 'reporting', 'отчетности', 'отчетность', 'bi-аналитик', 'bi engineer', 'bi-разработчик', 'bi engenineer', 'bi-инженер', 'разработке bi систем', 'ведущий bi'],
            'Product Analyst': ['product analyst', 'продуктовый аналитик', 'продакт аналитик', 'product analytics'],
            'Architect': ['architect', 'архитектор', 'solution architect', 'data architect', 'главный data инженер'],
            'Management': ['team lead', 'тимлид', 'head of', 'director', 'руководитель', 'менеджер по']
        }

        self.levels = {
            'Intern': ['intern', 'стажер', 'стажёр', 'trainee', 'практика'],
            'Junior': ['junior', 'младший', 'начинающий'],
            'Middle': ['middle', 'mid-level', 'средний'],
            'Senior': ['senior', 'старший', 'ведущий', 'principal'],
            'Lead': ['lead', 'тимлид', 'руководитель группы', 'главный'],
            'Head': ['head', 'director', 'начальник отдела', 'руководитель отдела']
        }

    def normalize(self, title: str) -> Dict[str, Optional[str]]:
        """Нормализует название вакансии"""
        if not title:
            return self._empty(title)

        t = title.lower().strip()
        level = self._detect_level(t)
        category = self._detect_category(t)
        normalized = f"{level + ' ' if level else ''}{category}".strip()

        return {
            "original_title": title,
            "general_title": normalized or "Other",
            "category": category or "Other",
            "level": level or ""
        }

    def _detect_level(self, text: str) -> Optional[str]:
        for lvl, words in self.levels.items():
            if any(w in text for w in words):
                return lvl
        return None

    def _detect_category(self, text: str) -> str:
        for cat, words in self.categories.items():
            if any(w in text for w in words):
                return cat
        if 'аналитик' in text or 'analyst' in text:
            return 'Analyst'
        return 'Other'

    def _empty(self, title: str):
        return {"original_title": title or "", "general_title": "Other", "category": "Other", "level": ""}

def normalize_titles_in_db(db: Database, only_missing: bool = True, batch_limit: int = NORMALIZE_BATCH_LIMIT) -> int:
    """
    Нормализует вакансии прямо в БД.
    - Берём строки из vacancies (у которых general_title/category/level пусты — если only_missing=True)
    - Считаем нормализацию
    - Пишем обратно UPDATE
    Возвращает количество обновлённых строк.
    """
    normalizer = JobTitleNormalizer()
    updated = 0

    conn = None
    cur = None
    try:
        conn = db.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        if only_missing:
            logging.info("🔎 Выбираем вакансии без нормализации…")
            cur.execute(
                """
                SELECT id, title
                FROM vacancies
                WHERE (general_title IS NULL OR general_title = '')
                  AND title IS NOT NULL
                ORDER BY id
                LIMIT %s
                """,
                (batch_limit,),
            )
        else:
            logging.info("🔎 Выбираем все вакансии для перезапуска нормализации…")
            cur.execute(
                """
                SELECT id, title
                FROM vacancies
                WHERE title IS NOT NULL
                ORDER BY id
                LIMIT %s
                """,
                (batch_limit,),
            )

        rows = cur.fetchall()
        if not rows:
            logging.info("✅ Нечего нормализовать — выборка пуста.")
            return 0

        logging.info(f"📦 Получили {len(rows)} записей на нормализацию.")

        for row in rows:
            vid = row["id"]
            title = row["title"] or ""

            norm = normalizer.normalize(title)

            cur.execute(
                """
                UPDATE vacancies
                SET general_title = %s,
                    category = %s,
                    level = %s
                WHERE id = %s
                """,
                (
                    norm["general_title"],
                    norm["category"],
                    norm["level"],
                    vid,
                ),
            )
            updated += 1

        conn.commit()
        logging.info(f"✅ Обновлено записей: {updated}")
        return updated

    except Exception as e:
        logging.error(f"❌ Ошибка нормализации в БД: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return updated

    finally:
        if cur:
            cur.close()
        if conn:
            db.return_connection(conn)
