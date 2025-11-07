"""
📊 Скрипт для нормализации вакансий под формат hh-tg-vacancy-bot
Берёт CSV с вакансиями → добавляет general_title, category, level
"""

import csv
from typing import Dict, Optional


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


def normalize_vacancies_csv(input_file: str, output_file: str = None):
    """Обработка CSV с вакансиями"""
    if output_file is None:
        output_file = input_file.replace('.csv', '_normalized.csv')

    norm = JobTitleNormalizer()
    results = []

    print(f"📖 Читаем файл: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            title = row.get('title', '')
            normalized = norm.normalize(title)
            results.append({**row, **normalized})

    print(f"Обработано {len(results)} вакансий")

    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)

    print(f"💾 Результат сохранён в {output_file}")
    print("✨ Готово!")


if __name__ == "__main__":
    INPUT = "vacancies.csv"  
    normalize_vacancies_csv(INPUT)
