"""
Скрипт для нормализации вакансий из CSV-файла.
Использует JobTitleNormalizer из src.normalize_titles.
"""

import csv
from src.normalize_titles import JobTitleNormalizer


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
