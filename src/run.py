# src/run.py

import asyncio
import pandas as pd
from src.parser import get_vacancy_links
from src.scraper import get_vacancy_details
from src.utils import setup_logger
from pathlib import Path

async def main():
    setup_logger()
    
    keyword = "Data Scientist"
    max_pages = 1  # можно увеличить позже
    
    print(f"🔎 Собираем вакансии по ключевому слову: {keyword}")
    links = await get_vacancy_links(keyword, max_pages=max_pages)
    print(f"🔗 Найдено {len(links)} ссылок")

    all_data = []
    for i, link in enumerate(links, 1):
        print(f"📄 [{i}/{len(links)}] Обрабатываем: {link}")
        try:
            vacancy_data = await get_vacancy_details(link)
            all_data.append(vacancy_data)
        except Exception as e:
            print(f"❌ Ошибка при обработке {link}: {e}")

    if all_data:
        df = pd.DataFrame(all_data)
        Path("data").mkdir(parents=True, exist_ok=True)
        df.to_csv("data/raw/vacancies.csv", index=False, encoding='utf-8-sig')
        print("✅ Данные сохранены в data/raw/vacancies.csv")
    else:
        print("⚠️ Нет данных для сохранения")

if __name__ == "__main__":
    asyncio.run(main())
