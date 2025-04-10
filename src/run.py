# src/run.py

import asyncio
from src.parser import get_vacancy_links
from src.utils import setup_logger

if __name__ == "__main__":
    setup_logger()

    keyword = "Data Scientist"
    links = asyncio.run(get_vacancy_links(keyword, max_pages=2))

    print(f"Найдено {len(links)} ссылок:")
    for link in links[:5]:  # покажем только первые 5
        print(link)
