# src/run.py

import asyncio
from src.parser import get_vacancy_links
from src.utils import setup_logger
from src.scraper import get_vacancy_details


async def main():
    setup_logger()

    keyword = "Data Scientist"
    links = await get_vacancy_links(keyword, max_pages=2)

    print(f"Найдено {len(links)} ссылок:")
    for link in links[:5]:
        print(link)

    if links:
        details = await get_vacancy_details(links[0])
        print("\nДетали вакансии:")
        print(details)

if __name__ == "__main__":
    asyncio.run(main())
