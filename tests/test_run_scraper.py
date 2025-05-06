# tests/test_run_scraper.py
import pytest
import asyncio
import pandas as pd

from src.run_scraper import run_scraper
from src.utils import load_existing_links
from src.utils import canonical_link

@pytest.mark.asyncio
async def test_run_scraper_filters_and_canonical(monkeypatch, tmp_path):
    # 1) Мокаем ключевые слова
    monkeypatch.setattr("src.config.SEARCH_KEYWORDS", ["KW1", "KW2"])
    
    # 2) Мокаем функцию получения ссылок: вернёт одно и то же ID с разными query
    async def fake_get_links(keyword, max_pages):
        return [
            f"https://hh.kz/vacancy/10?query={keyword}",
            f"https://hh.kz/vacancy/20?query={keyword}",
        ]
    monkeypatch.setattr("src.parser.get_vacancy_links", fake_get_links)
    
    # 3) Мокаем детали вакансии: просто возвращаем словарь с полем link
    async def fake_get_details(link, page):
        return {"link": link}
    monkeypatch.setattr("src.scraper.get_vacancy_details", fake_get_details)
    
    # 4) Заставляем load_existing_links вернуть одну каноническую ссылку
    monkeypatch.setattr("src.utils.load_existing_links",
                        lambda path: { "https://hh.kz/vacancy/20" })
    
    # 5) Запускаем скрапер
    df = await run_scraper(mode="daily")
    
    # Должны получить только вакансию 10 (канонизировано) — 20 отсеяна в existing
    assert isinstance(df, pd.DataFrame)
    links = set(df["link"].tolist())
    assert links == {"https://hh.kz/vacancy/10"}

    # Проверяем, что внутри DataFrame ссылки канонические
    for link in links:
        assert "?" not in link

