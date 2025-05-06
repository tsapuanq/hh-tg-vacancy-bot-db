# tests/test_publisher.py
import pytest
import pandas as pd
from datetime import datetime
from src.publisher import _to_bullets, format_message
from src.utils import load_today_rows

@pytest.fixture
def sample_row():
    """Имитируем pd.Series с минимальным набором полей."""
    data = {
        "title": "T",
        "company": "C",
        "location": "L",
        "salary_range": "100–200 $",
        "salary": None,
        "experience": "3 года",
        "employment_type": "Full-time",
        "schedule": "Полный день",
        "working_hours": "9–18",
        "work_format": "Удалёнка",
        "published_date": "01.01.2025",
        "link": "https://hh.kz/vacancy/123",
    }
    return pd.Series(data)

def test_to_bullets_list_and_str():
    # из списка
    assert _to_bullets(["a", "b"]) == "• a\n• b"
    # из repr-строки
    assert _to_bullets("['x','y']") == "• x\n• y"
    # невалидный формат
    assert _to_bullets("just text") == "• just text"

def test_format_message_contains_all_fields(sample_row):
    summary = {
        "responsibilities": ["r1", "r2"],
        "requirements": ["q1"],
        "about_company": "about"
    }
    txt = format_message(sample_row, summary)
    # должен содержать заголовок, эмодзи и буллеты
    assert "• r1" in txt and "• r2" in txt
    assert "• q1" in txt
    assert "*Город:* L" in txt
    assert "[Подробнее на hh](https://hh.kz/vacancy/123)" in txt

def test_load_today_rows(tmp_path, monkeypatch):
    # создаём тестовый CSV для сегодняшней даты
    today = datetime.now().strftime("%Y-%m-%d")
    csv = tmp_path / f"vacancies_clean_{today}.csv"
    df = pd.DataFrame([
        {"link": "A", "published_date_dt": today}
    ])
    df.to_csv(csv, index=False)
    
    # мокаем путь в конфиг
    monkeypatch.setattr("src.utils.get_today_processed_csv", lambda: str(csv))
    
    loaded = load_today_rows()
    assert isinstance(loaded, pd.DataFrame)
    assert loaded.shape[0] == 1
    assert loaded.iloc[0]["link"] == "A"
