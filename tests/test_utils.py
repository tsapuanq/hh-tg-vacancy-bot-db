# tests/test_utils.py
import pandas as pd
import tempfile
from pathlib import Path

import pytest

from src.utils import canonical_link, load_existing_links, save_to_main_csv

def test_canonical_link_variants():
    raw = [
        "https://hh.kz/vacancy/123?query=foo",
        "https://hh.kz/vacancy/456",
        "https://hh.kz/vacancy/789?param=1&other=2"
    ]
    expected = [
        "https://hh.kz/vacancy/123",
        "https://hh.kz/vacancy/456",
        "https://hh.kz/vacancy/789"
    ]
    assert [canonical_link(u) for u in raw] == expected

def test_load_existing_links_empty(tmp_path):
    # если файла нет, должен вернуть пустое множество
    assert load_existing_links(str(tmp_path/"no.csv")) == set()

def test_load_existing_links_after_save(tmp_path):
    # готовим CSV с «сырыми» ссылками
    df = pd.DataFrame([{"link": "https://hh.kz/vacancy/1?x=1"}])
    csv = tmp_path/"vacancies.csv"
    df.to_csv(csv, index=False)

    # load_existing_links должен вернуть канонический URL
    links = load_existing_links(str(csv))
    assert links == {"https://hh.kz/vacancy/1"}

def test_save_to_main_csv_deduplicates(tmp_path):
    # 1) создаём накопительный CSV
    old = pd.DataFrame([{"link": "https://hh.kz/vacancy/1?x=1", "foo": "A"}])
    csv = tmp_path/"vacancies.csv"
    old.to_csv(csv, index=False)

    # 2) новые данные: тот же ID под другим query + новый
    new = [
        {"link": "https://hh.kz/vacancy/1?y=2", "foo": "A"},
        {"link": "https://hh.kz/vacancy/2?foo=bar", "foo": "B"}
    ]
    save_to_main_csv(new, str(csv))

    df = pd.read_csv(csv)
    # дубли по vacancy 1 должны отсеяться, останутся только канонические 1 и 2
    assert set(df["link"]) == {"https://hh.kz/vacancy/1", "https://hh.kz/vacancy/2"}
    assert df.shape[0] == 2
