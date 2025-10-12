import sys, os

# 🧭 Добавляем путь к проекту, чтобы видеть src/
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(project_root)

# теперь импортируем
from src.cleaning import parse_russian_date, extract_city, normalize_city_name

# 🔍 пример текста с hh
html_text = "Вакансия опубликована 11 октября 2025 в Алматы"

# вытащим часть, как это делает scraper
raw_date = "11 октября 2025"
raw_city = extract_city(html_text)
normalized_city = normalize_city_name(raw_city)
parsed_date = parse_russian_date(raw_date)

print("🧩 RAW date:", raw_date)
print("📅 Parsed date:", parsed_date)
print("🏙️ Extracted city:", raw_city)
print("✅ Normalized city:", normalized_city)