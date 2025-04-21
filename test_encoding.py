from src.utils import clean_text_safe

samples = [
    "Обычный текст",
    b"Invalid \xdd byte".decode("utf-8", "replace"),
    "Текст\xa0с\xa0пробелами",
    None,
    12345,
]

for s in samples:
    print("⏺️ BEFORE:", s)
    print("✅ AFTER:", clean_text_safe(s))
    print()
