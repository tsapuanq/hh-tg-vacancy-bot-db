import sys
import os

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
print(f"Добавлен путь: {ROOT_DIR}")

import logging
import psycopg2.extras
from database import Database
from analytics.main_script_normalizing import JobTitleNormalizer

def normalize_titles_in_db(db: Database):
    """
    Обновляет general_title, category и level в таблице vacancies.
    """
    norm = JobTitleNormalizer()

    conn = db.get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    logging.info("🔍 Загружаем вакансии без нормализации...")
    cursor.execute("""
        SELECT id, title
        FROM vacancies
        WHERE general_title IS NULL
          AND title IS NOT NULL
          AND TRIM(title) <> '';
    """)
    rows = cursor.fetchall()
    logging.info(f"📦 Найдено {len(rows)} вакансий для нормализации")

    updated = 0
    for row in rows:
        vid = row["id"]
        title = row["title"]
        normalized = norm.normalize(title)

        cursor.execute("""
            UPDATE vacancies
            SET general_title = %s,
                category = %s,
                level = %s
            WHERE id = %s;
        """, (
            normalized["general_title"],
            normalized["category"],
            normalized["level"],
            vid
        ))

        updated += 1
        if updated % 100 == 0:
            conn.commit()
            logging.info(f"✅ Обновлено {updated} вакансий")

    conn.commit()
    cursor.close()
    db.return_connection(conn)
    logging.info(f"🏁 Готово! Обновлено {updated} вакансий")


if __name__ == "__main__":
    from src.config import DATABASE_URL
    db = Database(DATABASE_URL)
    normalize_titles_in_db(db)
