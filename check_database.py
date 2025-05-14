import psycopg2
from psycopg2 import Error
from datetime import datetime
import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env
load_dotenv()

# Получаем DATABASE_URL из .env
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не найден в .env. Проверь файл .env.")

try:
    # Подключаемся к базе данных
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    print("✅ Подключение к базе данных успешно установлено!")

    # Проверяем существование таблицы vacancies
    cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'vacancies')")
    table_exists = cursor.fetchone()[0]
    print("Таблица vacancies существует:", table_exists)

    if not table_exists:
        print("⚠️ Таблица vacancies не найдена. Создаем таблицу...")
        cursor.execute("""
            CREATE TABLE vacancies (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                description TEXT,
                cleaned_description TEXT,
                is_relevant BOOLEAN DEFAULT FALSE,
                summary_duties TEXT,
                summary_requirements TEXT,
                summary_company TEXT,
                url VARCHAR(255) UNIQUE NOT NULL,
                published_at TIMESTAMP,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sent_to_telegram BOOLEAN DEFAULT FALSE,
                location VARCHAR(255),
                salary_range VARCHAR(255),
                skills TEXT,
                published_date_dt VARCHAR(255),
                work_format VARCHAR(255),
                working_hours VARCHAR(255),
                company VARCHAR(255),
                salary VARCHAR(255),
                experience VARCHAR(255),
                employment_type VARCHAR(255),
                schedule VARCHAR(255)
            );
        """)
        conn.commit()
        print("✅ Таблица vacancies успешно создана!")

    # Вставляем тестовую запись
    test_url = "https://test.com/vacancy/123"
    cursor.execute("SELECT COUNT(*) FROM vacancies WHERE url = %s", (test_url,))
    if cursor.fetchone()[0] == 0:
        cursor.execute("""
            INSERT INTO vacancies (title, description, url, published_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
        """, ("Тестовая вакансия", "Описание тестовой вакансии", test_url, datetime.now()))
        conn.commit()
        print("✅ Тестовая запись успешно добавлена!")

    # Читаем тестовую запись
    cursor.execute("SELECT id, title, url FROM vacancies WHERE url = %s", (test_url,))
    result = cursor.fetchone()
    if result:
        print("✅ Тестовая запись найдена:", result)
    else:
        print("⚠️ Тестовая запись не найдена. Проверь таблицу.")

except (Exception, Error) as error:
    print(f"❌ Ошибка при работе с базой данных: {error}")
    if 'conn' in locals():
        conn.close()
    raise

finally:
    if 'conn' in locals() and conn:
        cursor.close()
        conn.close()
        print("🔚 Соединение с базой данных закрыто.")