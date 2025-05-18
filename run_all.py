# #run_all.py
# import asyncio
# import logging
# from src.run_scraper import run_scraper
# from src.cleaning import run_cleaning_pipeline
# from src.publisher import run_publisher
# from src.utils import setup_logger, determine_mode
# from database import Database
# import os

# if __name__ == "__main__":
#     setup_logger()

#     db = Database(os.getenv("DATABASE_URL"))
#     mode = determine_mode()
#     logging.info(f"[MODE] Выбран режим: {mode.upper()}")

#     logging.info("[STEP 1] Scraping raw data...")
#     asyncio.run(run_scraper(db, mode=mode))

#     logging.info("[STEP 2] Cleaning data...")
#     run_cleaning_pipeline(db)

#     logging.info("[STEP 3] Publishing to Telegram...")
#     run_publisher(db)

#     logging.info("✅ Pipeline completed successfully!")


# run_all.py
import asyncio
import logging
# Импортируем функции запуска основных этапов
from src.run_scraper import run_scraper
# run_cleaning_pipeline удален из cleaning.py, поэтому импорт убран
from src.publisher import run_publisher
# Импортируем утилиты
from src.utils import setup_logger, determine_mode
# Импортируем класс Database
from database import Database
import os

if __name__ == "__main__":
    setup_logger() # Настраиваем логгер один раз при старте

    db = None # Инициализируем None на случай ошибки подключения
    try:
        logging.info("🚀 Запуск основного пайплайна...")
        # Инициализация пула соединений к базе данных
        db = Database(os.getenv("DATABASE_URL"))

        mode = determine_mode() # Определяем режим работы (пока всегда 'daily')
        logging.info(f"[MODE] Выбран режим: {mode.upper()}")

        logging.info("[STEP 1/2] Scraping raw data...")
        # Запускаем скрейпер. Он теперь также выполняет первичную очистку и вставку в БД.
        asyncio.run(run_scraper(db, mode=mode))

        # Шаг 2 (очистка) удален из общего пайплайна.
        # LLM-обработка (фильтрация и суммаризация) интегрирована в publisher.

        logging.info("[STEP 2/2] Publishing to Telegram...")
        # Запускаем публикатор. Он выполняет LLM-обработку и отправку в Telegram.
        run_publisher(db)

        logging.info("✅ Pipeline completed successfully!")

    except Exception as e:
        logging.critical(f"❌ Критическая ошибка выполнения пайплайна: {e}", exc_info=True) # Логируем ошибку и traceback
        # В зависимости от ошибки, возможно, потребуется дополнительная обработка или уведомление.

    finally:
        # Гарантированное закрытие пула соединений БД при завершении
        if db:
            db.close_all()
            logging.info("🔌 Соединения БД закрыты.")