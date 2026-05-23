import asyncio
import logging
import os

from database import Database
from src.telegram_ingestor import run_telegram_ingestor
from src.utils import setup_logger


if __name__ == "__main__":
    setup_logger()
    db = None
    try:
        db = Database(os.getenv("DATABASE_URL"))
        asyncio.run(run_telegram_ingestor(db))
    except Exception as e:
        logging.critical("❌ Критическая ошибка Telegram source pipeline: %s", e, exc_info=True)
        raise
    finally:
        if db:
            db.close_all()
