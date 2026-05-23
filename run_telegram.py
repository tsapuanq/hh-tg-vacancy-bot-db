import asyncio
import argparse
import logging
import os

from database import Database
from src.telegram_ingestor import run_telegram_ingestor
from src.utils import setup_logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Telegram source ingestion pipeline.")
    parser.add_argument(
        "--no-publish",
        action="store_true",
        help="Read source posts, write/process DB rows, but do not publish approved posts.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    setup_logger()
    args = parse_args()
    db = None
    try:
        db = Database(os.getenv("DATABASE_URL"))
        asyncio.run(run_telegram_ingestor(db, publish=not args.no_publish))
    except Exception as e:
        logging.critical("❌ Критическая ошибка Telegram source pipeline: %s", e, exc_info=True)
        raise
    finally:
        if db:
            db.close_all()
