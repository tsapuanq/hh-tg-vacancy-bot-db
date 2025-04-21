from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from src.models import Vacancy
from src.utils import clean_text_safe
import logging

# def insert_if_not_exists(db: Session, vacancy: dict):
#     if not isinstance(vacancy, dict):
#         logging.warning("[DB] Vacancy is not a dict, skipping")
#         return

#     for key in [
#         "title", "company", "location", "description",
#         "skills", "work_format", "experience", "employment_type",
#         "about_company", "responsibilities", "requirements"
#     ]:
#         if key in vacancy:
#             vacancy[key] = clean_text_safe(vacancy[key])

def insert_if_not_exists(db: Session, vacancy: dict):
    from src.utils import clean_text_safe

    # Удаляем потенциально проблемные поля от LLM
    for key in ["about_company", "responsibilities", "requirements"]:
        if key in vacancy:
            del vacancy[key]

    # Очищаем остальные поля
    for key in [
        "title", "company", "location", "description", "skills", "work_format",
        "experience", "employment_type"
    ]:
        if key in vacancy:
            vacancy[key] = clean_text_safe(vacancy[key])

    try:
        db.add(Vacancy(**vacancy))
        db.commit()
    except Exception as e:
        db.rollback()
        logging.error(f"[DB] Unexpected error: {e}")

    try:
        db.add(Vacancy(**vacancy))
        db.commit()
    except IntegrityError as e:
        logging.warning(f"[DB] Integrity error: {e}")
        db.rollback()
    except Exception as e:
        logging.error(f"[DB] Unexpected error: {e}")
        db.rollback()


def get_unpublished(db: Session):
    return db.query(Vacancy).filter_by(published=False).all()

def mark_as_published(db: Session, link: str):
    db.query(Vacancy).filter_by(link=link).update({"published": True})
    db.commit()
