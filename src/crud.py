from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from src.models import Vacancy

def insert_if_not_exists(db: Session, vacancy: dict):
    new_vacancy = Vacancy(**vacancy)
    try:
        db.add(new_vacancy)
        db.commit()
    except IntegrityError:
        db.rollback()

def get_unpublished(db: Session):
    return db.query(Vacancy).filter_by(published=False).all()

def mark_as_published(db: Session, link: str):
    db.query(Vacancy).filter_by(link=link).update({"published": True})
    db.commit()
