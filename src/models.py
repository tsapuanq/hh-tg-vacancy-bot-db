from sqlalchemy import Column, Integer, String, Boolean, Date, Text
from src.db import Base

class Vacancy(Base):
    __tablename__ = "vacancies"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String)
    company = Column(String)
    location = Column(String)
    city = Column(String)
    salary = Column(String)                    # строкой, чтобы не терять формат
    description = Column(Text)
    experience = Column(String)
    employment_type = Column(String)
    schedule = Column(String)
    working_hours = Column(String)
    work_format = Column(String)
    link = Column(String, unique=True)
    published_date = Column(String)            # если это строка вида "17 апреля"
    published_date_dt = Column(Date)           # если есть дата в формате YYYY-MM-DD
    skills = Column(Text)                      # список → строка через запятую
    published = Column(Boolean, default=False) # для Telegram фильтрации
    about_company = Column(Text, nullable=True)
    responsibilities = Column(Text, nullable=True)
    requirements = Column(Text, nullable=True)
