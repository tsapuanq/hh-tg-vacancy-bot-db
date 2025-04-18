from src.db import Base, engine
from src.models import Vacancy

def init_db():
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    init_db()
