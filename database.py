# database.py
import psycopg2
from psycopg2 import pool
import logging  


class Database:
    """
    Простой класс для управления пулом соединений с PostgreSQL.
    """

    def __init__(self, connection_string):
        if not connection_string:
            raise ValueError("Connection string cannot be empty")
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, 20, connection_string
            )
            logging.info("✅ Пул соединений к БД инициализирован.")
        except Exception as e:
            logging.error(f"❌ Ошибка при инициализации пула соединений к БД: {e}")
            raise e

    def get_connection(self):
        """Получает соединение из пула."""
        try:
            return self.connection_pool.getconn()
        except Exception as e:
            logging.error(f"❌ Ошибка при получении соединения из пула: {e}")
            raise e  

    def return_connection(self, connection):
        """Возвращает соединение обратно в пул."""
        if connection:  
            try:
                if hasattr(connection, "rollback"):
                    connection.rollback()
                self.connection_pool.putconn(connection)
            except Exception as e:
                logging.error(f"❌ Ошибка при возврате соединения в пул: {e}")
        else:
            logging.warning(" attempted to return a None connection to the pool.")

    def close_all(self):
        """Закрывает все соединения в пуле."""
        logging.info("⏳ Закрытие всех соединений пула БД...")
        self.connection_pool.closeall()
        logging.info("✅ Соединения пула БД закрыты.")