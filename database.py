# #database.py
# import psycopg2
# from psycopg2 import pool

# class Database:
#     def __init__(self, connection_string):
#         self.connection_pool = psycopg2.pool.SimpleConnectionPool(
#             1, 20, connection_string
#         )

#     def get_connection(self):
#         return self.connection_pool.getconn()

#     def return_connection(self, connection):
#         self.connection_pool.putconn(connection)

#     def close_all(self):
#         self.connection_pool.closeall()


# database.py
import psycopg2
from psycopg2 import pool
import logging # Добавлен для логирования ошибок инициализации
import os # Добавлен на случай, если DB_URL читается здесь

class Database:
    """
    Простой класс для управления пулом соединений с PostgreSQL.
    """
    def __init__(self, connection_string):
        if not connection_string:
             raise ValueError("Connection string cannot be empty")
        try:
            # Инициализация пула соединений
            # minconn: минимальное количество соединений в пуле
            # maxconn: максимальное количество соединений в пуле
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, 20, connection_string
            )
            logging.info("✅ Пул соединений к БД инициализирован.")
        except Exception as e:
            logging.error(f"❌ Ошибка при инициализации пула соединений к БД: {e}")
            # Важно возбудить исключение, чтобы приложение знало, что нет подключения к БД
            raise e

    def get_connection(self):
        """Получает соединение из пула."""
        try:
            return self.connection_pool.getconn()
        except Exception as e:
             logging.error(f"❌ Ошибка при получении соединения из пула: {e}")
             raise e # Перевыбрасываем исключение

    def return_connection(self, connection):
        """Возвращает соединение обратно в пул."""
        if connection: # Убедимся, что connection не None
            try:
                # Важно откатить любые незавершенные транзакции перед возвратом
                if hasattr(connection, 'rollback'):
                    connection.rollback()
                self.connection_pool.putconn(connection)
            except Exception as e:
                 # Эта ошибка может быть критичной, т.к. может привести к утечке соединений
                 logging.error(f"❌ Ошибка при возврате соединения в пул: {e}")
                 # Не перевыбрасываем здесь, чтобы не прерывать основной поток,
                 # но нужно мониторить такие ошибки.
        else:
            logging.warning(" attempted to return a None connection to the pool.")


    def close_all(self):
        """Закрывает все соединения в пуле."""
        logging.info("⏳ Закрытие всех соединений пула БД...")
        self.connection_pool.closeall()
        logging.info("✅ Соединения пула БД закрыты.")

# if __name__ == "__main__": удален