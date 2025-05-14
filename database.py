import psycopg2
from psycopg2 import pool

class Database:
    def __init__(self, connection_string):
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            1, 20, connection_string
        )

    def get_connection(self):
        return self.connection_pool.getconn()

    def return_connection(self, connection):
        self.connection_pool.putconn(connection)

    def close_all(self):
        self.connection_pool.closeall()