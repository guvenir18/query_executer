import time

import psycopg
from psycopg import Connection, sql

from app.config import load_config

config = load_config()


class PostgresClient:
    """
    This class is used to communicated with Postgres
    """
    def __init__(self):
        """
        Initialize a PostgresClient object
        """

        self.conn = self.start_connection()
        self.cur = self.conn.cursor()

    def start_connection(self) -> Connection:
        """
        Start a Postgres connection
        :return:
        """
        return psycopg.connect(
            f""
            f"host={config.database.postgres.host} "
            f"port={config.database.postgres.port} "
            f"dbname={config.database.postgres.database} "
            f"user={config.database.postgres.username} "
            f"password={config.database.postgres.password} "
        )

    def execute_query(self, query: str):
        """
        Execute given query
        :param query: SQL query to execute
        :return: results and time taken
        """
        try:
            start_time = time.time()
            self.cur.execute(query)
            end_time = time.time()
            results = self.cur.fetchall()
            return results, (end_time - start_time)
        except Exception as e:
            print("Query failed: ", e)
            self.conn.rollback()

    def set_database(self, database_name: str):
        """
        Set (switch to) another Postgres database
        """
        self.cur.close()
        self.conn.close()
        self.conn = psycopg.connect(
            f"" 
            f"host={config.database.postgres.host} "
            f"port={config.database.postgres.port} "
            f"dbname={database_name} "
            f"user={config.database.postgres.username} "
            f"password={config.database.postgres.password} "
        )
        self.cur = self.conn.cursor()

    def get_size_of_database(self, database: str):
        """
        Get size (in MB) of a given PostgreSQL database
        """
        query = f"""
            SELECT ROUND(pg_database_size(%s) / 1024 / 1024, 2) AS size_mb;
        """
        self.cur.execute(query, (database,))
        result = self.cur.fetchone()
        return result[0] if result else 0.0

    def get_databases(self):
        """
        Returns all user-created databases from a Postgres connection
        """
        system_dbs = {'template0', 'template1', 'postgres'}
        query = "SELECT datname FROM pg_database WHERE datistemplate = false;"
        self.cur.execute(query)
        return [row[0] for row in self.cur.fetchall() if row[0] not in system_dbs]
