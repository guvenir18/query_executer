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

        self.addressbook_schema = config.database.addressbook.schema

    def start_connection(self) -> Connection:
        """
        Start a Postgres connection
        :return:
        """
        return psycopg.connect(
            f""
            f"host={config.database.postgres.host} "
            f"port={config.database.postgres.port} "
            f"dbname={config.database.postgres.name} "
            f"user={config.database.postgres.username} "
            f"password={config.database.postgres.password} "
        )

    def set_schema(self, schema: str):
        """
        Set database schema
        :param schema:
        :return:
        """
        self.cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        return self

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
