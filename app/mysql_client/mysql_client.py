import time

import mysql.connector

from app.config import load_config

config = load_config()


class MysqlClient:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host=config.database.mysql.host,
            user=config.database.mysql.user,
            password=config.database.mysql.password,
            database=config.database.mysql.db,
        )
        self.cursor = self.conn.cursor()

    def execute_query(self, query: str):
        """
        Execute given query
        :param query:
        :return:
        """
        try:
            start_time = time.time()
            self.cursor.execute(query)
            end_time = time.time()
            results = self.cursor.fetchall()
            return results, (end_time - start_time)
        except Exception as e:
            print("Query Failed: ", e)

    def switch_database(self, database_name: str):
        """
        Switch database
        :param database_name:
        :return:
        """
        self.cursor.execute(f"USE {database_name};")

    def get_tpch_table_size(self):
        query = """
            SELECT 
                ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS size_mb
            FROM 
                information_schema.tables
            WHERE 
                table_schema = 'tpch'
            GROUP BY 
                table_schema;
        """
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]
