import re
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

    def analyze_query(self, query: str):
        try:
            self.cursor.execute(f"EXPLAIN ANALYZE {query}")
            results = self.cursor.fetchone()
            return results
        except Exception as e:
            print("Query Analyze Failed: ", e)

    def set_database(self, database_name: str):
        """
        Set database
        :param database_name:
        :return:
        """
        self.cursor.execute(f"USE {database_name};")

    def get_size_of_database(self, database: str):
        """
        Get size of current selected database
        """
        query = f"""
            SELECT 
                ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS size_mb
            FROM 
                information_schema.tables
            WHERE 
                table_schema = "{database}"
            GROUP BY 
                table_schema;
        """
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def get_databases(self):
        """
        Returns all databases from a MySQL connection
        """
        system_dbs = {'information_schema', 'mysql', 'performance_schema', 'sys'}  # Filter out internal MYSQL databases
        query = "SHOW DATABASES;"
        self.cursor.execute(query)
        return [row[0] for row in self.cursor.fetchall() if row[0] not in system_dbs]

    def parse_explain_output(self, explain_result: str):
        """
        Parse output of EXPLAIN ANALYZE query and extract total execution time and rows affected
        """
        text = explain_result.replace('\n', ' ')

        total_time_match = re.search(
            r'->\s*Sort:.*?\(actual time=\d+\.?\d*\.\.(\d+\.?\d*)', text)

        filter_match = re.search(
            r'->\s*Filter:.*?actual time=\d+\.?\d*\.\.\d+\.?\d*\s+rows=([\d\.eE\+]+)', text)

        total_time = float(total_time_match.group(1)) if total_time_match else None
        filter_rows = float(filter_match.group(1)) if filter_match else None

        return total_time, filter_rows
