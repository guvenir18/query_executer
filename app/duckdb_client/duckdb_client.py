import duckdb
import os
from typing import Optional, Tuple, List

from app.config import load_config

config = load_config()


class DuckDbClient:
    def __init__(self):
        self.conn = duckdb.connect(database=config.duckdb.path)
        self.cursor = self.conn.cursor()

    async def execute_query(self, query: str) -> Tuple[Optional[List[tuple]], float]:
        """
        Execute a SQL query and return (results, execution_time)
        """
        import time
        try:
            start_time = time.time()
            result = self.cursor.execute(query).fetchall()
            duration = time.time() - start_time
            return result, duration
        except Exception as e:
            print("Query Failed:", e)
            return None, 0

    async def analyze_query(self, query: str) -> Optional[List[tuple]]:
        """
        Run EXPLAIN ANALYZE and return the query plan.
        """
        try:
            return self.cursor.execute(f"EXPLAIN ANALYZE {query}").fetchall()
        except Exception as e:
            print("Query Analyze Failed:", e)
            return None

    async def set_database(self, db_path: str):
        """
        Switch to a different DuckDB file.
        """
        if not db_path.endswith(".duckdb"):
            db_path += ".duckdb"
        self.db_path = db_path
        self.conn = duckdb.connect(database=db_path)
        self.cursor = self.conn.cursor()

    async def get_size_of_database(self) -> float:
        """
        Return the size of the DuckDB file in megabytes.
        """
        if not os.path.exists(self.db_path):
            return 0.0
        size_bytes = os.path.getsize(self.db_path)
        return round(size_bytes / 1024 / 1024, 2)

    async def get_databases(self, directory: str = ".") -> List[str]:
        """
        List all `.duckdb` files in the given directory.
        """
        return [
            f for f in os.listdir(directory)
            if f.endswith(".duckdb") and os.path.isfile(os.path.join(directory, f))
        ]
