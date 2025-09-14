import asyncio
from asyncio import Queue
from typing import Dict, List, Callable

from psycopg_pool import AsyncConnectionPool

from app.analyze_parsers import parse_analyze_mysql, extract_total_runtime, extract_runtime_and_filter_scans_duckdb, \
    extract_runtime_and_filter_scans_postgres
from app.config import load_config
from app.duckdb_client.duckdb_client import DuckDbClient
from app.helpers import build_all_queries
from app.mysql_client.async_mysql_client import AsyncMysqlClient
from app.mysql_client.create_pool import create_mysql_pool
from app.mysql_client.mysql_client import MysqlClient
from app.postgres_client.async_postgres_client import AsyncPostgresClient
from app.postgres_client.create_pool import create_postgres_pool
from app.postgres_client.postgres_client import PostgresClient
from app.types import BenchmarkQuery, ReadyQuery

config = load_config()


def start_db_connections():
    clients: Dict[str, MysqlClient | PostgresClient | DuckDbClient] = {}

    if config.database.mysql.enabled:
        mysql_client = MysqlClient()
        clients["MySQL"] = mysql_client

    if config.database.postgres.enabled:
        postgres_client = PostgresClient()
        clients["Postgres"] = postgres_client

    if config.database.duckdb.enabled:
        duckdb_client = DuckDbClient()
        clients["DuckDB"] = duckdb_client

    return clients


def get_min_max_of_column(client: MysqlClient, column: str):
    column_lower = column.lower()
    tables = client.get_table_list()
    for table in tables:
        column_list = client.get_column_list_of_table(table)
        normalized_columns = [col.lower() for col in column_list]
        if column_lower in normalized_columns:
            actual_column = column_list[normalized_columns.index(column_lower)]
            return client.get_min_max_of_column(table, actual_column)
    return 0, 0


class DatabaseQueueWorker:
    """
    Worker to execute queries asynchronously with 4 parallel workers,
    each using its own MysqlClient instance from a shared aiomysql pool.
    """

    def __init__(self, callback: Callable, num_workers: int = 5):
        self.callback = callback

        self.mysql_queue = Queue()
        self.postgres_queue = Queue()
        self.duckdb_queue = Queue()

        self.mysql_pool = None
        self.postgres_pool = None

        self.num_workers = num_workers
        self.postgres_in_progress = False
        self.duckdb_in_progress = False

        self.semaphore = asyncio.Semaphore(self.num_workers)

        asyncio.create_task(self.dispatch_loop())

    async def init(self):
        """
        Initializes the async connection pools.
        """
        self.mysql_pool = await create_mysql_pool()
        self.postgres_pool = create_postgres_pool()
        return self

    async def dispatch_loop(self):
        while True:
            if not self.mysql_queue.empty():
                queries, benchmark_query = await self.mysql_queue.get()
                _ = asyncio.create_task(self.run_mysql_task(queries, benchmark_query))

            if not self.postgres_queue.empty() and not self.postgres_in_progress:
                self.postgres_in_progress = True
                queries, benchmark_query = await self.postgres_queue.get()
                _ = asyncio.create_task(self.run_postgres_task(queries, benchmark_query))

            if not self.duckdb_queue.empty() and not self.duckdb_in_progress:
                self.duckdb_in_progress = True
                queries, benchmark_query = await self.duckdb_queue.get()
                _ = asyncio.create_task(self.run_duckdb_task(queries, benchmark_query))

            await asyncio.sleep(0.05)  # Avoid busy loop

    async def run_mysql_task(self, queries, benchmark_query):
        async with self.semaphore:
            try:
                async with self.mysql_pool.acquire() as conn:
                    client = await AsyncMysqlClient.create(conn)
                    await self.callback(queries, benchmark_query, client)
            except Exception as e:
                print("[MySQL] Error:", e)

    async def run_postgres_task(self, queries, benchmark_query):
        async with self.semaphore:
            try:
                async with self.postgres_pool.connection() as conn:
                    client = AsyncPostgresClient(conn)
                    await self.callback(queries, benchmark_query, client)
            except Exception as e:
                print("[Postgres] Error:", e)
            finally:
                self.postgres_in_progress = False

    async def run_duckdb_task(self, queries, benchmark_query):
        async with self.semaphore:
            try:
                client = DuckDbClient()
                await self.callback(queries, benchmark_query, client)
            except Exception as e:
                print("[DuckDb] Error:", e)
            finally:
                self.duckdb_in_progress = False

    def schedule_callback(self, queries, benchmark_query: BenchmarkQuery):
        db_type = benchmark_query.database
        if db_type == "MySQL":
            self.mysql_queue.put_nowait((queries, benchmark_query))
        elif db_type == "Postgres":
            self.postgres_queue.put_nowait((queries, benchmark_query))
        elif db_type == "DuckDB":
            self.duckdb_queue.put_nowait((queries, benchmark_query))
        else:
            print("Unknown database type:", db_type)


class ResultStorage:
    """
    In-memory storage for query results
    """
    def __init__(self):
        self.raw_result_list = []
        self.parsed_result_list = []
        self.lock = asyncio.Lock()


class BackendService:
    """
    Backend to handle query operations and result parsing
    """
    def __init__(self, callback_table_update=None):
        self.clients = start_db_connections()
        self.queue_worker = None
        self.result_storage = ResultStorage()
        self.callback_table_update = callback_table_update

    async def initialize_queue_worker(self):
        self.queue_worker = DatabaseQueueWorker(self.execute_query_batch)
        await self.queue_worker.init()

    def set_table_update_callback(self, callback):
        self.callback_table_update = callback

    async def execute_query_batch(self, queries: List[ReadyQuery], benchmark_query: BenchmarkQuery, client: AsyncMysqlClient):
        # Execute prepared queries and write results into storage
        print("Starting query batch execution")
        db_type = benchmark_query.database
        result_list = []
        parsed_result_list = []
        i = 1
        for ready_query in queries:
            try:
                query = ready_query.query
                result = await client.analyze_query(query)
                formatted_result = await self._process_result(result, ready_query, benchmark_query)
                result_list.append(result)
                parsed_result_list.append(formatted_result)
                print(f"{db_type} Query Completed {i}/{len(queries)}")
            except Exception as e:
                print(f"Error: {db_type} Query {i}/{len(queries)}")
                print("Error: ", e)
            finally:
                i += 1
        # For multithreaded solution
        async with self.result_storage.lock:
            print("Acquire lock")
            self.result_storage.parsed_result_list.append(parsed_result_list)
            self.result_storage.raw_result_list.append(result_list)
            # Maybe there is a better way to make a call for table update ?
            self.callback_table_update(benchmark_query)

    async def schedule_query_exectution(self, benchmark_query: BenchmarkQuery, range_values):
        queries = build_all_queries(benchmark_query.query, range_values)
        self.queue_worker.schedule_callback(queries, benchmark_query)
        print("Scheduled Query: ", benchmark_query.name)

    async def _process_result(self, result, ready_query: ReadyQuery, benchmark_query: BenchmarkQuery):
        """
        Process single query result, extract runtime and rows executed and format result
        """
        try:
            var_data = ready_query.variables
            var_list = [var['name'] for var in var_data]
            db_type = benchmark_query.database
            benchmark = benchmark_query.benchmark
            name = benchmark_query.name
            if benchmark_query.database == "MySQL":
                parsed_result = parse_analyze_mysql(result, var_list)
                total_runtime = extract_total_runtime(result)
            elif benchmark_query.database == "Postgres":
                postgres_parsed = extract_runtime_and_filter_scans_postgres(result, var_list)
                total_runtime = postgres_parsed["total_runtime"]
                parsed_result = postgres_parsed["filters"]
            elif benchmark_query.database == "DuckDB":
                duck_parsed = extract_runtime_and_filter_scans_duckdb(result, var_list)
                total_runtime = duck_parsed["total_runtime"]
                parsed_result = duck_parsed["filters"]
            else:
                total_runtime = 0

            formatted_result = {
                'server': db_type,
                'database': benchmark,
                'query': name,
                'runtime': total_runtime,
                'filter_1': var_data[0]['name'] if len(var_data) > 0 and 'name' in var_data[0] else '',
                'val_1': var_data[0]['value'] if len(var_data) > 0 and 'value' in var_data[0] else '',
                'rows_1': parsed_result[0]['total_rows'] if len(var_data) > 0 and parsed_result[0]['variable'] == var_data[0]['name'] else '',
                'filter_2': var_data[1]['name'] if len(var_data) > 1 and 'name' in var_data[1] else '',
                'val_2': var_data[1]['value'] if len(var_data) > 1 and 'value' in var_data[1] else '',
                'rows_2': parsed_result[1]['total_rows'] if len(var_data) > 1 and parsed_result[1]['variable'] == var_data[1]['name'] else '',
                'filter_3': var_data[2]['name'] if len(var_data) > 2 and 'name' in var_data[2] else '',
                'val_3': var_data[2]['value'] if len(var_data) > 2 and 'value' in var_data[2] else '',
                'rows_3': parsed_result[2]['total_rows'] if len(var_data) > 2 and parsed_result[2]['variable'] == var_data[2]['name'] else '',
            }
            return formatted_result
        except Exception as e:
            raise e
