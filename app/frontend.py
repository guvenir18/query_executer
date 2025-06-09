import json

from fastapi import FastAPI
from nicegui import ui

from app.config import load_config
from app.postgres_client.postgres_client import PostgresClient
from mysql_client.mysql_client import MysqlClient

config = load_config()
db_clients = {}

if config.database.mysql.enabled:
    mysql_client = MysqlClient()
    db_clients["MySQL"] = mysql_client

if config.database.postgres.enabled:
    postgres_client = PostgresClient()
    db_clients["Postgres"] = postgres_client


table_columns = [
    {'name': 'database', 'label': 'Database', 'field': 'database', 'required': True},
    {'name': 'benchmark', 'label': 'Benchmark', 'field': 'benchmark', 'required': True},
    {'name': 'query', 'label': 'Query', 'field': 'query', 'required': True},
    {'name': 'runtime', 'label': 'Runtime', 'field': 'runtime', 'required': True},
]

table_rows = []


@ui.page("/")
def main_page():
    def execute_query():
        query = sql_editor.value
        db_type = dropdown_db.value
        if db_type == "MySQL":
            _, time = mysql_client.execute_query(query)
        elif db_type == "Postgres":
            _, time = postgres_client.execute_query(query)
        else:
            time = 0
        table.add_row({"database": db_type, "benchmark": dropdown_bm.value, "query": "Query 1", "runtime": time})

    with ui.row():
        sql_editor = ui.codemirror()
        table = ui.table(columns=table_columns, rows=table_rows)
        with ui.column():
            dropdown_db = ui.select(options=list(db_clients.keys()), value="MySQL")
            dropdown_bm = ui.select(options=["TPC-H", "TPC-DS"], value="TPC-H")
            ui.button("Execute Query", on_click=execute_query)
            ui.button(
                "Download Results",
                on_click=lambda: ui.download.content(json.dumps(table_rows), "results.json")
            )


def init(fastapi_app: FastAPI) -> None:
    """
    Initialize nicegui with a given FastAPI object
    :param fastapi_app:
    :return:
    """
    ui.run_with(
        fastapi_app,
    )