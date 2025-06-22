import json
from typing import List, Tuple

from fastapi import FastAPI
from nicegui import ui, events
from nicegui.events import UploadEventArguments

from app.config import load_config
from app.helpers import extract_variables, build_all_queries
from app.postgres_client.postgres_client import PostgresClient
from app.types import BenchmarkQuery
from mysql_client.mysql_client import MysqlClient

config = load_config()
db_clients = {}

if config.database.mysql.enabled:
    mysql_client = MysqlClient()
    db_clients["MySQL"] = mysql_client

if config.database.postgres.enabled:
    postgres_client = PostgresClient()
    db_clients["Postgres"] = postgres_client


result_table_columns = [
    {'name': 'database', 'label': 'Database', 'field': 'database', 'required': True},
    {'name': 'benchmark', 'label': 'Benchmark', 'field': 'benchmark', 'required': True},
    {'name': 'query', 'label': 'Query', 'field': 'query', 'required': True},
    {'name': 'runtime', 'label': 'Runtime', 'field': 'runtime', 'required': True},
]

result_table_rows = []

benchmark_query_list = []

query_table_columns = [
    {'name': 'name', 'label': 'Name', 'field': 'name', 'required': True},
    {'name': 'database', 'label': 'Database', 'field': 'database', 'required': True},
    {'name': 'benchmark', 'label': 'Benchmark', 'field': 'benchmark', 'required': True},
]

query_table_rows = []


@ui.page("/")
def main_page():
    def execute_query(query, db_type):
        """
        Execute given query
        """
        if db_type == "MySQL":
            _, time = mysql_client.execute_query(query)
        elif db_type == "Postgres":
            _, time = postgres_client.execute_query(query)
        else:
            time = 0
        table.add_row({"database": db_type, "benchmark": dropdown_bm.value, "query": "Query 1", "runtime": time})

    def save_query():
        """
        Save current query on SQL editor into query table
        """
        query = sql_editor.value
        parameters = extract_variables(query)
        print(parameters)
        benchmark_query = BenchmarkQuery(
            query=query,
            parameters=parameters,
            db=dropdown_db.value,
            benchmark=dropdown_bm.value,
            name=name_input.value
        )
        benchmark_query_list.append(benchmark_query)
        query_table.add_row({
            'name': benchmark_query.name,
            'database': benchmark_query.db,
            'benchmark': benchmark_query.benchmark,
            'query': benchmark_query.query,
            'parameters': benchmark_query.parameters
        })

    def update_code_block():
        """
        Updates SQL code inspection block
        """
        code_block.content = query_table.selected[0]["query"]
        code_block.update()
        variable_parameters.refresh()

    @ui.refreshable
    def variable_parameters():
        def execute_selected_query():
            """
            Executes selected query iterating over parameter range values
            """
            range_values = []
            for handle in var_input_handles:
                range_value_string = handle.value
                range_value = tuple(int(x.strip()) for x in range_value_string.split(","))
                range_values.append({'name': handle.label, 'range': range_value, 'type': 'INT'})
            query = query_table.selected[0]['query']
            db_type = query_table.selected[0]['database']
            print(query)
            queries = build_all_queries(query, range_values)
            print(queries)
            for q in queries:
                execute_query(q, db_type)

        with ui.card():
            ui.label("Range Values for Parameters")
            ui.label("Example: For a variable starting from 10 to 100 with increment 5 -> '10,100,5'")
            ui.label("For fixed value, use a single value like '10'")
            if query_table.selected:
                var_input_handles = []
                variables = query_table.selected[0]["parameters"]
                for variable in variables:
                    with ui.column():
                        var_name = variable.get("name")
                        ui.label(
                            f"Range values for Parameter: {var_name}, Type: {variable.get("data_type")}")
                        var_input = ui.input(label=var_name)
                        var_input_handles.append(var_input)
                ui.button("Start Query Execution", on_click=execute_selected_query)

    @ui.refreshable
    def database_info():
        if dropdown_bm.value and dropdown_db.value:
            ui.label(f"Server: {dropdown_db.value}")
            ui.label(f"Database: {dropdown_bm.value}")
            ui.label(f"Size of database: {db_clients[dropdown_db.value].get_size_of_database(dropdown_bm.value)} MB")

    def on_change_dropdown_bm():
        db_clients[db_list[0]].set_database(dropdown_db.value)
        database_info.refresh()

    def on_click_import_queries():
        """
        Callback for "Import Queries" button
        """
        ui.download.content(json.dumps([bq.to_dict() for bq in benchmark_query_list]))

    def on_upload_import_queries(e: events.UploadEventArguments):
        """
        Callback for "Import Queries" upload block
        """
        text = e.content.read().decode("utf-8")
        data = json.loads(text)
        benchmark_query_list.extend(data)
        for benchmark_query in benchmark_query_list:
            query_table.add_row(benchmark_query)
        ui.notify("Upload done")

    # UI code starts here
    with ui.row():
        with ui.column():
            with ui.grid(columns="300px auto auto auto auto"):
                # SQL Code editor
                with ui.card():
                    sql_editor = ui.codemirror()
                # Database select
                with ui.card():
                    with ui.row():
                        with ui.column():
                            db_list = list(db_clients.keys())
                            name_input = ui.input(label="Query Name")
                            ui.label("Server")
                            dropdown_db = ui.select(options=db_list, label="Server", value=db_list[0])
                            ui.label("Database")
                            current_db = dropdown_db.value
                            dropdown_bm = ui.select(options=db_clients[current_db].get_databases(), label="Database", on_change=on_change_dropdown_bm)
                            ui.button(text="Save Query", on_click=save_query)
                            database_info()
                        with ui.column():
                            ui.button("Delete Selected")
                            ui.button("Export Queries", on_click=on_click_import_queries)
                            ui.label("Import Queries")
                            ui.upload(on_upload=on_upload_import_queries).classes('w-[200px]')
                # Query template table
                query_table = ui.table(rows=query_table_rows, columns=query_table_columns, row_key='name', on_select=update_code_block)
                query_table.set_selection('single')
                # Query inspection
                with ui.card():
                    ui.label("Inspect Selected Query")
                    code_block = ui.code(language="sql").classes('w-full')
                # Variable values
                variable_parameters()

            # Query results table
            table = ui.table(columns=result_table_columns, rows=result_table_rows)
            ui.button(
                "Download Results",
                on_click=lambda: ui.download.content(json.dumps(result_table_rows), "results.json")
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