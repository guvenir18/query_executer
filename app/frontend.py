import asyncio
import json

from fastapi import FastAPI
from nicegui import ui, events

from app.async_queue import QueueWorker
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
    {'name': 'id', 'label': 'Id', 'field': 'id', 'required': True},
    {'name': 'server', 'label': 'Server', 'field': 'server', 'required': True},
    {'name': 'database', 'label': 'Database', 'field': 'database', 'required': True},
    {'name': 'query', 'label': 'Query', 'field': 'query', 'required': True},
    {'name': 'download', 'label': 'Download', 'field': 'download', 'required': True},
]

result_table_rows = []

benchmark_query_list = []

query_table_columns = [
    {'name': 'name', 'label': 'Name', 'field': 'name', 'required': True},
    {'name': 'database', 'label': 'Database', 'field': 'database', 'required': True},
    {'name': 'benchmark', 'label': 'Benchmark', 'field': 'benchmark', 'required': True},
]

query_table_rows = []

batch_results = []

@ui.page("/")
def main_page():
    total_executed_batch = 0
    queries_in_queue = 0

    async def execute_query_batch(queries, query_template):
        global batch_results
        nonlocal total_executed_batch
        nonlocal queries_in_queue
        db_type = query_template['database']
        query_results = []
        print(f"Starting Query Batch {total_executed_batch}")
        i = 0
        for q in queries:

            var_data = q.variables
            result = await execute_query(q.query, db_type)
            query_results.append(
                {
                    'server': db_type,
                    'database': query_template['benchmark'],
                    'query': query_template['name'],
                    'result': result,
                    'var_1': var_data[0]['name'] if len(var_data) > 0 and 'name' in var_data[0] else '',
                    'val_1': var_data[0]['value'] if len(var_data) > 0 and 'value' in var_data[0] else '',
                    'var_2': var_data[1]['name'] if len(var_data) > 1 and 'name' in var_data[1] else '',
                    'val_2': var_data[1]['value'] if len(var_data) > 1 and 'value' in var_data[1] else '',
                    'var_3': var_data[2]['name'] if len(var_data) > 2 and 'name' in var_data[2] else '',
                    'val_3': var_data[2]['value'] if len(var_data) > 2 and 'value' in var_data[2] else '',
                }
            )
            print(f"Query N:[{i}/{len(queries)}] Done")
            queue_information.refresh(i, len(queries), True)
            i = i + 1
        result_table.add_row(
            {
                'id': total_executed_batch,
                'server': db_type,
                'database': query_template['benchmark'],
                'query': query_template['name'],
            }
        )
        batch_results.append(query_results)
        total_executed_batch += 1
        queries_in_queue -= 1
        queue_information.refresh(0, 0, False)

    async def execute_query(query, db_type):
        def run_sync():
            result = None
            if db_type == "MySQL":
                result = mysql_client.analyze_query(query)
            elif db_type == "Postgres":
                _, duration = postgres_client.execute_query(query)
            else:
                result = None
            return result

        return await asyncio.to_thread(run_sync)

    query_worker = QueueWorker(execute_query_batch)

    def on_click_save_query():
        """
        Save current query on SQL editor into query table
        """
        query = sql_editor.value
        parameters = extract_variables(query)
        print(parameters)
        benchmark_query = BenchmarkQuery(
            query=query,
            parameters=parameters,
            database=dropdown_db.value,
            benchmark=dropdown_bm.value,
            name=name_input.value
        )
        benchmark_query_list.append(benchmark_query)
        new_query = {
            'name': benchmark_query.name,
            'database': benchmark_query.database,
            'benchmark': benchmark_query.benchmark,
            'query': benchmark_query.query,
            'parameters': benchmark_query.parameters
        }
        add_query_to_table(new_query)

    def add_query_to_table(new_query):
        """
        Add a new query to the table
        """
        if any(query['name'] == new_query['name'] for query in query_table.rows):
            ui.notify(f"Query with name {new_query['name']} already exists")
            return

        query_table.add_row(new_query)

    def update_code_block():
        """
        Updates SQL code inspection block
        """
        code_block.content = query_table.selected[0]["query"]
        code_block.update()
        variable_parameters.refresh()

    @ui.refreshable
    def variable_parameters():
        def on_click_start_query_execution():
            """
            Executes selected query iterating over parameter range values
            """
            nonlocal queries_in_queue
            range_values = []
            for handle in var_input_handles:
                range_value_string = handle.value
                range_value = tuple(int(x.strip()) for x in range_value_string.split(","))
                range_values.append({'name': handle.label, 'range': range_value, 'type': 'INT'})
            query_template = query_table.selected[0]
            query = query_template['query']
            db_type = query_template['database']
            queries = build_all_queries(query, range_values)
            query_worker.schedule_callback(queries, query_template)
            print("Query added to queue")
            queries_in_queue += 1
            queue_information.refresh(0, 0, False)

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
                ui.button("Start Query Execution", on_click=on_click_start_query_execution)

    @ui.refreshable
    def database_info():
        if dropdown_bm.value and dropdown_db.value and False:
            ui.label(f"Server: {dropdown_db.value}")
            ui.label(f"Database: {dropdown_bm.value}")
            ui.label(f"Size of database: {db_clients[dropdown_db.value].get_size_of_database(dropdown_bm.value)} MB")

    def on_change_dropdown_bm():
        current_db = dropdown_db.value
        return
        db_clients[current_db].set_database(dropdown_bm.value)
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
        data_list = json.loads(text)
        for data in data_list:
            benchmark_query_list.append(BenchmarkQuery.from_dict(data))
            add_query_to_table(data)
        ui.notify("Upload done")

    def on_change_dropdown_db():
        """
        Callback for when database server changes (postgres or mysql)
        """
        current_db = dropdown_db.value
        return
        new_bm_list = db_clients[current_db].get_databases()
        dropdown_bm.options = new_bm_list
        dropdown_bm.value = new_bm_list[0] if len(new_bm_list) > 0 else None
        dropdown_bm.update()

    def on_row_download_result(msg):
        """
        Callback for download button of row
        """
        id = msg.args['key']
        row = msg.args['row']
        server = row["server"]
        db = row["database"]
        q = row["query"]
        result =  batch_results[id][0]
        result["result"] = str(result["result"])
        print(result)
        json_file = json.dumps(result)

        ui.download.content(json_file, f"{server}_{db}_{q}_{id}.json")

    @ui.refreshable
    def queue_information(i=0, total=0, executing=False):
        ui.label("Queue Information")
        ui.label(f"Query batch in queue: {queries_in_queue}")
        if executing:
            ui.label(f"Query Completed N:{i+1}/{total}")
        else:
            ui.label("No queries are executing currently")

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
                            name_input = ui.input(label="Query Name")
                            db_list = list(db_clients.keys())
                            ui.label("Server")
                            dropdown_db = ui.select(options=db_list, label="Server",
                                                    value=db_list[0],
                                                    on_change=on_change_dropdown_db)
                            current_db = dropdown_db.value
                            bm_list = []
                            dropdown_bm = ui.select(options=bm_list, label="Database", value=bm_list[0] if len(bm_list) > 0 else None, on_change=on_change_dropdown_bm)
                            ui.button(text="Save Query", on_click=on_click_save_query)
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
            with ui.row():
                with ui.card():
                    ui.label("Query Execution Results")
                    result_table = ui.table(columns=result_table_columns, rows=result_table_rows, row_key='id')
                    result_table.add_slot(
                        "body-cell-download",
                        """
                        <q-td :props="props">
                            <q-btn @click="$parent.$emit('action', props)" icon="download" flat />
                        </q-td>
                    """,
                    )
                    result_table.on("action", on_row_download_result)
                with ui.card():
                    queue_information()


def init(fastapi_app: FastAPI) -> None:
    """
    Initialize nicegui with a given FastAPI object
    :param fastapi_app:
    :return:
    """
    ui.run_with(
        fastapi_app,
    )