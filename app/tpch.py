from app.config import load_config

config = load_config()


class Tpch:
    def __init__(self):
        self.tpch_path = config.tpch.path

    def generate_tables(self):
        pass
    def get_table_paths(self):
        pass
