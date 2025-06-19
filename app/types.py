from dataclasses import dataclass
from typing import List

@dataclass
class QueryParameter:
    name: str
    data_type: str


class BenchmarkQuery:
    def __init__(self, query: str, parameters: List[QueryParameter], db: str, benchmark: str, name: str):
        self.query = query
        self.parameters = parameters
        self.db = db
        self.name = name
        self.benchmark = benchmark
        self.name = name


