from dataclasses import dataclass, asdict
from typing import List

@dataclass
class QueryParameter:
    name: str
    data_type: str

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)

    def to_dict(self):
        return asdict(self)


class BenchmarkQuery:
    def __init__(self, query: str, parameters: List[QueryParameter], db: str, benchmark: str, name: str):
        self.query = query
        self.parameters = parameters
        self.db = db
        self.name = name
        self.benchmark = benchmark
        self.name = name

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            query=data['query'],
            parameters=[QueryParameter.from_dict(p) for p in data['parameters']],
            db=data['db'],
            benchmark=data['benchmark'],
            name=data['name']
        )

    def to_dict(self):
        return {
            'query': self.query,
            'parameters': [p.to_dict() for p in self.parameters],
            'db': self.db,
            'benchmark': self.benchmark,
            'name': self.name,
        }


