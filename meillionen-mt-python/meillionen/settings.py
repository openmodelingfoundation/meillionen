import os.path
import pyarrow as pa
import pyarrow.dataset as pad

from typing import Any, Dict
from pydantic import BaseModel


class Settings(BaseModel):
    base_path: str


class Partitioning:
    def __init__(self, schema: pa.Schema):
        self.schema = schema

    @property
    def names(self):
        return self.schema.names

    def to_arrow(self):
        return pad.partitioning(self.schema)

    def complete(self, **kwargs):
        return Partition(self, kwargs=kwargs)


class Partition:
    def __init__(self, partitioning: Partitioning, kwargs: Dict[str, Any]):
        self.partitioning = partitioning
        self.kwargs = kwargs

    def to_path(self):
        return os.path.join(*(str(self.kwargs[name]) for name in self.partitioning.names))