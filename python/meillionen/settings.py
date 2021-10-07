import os.path
import pyarrow as pa
import pyarrow.dataset as pad

from typing import Any, Dict
from pydantic import BaseModel


class Settings(BaseModel):
    """
    Settings for a module.

    Currently used only to turn partial resource payloads into resource payloads. In the future
    it may have information about logging and how to access particular common resources (such as
    databases).
    """
    base_path: str

    def trial(self, name: str):
        """
        Used to build 'trial' settings objects which extend the base path of the settings object.
        """
        return self.__class__(base_path=os.path.join(self.base_path, name))


class Partitioning:
    """
    Describes additional data to be used to build up partial resource payload
    objects.

    Commonly used when broadcasting a model over a grid with different resources
    available at each grid point. Also useful for sensitivity analysis.
    """
    def __init__(self, schema: pa.Schema):
        self.schema = schema

    @property
    def names(self):
        return self.schema.names

    def to_arrow(self):
        """
        Convert the partition into a `pyarrow` partition so that final results can
        be loaded as partitioned parquet files (which are supported by pyarrow, spark,
        datafusion and other libraries)
        """
        return pad.partitioning(self.schema)

    def complete(self, **kwargs):
        """
        Build a partition object with the data filled in for each missing partition key
        """
        return Partition(self, kwargs=kwargs)


class Partition:
    def __init__(self, partitioning: Partitioning, kwargs: Dict[str, Any]):
        # FIXME: check to make sure that kwargs match partition key names and types
        self.partitioning = partitioning
        self.kwargs = kwargs

    def to_path(self):
        """
        Build a partial file path with the partition information
        """
        return os.path.join(*(str(self.kwargs[name]) for name in self.partitioning.names))