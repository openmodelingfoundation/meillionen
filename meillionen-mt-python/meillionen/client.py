import pathlib
from typing import Any, Dict, Optional

import pyarrow as pa
from pyarrow.dataset import DirectoryPartitioning, dataset

from .resource import FuncInterfaceClient, FuncRequest
from meillionen.meillionen import client_create_interface_from_cli, client_call_cli, FileResource, ParquetResource, \
    FeatherResource


class SimpleStorage:
    def __init__(self, path: pathlib.Path):


class PartitionedStorage:
    RESOURCE_CLASS = None
    FILE_NAME = None

    def __init__(self, path: pathlib.Path, partitioning: DirectoryPartitioning):
        self.path = path
        self.partitioning = partitioning

    def resource(self, params: Dict[str, Any]):
        schema: pa.schema = self.partitioning.schema
        segments = [params[name] for name in schema.names]
        path = self.path.joinpath(*segments, self.FILE_NAME)
        return self.RESOURCE_CLASS(str(path))


class PartitionedDirectory(PartitionedStorage):
    RESOURCE_CLASS = FileResource
    FILE_NAME = ''


class PartitionedParquet(PartitionedStorage):
    RESOURCE_CLASS = ParquetResource
    FILE_NAME = 'data.parquet'

    def to_dataset(self):
        return dataset(source=self.path, partitioning=self.partitioning)

    def to_table(self):
        return self.to_dataset().to_table()

    def to_pandas(self):
        return self.to_table().to_pandas()


class PartitionedFeather(PartitionedStorage):
    RESOURCE_CLASS = FeatherResource
    FILE_NAME = 'data.feather'

    def to_dataset(self):
        return dataset(source=self.path, partitioning=self.partitioning)

    def to_table(self):
        return self.to_dataset().to_table()

    def to_pandas(self):
        return self.to_table().to_pandas()


class LocalStorage:
    partitioned_storage = {
        FeatherResource: PartitionedFeather,
        FileResource: PartitionedDirectory,
        ParquetResource: PartitionedParquet,
    }

    default_extensions = {
        FeatherResource: '.feather',
        FileResource: '',
        ParquetResource: '.parquet'
    }

    def __init__(self, root_path: pathlib.Path):
        self.root_path = root_path
        self.resource_factories = {}

    def __getattr__(self, item):
        return self.resource_factories[item].resource

    def set_partitioned_resource(self, sink, resource_class, partitioning: DirectoryPartitioning):
        path = pathlib.Path(self.root_path, sink)
        store = self.partitioned_storage[resource_class](path=path, partitioning=partitioning)
        self.resource_factories[sink] = store

    def set_simple(self, sink, resource_class):
        path = self.root_path.joinpath(f'{sink}{self.default_extensions[resource_class]}')
        self.resource_factories[sink] = lambda **params: resource_class(path=path)

    def create_sinks(self, params):
        resource_sinks = {}
        for name, resource_factory in self.resource_factories.items():
            resource = resource_factory(**params)
            resource_sinks[name] = resource
        return resource_sinks


class ClientFunctionModel:
    """
    A client that supports calling meillionen models as command line programs
    """
    def __init__(self, interface: FuncInterfaceClient, path: str):
        """
        Create a command line interface to a meillionen model

        :param interface: metadata describing the model interface
        :param path: local path to the model. supports finding models
        accessible the PATH environment variable
        """
        self.interface = interface
        self.path = path
        self.storage = None

    def source(self, name):
        return self.interface.source(name)

    def sink(self, name):
        return self.interface.sink(name)

    def default_sinks(self):
        return { name: self.interface.DEFAULT_RESOURCE[validator] for name, validator in self.interface.sinks }

    def set_storage(self,
                   root_path: pathlib.Path,
                   partitioning: DirectoryPartitioning,
                   resource_overrides=None):
        root_path = root_path.joinpath(self.interface.name)
        storage = LocalStorage(root_path=root_path)
        resource_classes = self.default_sinks()
        if resource_overrides is not None:
            resource_classes.update(resource_overrides)

        for sink in self.interface.sink_names:
            resource_class = resource_classes[sink]
            storage.set_partitioned_resource(
                sink,
                partitioning=partitioning,
                resource_class=resource_class)
        self.storage = storage

    @classmethod
    def from_path(cls, path: str) -> 'ClientFunctionModel':
        """
        Create a command line interface client for a meillionen model

        :param path: local path to the model. supports finding models
        accessible the PATH environment variable
        :return: A client to call a meillionen model
        """
        response = client_create_interface_from_cli(path)
        interface = FuncInterfaceClient.from_recordbatch(response)
        return cls(interface=interface, path=path)

    def __call__(self, sources: Dict[str, Any], sinks: Optional[Dict[str, Any]] = None, params = None):
        if sinks is None:
            sinks = {}
        if params is None:
            params = {}
        base_sinks = self.storage.create_sinks(params=params)
        base_sinks.update(sinks)
        fr = FuncRequest(sources=sources, sinks=base_sinks)
        rb = fr.to_recordbatch(self.path)
        client_call_cli(self.path, rb)
        return sinks
