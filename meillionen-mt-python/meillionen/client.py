import pathlib
from typing import Any, Dict, Optional

import pyarrow as pa
from pyarrow.dataset import DirectoryPartitioning, dataset

from .resource import FuncInterfaceClient, FuncRequest
from meillionen.meillionen import client_create_interface_from_cli, client_call_cli, FileResource, ParquetResource, \
    FeatherResource


class PartitionedStorage:
    RESOURCE_CLASS = None
    FILE_NAME = None

    def __init__(self, path: pathlib.Path, partitioning: DirectoryPartitioning):
        self.path = path
        self.partitioning = partitioning

    def __call__(self, **params: Dict[str, Any]):
        schema: pa.schema = self.partitioning.schema
        segments = [str(params[name]) for name in schema.names]
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


class Storage:
    def __init__(self, **resource_factories):
        self.resource_factories = resource_factories

    def create_sink_resources(self, **params):
        return { name: rf(**params) for name, rf in self.resource_factories.items() }


class HandleProvider:
    def __init__(self, handle_providers):
        self.handle_providers = handle_providers

    @classmethod
    def from_interface(cls, interface):
        pass


class ClientFunctionModel:
    """
    A client that supports calling meillionen models as command line programs
    """
    def __init__(self, interface: FuncInterfaceClient, path: str, sink_handle_provider):
        """
        Create a command line interface to a meillionen model

        :param interface: metadata describing the model interface
        :param path: local path to the model. supports finding models
        :param sink_handle_provider: provides handles given settings
        accessible the PATH environment variable
        """
        self.interface = interface
        self.path = path
        self.sink_handle_provider = sink_handle_provider

    def source(self, name):
        return self.interface.source(name)

    def sink(self, name):
        return self.interface.sink(name)

    @classmethod
    def from_path(cls, name: str, path: str, handle_provider_overrides=None) -> 'ClientFunctionModel':
        """
        Create a command line interface client for a meillionen model

        :param name: name of model. used for storage
        :param path: local path to the model. supports finding models
        accessible the PATH environment variable
        :return: A client to call a meillionen model
        """
        response = client_create_interface_from_cli(path)
        interface = FuncInterfaceClient.from_recordbatch(name=name, recordbatch=response)
        hp = interface.handle_provider
        if handle_provider_overrides:
            for source_name, hf in handle_provider_overrides.items():
                hp[source_name] = hf
        return cls(interface=interface, path=path, sink_handle_provider=hp)

    def set_storage(self, storage):
        self.storage = storage

    def run_partition(self, sources: Dict[str, Any], params):
        sinks = self.storage.create_sink_resources(**params)
        self.run(sources=sources, sinks=sinks)
        return sinks

    def run(self, sources: Dict[str, Any], sinks: Dict[str, Any]):
        fr = FuncRequest(sources=sources, sinks=sinks)
        rb = fr.to_recordbatch(self.path)
        client_call_cli(self.path, rb)
        return sinks
