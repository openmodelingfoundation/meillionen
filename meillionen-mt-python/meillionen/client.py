import pathlib
from typing import Any, Dict, Optional

import pyarrow as pa
from pyarrow import dataset

from .function import FuncInterfaceClient, FuncRequest
from .resource import infer_resource
from meillionen.meillionen import client_create_interface_from_cli, client_call_cli, FileResource, ParquetResource, \
    FeatherResource


class PartitionedStorage:
    RESOURCE_CLASS = None
    FILE_NAME = None

    def __init__(self, path: pathlib.Path, partitioning: dataset.DirectoryPartitioning):
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
        return dataset.dataset(source=self.path, partitioning=self.partitioning)

    def to_table(self):
        return self.to_dataset().to_table()

    def to_pandas(self):
        return self.to_table().to_pandas()


class PartitionedFeather(PartitionedStorage):
    RESOURCE_CLASS = FeatherResource
    FILE_NAME = 'data.feather'

    def to_dataset(self):
        return dataset.dataset(source=self.path, partitioning=self.partitioning)

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
    def __init__(self, interface: FuncInterfaceClient, path: str, trial, sinks: Dict[str, Any], partitioning):
        """
        Create a command line interface to a meillionen model

        :param interface: metadata describing the model interface
        :param path: local path to the model. supports finding models
        :param sinks: provides handles given settings
        accessible the PATH environment variable
        """
        self.interface = interface
        self.path = path
        self.trial = trial
        self.sinks = sinks
        self.partitioning = partitioning

    def source(self, name):
        return self.interface.source(name)

    def sink(self, name):
        return self.interface.sink(name)

    @classmethod
    def from_path(cls, name: str, path: str, trial, sinks=None, partitioning=None) -> 'ClientFunctionModel':
        """
        Create a command line interface client for a meillionen model

        :param name: name of model. used for storage
        :param path: local path to the model. supports finding models
        accessible the PATH environment variable
        :return: A client to call a meillionen model
        """
        if sinks is None:
            sinks = {}
        if partitioning is None:
            partitioning = dataset.partitioning(pa.schema([]))

        response = client_create_interface_from_cli(path)
        interface = FuncInterfaceClient.from_recordbatch(name=name, recordbatch=response)
        sinks = cls.infer_sinks(interface, sinks)
        return cls(interface=interface, path=path, sinks=sinks, trial=trial, partitioning=partitioning)

    @classmethod
    def infer_sinks(cls, interface, sinks: Dict[str, Any]):
        sinks = sinks.copy()
        for sink_name in interface.sink_names:
            if sink_name not in sinks:
                sinks[sink_name] = infer_resource(interface.sink(sink_name))
        return sinks

    def _prepare(self, sources: Dict[str, Any], sinks: Optional[Dict[str, Any]] = None, partition: Optional[Dict[str, Any]] = None):
        if sinks is None:
            sinks = {}
        else:
            sinks = sinks.copy()
        if partition is None:
            partition = {}
        partition = [partition[name] for name in self.partitioning.schema.names]
        for sink_name in self.sinks:
            if sink_name not in sinks:
                sinks[sink_name] = self.sinks[sink_name]
            if hasattr(sinks[sink_name], 'build'):
                sinks[sink_name] = sinks[sink_name].build(
                    settings=self.trial.sinks,
                    partition=partition,
                    name=sink_name)
        sources = sources.copy()
        for source_name in sources:
            if hasattr(sources[source_name], 'build'):
                sources[source_name] = sources[source_name].build(
                    settings=self.trial.sources,
                    name=source_name)
        return {
            'sources': sources,
            'sinks': sinks
        }

    def run(self, sources: Dict[str, Any], sinks: Optional[Dict[str, Any]] = None, partition: Optional[Dict[str, Any]] = None):
        kwargs = self._prepare(sources=sources, sinks=sinks, partition=partition)
        fr = FuncRequest(**kwargs)
        rb = fr.to_recordbatch(self.path)
        client_call_cli(self.path, rb)
        return kwargs['sinks']
