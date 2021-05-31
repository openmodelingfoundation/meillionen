import pathlib
from typing import Any, Dict, Optional

import pyarrow as pa
from pyarrow import dataset

from .function import FuncInterfaceClient, FuncRequest
from .resource import infer_resource
from meillionen.meillionen import client_create_interface_from_cli, client_call_cli, FileResource, ParquetResource, \
    FeatherResource


class ResourceBuilder:
    def __init__(self, name, settings, partitioning):
        self.name = name
        self.settings = settings
        self.partitioning = partitioning

    def _complete(self, resource, partition):
        return resource.build(settings=self.settings, partition=partition, name=self.name)


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

    def source_schema(self, name):
        return self.interface.source(name)

    def sink_schema(self, name):
        return self.interface.sink(name)

    @property
    def source_names(self):
        return self.interface.source_names

    @property
    def sink_names(self):
        return self.interface.sink_names

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
