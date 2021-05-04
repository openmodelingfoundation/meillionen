from typing import Any, Dict

from pyarrow import RecordBatch

from .resource import FuncInterfaceClient, FuncRequest
from .meillionen import client_create_interface_from_cli, client_call_cli


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

    def source(self, name):
        return self.interface.source(name)

    def sink(self, name):
        return self.interface.sink(name)

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

    def __call__(self, sources: Dict[str, Any], sinks: Dict[str, Any]):
        fr = FuncRequest(sources=sources, sinks=sinks)
        rb = fr.to_recordbatch(self.path)
        client_call_cli(self.path, rb)
