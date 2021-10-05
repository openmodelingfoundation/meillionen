import flatbuffers
import sh
from typing import Any, Dict, Optional


import pyarrow as pa
from pyarrow import dataset

from .resource import infer_resource
from .server import Server
from .settings import Partition
from .interface.module_interface import ModuleInterface
from .interface.method_request import MethodRequest
from .interface.schema import get_handlers
from .response import Response


class CLIRef:
    def __init__(self, path):
        self.command = sh.Command(path)

    def get_interface(self) -> ModuleInterface:
        interface = self.command('interface')
        module = ModuleInterface.deserialize(interface.stdout)
        return module

    def run(self, mr: MethodRequest):
        builder = flatbuffers.Builder()
        # need to prefix the message by the size
        # so that calling program knows how much of
        # stdin is needed to interpret the MethodRequest
        builder.FinishSizePrefixed(mr.serialize(builder))
        msg = builder.Output()
        self.command('run', _in=bytes(msg))


class ServerRef:
    def __init__(self, server: Server):
        self.server = server

    def get_interface(self) -> ModuleInterface:
        return self.server.module

    def run(self, mr: MethodRequest):
        self.server.run(mr)


class Client:
    def __init__(self, module_ref, partitioning=None, settings=None):
        self.module_ref = module_ref
        self.module: ModuleInterface = module_ref.get_interface()
        self.partitioning = partitioning
        self.settings = settings

    def run_simple(self, mr: MethodRequest):
        self.module_ref.run(mr)
        method = self.module.get_method(mr)
        resources = mr.kwargs
        handlers = get_handlers(resources=resources, schemas=method.args)
        return Response(handlers=handlers, resources=resources)

    def run(self, class_name, method_name, resource_payloads, partition: Optional[Partition]=None):
        mr = MethodRequest.from_partial(
            class_name=class_name,
            method_name=method_name,
            resource_payloads=resource_payloads,
            partition=partition,
            settings=self.settings
        )
        return self.run_simple(mr)