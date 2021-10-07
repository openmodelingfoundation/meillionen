import os.path

import flatbuffers
import sh
from typing import Any, Dict, Optional

from .server import Server
from .settings import Partition
from .interface.module_interface import ModuleInterface
from .interface.method_request import MethodRequest
from .interface.schema import get_handlers, get_handler
from .interface.base import MethodRequestArg
from .response import Response


class CLIRef:
    """
    A CLI reference to a model

    The cli program must conform to the Meillionen cli interface
    """

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


class DockerImageRef:
    """
    A Docker Image reference to a model

    The docker image must have an entrypoint of a Meillionen cli program and
    have a working directory of `/code` in order to work
    """

    def __init__(self, image_name: str):
        # FIXME: support additional docker volume mounts for data that is not
        #  available in the directory the model is currently running in
        self.image_name = image_name
        curdir = os.getcwd()
        self.command(f'docker run -it --rm -v {curdir}:/code {image_name}')

    def get_interface(self) -> ModuleInterface:
        self.command('interface')
        module = ModuleInterface.deserialize(interface.stdout)
        return module

    def run(self, mr: MethodRequest):
        builder = flatbuffers.Builder()
        builder.FinishSizePrefixed(mr.serialize(builder))
        msg = builder.Output()
        self.command('run', _in=bytes(msg))


class ServerRef:
    """
    A server reference to a model

    This allows running a model in the same Python process.

    Mostly suited for interactive experimentation and model testing.
    """
    def __init__(self, server: Server):
        self.server = server

    def get_interface(self) -> ModuleInterface:
        return self.server.module

    def run(self, mr: MethodRequest):
        self.server.run(mr)


class Client:
    """
    A client to execute and inspect Meillionen models
    """
    def __init__(self, module_ref, settings=None):
        self.module_ref = module_ref
        self.module: ModuleInterface = module_ref.get_interface()
        self.settings = settings

    def run_simple(self, mr: MethodRequest):
        self.module_ref.run(mr)
        method = self.module.get_method(mr)
        resources = mr.kwargs
        handlers = get_handlers(resources=resources, schemas=method.args)
        return Response(handlers=handlers, resources=resources)

    def run(self, class_name, method_name, resource_payloads, partition: Optional[Partition]=None):
        """
        Runs a particular class method on the module

        :param class_name: the name of the class
        :param method_name: the name of the method to call
        :param resource_payloads: dict of argument names to resource payloads
        :param partition: an optional partition which describes how to build nested paths,
          to add key columns to tabular data or to add dimensions to tensor data
        """
        mr = MethodRequest.from_partial(
            class_name=class_name,
            method_name=method_name,
            resource_payloads=resource_payloads,
            partition=partition,
            settings=self.settings
        )
        return self.run_simple(mr)

    def save(self, mra: MethodRequestArg, resource, data, partition=None):
        """
        Saves a resource

        :param mra: metadata needed to locate the schema of a method argument
        :param resource: the resource metadata describing how and where to save the data to
        :param data: the data to save
        :param partition: the optional partition information. Useful for evaluating a model at
          many different parameter combinations
        """
        if hasattr(resource, 'complete'):
            resource = resource.complete(settings=self.settings, mra=mra, partition=partition)
        method = self.module.get_method(mra)
        schema = method.args[mra.arg_name]
        handler = get_handler(resource_payload=resource, schema=schema)
        handler.save(resource, data)
        return resource