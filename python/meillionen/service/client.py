from signal import SIGTERM

import grpc

import module_model_service_pb2
import module_model_service_pb2_grpc
import sh

from meillionen.interface.module_interface import ModuleInterface


class CLIRef:
    def __init__(self, model_path, uri):
        self.command = sh.Command(model_path)
        self.uri = uri
        self._running_process = self.command(f'serve --args {self.uri}', _bg=True)
        channel = grpc.insecure_channel(self.uri)
        stub = module_model_service_pb2_grpc.ModuleModelServiceStub(channel)
        self._stub = stub

    def get_interface(self) -> ModuleInterface:
        interface = self.command('interface')
        module = ModuleInterface.deserialize(interface.stdout)
        return module

    def finalize(self):
        self._running_process.signal(SIGTERM)
        self._running_process.wait()
        self._running_process = None
        self._stub = None

    def apply(self, requests):
        for res in self._stub.apply(module_model_service_pb2.Request(req.serialize()) for req in requests):
            response = Response.deserialize(res.payload)
            if response.is_error():
                raise response.as_error()
            yield response

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finalize()


class DockerImageRef:
    pass


class ServerRef:
    pass


class Client:
    """
    A client to execute and inspect Meillionen models
    """
    def __init__(self, module_ref, settings=None):
        self.module_ref = module_ref
        self.module: ModuleInterface = module_ref.get_interface()
        self.settings = settings

    def apply_simple(self, mr: MethodRequest):
        self.module_ref.apply(mr)
        method = self.module.get_method(mr)
        resources = mr.kwargs
        handlers = get_handlers(resources=resources, schemas=method.args)
        return Response(handlers=handlers, resources=resources)

    def apply(self, class_name, method_name, resource_payloads, partition: Optional[Partition]=None):
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
        return self.apply_simple(mr)


