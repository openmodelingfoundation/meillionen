import argparse
import flatbuffers
import grpc

import module_model_service_pb2_grpc
import sys

from meillionen.interface.method_request import MethodRequest
from meillionen.interface.module_interface import ModuleInterface
from signal import signal, SIGTERM


class ModuleModelServicer(module_model_service_pb2_grpc.ModuleModelServiceServicer):
    def __init__(self, module: ModuleInterface):
        self.module = module

    def _interface(self, kwargs):
        builder = flatbuffers.Builder()
        builder.Finish(self.module.serialize(builder))
        interface = builder.Output()
        sys.stdout.buffer.write(interface)

    def _serve(self, kwargs):
        server = grpc.Server()
        server.add_insecure_port(kwargs.uri)
        server.start()

        def handle_sigterm(*_):
            all_rpcs_done_event = server.stop(30)
            all_rpcs_done_event.wait(30)
            print('Shot down gracefully')

        signal(SIGTERM, handle_sigterm)
        server.wait_for_termination()

    def apply(self, request_iterator, context):
        for r in request_iterator:
            request = MethodRequest.deserialize(r.payload)
            response = self.module(request)
            yield response

    def _build_cli(self):
        p = argparse.ArgumentParser()
        sp = p.add_subparsers()

        serve = sp.add_parser('serve')
        serve.set_defaults(command='serve')
        serve.add_argument('uri', help='URI used to communicate with')
        interface = sp.add_parser('interface')
        interface.set_defaults(command='interface')

        return p

    def cli(self):
        """
        Returns an argparse cli to access the model with
        """
        args = self._build_cli().parse_args()
        if hasattr(args, 'command'):
            command = args.command
            {
                'interface': self._interface,
                'serve': self._serve
            }[command](args)
