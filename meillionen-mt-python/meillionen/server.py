import argparse
import io
import json
import sys

import flatbuffers
from meillionen.interface.method_request import MethodRequest
from meillionen.interface.module_interface import ModuleInterface


class Server:
    def __init__(self, module: ModuleInterface):
        self.module = module

    def _interface(self, kwargs):
        builder = flatbuffers.Builder()
        builder.Finish(self.module.serialize(builder))
        interface = builder.Output()
        sys.stdout.buffer.write(interface)

    def _describe(self, kwargs):
        return {
            'list': self._describe_list(),
            'detail': self._describe_detail(kwargs['class_name'])
        }[kwargs['describe_command']]

    def _describe_list(self):
        json.dump(list(self.module.classes.keys()), sys.stdout)

    def _describe_detail(self, class_name: str):
        # Not Implemented Yet
        json.dump(self.module.classes[class_name].metadata, sys.stdout)

    def _run(self, kwargs, fd: io.BytesIO = sys.stdin.buffer):
        req = MethodRequest.deserialize(fd.read())
        self.run(req)

    def run(self, req: MethodRequest):
        self.module(req)

    def _build_cli(self):
        p = argparse.ArgumentParser()
        sp = p.add_subparsers()

        interface_p = sp.add_parser('interface')
        interface_p.set_defaults(command='interface')

        describe_p = sp.add_parser('describe')
        describe_p.set_defaults(command='describe')
        describe_sp = describe_p.add_subparsers()

        describe_list_p = describe_sp.add_parser('list')
        describe_list_p.set_defaults(describe_command='list')

        describe_detail_p = describe_sp.add_parser('detail')
        describe_detail_p.set_defaults(describe_command='detail')
        describe_detail_p.add_argument('class_name')

        run_p = sp.add_parser('run')
        run_p.set_defaults(command='run')

        return p

    def cli(self):
        args = self._build_cli().parse_args()
        if hasattr(args, 'command'):
            command = args.command
            {
                'interface': self._interface,
                'describe': self._describe,
                'run': self._run
            }[command](args)
