from .meillionen import FuncRequest, FuncInterface
import json
import sh


class FunctionModelCLI:
    def __init__(self, interface: FuncInterface, path):
        self.interface = interface
        self.path = path

    @classmethod
    def from_path(cls, path):
        interface = FuncInterface.from_dict(json.loads(model('interface').stdout))
        return cls(interface=interface, path=path)

    def __call__(self, fr):
        self.interface.call_cli(self.path, fr)
