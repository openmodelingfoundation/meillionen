from .meillionen import FuncRequest, FuncInterface

import sh


class FunctionModelCLI:
    def __init__(self, interface: FuncInterface, path):
        self.interface = interface
        self.path = path

    @classmethod
    def from_path(cls, path):
        model = sh.Command(path)
        interface = FuncInterface.from_json(model('interface'))
        return cls(interface=interface, path=path)

    def __call__(self, fr):
        self.interface.call_cli(self.path, fr)
