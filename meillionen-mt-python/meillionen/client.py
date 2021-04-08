from .meillionen import FuncRequest, FuncInterface, create_interface_from_cli


class FunctionModelCLI:
    def __init__(self, interface: FuncInterface, path):
        self.interface = interface
        self.path = path

    @classmethod
    def from_path(cls, path):
        interface = create_interface_from_cli(path)
        return cls(interface=interface, path=path)

    def __call__(self, fr):
        self.interface.call_cli(self.path, fr)
