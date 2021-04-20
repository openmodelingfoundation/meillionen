from .meillionen import FuncRequest, FuncInterface, create_interface_from_cli


class FunctionModelCLI:
    """
    A client that supports calling meillionen models as command line programs
    """
    def __init__(self, interface: FuncInterface, path: str):
        """
        Create a command line interface to a meillionen model

        :param interface: metadata describing the model interface
        :param path: local path to the model. supports finding models
            accessible the PATH environment variable
        """
        self.interface = interface
        self.path = path

    @classmethod
    def from_path(cls, path: str) -> 'FunctionModelCLI':
        """
        Create a command line interface client for a meillionen model
        :param path: local path to the model. supports finding models
            accessible the PATH environment variable
        :return: A client to call a meillionen model
        """
        interface = create_interface_from_cli(path)
        return cls(interface=interface, path=path)

    def __call__(self, fr: FuncRequest):
        self.interface.call_cli(self.path, fr)
