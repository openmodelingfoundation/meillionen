from .meillionen import FuncRequest, FuncInterface, client_create_interface_from_cli, client_call_cli


class ClientFunctionModel:
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
    def from_path(cls, path: str) -> 'ClientFunctionModel':
        """
        Create a command line interface client for a meillionen model

        :param path: local path to the model. supports finding models
        accessible the PATH environment variable
        :return: A client to call a meillionen model
        """
        interface = client_create_interface_from_cli(path)
        return cls(interface=interface, path=path)

    def __call__(self, fr: FuncRequest):
        client_call_cli(self.interface, self.path, fr)
