import enum

from ._Mutability import _Mutability


class Mutability(enum.Enum):
    """
    The mutability of a schema.

    A readable schema is an input argument to a :class:`~meillionen.interface.method_interface.MethodInterface`

    A writable schema is an output argument to a :class:`~meillionen.interface.method_interface.MethodInterface`
    """
    read = _Mutability.read
    write = _Mutability.write

    def serialize(self):
        """
        Converts the mutability enum into a flatbuffer serializable format
        """
        return self.value

    @classmethod
    def deserialize(cls, value: int):
        """
        Turns the mutability int into an enum
        """
        return cls(value)