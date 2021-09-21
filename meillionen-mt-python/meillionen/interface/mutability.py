import enum

from ._Mutability import _Mutability


class Mutability(enum.Enum):
    read = _Mutability.read
    write = _Mutability.write
    read_write = _Mutability.read_write

    def serialize(self):
        return self.value

    @classmethod
    def deserialize(cls, value: int):
        return cls(value)