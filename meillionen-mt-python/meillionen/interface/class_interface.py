import flatbuffers

from . import _ClassInterface as ci
from .function_interface import MethodInterface
from .base import serialize_list
from meillionen.exceptions import MethodNotFound


class ClassInterface:
    def __init__(self, name, methods):
        self._name = name
        self._methods = methods

    @classmethod
    def from_interface(cls, interface: ci._ClassInterface):
        name = interface.Name()
        methods = {}
        for i in range(interface.MethodsLength()):
            method = MethodInterface.from_interface(interface.Methods(i))
            methods[method.name] = method
        return cls(name=name, methods=methods)

    @classmethod
    def deserialize(cls, buffer):
        interface = ci._ClassInterface.GetRootAs(buffer, 0)
        return cls.from_interface(interface)

    def serialize(self, builder: flatbuffers.Builder):
        name_off = builder.CreateString(self._name)
        methods_off = serialize_list(
            builder=builder,
            vector_builder=ci.StartMethodsVector,
            xs=self._methods
        )

        ci.Start(builder)
        ci.AddName(builder, name_off)
        ci.AddMethods(builder, methods_off)
        return ci.End(builder)

    def get_method(self, name: str) -> MethodInterface:
        try:
            return self._methods[name]
        except KeyError as e:
            raise MethodNotFound() from e
