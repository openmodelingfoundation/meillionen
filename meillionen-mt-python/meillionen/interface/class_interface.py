from typing import Dict

import flatbuffers

from . import _ClassInterface as ci
from .method_interface import MethodInterface, _MethodInterface
from .base import serialize_list, serialize_dict, FlatbufferMixin
from meillionen.exceptions import MethodNotFound


class _ClassInterface(ci._ClassInterface, FlatbufferMixin):
    METHOD_OFFSET = 8

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        return cls.get_root_as(buf, offset)

    def Methods(self, j):
        return self._get_resource(j, self.METHOD_OFFSET, _MethodInterface)


class ClassInterface:
    def __init__(self, name, methods: Dict[str, MethodInterface]):
        self._name = name
        self._methods = methods

    @property
    def name(self):
        return self._name

    @classmethod
    def from_interface(cls, interface: _ClassInterface):
        name = interface.Name().decode('utf-8')
        methods = {}
        for i in range(interface.MethodsLength()):
            method = MethodInterface.from_interface(interface.Methods(i))
            methods[method.name] = method
        return cls(name=name, methods=methods)

    @classmethod
    def deserialize(cls, buffer):
        interface = _ClassInterface.GetRootAs(buffer, 0)
        return cls.from_interface(interface)

    def serialize(self, builder: flatbuffers.Builder):
        name_off = builder.CreateString(self._name)
        methods_off = serialize_dict(
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
