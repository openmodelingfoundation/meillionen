from typing import Any, List, Dict

import flatbuffers

from . import _FunctionInterface as fi
from .schema import Schema, _Schema
from .base import deserialize_to_dict, serialize_dict, FlatbufferMixin
from meillionen.interface.mutability import Mutability


def default_handler(**kwargs):
    raise NotImplemented()


class _MethodInterface(fi._FunctionInterface, FlatbufferMixin):
    ARGS_OFFSET = 6

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        return cls.get_root_as(buf, offset)

    def Args(self, j):
        return self._get_resource(j, self.ARGS_OFFSET, _Schema)


class MethodInterface:
    def __init__(self, name, args: List[Any], handler=default_handler):
        self.name = name
        self.args = {s.name: s for s in args} if not hasattr(args, 'values') else args
        self.handler = handler

    def __call__(self, **kwargs):
        return self.handler(**kwargs)

    @classmethod
    def from_interface(cls, interface: _MethodInterface):
        name = interface.Name().decode('utf-8')
        args = deserialize_to_dict(
            constructor=Schema.from_class,
            getter=interface.Args,
            n=interface.ArgsLength())
        return cls(
            name=name,
            args=args
        )

    @classmethod
    def deserialize(cls, buffer):
        interface = _MethodInterface.GetRootAs(buffer, 0)
        return cls.from_interface(interface)

    def serialize(self, builder: flatbuffers.Builder):
        name_off = builder.CreateString(self.name)
        args_off = serialize_dict(
            vector_builder=fi.StartArgsVector,
            builder=builder,
            xs=self.args
        )
        fi.Start(builder)
        fi.AddName(builder, name_off)
        fi.AddArgs(builder, args_off)
        return fi.End(builder)

    def process_kwargs(self, kwargs: Dict[str, Any]):
        result = {}
        for name, resource in kwargs.items():
            handler = self.args[name]
            if handler.mutability == Mutability.read:
                result[name] = handler.load(resource)
            elif handler.mutability == Mutability.write:
                result[name] = (handler, resource)
        return result