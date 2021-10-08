import textwrap
from typing import Any, List, Dict

import flatbuffers

from . import _FunctionInterface as fi
from .schema import Schema, _Schema, Handlable, SchemaProxy
from .base import deserialize_to_dict, serialize_dict, FlatbufferMixin, leading_indent
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
    """
    A description of a method. It has a name, arguments (with schema information)
    and a handler (which runs the method)
    """
    def __init__(self, name, args: List[Any], handler=default_handler):
        self.name = name
        self.args: Dict[str, SchemaProxy] = {s.name: s for s in args} if not hasattr(args, 'values') else args
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
        """
        Builds a method interface from a buffer
        """
        interface = _MethodInterface.GetRootAs(buffer, 0)
        return cls.from_interface(interface)

    def serialize(self, builder: flatbuffers.Builder):
        """
        Serializes a method interface into a flatbuffer builder
        """
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
        """
        Preprocesses resource payloads to reduce boilerplate code in the handler
        """
        # FIXME: This method should removed and just pass a request class object to the handler
        result = {}
        for name, resource in kwargs.items():
            handler = self.args[name]
            if handler.mutability == Mutability.read:
                result[name] = handler.load(resource)
            elif handler.mutability == Mutability.write:
                result[name] = (handler, resource)
        return result

    def describe(self, indent=0):
        print(leading_indent(self.name, indent))
        print(leading_indent('-'*len(self.name), indent))

        for arg in self.args.values():
            arg.describe(indent=indent + 2)