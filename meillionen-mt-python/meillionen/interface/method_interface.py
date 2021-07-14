from typing import Any, List

import flatbuffers

from . import _FunctionInterface as fi
from .schema import Schema, _Schema
from .base import deserialize_to_dict, serialize_dict, FlatbufferMixin


def default_handler(sources, sinks):
    raise NotImplemented()


class _MethodInterface(fi._FunctionInterface, FlatbufferMixin):
    SINK_OFFSET = 6
    SOURCE_OFFSET = 8

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        return cls.get_root_as(buf, offset)

    def Sinks(self, j):
        return self._get_resource(j, self.SINK_OFFSET, _Schema)

    def Sources(self, j):
        return self._get_resource(j, self.SOURCE_OFFSET, _Schema)


class MethodInterface:
    def __init__(self, name, sinks: List[Any], sources: List[Any], handler=default_handler):
        self.name = name
        self.sinks = {s.name: s for s in sinks} if not hasattr(sinks, 'values') else sinks
        self.sources = {s.name: s for s in sources} if not hasattr(sources, 'values') else sources
        self.handler = handler

    def __call__(self, sources, sinks):
        return self.handler(sources=sources, sinks=sinks)

    @classmethod
    def from_interface(cls, interface: _MethodInterface):
        name = interface.Name().decode('utf-8')
        sinks = deserialize_to_dict(
            constructor=Schema.from_class,
            getter=interface.Sinks,
            n=interface.SinksLength())
        sources = deserialize_to_dict(
            constructor=Schema.from_class,
            getter=interface.Sources,
            n=interface.SourcesLength())
        return cls(
            name=name,
            sinks=sinks,
            sources=sources
        )

    @classmethod
    def deserialize(cls, buffer):
        interface = _MethodInterface.GetRootAs(buffer, 0)
        return cls.from_interface(interface)

    def serialize(self, builder: flatbuffers.Builder):
        name_off = builder.CreateString(self.name)
        sinks_off = serialize_dict(
            vector_builder=fi.StartSinksVector,
            builder=builder,
            xs=self.sinks
        )
        sources_off = serialize_dict(
            vector_builder=fi.StartSourcesVector,
            builder=builder,
            xs=self.sources
        )
        fi.Start(builder)
        fi.AddName(builder, name_off)
        fi.AddSinks(builder, sinks_off)
        fi.AddSources(builder, sources_off)
        return fi.End(builder)
