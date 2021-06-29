import flatbuffers

from . import _FunctionInterface as fi
from .schema import Schema
from .base import deserialize_to_dict, serialize_list


def default_handler(sources, sinks):
    raise NotImplemented()


class MethodInterface:
    def __init__(self, name, sinks, sources, handler=default_handler):
        self.name = name
        self.sinks = sinks
        self.sources = sources
        self.handler = handler

    def __call__(self, sources, sinks):
        return self.handler(sources=sources, sinks=sinks)

    @classmethod
    def from_interface(cls, interface: fi._FunctionInterface):
        name = interface.Name()
        sinks = deserialize_to_dict(
            constructor=Schema.from_schema,
            getter=interface.Sinks,
            n=interface.SinksLength())
        sources = deserialize_to_dict(
            constructor=Schema.from_schema,
            getter=interface.Sources,
            n=interface.SourcesLength())
        return cls(
            name=name,
            sinks=sinks,
            sources=sources
        )

    @classmethod
    def deserialize(cls, buffer):
        interface = fi._FunctionInterface.GetRootAs(buffer, 0)
        return cls.from_interface(interface)

    def serialize(self, builder: flatbuffers.Builder):
        name_off = builder.CreateString(self.name)
        sinks_off = serialize_list(
            vector_builder=fi.StartSinksVector,
            builder=builder,
            xs=self.sinks
        )
        sources_off = serialize_list(
            vector_builder=fi.StartSourcesVector,
            builder=builder,
            xs=self.sources
        )
        fi.Start(builder)
        fi.AddName(builder, name_off)
        fi.AddSinks(builder, sinks_off)
        fi.AddSources(builder, sources_off)
        return fi.End(builder)
