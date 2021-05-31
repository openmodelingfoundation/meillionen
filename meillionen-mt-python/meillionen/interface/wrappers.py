import io
from typing import Dict, Any, List

import flatbuffers
import meillionen.interface.ClassInterface as ci
import meillionen.interface.FunctionInterface as fi
import meillionen.interface.MethodRequest as mr
from ..registry import RESOURCES, SCHEMAS


def _serialize_vector_table(builder, start_offset_vector, offsets):
    start_offset_vector(builder, len(offsets))
    for offset in offsets:
        builder.PrependSOffsetTRelative(offset)
    builder.EndVector()


class MethodInterface:
    def __init__(self, method_name, sources, sinks):
        self.method_name = method_name
        self._sources = sources
        self._sinks = sinks

    def source(self, name):
        return self._sources[name]

    def sink(self, name):
        return self._sinks[name]

    def serialize(self, builder: flatbuffers.Builder):
        method_name_offset = builder.CreateString(self.method_name)
        sink_offsets = []
        for name, sink in self._sinks.items():
            resource_offset = sink.serialize_data(builder, name)
            sink_offsets.append(resource_offset)

        source_offsets = []
        for name, source in self._sources.items():
            resource_offset = source.serialize_data(builder, name)
            source_offsets.append(resource_offset)

        fi.Start(builder)
        fi.AddName(builder, method_name_offset)

        _serialize_vector_table(
            builder=builder,
            start_offset_vector=fi.StartSinksVector,
            offsets=sink_offsets
        )

        _serialize_vector_table(
            builder=builder,
            start_offset_vector=fi.StartSourcesVector,
            offsets=source_offsets
        )
        return fi.End(builder)

    @classmethod
    def from_class(cls, mi: fi.FunctionInterface) -> 'MethodInterface':
        assert not mi.SourcesIsNone()
        assert not mi.SinksIsNone()

        method_name = mi.Name()

        sources = {}
        n = mi.SourcesLength()
        for i in range(n):
            schema_raw = mi.Sources(i)
            name, source = SCHEMAS.deserialize(schema_raw)
            sources[name] = source

        sinks = {}
        n = mi.SinksLength()
        for i in range(n):
            schema_raw = mi.Sinks(i)
            name, sink = SCHEMAS.deserialize(schema_raw)
            sinks[name] = sink

        return cls(
            method_name=method_name,
            sources=sources,
            sinks=sinks
        )

    @classmethod
    def deserialize(cls, buffer):
        mi = fi.FunctionInterface.GetRootAs(buffer, 0)
        return cls.from_class(mi)


class ClassInterface:
    def __init__(self, class_name, methods: List[MethodInterface]):
        self.class_name = class_name
        self.methods = {m.method_name: m for m in methods}
        self.offsets = None

    def serialize(self, builder):
        self.offsets = {'class_name': builder.CreateString(self.class_name)}
        method_offsets = []
        for name, method in self.methods.items():
            method_offset = method.serialize(builder)
            method_offsets.append(method_offset)

        ci.Start(builder)
        ci.AddName(builder, self.offsets['class_name'])
        _serialize_vector_table(
            builder=builder,
            start_offset_vector=ci.StartMethodsVector,
            offsets=method_offsets
        )
        ci.End(builder)

    def method(self, name):
        return self.methods[name]

    @classmethod
    def from_class(cls, class_interface: ci.ClassInterface):
        class_name = class_interface.Name()
        assert not class_interface.MethodsIsNone()
        methods = []
        n = class_interface.MethodsLength()
        for i in range(n):
            method = MethodInterface.from_class(class_interface.Methods(i))
            methods.append(method)
        return cls(class_name=class_name, methods=methods)

    @classmethod
    def deserialize(cls, buffer):
        class_interface = ci.ClassInterface.GetRootAs(buffer, 0)
        return cls.from_class(class_interface)


class MethodRequest:
    def __init__(self, class_name, method_name, sources, sinks):
        self.class_name = class_name
        self.method_name = method_name
        self._sources = sources
        self._sinks = sinks

    def sink(self, name):
        return self._sinks[name]

    @property
    def sinks(self):
        return self._sinks.items()

    def source(self, name):
        return self._sources[name]

    @property
    def sources(self):
        return self._sources.items()

    def serialize(self, builder: flatbuffers.Builder):
        class_name_offset = builder.CreateString(self.class_name)
        method_name_offset = builder.CreateString(self.method_name)
        sink_offsets = []
        for name, sink in self.sinks.items():
            resource_offset = sink.serialize_data(builder, name)
            sink_offsets.append(resource_offset)

        source_offsets = []
        for name, source in self.sources.items():
            resource_offset = source.serialize_data(builder, name)
            source_offsets.append(resource_offset)

        mr.Start(builder)
        mr.AddClassName(builder, class_name_offset)
        mr.AddMethodName(builder, method_name_offset)

        _serialize_vector_table(
            builder=builder,
            start_offset_vector=fi.StartSinksVector,
            offsets=sink_offsets
        )

        _serialize_vector_table(
            builder=builder,
            start_offset_vector=fi.StartSourcesVector,
            offsets=source_offsets
        )
        return fi.End(builder)

    @classmethod
    def deserialize(cls, buffer):
        data = mr.MethodRequest.GetRootAs(buffer, 0)
        sources = {}
        n = data.SourcesLength()
        for i in range(n):
            raw_resource = data.Sources(i)
            name, source = RESOURCES.deserialize(raw_resource)
            sources[name] = source

        sinks = {}
        n = data.SinksLength()
        for i in range(n):
            raw_resource = data.Sinks(i)
            name, sink = RESOURCES.deserialize(raw_resource)
            sinks[name] = sink

        cls(
            class_name=data.ClassName(),
            method_name=data.MethodName(),
            sources=sources,
            sinks=sinks)
