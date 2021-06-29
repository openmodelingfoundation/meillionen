import io

import flatbuffers
import pyarrow as pa

from . import _Schema as s
from .base import field_to_bytesio, serialize_list
from .resource import get_resource_class


class DataFrameSchema:
    def __init__(self, arrow_schema: pa.Schema):
        self.arrow_schema = arrow_schema

    @classmethod
    def deserialize(cls, buffer: io.BytesIO):
        schema = pa.ipc.read_schema(buffer)
        return cls(arrow_schema=schema)

    def serialize(self, builder: flatbuffers.Builder):
        buf: pa.Buffer = self.arrow_schema.serialize()
        off = builder.CreateByteVector(buf.to_pybytes())
        return off


class TensorSchema:
    pass


class Schemaless:
    @classmethod
    def deserialize(cls, buffer: io.BytesIO):
        return cls()

    def serialize(self, builder: flatbuffers.Builder):
        return


_SCHEMA_CLASSES = {
    s.name: s for s in
    [
        DataFrameSchema,
        Schemaless
    ]
}


def clear_schema_classes():
    _SCHEMA_CLASSES.clear()


def register_schema_class(cls):
    _SCHEMA_CLASSES[cls.name] = cls


def get_schema_class(name):
    return _SCHEMA_CLASSES[name]


class _Schema(s._Schema):
    PAYLOAD_OFFSET = 8

    def PayloadAsBytesIO(self):
        return field_to_bytesio(tab=self._tab, field_offset=self.PAYLOAD_OFFSET)


class Schema:
    def __init__(self, name, schema, resource_classes):
        self.name = name
        self.schema = schema
        self.resource_classes = resource_classes

    def serialize(self, builder: flatbuffers.Builder):
        name_off = builder.CreateString(self.name)
        type_name_off = builder.CreateString(self.schema.name)
        payload_off = self.schema.serialize(builder)
        resource_names_off = serialize_list(
            builder=builder,
            vector_builder=s.StartResourceNamesVector,
            xs=self.resource_classes
        )
        s.Start(builder)
        s.AddName(builder, name_off)
        s.AddTypeName(builder, type_name_off)
        s.AddPayload(builder, payload_off)
        s.AddResourceNames(builder, resource_names_off)
        schema_off = s.End(builder)
        return schema_off

    @classmethod
    def from_schema(cls, schema: _Schema):
        name = schema.Name()
        payload_class = get_schema_class(schema.TypeName())
        payload = payload_class.deserialize(schema.PayloadAsBytesIO())
        resource_classes = []
        for i in range(schema.ResourceNamesLength()):
            resource_name = schema.ResourceNames(i)
            resource_class = get_resource_class(resource_name)
            resource_classes.append(resource_class)
        cls(name=name, schema=payload, resource_classes=resource_classes)