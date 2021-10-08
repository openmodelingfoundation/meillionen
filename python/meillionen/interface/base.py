import textwrap
from typing import Dict, Any

import flatbuffers
import io


def leading_indent(s: str, indent: int):
    return textwrap.indent(s, ' ' * indent)


def deserialize_to_dict(constructor, getter, n):
    xs = {}
    for i in range(n):
        x = constructor(getter(i))
        xs[x.name] = x
    return xs


def serialize_dict(builder: flatbuffers.Builder, vector_builder, xs: Dict[str, Any]):
    xs_list_off = []
    for x in xs.values():
        xs_off = x.serialize(builder)
        xs_list_off.append(xs_off)

    vector_builder(builder, len(xs))
    for off in xs_list_off:
        builder.PrependUOffsetTRelative(off)
    return builder.EndVector()


def serialize_list(builder: flatbuffers.Builder, vector_builder, xs):
    xs_list_off = []
    for x in xs:
        xs_off = x.serialize(builder)
        xs_list_off.append(xs_off)

    vector_builder(builder, len(xs))
    for off in xs_list_off:
        builder.PrependUOffsetTRelative(off)
    return builder.EndVector()


def field_to_bytesio(tab, field_offset):
    o = flatbuffers.number_types.UOffsetTFlags.py_type(tab.Offset(field_offset))
    if o == 0:
        return None
    offset = tab.Vector(o)
    length = tab.VectorLen(o)
    bo = io.BytesIO(memoryview(tab.Bytes)[offset:(offset + length)])
    return bo


class FlatbufferMixin:
    @classmethod
    def get_root_as(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = cls()
        x.Init(buf, n + offset)
        return x

    def _get_resource(self, j, offset, klass):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(offset))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            obj = klass()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None


class MethodRequestArg:
    def __init__(self, class_name: str, method_name: str, arg_name: str):
        self.class_name = class_name
        self.method_name = method_name
        self.arg_name = arg_name