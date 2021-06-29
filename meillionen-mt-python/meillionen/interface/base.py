import flatbuffers
import io


def deserialize_to_dict(constructor, getter, n):
    xs = {}
    for i in range(n):
        x = constructor(getter(i))
        xs[x.name] = x
    return xs


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