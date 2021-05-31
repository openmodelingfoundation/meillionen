import io
import flatbuffers
import meillionen.interface.Resource as ri
import meillionen.interface.Schema as schema


def field_to_bytesio(tab, field_offset):
    o = flatbuffers.number_types.UOffsetTFlags.py_type(tab.Offset(field_offset))
    if o == 0:
        return None
    offset = tab.Vector(o)
    length = tab.VectorLen(o)
    bo = io.BytesIO(memoryview(tab.Bytes)[offset:(offset + length)])
    return bo


class Schema(schema.Schema):
    PAYLOAD_OFFSET = 8

    def PayloadAsBytesIO(self):
        return field_to_bytesio(tab=self._tab, field_offset=self.PAYLOAD_OFFSET)


class Resource(ri.Resource):
    PAYLOAD_OFFSET = 8

    def PayloadAsBytesIO(self):
        return field_to_bytesio(tab=self._tab, field_offset=self.PAYLOAD_OFFSET)