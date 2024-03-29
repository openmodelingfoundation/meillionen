# automatically generated by the FlatBuffers compiler, do not modify

# namespace: interface

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class _Resource(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = _Resource()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAs_Resource(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # _Resource
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # _Resource
    def Name(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # _Resource
    def TypeName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # _Resource
    def Payload(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.Uint8Flags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 1))
        return 0

    # _Resource
    def PayloadAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Uint8Flags, o)
        return 0

    # _Resource
    def PayloadLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # _Resource
    def PayloadIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        return o == 0

def Start(builder): builder.StartObject(3)
def _ResourceStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddName(builder, name): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(name), 0)
def _ResourceAddName(builder, name):
    """This method is deprecated. Please switch to AddName."""
    return AddName(builder, name)
def AddTypeName(builder, typeName): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(typeName), 0)
def _ResourceAddTypeName(builder, typeName):
    """This method is deprecated. Please switch to AddTypeName."""
    return AddTypeName(builder, typeName)
def AddPayload(builder, payload): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(payload), 0)
def _ResourceAddPayload(builder, payload):
    """This method is deprecated. Please switch to AddPayload."""
    return AddPayload(builder, payload)
def StartPayloadVector(builder, numElems): return builder.StartVector(1, numElems, 1)
def _ResourceStartPayloadVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartPayloadVector(builder, numElems)
def End(builder): return builder.EndObject()
def _ResourceEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)