# automatically generated by the FlatBuffers compiler, do not modify

# namespace: interface

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class FunctionInterface(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = FunctionInterface()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsFunctionInterface(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # FunctionInterface
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # FunctionInterface
    def Name(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # FunctionInterface
    def Sinks(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from meillionen.interface.Schema import Schema
            obj = Schema()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # FunctionInterface
    def SinksLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # FunctionInterface
    def SinksIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        return o == 0

    # FunctionInterface
    def Sources(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from meillionen.interface.Schema import Schema
            obj = Schema()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # FunctionInterface
    def SourcesLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # FunctionInterface
    def SourcesIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        return o == 0

def Start(builder): builder.StartObject(3)
def FunctionInterfaceStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddName(builder, name): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(name), 0)
def FunctionInterfaceAddName(builder, name):
    """This method is deprecated. Please switch to AddName."""
    return AddName(builder, name)
def AddSinks(builder, sinks): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(sinks), 0)
def FunctionInterfaceAddSinks(builder, sinks):
    """This method is deprecated. Please switch to AddSinks."""
    return AddSinks(builder, sinks)
def StartSinksVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def FunctionInterfaceStartSinksVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartSinksVector(builder, numElems)
def AddSources(builder, sources): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(sources), 0)
def FunctionInterfaceAddSources(builder, sources):
    """This method is deprecated. Please switch to AddSources."""
    return AddSources(builder, sources)
def StartSourcesVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def FunctionInterfaceStartSourcesVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartSourcesVector(builder, numElems)
def End(builder): return builder.EndObject()
def FunctionInterfaceEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)