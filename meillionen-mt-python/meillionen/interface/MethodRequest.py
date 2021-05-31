# automatically generated by the FlatBuffers compiler, do not modify

# namespace: interface

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class MethodRequest(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = MethodRequest()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsMethodRequest(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # MethodRequest
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # MethodRequest
    def ClassName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # MethodRequest
    def MethodName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # MethodRequest
    def Sources(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from meillionen.interface.Resource import Resource
            obj = Resource()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # MethodRequest
    def SourcesLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # MethodRequest
    def SourcesIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        return o == 0

    # MethodRequest
    def Sinks(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from meillionen.interface.Resource import Resource
            obj = Resource()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # MethodRequest
    def SinksLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # MethodRequest
    def SinksIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        return o == 0

def Start(builder): builder.StartObject(4)
def MethodRequestStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddClassName(builder, className): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(className), 0)
def MethodRequestAddClassName(builder, className):
    """This method is deprecated. Please switch to AddClassName."""
    return AddClassName(builder, className)
def AddMethodName(builder, methodName): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(methodName), 0)
def MethodRequestAddMethodName(builder, methodName):
    """This method is deprecated. Please switch to AddMethodName."""
    return AddMethodName(builder, methodName)
def AddSources(builder, sources): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(sources), 0)
def MethodRequestAddSources(builder, sources):
    """This method is deprecated. Please switch to AddSources."""
    return AddSources(builder, sources)
def StartSourcesVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def MethodRequestStartSourcesVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartSourcesVector(builder, numElems)
def AddSinks(builder, sinks): builder.PrependUOffsetTRelativeSlot(3, flatbuffers.number_types.UOffsetTFlags.py_type(sinks), 0)
def MethodRequestAddSinks(builder, sinks):
    """This method is deprecated. Please switch to AddSinks."""
    return AddSinks(builder, sinks)
def StartSinksVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def MethodRequestStartSinksVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartSinksVector(builder, numElems)
def End(builder): return builder.EndObject()
def MethodRequestEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)