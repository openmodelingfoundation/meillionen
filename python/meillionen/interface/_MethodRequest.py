# automatically generated by the FlatBuffers compiler, do not modify

# namespace: interface

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class _MethodRequest(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = _MethodRequest()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAs_MethodRequest(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # _MethodRequest
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # _MethodRequest
    def ClassName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # _MethodRequest
    def MethodName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # _MethodRequest
    def Args(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from meillionen.interface._Resource import _Resource
            obj = _Resource()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # _MethodRequest
    def ArgsLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # _MethodRequest
    def ArgsIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        return o == 0

def Start(builder): builder.StartObject(3)
def _MethodRequestStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddClassName(builder, className): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(className), 0)
def _MethodRequestAddClassName(builder, className):
    """This method is deprecated. Please switch to AddClassName."""
    return AddClassName(builder, className)
def AddMethodName(builder, methodName): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(methodName), 0)
def _MethodRequestAddMethodName(builder, methodName):
    """This method is deprecated. Please switch to AddMethodName."""
    return AddMethodName(builder, methodName)
def AddArgs(builder, args): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(args), 0)
def _MethodRequestAddArgs(builder, args):
    """This method is deprecated. Please switch to AddArgs."""
    return AddArgs(builder, args)
def StartArgsVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def _MethodRequestStartArgsVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartArgsVector(builder, numElems)
def End(builder): return builder.EndObject()
def _MethodRequestEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)