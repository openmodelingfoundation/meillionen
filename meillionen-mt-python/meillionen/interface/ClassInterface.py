# automatically generated by the FlatBuffers compiler, do not modify

# namespace: interface

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class ClassInterface(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = ClassInterface()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsClassInterface(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # ClassInterface
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # ClassInterface
    def Name(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # ClassInterface
    def TypeName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # ClassInterface
    def Methods(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from meillionen.interface.FunctionInterface import FunctionInterface
            obj = FunctionInterface()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # ClassInterface
    def MethodsLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # ClassInterface
    def MethodsIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        return o == 0

def Start(builder): builder.StartObject(3)
def ClassInterfaceStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddName(builder, name): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(name), 0)
def ClassInterfaceAddName(builder, name):
    """This method is deprecated. Please switch to AddName."""
    return AddName(builder, name)
def AddTypeName(builder, typeName): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(typeName), 0)
def ClassInterfaceAddTypeName(builder, typeName):
    """This method is deprecated. Please switch to AddTypeName."""
    return AddTypeName(builder, typeName)
def AddMethods(builder, methods): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(methods), 0)
def ClassInterfaceAddMethods(builder, methods):
    """This method is deprecated. Please switch to AddMethods."""
    return AddMethods(builder, methods)
def StartMethodsVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def ClassInterfaceStartMethodsVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartMethodsVector(builder, numElems)
def End(builder): return builder.EndObject()
def ClassInterfaceEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)