from . import _MethodResponseError as mre
from .base import FlatbufferMixin

import flatbuffers


class _MethodResponseError(mre._MethodResponseError, FlatbufferMixin):
    pass


class MethodResponseError:
    def __init__(self, name: str, trace: str):
        self.name = name
        self.trace = trace

    @classmethod
    def from_error(cls, error: Exception):
        name = error.__class__.__name__
        trace = str(error.__traceback__)

    def serialize(self, builder: flatbuffers.Builder):
        name_off = builder.CreateString(self.name)
        trace_off = builder.CreateString(self.trace)
        mre.Start(builder)
        mre.AddName(builder, name_off)
        mre.AddTrace(builder, trace_off)
        return mre.End(builder)

    @classmethod
    def from_class(cls, builder: flatbuffers.Builder):
        pass