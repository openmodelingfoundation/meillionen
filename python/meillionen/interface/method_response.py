from . import _MethodResponse as mr
from . import _MethodResponseType as mrt
from .base import FlatbufferMixin

import flatbuffers


class _MethodResponse(mr._MethodResponse, FlatbufferMixin):
    pass


class MethodResponse:
    @classmethod
    def from_success(cls, value: mrs.Metho):
        typ = mrt._MethodResponseType.success


    @classmethod
    def from_failure(cls):
        pass

