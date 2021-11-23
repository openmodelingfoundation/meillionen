from . import _MethodResponseSuccess as mrs
from .base import FlatbufferMixin, serialize_list

import flatbuffers

from .resource import Resource


class _MethodResponseSuccess(mrs._MethodResponseSuccess):
    pass


class MethodResponseSuccess:
    def __init__(self, resources=None):
        self.resources = resources if resources is not None else []

    def serialize(self, builder: flatbuffers.Builder):
        data = serialize_list(
            builder=builder,
            vector_builder=mrs.StartResourcesVector,
            xs=self.resources)
        mrs.Start(builder)
        mrs.AddResources(builder, data)
        return mrs.End(builder)

    @classmethod
    def _deserialize_resources(cls, getter, n):
        data = {}
        for i in range(n):
            _resource = getter(i)
            resource = Resource.from_class(_resource)
            data[resource.name] = resource
        return data

    @classmethod
    def from_class(cls, _mrs: _MethodResponseSuccess):
        data = cls._deserialize_resources(
            getter=_mrs.Resources,
            n=_mrs.ResourcesLength())
        return cls(resources=data)
