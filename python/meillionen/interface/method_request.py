import io

import flatbuffers
from meillionen.interface.base import MethodRequestArg

from ..settings import Settings
from . import _MethodRequest as mr
from .resource import deserialize_resource_payload, Resource
from .base import FlatbufferMixin


class _MethodRequest(mr._MethodRequest, FlatbufferMixin):
    ARGS_OFFSET = 8

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        return cls.get_root_as(buf, offset)

    # _MethodRequest
    def Args(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(self.ARGS_OFFSET))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from meillionen.interface.resource import _Resource
            obj = _Resource()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None


class MethodRequest:
    """
    A request to call a model method
    """
    def __init__(self, class_name, method_name, kwargs):
        self.class_name = class_name
        self.method_name = method_name
        self.kwargs = kwargs

    @classmethod
    def from_partial(cls, settings: Settings, class_name: str, method_name: str, resource_payloads, partition):
        """
        settings:
        """
        kwargs = {}
        for name, resource_payload in resource_payloads.items():
            mra = MethodRequestArg(class_name=class_name, method_name=method_name, arg_name=name)
            if hasattr(resource_payload, 'complete'):
                kwargs[name] = resource_payload.complete(settings=settings, mra=mra, partition=partition)
            else:
                kwargs[name] = resource_payload
        return cls(class_name=class_name, method_name=method_name, kwargs=kwargs)

    @staticmethod
    def _serialize_resources(builder: flatbuffers.Builder, resources):
        offsets = []
        for name, resource_payload in resources.items():
            resource = Resource(name=name, resource_payload=resource_payload)
            offsets.append(resource.serialize(builder))
        mr.StartArgsVector(builder, len(offsets))
        for offset in offsets:
            builder.PrependUOffsetTRelative(offset)
        return builder.EndVector()

    @staticmethod
    def _deserialize_resources(getter, n):
        resources = {}
        for i in range(n):
            resource = getter(i)
            name = resource.Name().decode('utf-8')
            resource_payload = deserialize_resource_payload(resource)
            resources[name] = resource_payload
        return resources

    @classmethod
    def deserialize(cls, buffer: io.BytesIO):
        req = _MethodRequest.GetRootAs(buffer, 0)
        class_name = req.ClassName().decode('utf-8')
        method_name = req.MethodName().decode('utf-8')
        kwargs = cls._deserialize_resources(getter=req.Args, n=req.ArgsLength())
        return cls(
            class_name=class_name,
            method_name=method_name,
            kwargs=kwargs
        )

    def serialize(self, builder: flatbuffers.Builder):
        """
        Serialize a method request into a flatbuffer builder

        :param builder: the flatbuffer builder
        """
        cls_off = builder.CreateString(self.class_name)
        method_off = builder.CreateString(self.method_name)
        kwargs_off = self._serialize_resources(builder, self.kwargs)
        mr.Start(builder)
        mr.AddClassName(builder, cls_off)
        mr.AddMethodName(builder, method_off)
        mr.AddArgs(builder, kwargs_off)
        return mr.End(builder)

    def get_arg(self, name):
        """
        Get the path metadata for a argument

        :param name: the name of the argument
        """
        return MethodRequestArg(class_name=self.class_name, method_name=self.method_name, arg_name=self.kwargs[name])