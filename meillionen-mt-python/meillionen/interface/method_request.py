import flatbuffers

from . import _MethodRequest as mr
from .resource import deserialize_resource_payload, Resource


class MethodRequest:
    def __init__(self, class_name, method_name, kwargs):
        self.class_name = class_name
        self.method_name = method_name
        self.kwargs = kwargs

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
    def deserialize(cls, buffer):
        req = mr._MethodRequest.GetRootAs(buffer, 0)
        class_name = req.ClassName()
        method_name = req.MethodName()
        kwargs = cls._deserialize_resources(getter=req.Args, n=req.ArgsLength())
        return cls(
            class_name=class_name,
            method_name=method_name,
            kwargs=kwargs
        )

    def serialize(self, builder: flatbuffers.Builder):
        cls_off = builder.CreateString(self.class_name)
        method_off = builder.CreateString(self.method_name)
        kwargs_off = self._serialize_resources(builder, self.kwargs)
        mr.Start(builder)
        mr.AddClassName(builder, cls_off)
        mr.AddMethodName(builder, method_off)
        mr.AddArgs(builder, kwargs_off)
        return mr.End(builder)
