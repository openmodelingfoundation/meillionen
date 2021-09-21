import flatbuffers

from . import _MethodRequest as mr
from .resource import deserialize_resource


class MethodRequest:
    def __init__(self, class_name, method_name, kwargs):
        self.class_name = class_name
        self.method_name = method_name
        self.kwargs = kwargs

    @staticmethod
    def _serialize_resources(builder: flatbuffers.Builder, resources):
        mr.StartSinksVector(builder, len(resources))
        for resource in resources.values():
            resource.serialize(builder)
        return builder.EndVector()

    @staticmethod
    def _deserialize_resources(getter, n):
        resources = {}
        for i in range(n):
            resource = deserialize_resource(getter(i))
            resources[resource.name] = resource
        return resources

    @classmethod
    def deserialize(cls, buffer):
        req = mr._MethodRequest.GetRootAs(buffer, 0)
        class_name = req.ClassName()
        method_name = req.MethodName()
        kwargs = cls._deserialize_resources(getter=req.Args, n=req.SinksLength())
        return cls(
            class_name=class_name,
            method_name=method_name,
            kwargs=kwargs
        )

    def serialize(self, builder: flatbuffers.Builder):
        cls_off = builder.CreateString(self.class_name)
        method_off = builder.CreateString(self.method_name)
        args_off = self._serialize_resources(builder, self.args)
        sources_off = self._serialize_resources(builder, self.sources)
        mr.Start(builder)
        mr.AddMethodName(builder, cls_off)
        mr.AddMethodName(builder, method_off)
        mr.AddArgs(builder, args_off)
        return mr.End(builder)
