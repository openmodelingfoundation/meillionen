from typing import Dict

import flatbuffers

from .base import deserialize_to_dict, serialize_list
from .class_interface import ClassInterface
from .method_request import MethodRequest
from . import _ModuleInterface as mi
from meillionen.exceptions import ClassNotFound


class ModuleInterface:
    def __init__(self, classes: Dict[str, ClassInterface]):
        self.classes = classes

    def __call__(self, req: MethodRequest):
        try:
            klass = self.classes[req.class_name]
        except KeyError as e:
            raise ClassNotFound() from e

        method = klass.get_method(req.method_name)
        return method(sinks=req.sinks, sources=req.sources)

    @classmethod
    def deserialize(cls, buffer):
        interface = mi._ModuleInterface.GetRootAs(buffer, 0)
        classes = deserialize_to_dict(
            constructor=ClassInterface.from_interface,
            getter=interface.Classes,
            n=interface.ClassesLength())
        return cls(classes=classes)

    def serialize(self, builder: flatbuffers.Builder):
        class_off = serialize_list(
            builder=builder,
            vector_builder=mi.StartClassesVector,
            xs=self.classes
        )
        mi.Start(builder)
        mi.AddClasses(builder, class_off)
        return mi.End(builder)

    def handle(self, req: MethodRequest):
        klass = self.classes[req.class_name]
        method = klass._methods[req.method_name]
        return {
            'method': method
        }