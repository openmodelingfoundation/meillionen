import dataclasses
import inspect
from typing import Any, Tuple, Dict


@dataclasses.dataclass
class MethodCall:
    id: Any
    attribute: str
    args: Tuple
    kwargs: Dict[str, Any]

    def __call__(self, model):
        return getattr(model, self.attribute)(*self.args, **self.kwargs)


@dataclasses.dataclass
class Variable:
    method_call: MethodCall


class MethodTracer:
    def __init__(self, tracer, model_id, attribute):
        self.tracer = tracer
        self.model_id = model_id
        self.attribute = attribute

    def __call__(self, *args, **kwargs):
        method_call = MethodCall(self.model_id, self.attribute, args, kwargs)
        self.tracer.add(method_call)
        return Variable(method_call)


class BMIModelTracer:
    def __init__(self, tracer, model_id):
        self._tracer = tracer
        self.model_id = model_id

    def __getattr__(self, item):
        return MethodTracer(self._tracer, self.model_id, item)


class Tracer:
    def __init__(self):
        self.instructions = []

    def add(self, instruction):
        self.instructions.append(instruction)

    def trace(self, f):
        sig = inspect.signature(f)
        kwargs = {}
        for param in sig.parameters:
            kwargs[param] = BMIModelTracer(self, param)
        f(**kwargs)


def simulate(waves, cem):
    for time in range(1000):
        waves.update()
        angle = waves.get_value('wave_angle')
        cem.set_value('wave_angle', angle)
        cem.update()


tracer = Tracer()
tracer.trace(simulate)