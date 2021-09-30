from .common import mod_int, mr
from flatbuffers import Builder

from meillionen.interface.method_request import MethodRequest
from meillionen.interface.module_interface import ModuleInterface


def test_module_interface_round_trip():
    m = mod_int.handle(mr)
    assert m.name == 'run'

    builder = Builder()
    builder.Finish(mod_int.serialize(builder))
    data = builder.Output()
    mi2 = ModuleInterface.deserialize(data)
    m = mi2.handle(mr)
    assert m.name == 'run'


def test_method_request_round_trip():
    builder = Builder()
    builder.Finish(mr.serialize(builder))
    data = builder.Output()
    mr2 = MethodRequest.deserialize(data)
    assert mr2.class_name == mr.class_name
    assert mr2.method_name == mr.method_name