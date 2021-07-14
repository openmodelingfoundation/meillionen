import pytest

import pyarrow as pa
from flatbuffers import Builder

from meillionen.interface.method_request import MethodRequest
from meillionen.interface.class_interface import  ClassInterface
from meillionen.interface.method_interface import  MethodInterface
from meillionen.interface.module_interface import ModuleInterface
from meillionen.interface.schema import PandasHandler


mi = MethodInterface(
    'run',
    sources=[
        PandasHandler(
            name='daily',
            s=pa.schema([
                ('irrigation', pa.float32()),
                ('temp_max', pa.float32()),
                ('temp_min', pa.float32())
            ])
        ),
        PandasHandler(
            name='yearly',
            s=pa.schema([
                ('plant_leaves_max_number', pa.float32()),
                ('plant_nb', pa.float32())
            ])
        )
    ],
    sinks=[
        PandasHandler(
            name='soil',
            s=pa.schema([
                ('day_of_year', pa.float32()),
                ('soil_daily_runoff', pa.float32())
            ])
        )
    ])

ci = ClassInterface(
    'simplecrop',
    methods={
        m.name: m for m in [mi]
    }
)

mi = ModuleInterface([
    ci
])


def test_module_interface_round_trip():
    mr = MethodRequest(class_name='simplecrop', method_name='run', sinks={}, sources={})
    m = mi.handle(mr)
    assert m.name == 'run'

    builder = Builder()
    builder.Finish(mi.serialize(builder))
    data = builder.Output()
    mi2 = ModuleInterface.deserialize(data)
    m = mi.handle(mr)
    assert m.name == 'run'