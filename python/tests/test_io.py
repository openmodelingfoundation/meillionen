from typing import Tuple

import flatbuffers
import netCDF4
import numpy as np
import pandas as pd
import pyarrow as pa
from meillionen.interface.class_interface import ClassInterface
from meillionen.interface.method_interface import MethodInterface
from meillionen.interface.module_interface import ModuleInterface
from meillionen.interface.method_request import MethodRequest
from meillionen.interface.mutability import Mutability
from meillionen.interface.schema import PandasHandler, NetCDFSliceHandler
from meillionen.interface.resource import Feather, NetCDF
import xarray as xr
from meillionen.server import Server


def test_call_method():
    soil_df = pd.DataFrame(data={
        'day_of_year': pd.Series(data=[1,2,3], dtype='int32'),
        'acidity': pd.Series(data=[6,9,7.0], dtype='float32')
    })
    yields_df = pd.DataFrame(data={
        'day_of_year': soil_df.day_of_year,
        'yield_mass': pd.Series(data=[6,5,7.0], dtype='float32')
    })

    soil_resource = Feather('tests/data/soil.feather')
    yields_resource = Feather('tests/data/yields.feather')

    soil_handler = PandasHandler(
        name='soil',
        s=pa.schema([
            pa.field(name='day_of_year', type=pa.int32(), nullable=False),
            pa.field(name='acidity', type=pa.float32(), nullable=False)
        ]),
        mutability=Mutability.read
    )
    yields_handler = PandasHandler(
        name='yields',
        s=pa.schema([
            pa.field(name='day_of_year', type=pa.int32(), nullable=False),
            pa.field(name='yield_mass', type=pa.float32(), nullable=False)
        ]),
        mutability=Mutability.write
    )

    def run(soil: pd.DataFrame, yields: Tuple[PandasHandler, Feather]):
        yield_handler, yield_resource = yields
        ys = soil.assign(yield_mass= 7 - np.abs(soil.acidity - 7)).drop(columns=['acidity'])
        yield_handler.save(yield_resource, data=ys)

    mi = MethodInterface(
        name='run',
        args=[
            soil_handler,
            yields_handler
        ],
        handler=run
    )
    ci = ClassInterface(name='yield_calculator', methods=[mi])
    mod_int = ModuleInterface([ci])

    mr = MethodRequest(
        class_name='yield_calculator',
        method_name='run',
        kwargs={
            'soil': soil_resource,
            'yields': yields_resource
        })

    soil_handler.save(soil_resource, data=soil_df)
    mod_int(mr)
    yields_result_df = yields_handler.load(yields_resource)
    assert yields_result_df.equals(yields_df)

    server = Server(mod_int)
    builder = flatbuffers.Builder()
    builder.Finish(mr.serialize(builder))
    fd = builder.Output()
    server._run(fd=fd)


def test_load_feather():
    source = Feather(path="../examples/crop-pipeline/workflows/inputs/yearly.feather")
    df = PandasHandler(name='daily', s=pa.schema([]), mutability=Mutability.read).load(source)
    assert df.shape == (1, 26)


def test_save_netcdf():
    sink = NetCDF(
        path='data/swid.nc',
        variable='soil_water_infiltration__depth'
    )
    swid_schema = {'x': 6, 'y': 11, 'time': 365}
    with NetCDFSliceHandler(name='daily', data_type='f4', dimensions=['x', 'y', 'time'], mutability=Mutability.write).save(sink, swid_schema) as swid:
        xs = xr.DataArray(np.array(range(6*11)).reshape((6, 11)), dims=('x', 'y'))
        swid.set({'time': 5}, xs)
    with netCDF4.Dataset('data/swid.nc', 'r') as swid:
        assert swid.variables.keys() == {'soil_water_infiltration__depth'}
        v = swid['soil_water_infiltration__depth']
        assert v.dimensions == ('x', 'y', 'time')
        assert v[5,10,5] == 65