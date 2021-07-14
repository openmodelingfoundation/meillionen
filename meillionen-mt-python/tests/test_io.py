import netCDF4
import numpy as np
import pyarrow as pa
from meillionen.interface.schema import PandasHandler, NetCDFSliceHandler
from meillionen.interface.resource import Feather, NetCDF
import xarray as xr


def test_load_feather():
    source = Feather(path="../examples/crop-pipeline/workflows/inputs/yearly.feather")
    df = PandasHandler(name='daily', s=pa.schema([])).load(source)
    assert df.shape == (1, 26)


def test_save_netcdf():
    sink = NetCDF(
        path='data/swid.nc',
        variable='soil_water_infiltration__depth'
    )
    swid_schema = {'x': 6, 'y': 11, 'time': 365}
    with NetCDFSliceHandler(name='daily', data_type='f4', dimensions=['x', 'y', 'time']).save(sink, swid_schema) as swid:
        xs = xr.DataArray(np.array(range(6*11)).reshape((6, 11)), dims=('x', 'y'))
        swid.set({'time': 5}, xs)
    with netCDF4.Dataset('data/swid.nc', 'r') as swid:
        assert swid.variables.keys() == {'soil_water_infiltration__depth'}
        v = swid['soil_water_infiltration__depth']
        assert v.dimensions == ('x', 'y', 'time')
        assert v[5,10,5] == 65