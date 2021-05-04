import netCDF4
import numpy as np
from meillionen.resource import PandasLoaderSaver, FeatherResource, NetCDFResource, NetCDFPreSaver, Unvalidated
import pytest
import xarray as xr


def test_load_feather():
    source = FeatherResource.from_dict({"path": "../examples/crop-pipeline/simplecrop/data/yearly.feather"})
    df = PandasLoaderSaver(Unvalidated()).load(source)
    assert df.shape == (1, 26)


def test_save_netcdf():
    sink = NetCDFResource(
        path='data/swid.nc',
        variable='soil_water_infiltration__depth',
        dimensions=['x', 'y', 'time']
    )
    swid_schema = [('x', 6), ('y', 11), ('time', 365)]
    with NetCDFPreSaver(Unvalidated()).load(sink, swid_schema) as swid:
        xs = xr.DataArray(np.array(range(6*11)).reshape((6, 11)), dims=('x', 'y'))
        swid.set_slice(xs, time=5)
    with netCDF4.Dataset('data/swid.nc', 'r') as swid:
        assert swid.variables.keys() == {'soil_water_infiltration__depth'}
        v = swid['soil_water_infiltration__depth']
        assert v.dimensions == ('x', 'y', 'time')
        assert v[5,10,5] == 65