import pytest
import numpy as np
import xarray as xr
from meillionen import source_handler


def test_netcdf_source_handler():
    netcdf_schema = {
        "label": "Surface Water Depth",
        "dimensions": ["x", "y", "day"],
        "data_type": "Float64"
    }
    netcdf_data = {
        'type': 'NetCDF',
        'path': 'data/swd.nc',
        'variable': 'surface_water__depth'
    }
    swd = source_handler.handle(data=netcdf_data, schema=netcdf_schema)
    assert swd[:,0,0].equals(xr.DataArray(np.array([1,1], dtype=np.float32), dims=('x',)))
    assert swd.sel(y=1,day=0).equals(xr.DataArray(np.array([2,2], dtype=np.float32), dims=('x',)))


