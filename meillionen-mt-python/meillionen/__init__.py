from typing import Callable

from .meillionen import FuncInterface, StoreRef, FuncRequest, get_tensor_formats
import netCDF4
import xarray as xr
import pyarrow.parquet as pq


def dataframe_parquet_loader(data, schema):
    df = pq.read_table(source=data['path'])
    return df


def tensor_netcdf_loader(data, schema) -> xr.DataArray:
    dataset = netCDF4.Dataset(data['path'])
    variable = dataset[data['variable']]
    dimensions = variable.dimensions
    assert set(schema['dimensions']) == set(dimensions), f"{schema['dimensions']} != {dimensions}"
    mem_var = xr.DataArray(variable[(slice(s) for s in variable.shape)], dims=dimensions)
    return mem_var


class MissingSourceHandler(KeyError): pass


class SourceHandler:
    def __init__(self):
        self.handlers = {}

    def register(self, typename: str, handler: Callable):
        self.handlers[typename] = handler

    def handle(self, data, schema):
        typename = data['type']
        if typename not in self.handlers:
            raise MissingSourceHandler(typename)
        return self.handlers[typename](data=data, schema=schema)


source_handler = SourceHandler()
source_handler.register('NetCDF', tensor_netcdf_loader)