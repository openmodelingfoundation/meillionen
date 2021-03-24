from typing import Callable, Type, Any

from .meillionen import FuncInterface, FuncRequest
import netCDF4
import xarray as xr
import pyarrow.parquet as pq


class MissingSourceHandler(KeyError): pass


def dataframe_parquet_loader(data, schema):
    df = pq.read_table(source=data['path'])
    return df


def tensor_netcdf_source_handler(metadata, schema) -> xr.DataArray:
    dataset = xr.open_dataset(metadata['path'])
    variable = dataset.data_vars[metadata['variable']]
    assert set(variable.dims) == set(schema['dimensions'])
    return variable


class SourceHandler:
    def __init__(self):
        self.handlers = {}

    def register(self, typename: str, handler: Callable):
        self.handlers[typename] = handler

    def handle(self, metadata, schema):
        typename = metadata['type']
        if typename not in self.handlers:
            raise MissingSourceHandler(typename)
        return self.handlers[typename](metadata=metadata, schema=schema)


source_handler = SourceHandler()
source_handler.register('NetCDF', tensor_netcdf_source_handler)


def tensor_xarray_netcdf_sink_handler(data: xr.DataArray, metadata, schema):
    """
    :param data: a handle to the dataset
    :param metadata: where to save the result to
    :param schema: invariants to check the data against
    :return:
    """
    assert set(data.dims) == set(schema['dimensions'])
    assert data.name == schema['variable']
    data.to_netcdf(file=metadata['path'])


class SinkHandler:
    def __init__(self):
        self.handlers = {}

    def register(self, typename: (Type[Any], str), handler: Callable):
        self.handlers[typename] = handler

    def handle(self, data, metadata, schema):
        typename = (type(data), metadata['type'])
        self.handlers[typename](data=data, metadata=metadata, schema=schema)


sink_handler = SinkHandler()
sink_handler.register((xr.DataArray, 'NetCDF'), tensor_xarray_netcdf_sink_handler)