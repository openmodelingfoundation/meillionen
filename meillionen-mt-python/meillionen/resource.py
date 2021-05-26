"""
Resources are references to datasets that provide the information necessary to
load or save the data later.
"""
import functools
import os
import pathlib
from typing import Dict, Union, Any, List, Optional, Literal

from pyarrow.dataset import dataset, DirectoryPartitioning
import numpy as np
import pandas as pd
import pyarrow as pa
import netCDF4
import xarray as xr
from landlab.io import read_esri_ascii, write_esri_ascii
from meillionen.meillionen import \
    FeatherResource as _FeatherResource, \
    FileResource as _FileResource, \
    NetCDFResource as _NetCDFResource, \
    ParquetResource as _ParquetResource, \
    DataFrameValidator, \
    TensorValidator, \
    Unvalidated


class BasePathResource:
    resource_class = None

    def __init__(self, ext: str, base_path: Optional[str] = None, name: Optional[str] = None):
        self._ext = ext
        self._base_path = base_path
        self._name = name

    def build_kwargs(self, settings, partition, name: str):
        base_path = self._base_path if self._base_path else settings.base_path
        name = self._name if self._name else name
        partition = [str(p) for p in partition]
        path = os.path.join(base_path, *partition, f'{name}.{self._ext}')
        return {'path': path}

    def build(self, settings, partition, name: str):
        kwargs = self.build_kwargs(settings=settings, partition=partition, name=name)
        return self.resource_class(**kwargs)


class FeatherResource(BasePathResource):
    resource_class = _FeatherResource

    def __init__(self, base_path: Optional[str] = None, name: Optional[str] = None):
        super().__init__('.feather', base_path=base_path, name=name)


class FileResource(BasePathResource):
    resource_class = _FileResource


class NetCDFResource(BasePathResource):
    def __init__(self, base_path: Optional[str] = None, name: Optional[str] = None):
        super().__init__('.nc', base_path=base_path, name=name)

    def build(self, settings, partition, name: str):
        kwargs = self.build_kwargs(settings=settings, partition=partition, name=name)
        kwargs['variable'] = name
        return self.resource_class(**kwargs)


class ParquetResource(BasePathResource):
    resource_class = _ParquetResource

    def __init__(self, base_path: Optional[str] = None, name: Optional[str] = None):
        super().__init__('.parquet', base_path=base_path, name=name)


@functools.singledispatch
def infer_resource(validator):
    raise NotImplemented()


@infer_resource.register(DataFrameValidator)
def _(validator):
    return ParquetResource()


@infer_resource.register(TensorValidator)
def _(validator):
    return NetCDFResource()


@infer_resource.register(Unvalidated)
def _(validator):
    return FileResource(validator.to_dict()['ext'])


RESOURCES = {
    FILE_RESOURCE: FileResource,
    FEATHER_RESOURCE: FeatherResource,
    NETCDF_RESOURCE: NetCDFResource,
    PARQUET_RESOURCE: ParquetResource,
}

DATAFRAME_VALIDATOR = 'meillionen::DataFrameValidator'
TENSOR_VALIDATOR = 'meillionen::TensorValidator'
UNVALIDATED = 'meillionen::Unvalidated'

VALIDATORS = {
    DATAFRAME_VALIDATOR: DataFrameValidator,
    TENSOR_VALIDATOR: TensorValidator,
    UNVALIDATED: Unvalidated,
}
