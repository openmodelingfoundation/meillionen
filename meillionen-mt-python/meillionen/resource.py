"""
Resources are references to datasets that provide the information necessary to
load or save the data later.
"""
import functools
import os
import pathlib
from typing import Dict, Union, Any, List, Optional

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


class FeatherResource:
    ext = '.feather'

    def __call__(self, settings):
        path = settings.prefix.joinpath(self.ext)
        return _FeatherResource(path=path)


class FileResource:
    def __init__(self, ext):
        self.ext = ext

    def __call__(self, settings):
        path = settings.prefix.joinpath(self.ext)
        return _FileResource(path=path)


class NetCDFResource:
    ext = '.nc'

    def __init__(self, variable):
        self.variable = variable

    def __call__(self, settings):
        path = settings.prefix.joinpath(self.ext)
        return _NetCDFResource(path=path, variable=self.variable)


class ParquetResource:
    ext = '.parquet'

    def __call__(self, settings):
        path = settings.prefix.joinpath(self.ext)
        return _ParquetResource(path=path)


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
