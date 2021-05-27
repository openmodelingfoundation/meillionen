import netCDF4
import numpy as np
import pandas as pd
import pathlib
from typing import List, Dict, Any, Union
import xarray as xr

from landlab.io import read_esri_ascii, write_esri_ascii
from meillionen.resource import FEATHER_RESOURCE, PARQUET_RESOURCE, NETCDF_RESOURCE, FILE_RESOURCE
from meillionen.meillionen import DataFrameValidator, TensorValidator, NetCDFResource


def _mkdir_p(path):
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)



class DataFrameResourceBase:
    RESOURCE_TYPES = []

    def __init__(self, validator: DataFrameValidator):
        self.validator = validator

    @classmethod
    def _normalize_fields(cls, fields: List[Dict[str, Any]]):
        fields = fields.copy()
        for field in fields:
            for (attr, val) in [('nullable', False), ('dict_id', 0), ('dict_is_ordered', False)]:
                if not hasattr(field, attr):
                    field[attr] = val
        return fields


class PandasHandler(DataFrameResourceBase):
    """
    A loader/saver to retrieve and persist a pandas dataframe to the location and format specified by a resource
    """
    RESOURCE_TYPES = [FEATHER_RESOURCE, PARQUET_RESOURCE]

    PANDAS_LOADERS = {
        FEATHER_RESOURCE: pd.read_feather,
        PARQUET_RESOURCE: pd.read_parquet
    }

    PANDAS_SAVERS = {
        FEATHER_RESOURCE: 'to_feather',
        PARQUET_RESOURCE: 'to_parquet'
    }

    @classmethod
    def from_kwargs(cls, description, columns, resources=None):
        cls._normalize_fields(columns['fields'])
        validator = DataFrameValidator.from_dict({
            'resources': cls.RESOURCE_TYPES if not resources else resources,
            'description': description,
            'columns': columns
        })
        return cls(validator=validator)

    def load(self, resource):
        """
        Loads a resource into pandas

        :param resource: metadata describing the location and format of a dataset
        :return: A pandas dataframe
        """
        path = resource.to_dict()['path']
        return self.PANDAS_LOADERS[resource.name](path)

    def save(self, resource, data: pd.DataFrame):
        """
        Saves a pandas dataframe to the location and in the format specified by a resource

        :param sink_resource: metadata describing the location and format to save a dataset to
        :param data: the data to be saved
        """
        path = resource.to_dict()['path']
        _mkdir_p(path)
        getattr(data, self.PANDAS_SAVERS[resource.name])(path)


def _netcdf_from_kwargs(cls, description, data_type, dimensions):
    validator = TensorValidator.from_dict({
        'resources': cls.RESOURCE_TYPES,
        'description': description,
        'dimensions': dimensions,
        'data_type': data_type,
    })
    return cls(validator)


def _netcdf_create_variable(validator, sink, dimensions):
    sink = sink.to_dict()
    dimnames = validator.to_dict()['dimensions']
    _mkdir_p(sink['path'])
    dataset = netCDF4.Dataset(sink['path'], mode='w')
    for dim in dimnames:
        size = dimensions[dim]
        print((dim, size))
        dataset.createDimension(dim, size)
    variable = dataset.createVariable(sink['variable'], 'f4', dimnames)
    return dataset, variable


class NetCDFHandler:
    RESOURCE_TYPES = [NETCDF_RESOURCE]

    NETCDF_SAVERS = {
        NETCDF_RESOURCE: NetCDFResource
    }

    def __init__(self, validator: TensorValidator):
        self.validator = validator

    @classmethod
    def from_kwargs(cls, description, data_type, dimensions):
        return _netcdf_from_kwargs(
            cls,
            description=description,
            data_type=data_type,
            dimensions=dimensions)

    def load(self, resource):
        resource = resource.to_dict()
        ds = netCDF4.Dataset(resource['path'])
        return ds[resource['variable']]

    def save(self, resource, data: xr.DataArray):
        resource = resource.to_dict()
        data = data.transpose(resource['dimensions'])
        ds, variable = _netcdf_create_variable(self.validator, sink=resource, dimensions=data.dims)
        variable[:] = data


class NetCDFSliceHandler:
    RESOURCE_TYPES = [NETCDF_RESOURCE]

    NETCDF_SAVERS = {
        NETCDF_RESOURCE: NetCDFResource
    }

    def __init__(self, validator: TensorValidator):
        self.validator = validator

    @classmethod
    def from_kwargs(cls, description, data_type, dimensions):
        return _netcdf_from_kwargs(
            cls,
            description=description,
            data_type=data_type,
            dimensions=dimensions)

    def load(self, resource):
        return NetCDFSliceLoader(source=resource)

    def save(self, resource, dimensions):
        return NetCDFSliceSaver(validator=self.validator, sink=resource, dimensions=dimensions)


NDSlice = Dict[str, Union[int, slice]]


class NetCDFSliceLoader:
    def __init__(self, source):
        self.source = source
        source = source.to_dict()
        self.dataset = netCDF4.Dataset(source['path'], mode='r')
        self.variable = self.dataset[source['variable']]

    def get(self, slices: NDSlice):
        vdims = self.variable.dimensions
        xs = tuple(slices[d] if d in slices else slice(None) for d in vdims)
        return self.variable[xs]


class NetCDFSliceSaver:
    def __init__(self, validator, sink, dimensions):
        self.validator = validator
        self.dataset, self.variable = _netcdf_create_variable(
            validator=validator,
            sink=sink,
            dimensions=dimensions)

    def set(self, slices: Dict[str, Union[int, slice]], array: xr.DataArray):
        """
        Save an array to a slice of a NetCDF variable

        :param array: A dimensioned array
        :param slices: Where to save the dimensioned array to. Dimensions of array passed in
          and NetCDF variable names must match
        """
        vdims = self.variable.dimensions
        xs = tuple(slices[d] if d in slices else slice(None) for d in vdims)
        vdims_remaining = [v for v in vdims if v not in slices.keys()]
        self.variable[xs] = array.transpose(*vdims_remaining)

    def _close(self):
        self.dataset.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close()


class LandLabGridHandler:
    """
    A loader to load raster files into landlab grid objects
    """

    RESOURCE_TYPES = [FILE_RESOURCE]

    def __init__(self, validator):
        self.validator = validator

    @classmethod
    def from_kwargs(cls, description, dimensions, data_type):
        validator = TensorValidator.from_dict({
            'resources': cls.RESOURCE_TYPES,
            'description': description,
            'dimensions': dimensions,
            'data_type': data_type,
        })
        return cls(validator)

    def load(self, resource):
        """
        Loads a raster file into a landlab grid

        :param resource: metadata describing where the raster is located and its format
        :return: A landlab grid
        """
        dem = resource.to_dict()
        mg, z = read_esri_ascii(dem['path'],name='topographic__elevation')
        mg.status_at_node[mg.nodes_at_right_edge] = mg.BC_NODE_IS_FIXED_VALUE
        mg.status_at_node[np.isclose(z, -9999)] = mg.BC_NODE_IS_CLOSED
        return mg

    def save(self, resource, mg):
        """
        Save a landlab model grid into an esri ascii grid

        :param resource: metadata describing where to save the model grid to
        :param mg: a landlab model grid
        """
        dem = resource.to_dict()
        write_esri_ascii(dem['path'], mg)