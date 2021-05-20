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
    ResourceBuilder, \
    FeatherResource, \
    FileResource, \
    NetCDFResource, \
    ParquetResource, \
    DataFrameValidator, \
    TensorValidator, \
    Unvalidated


FILE_RESOURCE = 'meillionen::FileResource'
FEATHER_RESOURCE = 'meillionen::FeatherResource'
NETCDF_RESOURCE = 'meillionen::NetCDFResource'
PARQUET_RESOURCE = 'meillionen::ParquetResource'

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


def _mkdir_p(path):
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)


class FuncBase:
    SERIALIZERS = None

    def __init__(self, sources: Dict[str, Any], sinks: Dict[str, Any]):
        self._sources = sources
        self._sinks = sinks

    @property
    def sinks(self):
        return self._sinks.items()

    @property
    def sink_names(self):
        return self._sinks.keys()

    @property
    def sources(self):
        return self._sources.items()

    @property
    def source_names(self):
        return self._sources.keys()

    def sink(self, name):
        return self._sinks[name]

    def source(self, name):
        return self._sources[name]

    @classmethod
    def _deserialize(cls, resources: pa.StringArray, payload: pa.BinaryArray, index: int):
        resource_name = str(resources[index])
        return cls.SERIALIZERS[resource_name].from_arrow_array(payload, index)

    @classmethod
    def from_recordbatch(cls, recordbatch: pa.RecordBatch):
        field: pa.StringArray = recordbatch.column("field")
        assert field.type == pa.string()
        name: pa.StringArray = recordbatch.column("name")
        assert name.type == pa.string()
        resources: pa.StringArray = recordbatch.column("resource")
        assert resources.type == pa.string()
        payload: pa.BinaryArray = recordbatch.column("payload")
        assert payload.type == pa.binary()
        data = {
            "source": {},
            "sink": {},
        }
        for index in range(recordbatch.num_rows):
            r = cls._deserialize(
                resources=resources,
                payload=payload,
                index=index)
            ftype = str(field[index])
            arg = str(name[index])
            data[ftype][arg] = r
        sinks = data['sink']
        sources = data['source']
        return cls(sources=sources, sinks=sinks)

    def to_recordbatch(self, program_name):
        rb = ResourceBuilder(program_name)
        for (resource, kwargs) in self._rows():
            resource.to_builder(**kwargs, rb=rb)
        return rb.pop()


class FuncRequest(FuncBase):
    SERIALIZERS = RESOURCES

    def _rows(self):
        for (name, sink) in self._sinks.items():
            yield (sink, {'field': 'sink', 'name': name})
        for (name, source) in self._sources.items():
            yield (source, {'field': 'source', 'name': name})


class FuncInterfaceClient(FuncBase):
    SERIALIZERS = VALIDATORS

    DEFAULT_RESOURCE = {
        DataFrameValidator: ParquetResource,
        TensorValidator: NetCDFResource,
        Unvalidated: FileResource
    }

    def __init__(self, name, sources: Dict[str, Any], sinks: Dict[str, Any]):
        super().__init__(sources=sources, sinks=sinks)
        self.name = name

    def _rows(self):
        for (name, sink) in self._sinks.items():
            yield (sink, {'field': 'sink', 'name': name})
        for (name, source) in self._sources.items():
            yield (source, {'field': 'source', 'name': name})


class FuncInterfaceServer(FuncBase):
    SERIALIZERS = VALIDATORS

    def _rows(self):
        for (name, sink) in self._sinks.items():
            yield (sink.validator, {'field': 'sink', 'name': name})
        for (name, source) in self._sources.items():
            yield (source.validator, {'field': 'source', 'name': name})


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
        getattr(data, self.PANDAS_SAVERS[resource.name])(path)


class NetCDFHandler:
    RESOURCE_TYPES = [NETCDF_RESOURCE]

    NETCDF_SAVERS = {
        NETCDF_RESOURCE: NetCDFResource
    }

    def __init__(self, validator: TensorValidator):
        self.validator = validator

    @classmethod
    def from_kwargs(cls, description, data_type, dimensions):
        validator = TensorValidator.from_dict({
            'resources': cls.RESOURCE_TYPES,
            'description': description,
            'dimensions': dimensions,
            'data_type': data_type,
        })
        return cls(validator)

    def load(self, resource):
        return NetCDFSliceLoader(source=resource)

    def save(self, resource, dimensions):
        return NetCDFSliceSaver(sink=resource, dimensions=dimensions)


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
    def __init__(self, sink, dimensions):
        self.sink = sink
        sink = self.sink.to_dict()
        _mkdir_p(sink['path'])
        self.dataset = netCDF4.Dataset(sink['path'], mode='w')
        for dim, size in dimensions:
            print((dim, size))
            self.dataset.createDimension(dim, size)
        self.variable = self.dataset.createVariable(sink['variable'], 'f4', [f[0] for f in dimensions])

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