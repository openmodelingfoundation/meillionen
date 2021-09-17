import functools
import io
import json
import pathlib
from typing import Union, Dict, List, Any

import flatbuffers
import netCDF4
import numpy as np
import pandas as pd
import pyarrow as pa
import xarray as xr
from landlab.io import read_esri_ascii, write_esri_ascii

from . import _Schema as s
from .base import field_to_bytesio
from .resource import get_resource_class, Feather, Parquet, NetCDF, OtherFile


def _mkdir_p(path):
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)


class DataFrameSchema:
    name = 'meillionen::schema::DataFrameSchema'

    def __init__(self, arrow_schema: pa.Schema):
        self.arrow_schema = arrow_schema

    @classmethod
    def deserialize(cls, buffer: io.BytesIO):
        schema = pa.ipc.read_schema(buffer)
        return cls(arrow_schema=schema)

    def serialize(self, builder: flatbuffers.Builder):
        buf: pa.Buffer = self.arrow_schema.serialize()
        off = builder.CreateByteVector(buf.to_pybytes())
        return off


class TensorSchema:
    name = 'meillionen::schema::TensorSchema'

    def __init__(self, data_type: str, dimensions: List[str]):
        self.data_type = data_type
        self.dimensions = dimensions

    @classmethod
    def deserialize(cls, buffer: io.BytesIO):
        data = json.load(buffer)
        return cls(data_type=data['data_type'], dimensions=data['dimensions'])

    def serialize(self, builder: flatbuffers.Builder):
        data = json.dumps({'data_type': self.data_type, 'dimensions': self.dimensions})
        return builder.CreateByteVector(data)


class Schemaless:
    name = 'meillionen::schema::Schemaless'

    @classmethod
    def deserialize(cls, buffer: io.BytesIO):
        return cls()

    def serialize(self, builder: flatbuffers.Builder):
        return


_SCHEMA_CLASSES = {
    s.name: s for s in
    [
        DataFrameSchema,
        TensorSchema,
        Schemaless
    ]
}


def clear_schema_classes():
    _SCHEMA_CLASSES.clear()


def register_schema_class(cls):
    _SCHEMA_CLASSES[cls.name] = cls


def get_schema_class(name):
    return _SCHEMA_CLASSES[name]


class _Schema(s._Schema):
    PAYLOAD_OFFSET = 10

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = cls()
        x.Init(buf, n + offset)
        return x

    def PayloadAsBytesIO(self):
        return field_to_bytesio(tab=self._tab, field_offset=self.PAYLOAD_OFFSET)


class Schema:
    def __init__(self, name, schema, resource_classes):
        self.name = name
        self.schema = schema
        self.resource_classes = resource_classes

    def _serialize_resource_names(self, builder: flatbuffers.Builder, resource_classes: List[Any]):
        n = len(resource_classes)
        offsets = [builder.CreateString(resource_classes[i].name) for i in range(n)]
        s.StartResourceNamesVector(builder, n)
        for i in range(n):
            builder.PrependUOffsetTRelative(offsets[i])
        return builder.EndVector()

    def serialize(self, builder: flatbuffers.Builder):
        name_off = builder.CreateString(self.name)
        type_name_off = builder.CreateString(self.schema.name)
        payload_off = self.schema.serialize(builder)
        resource_names_off = self._serialize_resource_names(builder, self.resource_classes)
        s.Start(builder)
        s.AddName(builder, name_off)
        s.AddTypeName(builder, type_name_off)
        s.AddPayload(builder, payload_off)
        s.AddResourceNames(builder, resource_names_off)
        schema_off = s.End(builder)
        return schema_off

    @classmethod
    def from_class(cls, schema: _Schema):
        name = schema.Name().decode('utf-8')
        payload_class = get_schema_class(schema.TypeName().decode('utf-8'))
        payload = payload_class.deserialize(schema.PayloadAsBytesIO())
        resource_classes = []
        for i in range(schema.ResourceNamesLength()):
            resource_name = schema.ResourceNames(i).decode('utf-8')
            resource_class = get_resource_class(resource_name)
            resource_classes.append(resource_class)
        return cls(name=name, schema=payload, resource_classes=resource_classes)


class PandasHandler:
    def __init__(self, name: str, s: pa.Schema):
        self.schema = Schema(name=name, schema=DataFrameSchema(s), resource_classes=[Feather, Parquet])

    @property
    def name(self):
        return self.schema.name

    def serialize(self, builder: flatbuffers.Builder):
        return self.schema.serialize(builder)

    @functools.singledispatchmethod
    def load(self, resource):
        raise NotImplemented()

    @load.register
    def _load(self, resource: Feather):
        return pd.read_feather(resource.path)

    @load.register
    def _load(self, resource: Parquet):
        return pd.read_parquet(resource.path)

    @functools.singledispatchmethod
    def save(self, resource, data):
        raise NotImplemented()

    @save.register
    def _save(self, resource: Feather, data: pd.DataFrame):
        return data.to_feather(resource.path)

    @save.register
    def _save(self, resource: Parquet, data: pd.DataFrame):
        return data.to_parquet(resource.path)


class NetCDFHandler:
    def __init__(self, name: str, data_type: str, dimensions: List[str]):
        ts = TensorSchema(data_type=data_type, dimensions=dimensions)
        self.schema = Schema(name=name, schema=ts, resource_classes=[NetCDF])

    @property
    def name(self):
        return self.schema.name

    @property
    def data_type(self):
        return self.schema.schema.data_type

    @property
    def dimensions(self):
        return self.schema.schema.dimensions

    def serialize(self, builder: flatbuffers.Builder):
        return self.schema.serialize(builder)

    @functools.singledispatchmethod
    def load(self, resource):
        raise NotImplemented()

    @load.register
    def _load(self, resource: NetCDF):
        ds = netCDF4.Dataset(resource.path)
        return ds[resource.variable]

    @functools.singledispatchmethod
    def save(self, resource, data):
        raise NotImplemented()

    @save.register
    def _save(self, resource: NetCDF, data: xr.DataArray):
        data = data.transpose(self.dimensions)
        ds, variable = _netcdf_create_variable(self.schema.schema, sink=resource, dimensions=data.dims)
        variable[:] = data


def _netcdf_create_variable(schema, sink, dimensions):
    dimnames = schema.dimensions
    _mkdir_p(sink.path)
    dataset = netCDF4.Dataset(sink.path, mode='w')
    for dim in dimnames:
        size = dimensions[dim]
        print((dim, size))
        dataset.createDimension(dim, size)
    variable = dataset.createVariable(sink.variable, 'f4', dimnames)
    return dataset, variable


class NetCDFSliceHandler:
    def __init__(self, name: str, data_type: str, dimensions: List[str]):
        self.schema = Schema(
            name=name,
            schema=TensorSchema(data_type=data_type, dimensions=dimensions),
            resource_classes=[NetCDF]
        )

    @property
    def name(self):
        return self.schema.name

    def serialize(self, builder: flatbuffers.Builder):
        return self.schema.serialize(builder)

    @functools.singledispatchmethod
    def load(self, resource):
        raise NotImplemented()

    @load.register
    def _load(self, resource: NetCDF):
        return NetCDFSliceLoader(source=resource)

    @functools.singledispatchmethod
    def save(self, resource, data):
        raise NotImplemented()

    @save.register
    def _save(self, resource: NetCDF, data):
        return NetCDFSliceSaver(schema=self.schema, sink=resource, dimensions=data)


NDSlice = Dict[str, Union[int, slice]]


class NetCDFSliceLoader:
    def __init__(self, source: NetCDF):
        self.source = source
        self.dataset = netCDF4.Dataset(source.path, mode='r')
        self.variable = self.dataset[source.variable]

    def get(self, slices: NDSlice):
        vdims = self.variable.dimensions
        xs = tuple(slices[d] if d in slices else slice(None) for d in vdims)
        return self.variable[xs]


class NetCDFSliceSaver:
    def __init__(self, schema, sink: NetCDF, dimensions):
        self.schema = schema
        self.dataset, self.variable = _netcdf_create_variable(
            schema=schema.schema,
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
    def __init__(self, name: str, data_type: str):
        dimensions = ['x', 'y']
        ts = TensorSchema(data_type=data_type, dimensions=dimensions)
        self.schema = Schema(name=name, schema=ts, resource_classes=[OtherFile.name])

    @property
    def name(self):
        return self.schema.name

    def serialize(self, builder: flatbuffers.Builder):
        return self.schema.serialize(builder)

    @functools.singledispatchmethod
    def load(self, resource):
        raise NotImplemented()

    @load.register
    def _load(self, resource: OtherFile):
        mg, z = read_esri_ascii(resource.path, name='topographic__elevation')
        mg.status_at_node[mg.nodes_at_right_edge] = mg.BC_NODE_IS_FIXED_VALUE
        mg.status_at_node[np.isclose(z, -9999)] = mg.BC_NODE_IS_CLOSED
        return mg

    @functools.singledispatchmethod
    def save(self, resource, data):
        raise NotImplemented()

    @save.register
    def _save(self, resource: OtherFile, data):
        write_esri_ascii(resource.path, data)