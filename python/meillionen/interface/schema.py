import functools
import io
import json
import os.path
import pathlib
import textwrap
from typing import Union, Dict, List, Any

import flatbuffers
import netCDF4
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.feather as paf
import pyarrow.parquet as pap
import xarray as xr
from landlab.io import read_esri_ascii, write_esri_ascii

from abc import abstractmethod
from typing import Optional, Protocol, Type, TypeVar
from . import _Schema as s
from .mutability import Mutability
from .base import field_to_bytesio, leading_indent
from ..exceptions import HandlerNotFound, ExtensionHandlerNotFound, ValidationError, DataFrameValidationError, \
    SchemaTypeMismatchValidationError
from .resource import get_resource_payload_class, Feather, Parquet, NetCDF, OtherFile, ResourcePayloadable


def _mkdir_p(path):
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)


_SP = TypeVar('_SP')


class SchemaPayloadable(Protocol):
    """
    A schema payload describes the shape of data a method argument expects
    """

    name: str

    @classmethod
    @abstractmethod
    def deserialize(cls: Type[_SP], buffer) -> _SP:
        """
        Builds the schema payload from a buffer

        :param buffer: the buffer to load from
        """
        raise NotImplementedError()

    @abstractmethod
    def serialize(self, builder: flatbuffers.Builder):
        """
        Serializes the schema payload into a flatbuffer builder

        :param builder: the flatbuffer builder
        """
        raise NotImplementedError()


class DataFrameSchemaPayload(SchemaPayloadable):
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

    def describe(self, indent: int=0):
        print(leading_indent(f'{self.name}', indent))
        print(leading_indent(repr(self.arrow_schema), indent))

    def validate(self, payload: 'DataFrameSchemaPayload'):
        _validate_arrow_schema(self.arrow_schema, payload.arrow_schema)


class TensorSchemaPayload(SchemaPayloadable):
    name = 'meillionen::schema::TensorSchema'

    def __init__(self, data_type: str, dimensions: List[str]):
        self.data_type = data_type
        self.dimensions = dimensions

    @classmethod
    def deserialize(cls, buffer: io.BytesIO):
        data = json.load(buffer)
        return cls(data_type=data['data_type'], dimensions=data['dimensions'])

    def serialize(self, builder: flatbuffers.Builder):
        data = json.dumps({'data_type': self.data_type, 'dimensions': self.dimensions}).encode('utf-8')
        return builder.CreateByteVector(data)

    def describe(self, indent: int=0):
        print(leading_indent(self.name, indent))
        desc = textwrap.dedent(f'''\
        data_type: {self.data_type}
        dimensions: {repr(self.dimensions)}\
        ''')
        print(leading_indent(desc, indent))

    def validate(self, payload: SchemaPayloadable):
        if not isinstance(payload, self.__class__):
            raise SchemaTypeMismatchValidationError(expected=self.__class__, actual=type(payload))


class SchemalessPayload(SchemaPayloadable):
    name = 'meillionen::schema::Schemaless'

    @classmethod
    def deserialize(cls, buffer: io.BytesIO):
        return cls()

    def serialize(self, builder: flatbuffers.Builder):
        data = json.dumps('').encode('utf-8')
        return builder.CreateByteVector(data)

    def describe(self, indent: int=0):
        print(leading_indent(self.name, indent))

    def validate(self, payload: SchemaPayloadable):
        return


_SCHEMA_CLASSES = {
    s.name: s for s in
    [
        DataFrameSchemaPayload,
        TensorSchemaPayload,
        SchemalessPayload
    ]
}


def clear_schema_payload_classes():
    _SCHEMA_CLASSES.clear()


def register_schema_payload_class(cls):
    _SCHEMA_CLASSES[cls.name] = cls


def get_schema_payload_class(name):
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
    def __init__(self, name, schema, resource_classes, mutability: Mutability):
        self.name = name
        self.payload = schema
        self.resource_classes = resource_classes
        self.mutability = mutability

    def _serialize_resource_names(self, builder: flatbuffers.Builder, resource_classes: List[Any]):
        n = len(resource_classes)
        offsets = [builder.CreateString(resource_classes[i].name) for i in range(n)]
        s.StartResourceNamesVector(builder, n)
        for i in range(n):
            builder.PrependUOffsetTRelative(offsets[i])
        return builder.EndVector()

    def serialize(self, builder: flatbuffers.Builder):
        name_off = builder.CreateString(self.name)
        type_name_off = builder.CreateString(self.payload.name)
        payload_off = self.payload.serialize(builder)
        resource_names_off = self._serialize_resource_names(builder, self.resource_classes)
        s.Start(builder)
        s.AddName(builder, name_off)
        s.AddTypeName(builder, type_name_off)
        s.AddMutability(builder, self.mutability.serialize())
        s.AddPayload(builder, payload_off)
        s.AddResourceNames(builder, resource_names_off)
        schema_off = s.End(builder)
        return schema_off

    @classmethod
    def from_class(cls, schema: _Schema):
        name = schema.Name().decode('utf-8')
        mutability = Mutability(schema.Mutability())
        payload_class = get_schema_payload_class(schema.TypeName().decode('utf-8'))
        payload = payload_class.deserialize(schema.PayloadAsBytesIO())
        resource_classes = []
        for i in range(schema.ResourceNamesLength()):
            resource_name = schema.ResourceNames(i).decode('utf-8')
            resource_class = get_resource_payload_class(resource_name)
            resource_classes.append(resource_class)
        return cls(name=name, schema=payload, resource_classes=resource_classes, mutability=mutability)

    def describe(self, indent=0):
        desc = textwrap.dedent(f'''\
        name: {self.name}
        resource_classes: {repr(self.resource_classes)}
        mutability: {self.mutability}
        payload:\
        ''')
        print(leading_indent(desc, indent))
        self.payload.describe(indent + 2)


class SchemaProxy:
    @property
    def name(self):
        return self._schema.name

    @property
    def mutability(self):
        return self._schema.mutability

    @property
    def resource_classes(self):
        return self._schema.resource_classes

    @property
    def payload(self):
        return self._schema.payload

    def __repr__(self):
        return f'{self.__class__.name}.from_schema({self._schema})'

    def describe(self, indent):
        return self._schema.describe(indent=indent)


_H = TypeVar('_H')


class Handlable(Protocol):
    @classmethod
    @abstractmethod
    def from_schema(cls: Type[_H], schema: Schema) -> _H:
        """
        Builds the handler from the schema

        :param schema: the schema to build the handler with
        """
        raise NotImplementedError()

    @abstractmethod
    def serialize(self, builder: flatbuffers.Builder):
        """
        Serialize a handler into a flatbuffer builder using only schema information
        """
        raise NotImplementedError()

    @abstractmethod
    def load(self, resource):
        """
        Loads a resource into memory

        :param resource: the resource to load
        """
        raise NotImplementedError()

    @abstractmethod
    def save(self, resource, data):
        """
        Save data using the metadata provided by the resource

        :param resource: the metadata desribing where and how to save the data
        :param data: the data to save
        """
        raise NotImplementedError()


def _validate_arrow_schema(s: pa.Schema, inferred_s: pa.Schema):
    """
    Ensure one dataframe arrow schema is compatible with another

    :param s: the original schema
    :param inferred_s: the schema inferred from a dataframe
    """
    missing_colnames = []
    type_mismatches = []

    # insure inferred schema has all columns
    for name in s.names:
        field_s = s.field(name)
        try:
            field_inf = inferred_s.field(name)
        except KeyError as e:
            missing_colnames.append(name)
            continue

        type_s = field_s.type
        type_inf = field_inf.type
        if pa.types.is_floating(type_s) and \
                pa.types.is_floating(type_inf) and \
                type_s.bit_width <= type_inf.bit_width:
            continue
        if pa.types.is_signed_integer(type_s) and \
                pa.types.is_signed_integer(type_inf) and \
                type_s.bit_width <= type_inf.bit_width:
            continue
        if pa.types.is_unsigned_integer(type_s) and \
                pa.types.is_unsigned_integer(type_inf) and \
                type_s.bit_width <= type_inf.bit_width:
            continue

        if field_s.type != field_inf.type:
            type_mismatches.append({
                'actual': field_inf,
                'expected': field_s
            })
    if missing_colnames or type_mismatches:
        raise DataFrameValidationError(missing_columns=missing_colnames, type_mismatches=type_mismatches)


class PandasHandler(SchemaProxy, Handlable):
    RESOURCE_CLASSES = [Feather, Parquet]

    def __init__(self, name: str, s: pa.Schema, mutability: Mutability):
        self._schema = Schema(
            name=name,
            schema=DataFrameSchemaPayload(s),
            resource_classes=self.RESOURCE_CLASSES,
            mutability=mutability)

    @classmethod
    def from_schema(cls, schema: Schema):
        return cls(
            name=schema.name,
            s=schema.payload.arrow_schema,
            mutability=schema.mutability
        )

    def serialize(self, builder: flatbuffers.Builder):
        return self._schema.serialize(builder)

    @functools.singledispatchmethod
    def load(self, resource):
        raise NotImplemented()

    @load.register
    def _load(self, resource: Feather):
        df = pd.read_feather(resource.path)
        inferred = pa.Schema.from_pandas(df=df)
        self._schema.payload.validate(DataFrameSchemaPayload(inferred))
        return df

    @load.register
    def _load(self, resource: Parquet):
        df = pd.read_parquet(resource.path)
        inferred = pa.Schema.from_pandas(df=df)
        self._schema.payload.validate(DataFrameSchemaPayload(inferred))
        return df.to_pandas()

    @functools.singledispatchmethod
    def save(self, resource, data):
        raise NotImplemented()

    @save.register
    def _save(self, resource: Feather, data: pd.DataFrame):
        _mkdir_p(resource.path)
        return data.to_feather(resource.path)

    @save.register
    def _save(self, resource: Parquet, data: pd.DataFrame):
        _mkdir_p(resource.path)
        return data.to_parquet(resource.path)


class NetCDFHandler(SchemaProxy, Handlable):
    RESOURCE_CLASSES = [NetCDF]

    def __init__(self, name: str, data_type: str, dimensions: List[str], mutability: Mutability):
        ts = TensorSchemaPayload(data_type=data_type, dimensions=dimensions)
        self._schema = Schema(
            name=name,
            schema=ts,
            resource_classes=self.RESOURCE_CLASSES,
            mutability=mutability)

    @classmethod
    def from_schema(cls, schema: Schema):
        return cls(
            name=schema.name,
            data_type=schema.payload.data_type,
            dimensions=schema.payload.dimensions,
            mutability=schema.mutability)

    def serialize(self, builder: flatbuffers.Builder):
        return self._schema.serialize(builder)

    @functools.singledispatchmethod
    def load(self, resource):
        raise NotImplemented()

    @load.register
    def _load(self, resource: NetCDF):
        ds = netCDF4.Dataset(resource.path)
        try:
            return ds[resource.variable][:]
        finally:
            if ds.isopen():
                ds.close()

    @functools.singledispatchmethod
    def save(self, resource, data):
        raise NotImplemented()

    @save.register
    def _save(self, resource: NetCDF, data: xr.DataArray):
        data = data.transpose(self.dimensions)
        ds, variable = _netcdf_create_variable(self._schema.payload, resource=resource, dimensions=data.dims)
        try:
            variable[:] = data
        finally:
            if ds.isopen():
                ds.close()


def _netcdf_create_variable(schema, resource, dimensions):
    dimnames = schema.dimensions
    _mkdir_p(resource.path)
    dataset = netCDF4.Dataset(resource.path, mode='w')
    for dim in dimnames:
        size = dimensions[dim]
        print((dim, size))
        dataset.createDimension(dim, size)
    variable = dataset.createVariable(resource.variable, 'f4', dimnames)
    return dataset, variable


class NetCDFSliceHandler(SchemaProxy, Handlable):
    RESOURCE_CLASSES = [NetCDF]

    def __init__(self, name: str, data_type: str, dimensions: List[str], mutability: Mutability):
        self._schema = Schema(
            name=name,
            schema=TensorSchemaPayload(data_type=data_type, dimensions=dimensions),
            resource_classes=self.RESOURCE_CLASSES,
            mutability=mutability
        )

    @classmethod
    def from_schema(cls, schema: Schema):
        return cls(
            name=schema.name,
            data_type=schema.payload.data_type,
            dimensions=schema.payload.dimensions,
            mutability=schema.mutability
        )

    def serialize(self, builder: flatbuffers.Builder):
        return self._schema.serialize(builder)

    @functools.singledispatchmethod
    def load(self, resource):
        raise NotImplemented()

    @load.register
    def _load(self, resource: NetCDF):
        return NetCDFSliceLoader(resource=resource)

    @functools.singledispatchmethod
    def save(self, resource, data):
        raise NotImplemented()

    @save.register
    def _save(self, resource: NetCDF, data):
        return NetCDFSliceSaver(schema=self._schema, resource=resource, dimensions=data)


NDSlice = Dict[str, Union[int, slice]]


class NetCDFSliceLoader:
    def __init__(self, resource: NetCDF):
        self.resource = resource
        self.dataset = netCDF4.Dataset(resource.path, mode='r')
        self.variable = self.dataset[resource.variable]

    def get(self, slices: NDSlice):
        vdims = self.variable.dimensions
        xs = tuple(slices[d] if d in slices else slice(None) for d in vdims)
        return self.variable[xs]


class NetCDFSliceSaver:
    def __init__(self, schema, resource: NetCDF, dimensions):
        self.schema = schema
        self.dataset, self.variable = _netcdf_create_variable(
            schema=schema.payload,
            resource=resource,
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


class LandLabGridHandler(SchemaProxy, Handlable):
    RESOURCE_CLASSES = [OtherFile]

    def __init__(self, name: str, data_type: str, mutability: Mutability):
        dimensions = ['x', 'y']
        ts = TensorSchemaPayload(data_type=data_type, dimensions=dimensions)
        self._schema = Schema(name=name, schema=ts, resource_classes=self.RESOURCE_CLASSES, mutability=mutability)

    @classmethod
    def from_schema(cls, schema: Schema):
        return cls(
            name=schema.name,
            data_type=schema.payload.data_type,
            mutability=schema.mutability)

    def serialize(self, builder: flatbuffers.Builder):
        return self._schema.serialize(builder)

    @property
    def data_type(self):
        return self._schema.payload.data_type

    @property
    def dimensions(self):
        return self._schema.payload.dimensions

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


class DirHandler(SchemaProxy, Handlable):
    RESOURCE_CLASSES = [OtherFile]

    def __init__(self, name: str, mutability: Mutability):
        self._schema = Schema(
            name=name,
            schema=SchemalessPayload(),
            resource_classes=self.RESOURCE_CLASSES,
            mutability=mutability)

    @classmethod
    def from_schema(cls, schema: Schema):
        return cls(
            name=schema.name,
            mutability=schema.mutability)

    def serialize(self, builder: flatbuffers.Builder):
        return self._schema.serialize(builder)

    def load(self, resource):
        raise NotImplemented()

    @functools.singledispatchmethod
    def save(self, resource):
        raise NotImplemented()

    @save.register
    def _save(self, resource: OtherFile):
        path = resource.path
        p = pathlib.Path(path)
        p.mkdir(parents=True, exist_ok=True)
        return path


class PassthroughHandlerMapper:
    def __init__(self, handler_cls):
        self.handler_cls = handler_cls

    def to_handler(self, resource, schema):
        return self.handler_cls.from_schema(schema)


class FileExtensionHandlerMapper:
    """
    A handler mapper that inspects a resources file extension to determine what handler
    should wrap a particular schema
    """
    def __init__(self):
        self.ext_map = {}

    def register_ext(self, ext: str, handler_cls):
        self.ext_map[ext] = handler_cls

    def to_handler(self, resource: OtherFile, schema: Schema):
        ext = os.path.splitext(resource.path)[1]
        try:
            handler_cls = self.ext_map[ext]
        except KeyError as e:
            raise ExtensionHandlerNotFound(ext) from e
        return handler_cls.from_schema(schema)

FILE_EXT_HANDLER_MAPPER = FileExtensionHandlerMapper()
FILE_EXT_HANDLER_MAPPER.register_ext('.asc', LandLabGridHandler)
FILE_EXT_HANDLER_MAPPER.register_ext('', DirHandler)

_RESOURCE_HANDLER_MAPPER = {
    OtherFile: FILE_EXT_HANDLER_MAPPER,
    Feather: PassthroughHandlerMapper(PandasHandler),
    Parquet: PassthroughHandlerMapper(PandasHandler),
    NetCDF: PassthroughHandlerMapper(NetCDFHandler),
}


def clear_handler_classes():
    _RESOURCE_HANDLER_MAPPER.clear()


def register_resource_handler_mapper(resource_cls, handler_mapper):
    _RESOURCE_HANDLER_MAPPER[resource_cls] = handler_mapper


def all_resource_handler_mappers(name):
    return _RESOURCE_HANDLER_MAPPER


def get_handler(resource_payload, schema: Schema):
    """
    Retrieve a handler given a resource payload and a schema
    """
    handler_mapper = _RESOURCE_HANDLER_MAPPER[type(resource_payload)]
    return handler_mapper.to_handler(resource_payload, schema)


def get_handlers(resources, schemas: Dict[str, Schema]):
    handlers = {}
    for name, schema in schemas.items():
        resource = resources[name]
        handler = get_handler(resource, schema)
        handlers[name] = handler
    return handlers