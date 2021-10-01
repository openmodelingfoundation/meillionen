import flatbuffers
import io
import json
import numpy as np
import os.path
import pyarrow as pa

from ..settings import Partition, Partitioning
from . import _Resource as r
from .base import field_to_bytesio, MethodRequestArg
from meillionen.exceptions import ResourceNotFound


class _Resource(r._Resource):
    PAYLOAD_OFFSET = 8

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = cls()
        x.Init(buf, n + offset)
        return x

    def PayloadAsBytesIO(self):
        return field_to_bytesio(tab=self._tab, field_offset=self.PAYLOAD_OFFSET)


class Resource:
    """A serializable resource. Should only be used internally."""
    def __init__(self, name: str, resource_payload):
        self.name = name
        self.resource_payload = resource_payload

    @classmethod
    def from_class(cls, resource: _Resource):
        name = resource.Name().decode('utf-8')
        type_name = resource.TypeName().decode('utf-8')
        payload_class = get_resource_payload_class(type_name)
        resource_payload = payload_class.deserialize(resource.PayloadAsBytesIO())
        return cls(name=name, resource_payload=resource_payload)

    def serialize(self, builder: flatbuffers.Builder):
        name_off = builder.CreateString(self.name)
        type_name_off = builder.CreateString(self.resource_payload.name)
        payload_off = self.resource_payload.serialize(builder)
        r.Start(builder)
        r.AddName(builder, name_off)
        r.AddTypeName(builder, type_name_off)
        r.AddPayload(builder, payload_off)
        return r.End(builder)


class PartialResource:
    def __init__(self, resource_payload_class, kwargs):
        self.resource_payload_class = resource_payload_class
        self.kwargs = kwargs

    def complete(self, settings, mra: MethodRequestArg, partition=None):
        return self.resource_payload_class.from_kwargs(
            kwargs=self.kwargs,
            settings=settings,
            mra=mra,
            partition=partition)


def build_path(settings, mra: MethodRequestArg, ext, partition=None):
    base_path = os.path.join(settings.base_path, mra.class_name, mra.method_name, mra.arg_name)
    if partition:
        path = os.path.join(base_path, partition.to_path(), f'data.{ext}')
    else:
        path = f'{base_path}.{ext}'
    return path


class OtherFile:

    """A file payload resource

    Together with a name a full resource can be created"""
    name = 'meillionen::resource::OtherFile'

    def __init__(self, path):
        self.path = path

    @classmethod
    def partial(cls, ext):
        return PartialResource(cls, {'ext': ext})

    @classmethod
    def from_kwargs(cls, kwargs, settings, mra: MethodRequestArg, partition):
        path = build_path(
            settings=settings,
            mra=mra,
            partition=partition,
            ext=kwargs['ext']
        )
        return cls(path=path)

    @classmethod
    def deserialize(cls, buffer):
        path = json.load(buffer)['path']
        return cls(path=path)

    def serialize(self, builder: flatbuffers.Builder):
        return builder.CreateByteVector(json.dumps({'path': self.path}).encode('utf-8'))


class Parquet:
    """A Parquet file resource payload"""

    name = 'meillionen::resource::Parquet'
    ext = 'parquet'

    def __init__(self, path):
        self.path = path

    @classmethod
    def partial(cls):
        return PartialResource(cls, {})

    @classmethod
    def from_kwargs(cls, kwargs, settings, mra: MethodRequestArg, partition=None):
        path = build_path(
            settings=settings,
            class_name=class_name,
            method_name=method_name,
            name=name,
            partition=partition,
            ext=cls.ext
        )
        return cls(path=path)

    @classmethod
    def deserialize(cls, buffer):
        path = json.load(buffer)['path']
        return cls(path=path)

    def serialize(self, builder: flatbuffers.Builder):
        data = json.dumps({'path': self.path})
        return builder.CreateByteVector(data)


class Feather:
    """A Feather file resource payload"""

    name = 'meillionen::resource::Feather'
    ext = 'feather'

    def __init__(self, path):
        self.path = path

    @classmethod
    def partial(cls):
        return PartialResource(cls, {})

    @classmethod
    def from_kwargs(cls, kwargs, settings, mra: MethodRequestArg, partition=None):
        path = build_path(
            settings=settings,
            mra=mra,
            ext=cls.ext,
            partition=partition
        )
        return cls(path=path)

    @classmethod
    def deserialize(cls, buffer):
        path = json.load(buffer)['path']
        return cls(path=path)

    def serialize(self, builder: flatbuffers.Builder):
        data = np.frombuffer(json.dumps({'path': self.path}).encode('utf-8'), dtype=np.uint8)
        return builder.CreateNumpyVector(data)


class NetCDF:
    """A NetCDF file resource payload"""

    name = 'meillionen::resource::NetCDF'
    ext = 'nc'

    def __init__(self, path, variable):
        self.path = path
        self.variable = variable

    @classmethod
    def partial(cls, variable):
        return PartialResource(cls, {'variable': variable})

    @classmethod
    def from_kwargs(cls, kwargs, settings, mra: MethodRequestArg, partition=None):
        path = build_path(
            settings=settings,
            mra=mra,
            ext=cls.ext,
            partition=partition
        )
        return cls(path=path, variable=kwargs['variable'])

    @classmethod
    def deserialize(cls, buffer):
        data = json.load(buffer)
        path = data['path']
        variable = data['variable']
        return cls(path=path, variable=variable)

    def serialize(self, builder: flatbuffers.Builder):
        data = json.dumps({'path': self.path, 'variable': self.variable}).encode('utf-8')
        return builder.CreateByteVector(data)


_RESOURCE_CLASSES = {
    r.name: r for r in
    [
        Feather,
        NetCDF,
        Parquet,
        OtherFile
    ]
}


def deserialize_resource_payload(resource: _Resource):
    type_name = resource.TypeName().decode('utf-8')
    buffer = resource.PayloadAsBytesIO()
    try:
        return get_resource_payload_class(type_name).deserialize(buffer)
    except KeyError as e:
        raise ResourceNotFound() from e


def clear_resource_payload_classes():
    _RESOURCE_CLASSES.clear()


def register_resource_payload_class(cls):
    _RESOURCE_CLASSES[cls.name] = cls


def get_resource_payload_class(name):
    return _RESOURCE_CLASSES[name]