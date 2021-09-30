import flatbuffers
import io
import json
import numpy as np

from . import _Resource as r
from .base import field_to_bytesio
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


class OtherFile:

    """A file payload resource

    Together with a name a full resource can be created"""
    name = 'meillionen::resource::OtherFile'

    def __init__(self, path):
        self.path = path

    @classmethod
    def deserialize(cls, buffer):
        path = json.load(buffer)['path']
        return cls(path=path)

    def serialize(self, builder: flatbuffers.Builder):
        return builder.CreateByteVector(json.dumps({'path': self.path}).encode('utf-8'))


class Parquet:
    """A Parquet file resource payload"""

    name = 'meillionen::resource::Parquet'

    def __init__(self, path):
        self.path = path

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

    def __init__(self, path):
        self.path = path

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

    def __init__(self, path, variable):
        self.path = path
        self.variable = variable

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