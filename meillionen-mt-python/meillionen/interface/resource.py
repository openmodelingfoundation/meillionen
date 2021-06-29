import io
import json

import flatbuffers

from . import _Resource as r
from .base import field_to_bytesio
from meillionen.exceptions import ResourceNotFound


class _Resource(r._Resource):
    PAYLOAD_OFFSET = 8

    def PayloadAsBytesIO(self):
        return field_to_bytesio(tab=self._tab, field_offset=self.PAYLOAD_OFFSET)


class OtherFile:
    name = 'meillionen::resource::OtherFile'

    def __init__(self, path):
        self.path = path

    @classmethod
    def deserialize(cls, buffer):
        path = json.load(buffer)['path']
        return cls(path=path)

    def serialize(self, builder: flatbuffers.Builder):
        return builder.CreateByteVector(json.dumps({'path': self.path}))


class Parquet:
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
    name = 'meillionen::resource::Feather'

    def __init__(self, path):
        self.path = path

    @classmethod
    def deserialize(cls, buffer):
        path = json.load(buffer)['path']
        return cls(path=path)

    def serialize(self, builder: flatbuffers.Builder):
        data = json.dumps({'path': self.path})
        return builder.CreateByteVector(data)


class NetCDF:
    name = 'meillionen::resource::NetCDF'

    def __init__(self, path, dataset):
        self.path = path
        self.dataset = dataset

    @classmethod
    def deserialize(cls, buffer):
        data = json.load(buffer)
        path = data['path']
        dataset = data['path']
        return cls(path=path, dataset=dataset)

    def serialize(self, builder: flatbuffers.Builder):
        data = json.dumps({'path': self.path, 'dataset': self.dataset})
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


def deserialize_resource(resource: _Resource):
    type_name = resource.TypeName()
    buffer = resource.PayloadAsBytesIO()
    try:
        return _RESOURCE_CLASSES[type_name].deserialize(buffer)
    except KeyError as e:
        return ResourceNotFound(*e.args)


def clear_resource_classes():
    _RESOURCE_CLASSES.clear()


def register_resource_class(cls):
    _RESOURCE_CLASSES[cls.name] = cls


def get_resource_class(name):
    return _RESOURCE_CLASSES[name]