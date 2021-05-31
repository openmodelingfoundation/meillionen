from .interface.bytesio import Resource, Schema
from meillionen.meillionen import FileResource, FeatherResource, NetCDFResource, ParquetResource, \
    DataFrameSchema, TensorSchema, Schemaless
from typing import List


class ResourceRegistry:
    def __init__(self, resources: List):
        self._resources = resources
        self._resource_lookups = { r.name: r for r in resources }

    def register(self, resources: List):
        for resource in resources:
            self._resources.append(resource)
            self._resource_lookups[resource.name] = resource

    def deserialize(self, resource: Resource):
        name = resource.Name()
        type_name = resource.TypeName()
        payload = resource.PayloadAsBytesIO()
        r = self._resource_lookups[type_name].deserialize(payload)
        return name, r


class SchemaRegistry:
    def __init__(self, schemas: List):
        self._schemas = schemas
        self._schema_lookups = { s.name: s for s in schemas }

    def register(self, schemas: List):
        for schema in schemas:
            self._schemas.append(schema)

    def deserialize(self, schema: Schema):
        name = schema.Name()
        type_name = schema.TypeName()
        payload = schema.PayloadAsBytesIO()
        r = self._resource_lookups[type_name].deserialize(payload)
        return name, r


RESOURCES = ResourceRegistry([
    FileResource,
    FeatherResource,
    NetCDFResource,
    ParquetResource
])


SCHEMAS = SchemaRegistry([
    DataFrameSchema,
    TensorSchema,
    Schemaless
])