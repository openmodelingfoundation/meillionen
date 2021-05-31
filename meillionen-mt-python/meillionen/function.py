import pyarrow as pa
from typing import Dict, Any

from meillionen.meillionen import ResourceBuilder, DataFrameSchema, TensorSchema, Schemaless
from meillionen.resource import RESOURCES, VALIDATORS


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
    def _from_recordbatch_to_sinks_sources(cls, recordbatch):
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
        return data

    def to_recordbatch(self, program_name):
        rb = ResourceBuilder(program_name)
        for (resource, kwargs) in self._rows():
            resource.to_builder(**kwargs, rb=rb)
        return rb.pop()


class FuncRequest(FuncBase):
    SERIALIZERS = RESOURCES

    @classmethod
    def from_recordbatch(cls, recordbatch: pa.RecordBatch):
        data = cls._from_recordbatch_to_sinks_sources(recordbatch)
        return cls(sources=data['source'], sinks=data['sink'])

    def _rows(self):
        for (name, sink) in self._sinks.items():
            yield (sink, {'field': 'sink', 'name': name})
        for (name, source) in self._sources.items():
            yield (source, {'field': 'source', 'name': name})


class FuncInterfaceClient(FuncBase):
    SERIALIZERS = VALIDATORS

    def __init__(self, name, sources: Dict[str, Any], sinks: Dict[str, Any]):
        super().__init__(sources=sources, sinks=sinks)
        self.name = name

    @classmethod
    def from_recordbatch(cls, name: str, recordbatch: pa.RecordBatch):
        data = cls._from_recordbatch_to_sinks_sources(recordbatch)
        return cls(name=name, sources=data['source'], sinks=data['sink'])

    def _rows(self):
        for (name, sink) in self._sinks.items():
            yield (sink, {'field': 'sink', 'name': name})
        for (name, source) in self._sources.items():
            yield (source, {'field': 'source', 'name': name})


class FuncInterfaceServer(FuncBase):
    SERIALIZERS = VALIDATORS

    @classmethod
    def from_recordbatch(cls, recordbatch: pa.RecordBatch):
        data = cls._from_recordbatch_to_sinks_sources(recordbatch)
        return cls(sources=data['source'], sinks=data['sink'])

    def _rows(self):
        for (name, sink) in self._sinks.items():
            yield (sink.schema, {'field': 'sink', 'name': name})
        for (name, source) in self._sources.items():
            yield (source.schema, {'field': 'source', 'name': name})
