from typing import Dict, Any

import pyarrow as pa
from meillionen.meillionen import server_process_cli_stdin, ResourceBuilder, FuncInterface


class ServerFunctionModel:
    def __init__(self, name: str, sinks: Dict[str, Any], sources: Dict[str, Any], loader_provider, saver_provider):
        rbuilder = ResourceBuilder(name)
        for (sink_name, sink) in sinks.items():
            rbuilder.add('sink', sink_name, sink.serialize())
        for (source_name, source) in sources.items():
            rbuilder.add('source', source_name, source.serialize())
        self.interface = FuncInterface(rbuilder)
        self.sinks = sinks
        self.sources = sources

    def process_stdin(self):
        request = pa.Table.from_batches([server_process_cli_stdin(self.interface)])
        for row_ind in request.num_rows:
            field = str(request.column('field')[row_ind])
            name = str(request.column('name')[row_ind])
            assert name in ['sink', 'source']
            resource_name = str(request.column('resource')[row_ind])
            payload = request.column('payload')
            resource = getattr(self, name)[resource_name](payload, row_ind)

    def get_sink(self, name: str):
        return self.sinks[name]

    def get_source(self, name: str):
        return self.sources[name]

