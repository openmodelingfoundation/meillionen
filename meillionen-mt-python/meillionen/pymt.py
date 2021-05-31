from meillionen.client import ClientFunctionModel


class PyMTFunctionModel:
    def __init__(self):
        self.sources = {}
        self.sinks = {}
        self.model: ClientFunctionModel = None
        self.partition = None

    def initialize(self, model):
        self.model = model

    @classmethod
    def setup(cls):
        return None

    def get_input_var_names(self):
        return self.model.source_names

    def get_input_var_type(self, name):
        return self.model.source_schema(name)

    def get_output_var_names(self):
        return self.model.sink_names

    def get_output_var_type(self, name):
        return self.model.sink_schema(name)

    def set_partition(self, partition):
        self.partition = partition

    def set_value(self, source_name, source):
        self.sources[source_name] = source

    def get_value(self, sink_name):
        return self.sinks[sink_name]

    def update(self):
        self.sinks = self.model.run(sources=self.sources, partition=self.partition)

    def finalize(self):
        self.sinks = {}