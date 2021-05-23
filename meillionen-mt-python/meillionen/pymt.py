class PyMTFunctionModel:
    def __init__(self):
        self.sources = {}
        self.sinks = {}
        self.model = None

    def initialize(self, model, config):
        self.config = config

    @classmethod
    def setup(cls, model, trial):
        return model, trial.add_model_name(model.name)

    def set_value(self, source_name, source):
        self.sources[source_name] = source

    def get_value(self, sink_name):
        return self.sinks[sink_name]

    def update(self):
        self.sinks = self.model.run(sources=self.sources, params=self.config.partitition_params)

    def finalize(self):
        self.sinks = {}