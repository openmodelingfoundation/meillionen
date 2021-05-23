from meillionen.client import ClientFunctionModel
from pydantic import BaseModel


class PathSettings(BaseModel):
    input: str
    output: str


class ModelRef(BaseModel):
    name: str


class CLIModelRef(ModelRef):
    path: str

    def resolve(self, storage=None, partitioning=None):
        """
        Turns a model ref into a model with storage

        A model with storage knows how to construct its sink resources

        If a partitioning is passed then a partitioned model with storage is created
        """
        return ClientFunctionModel.from_path(
            name=self.name,
            path=self.path,
            storage=storage,
            partitioning=partitioning)


class Experiment:
    def __init__(self, path_settings: PathSettings):
        self.path = path_settings
        self.models = {}
        self.resource_class_provider = {}
        self.partitioned_resource_class_provider = {}

    def set_model(self, model_ref: ModelRef, storage=None):
        _storage = self.resource_class_provider.override_with(storage)
        model = model_ref.resolve(storage=_storage)
        self.models[model_ref.name] = model

    def set_partitioned_model(self, model_ref: ModelRef, partitioning, storage=None):
        _storage = self.partitioned_resource_class_provider.override_with(storage)
        model = model_ref.resolve(partitioning=partitioning, storage=_storage)
        self.models[model_ref.name] = model

    def trial(self, name) -> 'Trial':
        return Trial(name=name, project=self)


class Trial:
    def __init__(self, name: str, project: Experiment):
        self.name = name
        self.project = project

    def __getattr__(self, item):
        return self.project.models[item]