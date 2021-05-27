import os

from meillionen.client import ClientFunctionModel
from pydantic import BaseModel


class PathSettings(BaseModel):
    base_path: str


class ModelRef(BaseModel):
    name: str


class Experiment:
    def __init__(self, sources: PathSettings, sinks: PathSettings):
        self.sinks = sinks
        self.sources = sources

    def trial(self, name) -> 'Trial':
        return Trial(name=name, project=self)


class Trial:
    def __init__(self, name: str, project: Experiment):
        self.name = name
        self.sinks = project.sinks.copy()
        self.sinks.base_path = os.path.join(self.sinks.base_path, name)
        self.sources = project.sources