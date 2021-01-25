from .meillionen import PyFuncInterface, PyStoreRef
import pandas as pd
import sh
from typing import Any, AnyStr, Dict


class ModelBuilderCLI:
    def __init__(self, path):
        self.path = path
        self.interface = self._get_interface()

    def _get_interface(self):
        interface_data = sh.Command(self.path)
        return PyFuncInterface.from_json(interface_data)

    def build(self, stores: Dict[AnyStr, Any]):
        pass


class ModelCLI:
    def __init__(self, path, sinks, sources):
        self.path = path
        self.sinks = sinks
        self.sources = sources

    def run(self, data: Dict[AnyStr, Any]):
        cli = sh.Command(self.path)
        # write to file system
        for name, source in self.sources.items():
            df: pd.DataFrame = data[name]
            df.to_parquet(source)
        cli(**self.sources)

        sink_data = {}
        for name, sink in self.sinks.items():
            sink_data[name] = pd.read_parquet(sink)

        return sink_data