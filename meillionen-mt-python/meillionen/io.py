from typing import Dict, Union

from landlab.io import read_esri_ascii, write_esri_ascii
import netCDF4
import numpy as np
import pandas as pd
import pathlib
import pickle
import xarray as xr


def mkdir_p(path):
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)


class NetCDF4Saver:
    def __init__(self, sink, sink_schema):
        self.sink = sink
        sink = sink.to_dict()
        self.dataset = netCDF4.Dataset(sink['path'], mode='w')
        for dim, size in sink_schema:
            print((dim, size))
            self.dataset.createDimension(dim, size)
        self.variable = self.dataset.createVariable(sink['variable'], 'f4', [f[0] for f in sink_schema])

    def set_slice(self, array: xr.DataArray, **slices: Dict[str, Union[int, slice]]):
        vdims = self.variable.dimensions
        xs = tuple(slices[d] if d in slices else slice(None) for d in vdims)
        vdims_remaining = [v for v in vdims if v not in slices.keys()]
        self.variable[xs] = array.transpose(*vdims_remaining)

    def close(self):
        self.dataset.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class XArrayLoader:
    @staticmethod
    def _load_netcdf(source) -> xr.DataArray:
        dataset = xr.open_dataset(source['path'])
        return dataset[source['variable']]

    @staticmethod
    def load(source_resource):
        source = source_resource.to_dict()
        return _XARRAY_LOADER[source['type']](source)


_XARRAY_LOADER = {
    'NetCDFResource': XArrayLoader._load_netcdf
}


class XArraySaver:
    @staticmethod
    def save(sink_resource, data: xr.DataArray):
        sink = sink_resource.to_dict()
        assert sink['type'] == 'NetCDFResource'
        data.to_netcdf(sink['path'])


class PandasLoader:
    @staticmethod
    def _load_feather(source) -> pd.DataFrame:
        return pd.read_feather(source['path'])

    @staticmethod
    def _load_parquet(source) -> pd.DataFrame:
        return pd.read_parquet(source['path'])

    @staticmethod
    def load(source_resource):
        source = source_resource.to_dict()
        return _PANDAS_LOADER[source['type']](source)


_PANDAS_LOADER = {
    'FeatherResource': PandasLoader._load_feather,
    'ParquetResource': PandasLoader._load_parquet
}


class PandasSaver:
    @staticmethod
    def save(sink_resource, data: pd.DataFrame):
        sink = sink_resource.to_dict()
        mkdir_p(sink['path'])
        if sink['type'] == 'FeatherResource':
            data.to_feather(sink['path'])
            return
        elif sink['type'] == 'ParquetResource':
            data.to_parquet(sink['path'])
            return
        raise ValueError(f'{sink["type"]} is not valid type for pandas saver')


class LandLabLoader:
    @staticmethod
    def load(dem_resource):
        dem = dem_resource.to_dict()
        mg, z = read_esri_ascii(dem['path'],name='topographic__elevation')
        mg.status_at_node[mg.nodes_at_right_edge] = mg.BC_NODE_IS_FIXED_VALUE
        mg.status_at_node[np.isclose(z, -9999)] = mg.BC_NODE_IS_CLOSED
        return mg


class PickleLoader:
    @staticmethod
    def load(model_resource):
        model = model_resource.to_dict()
        return pickle.load(model['path'])


class PickleSaver:
    @staticmethod
    def save(model_resource, model):
        model_meta = model_resource.to_dict()
        mkdir_p(model_meta['path'])
        pickle.dump(model, model_meta['path'])


class LandLabSaver:
    @staticmethod
    def save(dem_resource, mg, variable: str):
        dem_meta = dem_resource.to_dict()
        mkdir_p(dem_meta['path'])
        write_esri_ascii(dem_meta['path'], mg, variable, clobber=True)