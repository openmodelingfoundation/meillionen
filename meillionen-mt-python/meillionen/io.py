"""
Loaders and savers reduce the amount of boilerplate needed load and save
resources in meillionen by providing loaders and savers for commonly used
numerical python packages such as xarray, pandas and netcdf
"""

from typing import Dict, Union

from landlab.io import read_esri_ascii, write_esri_ascii
import netCDF4
import numpy as np
import pandas as pd
import pathlib
import pickle
import xarray as xr


def _mkdir_p(path):
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)


class NetCDF4Saver:
    """
    A saver that allows incrementally saving array slices to a NetCDF file

    Intended to allow the saving or large arrays that may not fit into memory
    """
    def __init__(self, sink, sink_schema):
        self.sink = sink
        sink = sink.to_dict()
        _mkdir_p(sink['path'])
        self.dataset = netCDF4.Dataset(sink['path'], mode='w')
        for dim, size in sink_schema:
            print((dim, size))
            self.dataset.createDimension(dim, size)
        self.variable = self.dataset.createVariable(sink['variable'], 'f4', [f[0] for f in sink_schema])

    def set_slice(self, array: xr.DataArray, **slices: Dict[str, Union[int, slice]]):
        """
        Save an array to a slice of a NetCDF variable

        :param array: A dimensioned array
        :param slices: Where to save the dimensioned array to. Dimensions of array passed in
          and NetCDF variable names must match
        """
        vdims = self.variable.dimensions
        xs = tuple(slices[d] if d in slices else slice(None) for d in vdims)
        vdims_remaining = [v for v in vdims if v not in slices.keys()]
        self.variable[xs] = array.transpose(*vdims_remaining)

    def _close(self):
        self.dataset.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close()


class XArrayLoader:
    """
    A loader to import data into an XArray.
    """
    @staticmethod
    def _load_netcdf(source) -> xr.DataArray:
        dataset = xr.open_dataset(source['path'])
        return dataset[source['variable']]

    @staticmethod
    def load(source_resource) -> xr.DataArray:
        """
        Load resource into an XArray
        :param source_resource: metadata describing how to access the resource
        :return: An XArray
        """
        source = source_resource.to_dict()
        return _XARRAY_LOADER[source['type']](source)


_XARRAY_LOADER = {
    'NetCDFResource': XArrayLoader._load_netcdf
}


class XArraySaver:
    """
    A saver to persist data in the format and location specified by a resource
    """
    @staticmethod
    def save(sink_resource, data: xr.DataArray):
        """
        Save the XArray to a location and format specified by the resource
        
        :param sink_resource: metadata describing where to save the data to and what format
          to save the data in
        :param data: the data to be saved
        """
        sink = sink_resource.to_dict()
        assert sink['type'] == 'NetCDFResource'
        data.to_netcdf(sink['path'])


class PandasLoader:
    """
    A loader to import tabular data into pandas given a resource
    """
    @staticmethod
    def _load_feather(source) -> pd.DataFrame:
        return pd.read_feather(source['path'])

    @staticmethod
    def _load_parquet(source) -> pd.DataFrame:
        return pd.read_parquet(source['path'])

    @staticmethod
    def load(source_resource):
        """
        Loads a resource into pandas

        :param source_resource: metadata describing the location and format of a dataset
        :return: A pandas dataframe
        """
        source = source_resource.to_dict()
        return _PANDAS_LOADER[source['type']](source)


_PANDAS_LOADER = {
    'FeatherResource': PandasLoader._load_feather,
    'ParquetResource': PandasLoader._load_parquet
}


class PandasSaver:
    """
    A saver to persist a pandas dataframe to the location and format specified by a resource
    """
    @staticmethod
    def save(sink_resource, data: pd.DataFrame):
        """
        Saves a pandas dataframe to the location and in the format specified by a resource

        :param sink_resource: metadata describing the location and format to save a dataset to
        :param data: the data to be saved
        """
        sink = sink_resource.to_dict()
        _mkdir_p(sink['path'])
        if sink['type'] == 'FeatherResource':
            data.to_feather(sink['path'])
            return
        elif sink['type'] == 'ParquetResource':
            data.to_parquet(sink['path'])
            return
        raise ValueError(f'{sink["type"]} is not valid type for pandas saver')


class LandLabLoader:
    """
    A loader to load raster files into landlab grid objects
    """
    @staticmethod
    def load(dem_resource):
        """
        Loads a raster file into a landlab grid

        :param dem_resource: metadata describing where the raster is located and its format
        :return: A landlab grid
        """
        dem = dem_resource.to_dict()
        mg, z = read_esri_ascii(dem['path'],name='topographic__elevation')
        mg.status_at_node[mg.nodes_at_right_edge] = mg.BC_NODE_IS_FIXED_VALUE
        mg.status_at_node[np.isclose(z, -9999)] = mg.BC_NODE_IS_CLOSED
        return mg


class PickleLoader:
    """
    A loader to restore a python object from the file system
    """
    @staticmethod
    def load(model_resource):
        """
        Loads a pickle from the file system

        :param model_resource: an object describing the location of the pickle
        :return: A picklable python object
        """
        model = model_resource.to_dict()
        return pickle.load(model['path'])


class PickleSaver:
    """
    A saver to persist picklable python objects
    """
    @staticmethod
    def save(model_resource, model):
        """
        Saves picklable objects

        :param model_resource: metadata describing where to save the pickle to
        :param model: a picklable python object
        """
        model_meta = model_resource.to_dict()
        _mkdir_p(model_meta['path'])
        pickle.dump(model, model_meta['path'])


class LandLabSaver:
    """
    A saver to persist landlab grid objects
    """
    @staticmethod
    def save(dem_resource, mg, variable: str):
        """
        Saves a model grid to a location and in a format specified by a resource

        :param dem_resource: metadata describing the location and format to save the data in
        :param mg: the model grid object
        :param variable: name of variable in model grid to save
        """
        dem_meta = dem_resource.to_dict()
        _mkdir_p(dem_meta['path'])
        write_esri_ascii(dem_meta['path'], mg, variable, clobber=True)