import pandas as pd
import xarray as xr


class XArrayLoader:
    @staticmethod
    def _load_netcdf(source) -> xr.DataArray:
        dataset = xr.open_dataset(source['path'])
        return dataset[source['variable']]

    @staticmethod
    def load(source_resource):
        source = source_resource.to_dict()
        _PANDAS_LOADER[source['type']](source)


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
    def load(source_resource):
        source = source_resource.to_dict()
        _PANDAS_LOADER[source['type']](source)


_PANDAS_LOADER = {
    'FeatherResource': PandasLoader._load_feather
}


class PandasSaver:
    @staticmethod
    def save(sink_resource, data: pd.DataFrame):
        sink = sink_resource.to_dict()
        assert sink['type'] == 'FeatherResource'
        data.to_feather(sink['path'])