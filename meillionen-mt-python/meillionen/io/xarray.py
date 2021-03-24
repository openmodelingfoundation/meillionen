import xarray as xr


def load_netcdf(resource_desc, schema) -> xr.DataArray:
    return xr.open_dataset(resource_desc)


class XArrayLoader:
    def __init__(self):
        self.handlers = {}

    def load(self, resource_desc, schema) -> xr.DataArray:
        return self.handlers[resource_desc['type']](resource_desc, schema)
