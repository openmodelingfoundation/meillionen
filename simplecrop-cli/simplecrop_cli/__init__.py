from .simplecrop_cli import CDFStore, F64CDFVariableRef, run, to_dataframe, SimpleCrop


class CDFStoreConfig:
    def __init__(self, path):
        self.path = path

    @property
    def storetype(self):
        return "netcdf"


class Store:
    def __init__(self, config):
        assert config.storetype == "netcdf"
        self.store = CDFStore(config.path)

    def get_value(self, variable_name):
        # when more than one datatype is supported this function
        # will get the datatype of the variable name from the store
        # and then choose the appropriate getter for the type
        return VariableRef(self.store.get_f64_value(variable_name))


class VariableRef:
    def __init__(self, variable_ref):
        self.variable_ref = variable_ref